"""Configuration dataclasses for tiders pipelines.

This module defines every configuration type used to assemble a pipeline:

* **Writer configs** – one per storage backend (ClickHouse, Iceberg, Delta Lake,
  PyArrow dataset, DuckDB).
* **Step configs** – one per transformation step (EVM/SVM decoding, casting,
  encoding, custom Polars/DataFusion transforms, etc.).
* **Pipeline** – the top-level object that ties a data provider, query, writer,
  and ordered list of steps together.
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Callable, TYPE_CHECKING

from tiders_core.ingest import ProviderConfig, Query
from tiders_core.svm_decode import InstructionSignature, LogSignature
import pyarrow as pa
import pyarrow.dataset as pa_dataset
import pyarrow.fs as pa_fs

if TYPE_CHECKING:
    from clickhouse_connect.driver.asyncclient import AsyncClient as ClickHouseClient
    from pyiceberg.catalog import Catalog as IcebergCatalog
    import deltalake
    import duckdb
    import polars as pl
    import datafusion

logger = logging.getLogger(__name__)


class WriterKind(str, Enum):
    """Supported storage backends for pipeline output.

    Each member maps to a concrete ``DataWriter`` implementation and a
    corresponding writer config dataclass.
    """

    CLICKHOUSE = "clickhouse"
    ICEBERG = "iceberg"
    DELTA_LAKE = "delta_lake"
    PYARROW_DATASET = "pyarrow_dataset"
    DUCKDB = "duckdb"


class StepKind(str, Enum):
    """Supported transformation steps that can be applied to pipeline data.

    Each member corresponds to a step module in :mod:`tiders.steps` and an
    associated config dataclass.
    """

    EVM_DECODE_EVENTS = "evm_decode_events"
    CAST = "cast"
    HEX_ENCODE = "hex_encode"
    CAST_BY_TYPE = "cast_by_type"
    BASE58_ENCODE = "base58_encode"
    U256_TO_BINARY = "u256_to_binary"
    SVM_DECODE_INSTRUCTIONS = "svm_decode_instructions"
    SVM_DECODE_LOGS = "svm_decode_logs"
    JOIN_BLOCK_DATA = "join_block_data"
    JOIN_SVM_TRANSACTION_DATA = "join_svm_transaction_data"
    JOIN_EVM_TRANSACTION_DATA = "join_evm_transaction_data"
    GLACIERS_EVENTS = "glaciers_events"
    SET_CHAIN_ID = "set_chain_id"
    DATAFUSION = "datafusion"
    POLARS = "polars"


@dataclass
class IcebergWriterConfig:
    """Configuration for the Apache Iceberg writer.

    Attributes:
        namespace: The Iceberg namespace (database) to write tables into.
        catalog: A ``pyiceberg`` catalog instance used to create and load tables.
        write_location: The storage URI where Iceberg data files are written.
    """

    namespace: str
    catalog: "IcebergCatalog"
    write_location: str


@dataclass
class DeltaLakeWriterConfig:
    """Configuration for the Delta Lake writer.

    Attributes:
        data_uri: Base URI where Delta tables are stored. Each table is written
            to ``<data_uri>/<table_name>/``.
        partition_by: Mapping of table name to a list of column names used for
            partitioning. Tables not present in the mapping are unpartitioned.
        storage_options: Optional cloud storage credentials / options passed to
            ``deltalake.write_deltalake``.
        writer_properties: Optional Parquet writer properties for Delta Lake.
        anchor_table: If set, this table is written last (after all other tables)
            to serve as an ordering guarantee for downstream consumers.
    """

    data_uri: str
    partition_by: Dict[str, list[str]] = field(default_factory=dict)
    storage_options: Optional[Dict[str, str]] = None
    writer_properties: Optional["deltalake.WriterProperties"] = None
    anchor_table: Optional[str] = None


@dataclass
class ClickHouseSkipIndex:
    """Describes a ClickHouse data-skipping index to add after table creation.

    Attributes:
        name: The index name.
        val: The index expression (e.g. a column name or expression).
        type_: The index type (e.g. ``"minmax"``, ``"bloom_filter"``).
        granularity: The index granularity.
    """

    name: str
    val: str
    type_: str
    granularity: int


@dataclass
class ClickHouseWriterConfig:
    """Configuration for the ClickHouse writer.

    Attributes:
        client: An async ClickHouse client (``clickhouse_connect``).
        codec: Per-table, per-column compression codecs.
            ``{"table": {"column": "ZSTD(3)"}}``.
        order_by: Per-table ordering key columns.
            ``{"table": ["col_a", "col_b"]}``.
        engine: The ClickHouse table engine clause (default ``MergeTree()``).
        skip_index: Per-table list of data-skipping indexes to create.
        anchor_table: If set, this table is inserted last to provide ordering
            guarantees for downstream consumers.
        create_tables: When ``True`` (the default), tables are auto-created on
            the first insert using the Arrow schema.
    """

    client: "ClickHouseClient"
    codec: Dict[str, Dict[str, str]] = field(default_factory=dict)
    order_by: Dict[str, List[str]] = field(default_factory=dict)
    engine: str = "MergeTree()"
    skip_index: Dict[str, List[ClickHouseSkipIndex]] = field(default_factory=dict)
    anchor_table: Optional[str] = None
    create_tables: bool = True


@dataclass
class PyArrowDatasetWriterConfig:
    """Configuration for the PyArrow dataset writer.

    Writes tables as Parquet files using ``pyarrow.dataset.write_dataset``.
    Each table is stored under ``<base_dir>/<table_name>/``.

    Attributes:
        base_dir: Root directory for all output datasets.
        basename_template: Template for output file names (e.g.
            ``"part-{i}.parquet"``). A monotonic counter is appended to avoid
            collisions across successive pushes.
        partitioning: Per-table partitioning scheme. Values can be a list of
            column names or a ``pyarrow.dataset.Partitioning`` object.
        partitioning_flavor: Per-table partitioning flavor (e.g. ``"hive"``).
        filesystem: Optional PyArrow filesystem for remote storage (S3, GCS, …).
        file_options: Optional Parquet file write options.
        use_threads: Whether to use threads for writing (default ``True``).
        max_partitions: Maximum number of partitions to write (default 1024).
        max_open_files: Maximum number of files to keep open simultaneously.
        max_rows_per_file: Maximum rows per output file (0 = unlimited).
        min_rows_per_group: Minimum rows per row group in Parquet files.
        max_rows_per_group: Maximum rows per row group (default 1 048 576).
        create_dir: Whether to create the output directory if it doesn't exist.
        anchor_table: If set, this table is written last.
    """

    base_dir: str
    basename_template: Optional[str] = None
    partitioning: Dict[str, pa_dataset.Partitioning | list[str]] = field(
        default_factory=dict
    )
    partitioning_flavor: Dict[str, str] = field(default_factory=dict)
    filesystem: Optional[pa_fs.FileSystem] = None
    file_options: Optional[pa_dataset.FileWriteOptions] = None
    use_threads: bool = True
    max_partitions: int = 1024
    max_open_files: int = 1024
    max_rows_per_file: int = 0
    min_rows_per_group: int = 0
    max_rows_per_group: int = 1024 * 1024
    create_dir: bool = True
    anchor_table: Optional[str] = None


@dataclass
class DuckdbWriterConfig:
    """Configuration for the DuckDB writer.

    Attributes:
        connection: An open DuckDB connection. Tables are created automatically
            on the first push if they don't already exist.
    """

    connection: "duckdb.DuckDBPyConnection"


@dataclass
class Writer:
    """Pairs a :class:`WriterKind` with its backend-specific configuration.

    Attributes:
        kind: The storage backend to use.
        config: The configuration object matching ``kind``.
    """

    kind: WriterKind
    config: (
        ClickHouseWriterConfig
        | IcebergWriterConfig
        | DeltaLakeWriterConfig
        | PyArrowDatasetWriterConfig
        | DuckdbWriterConfig
    )


@dataclass
class EvmDecodeEventsConfig:
    """Configuration for the EVM event log decoding step.

    Decodes raw EVM log entries into structured columns based on an event ABI
    signature.

    Attributes:
        event_signature: The Solidity event signature string
            (e.g. ``"Transfer(address indexed,address indexed,uint256)"``).
        allow_decode_fail: When ``True``, rows that fail to decode are kept with
            null values instead of raising an error.
        filter_by_topic0: When ``True``, only log rows whose ``topic0`` matches
            the event signature's keccak-256 hash are decoded.
        input_table: Name of the source table in the data dictionary
            (default ``"logs"``).
        output_table: Name of the output table for decoded results
            (default ``"decoded_logs"``).
        hstack: When ``True`` (the default), decoded columns are horizontally
            stacked with the original input columns.
    """

    event_signature: str
    allow_decode_fail: bool = False
    filter_by_topic0: bool = False
    input_table: str = "logs"
    output_table: str = "decoded_logs"
    hstack: bool = True


@dataclass
class GlaciersEventsConfig:
    """Configuration for the Glaciers ABI-database event decoding step.

    Attributes:
        abi_db_path: Path to the local ABI database used for signature lookup.
        decoder_type: The type of data to decode (``"log"`` or other supported
            types).
        input_table: Name of the source table (default ``"logs"``).
        output_table: Name of the output table (default ``"decoded_logs"``).
    """

    abi_db_path: str
    decoder_type: str = "log"
    input_table: str = "logs"
    output_table: str = "decoded_logs"


@dataclass
class SvmDecodeInstructionsConfig:
    """Configuration for the SVM (Solana) instruction decoding step.

    Decodes raw Solana instruction data into structured columns using an
    Anchor/Borsh instruction signature.

    Attributes:
        instruction_signature: The instruction schema describing discriminator,
            parameter types, and account names.
        allow_decode_fail: When ``True``, rows that fail to decode produce nulls
            instead of raising an error.
        filter_by_discriminator: When ``True``, only instruction rows whose data
            starts with the matching discriminator are decoded.
        input_table: Name of the source table (default ``"instructions"``).
        output_table: Name of the output table
            (default ``"decoded_instructions"``).
        hstack: When ``True``, decoded columns are stacked alongside the
            original input columns.
    """

    instruction_signature: InstructionSignature
    allow_decode_fail: bool = False
    filter_by_discriminator: bool = False
    input_table: str = "instructions"
    output_table: str = "decoded_instructions"
    hstack: bool = True


@dataclass
class SvmDecodeLogsConfig:
    """Configuration for the SVM (Solana) log decoding step.

    Decodes raw Solana program log entries into structured columns using a log
    signature definition.

    Attributes:
        log_signature: The log schema describing parameter types.
        allow_decode_fail: When ``True``, rows that fail to decode produce nulls
            instead of raising an error.
        input_table: Name of the source table (default ``"logs"``).
        output_table: Name of the output table (default ``"decoded_logs"``).
        hstack: When ``True``, decoded columns are stacked alongside the
            original input columns.
    """

    log_signature: LogSignature
    allow_decode_fail: bool = False
    input_table: str = "logs"
    output_table: str = "decoded_logs"
    hstack: bool = True


@dataclass
class CastConfig:
    """Configuration for the column-level type casting step.

    Casts specific columns in a single table to new Arrow data types.

    Attributes:
        table_name: The name of the table whose columns should be cast.
        mappings: A mapping of column name to target ``pyarrow.DataType``.
        allow_cast_fail: When ``True``, values that cannot be cast are set to
            null instead of raising an error.
    """

    table_name: str
    mappings: Dict[str, pa.DataType]
    allow_cast_fail: bool = False


@dataclass
class HexEncodeConfig:
    """Configuration for the hex-encoding step.

    Converts binary columns to their hexadecimal string representation.

    Attributes:
        tables: List of table names to process. When ``None``, all tables in the
            data dictionary are processed.
        prefixed: When ``True`` (the default), output strings are ``0x``-prefixed.
    """

    tables: Optional[list[str]] = None
    prefixed: bool = True


@dataclass
class U256ToBinaryConfig:
    """Configuration for the U256-to-binary conversion step.

    Converts ``Decimal256`` columns to a fixed-size binary representation,
    including columns nested inside structs and lists.

    Attributes:
        tables: List of table names to process. When ``None``, all tables are
            processed.
    """

    tables: Optional[list[str]] = None


@dataclass
class Base58EncodeConfig:
    """Configuration for the Base58-encoding step.

    Converts binary columns to Base58-encoded strings, commonly used for Solana
    public keys and signatures.

    Attributes:
        tables: List of table names to process. When ``None``, all tables are
            processed.
    """

    tables: Optional[list[str]] = None


@dataclass
class CastByTypeConfig:
    """Configuration for the type-level casting step.

    Casts all columns of a given Arrow data type to a different type across
    every table in the data dictionary.

    Attributes:
        from_type: The source ``pyarrow.DataType`` to match.
        to_type: The target ``pyarrow.DataType`` to cast matching columns to.
        allow_cast_fail: When ``True``, values that cannot be cast are set to
            null instead of raising an error.
    """

    from_type: pa.DataType
    to_type: pa.DataType
    allow_cast_fail: bool = False


@dataclass
class PolarsStepConfig:
    """Configuration for a custom Polars transformation step.

    Allows users to supply an arbitrary function that receives all tables as
    Polars DataFrames and returns transformed DataFrames.

    Attributes:
        runner: A callable ``(tables, context) -> tables`` where ``tables`` is a
            dict mapping table names to ``polars.DataFrame`` objects.
        context: An optional user-defined value passed as the second argument to
            ``runner``.
    """

    runner: Callable[[Dict[str, "pl.DataFrame"], Optional[Any]], Dict[str, "pl.DataFrame"]]
    context: Optional[Any] = None


@dataclass
class DataFusionStepConfig:
    """Configuration for a custom DataFusion transformation step.

    Allows users to supply an arbitrary function that receives a DataFusion
    session context and all tables as DataFusion DataFrames, and returns
    transformed DataFrames.

    Attributes:
        runner: A callable ``(ctx, tables, context) -> tables`` where ``ctx`` is
            a ``datafusion.SessionContext`` and ``tables`` maps table names to
            ``datafusion.DataFrame`` objects.
        context: An optional user-defined value passed as the third argument to
            ``runner``.
    """

    runner: Callable[
        ["datafusion.SessionContext", Dict[str, "datafusion.DataFrame"], Optional[Any]],
        Dict[str, "datafusion.DataFrame"],
    ]
    context: Optional[Any] = None


@dataclass
class SetChainIdConfig:
    """Configuration for the set-chain-id step.

    Adds (or replaces) a ``chain_id`` column with a constant value on every
    table in the data dictionary.

    Attributes:
        chain_id: The chain identifier to set (e.g. ``1`` for Ethereum mainnet).
    """

    chain_id: int


@dataclass
class EvmTableAliases:
    """Optional table name overrides for EVM data sources.

    When provided, ingested tables are renamed from their default names to the
    specified aliases before steps are applied.

    Attributes:
        blocks: Alias for the ``blocks`` table.
        transactions: Alias for the ``transactions`` table.
        logs: Alias for the ``logs`` table.
        traces: Alias for the ``traces`` table.
    """

    blocks: Optional[str] = None
    transactions: Optional[str] = None
    logs: Optional[str] = None
    traces: Optional[str] = None


@dataclass
class SvmTableAliases:
    """Optional table name overrides for SVM (Solana) data sources.

    When provided, ingested tables are renamed from their default names to the
    specified aliases before steps are applied.

    Attributes:
        blocks: Alias for the ``blocks`` table.
        transactions: Alias for the ``transactions`` table.
        instructions: Alias for the ``instructions`` table.
        logs: Alias for the ``logs`` table.
        balances: Alias for the ``balances`` table.
        token_balances: Alias for the ``token_balances`` table.
        rewards: Alias for the ``rewards`` table.
    """

    blocks: Optional[str] = None
    transactions: Optional[str] = None
    instructions: Optional[str] = None
    logs: Optional[str] = None
    balances: Optional[str] = None
    token_balances: Optional[str] = None
    rewards: Optional[str] = None


@dataclass
class Step:
    """A single transformation step in a pipeline.

    Attributes:
        kind: The type of transformation to apply.
        config: The configuration object matching ``kind``.
        name: An optional human-readable name used in log messages.
    """

    kind: StepKind
    config: (
        EvmDecodeEventsConfig
        | CastConfig
        | HexEncodeConfig
        | U256ToBinaryConfig
        | CastByTypeConfig
        | Base58EncodeConfig
        | SvmDecodeInstructionsConfig
        | SvmDecodeLogsConfig
        | PolarsStepConfig
        | DataFusionStepConfig
        | GlaciersEventsConfig
        | SetChainIdConfig
    )
    name: Optional[str] = None


@dataclass
class Pipeline:
    """Top-level pipeline definition tying together ingestion, transformation, and output.

    Attributes:
        provider: The data provider configuration (RPC endpoint, credentials, …).
        query: The blockchain query specifying which data to ingest.
        writer: The writer configuration specifying where to store results.
        steps: An ordered list of transformation steps applied to each batch of
            ingested data.
        table_aliases: Optional table name overrides applied to raw ingested
            tables before any steps run.
    """

    provider: ProviderConfig
    query: Query
    writer: Writer
    steps: List[Step]
    table_aliases: Optional[EvmTableAliases | SvmTableAliases] = None


__all__ = [
    "Pipeline",
    "Step",
    "EvmDecodeEventsConfig",
    "EvmTableAliases",
    "SvmTableAliases",
    "CastConfig",
    "HexEncodeConfig",
    "U256ToBinaryConfig",
    "CastByTypeConfig",
    "Base58EncodeConfig",
    "SvmDecodeInstructionsConfig",
    "SvmDecodeLogsConfig",
    "PolarsStepConfig",
    "DataFusionStepConfig",
    "GlaciersEventsConfig",
    "SetChainIdConfig",
    "Writer",
    "StepKind",
    "WriterKind",
    "ClickHouseWriterConfig",
    "IcebergWriterConfig",
    "DeltaLakeWriterConfig",
    "PyArrowDatasetWriterConfig",
    "DuckdbWriterConfig",
]
