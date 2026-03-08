# This example indexes Uniswap V3 Factory PoolCreated events.
# Tiders is published to PyPI as tiders and tiders-core.
# To install it, run: pip install tiders tiders-core
# Or with uv: uv pip install tiders tiders-core

# You can run this script with:
# uv run examples/uniswap_v3_pool_created.py --provider hypersync --from_block 12369621 --to_block 12370621
# uv run examples/uniswap_v3_pool_created.py --provider sqd --from_block 12369621 --to_block 12370621
# uv run examples/uniswap_v3_pool_created.py --provider rpc --rpc_url https://your-rpc-url --from_block 12369621 --to_block 12370621

# After run, parquet files are written to data/uniswap_v3_pool_created/

import argparse
import asyncio
import pyarrow as pa
import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

from tiders import config as cc  # noqa: E402
from tiders import run_pipeline  # noqa: E402
from tiders_core import evm_abi_events, ingest  # noqa: E402

UNISWAP_V3_FACTORY = "0x1f98431c8ad98523631ae4a59f267346ea31f984"
DEFAULT_HYPERSYNC_URL = "https://eth.hypersync.xyz"
DEFAULT_SQD_URL = "https://portal.sqd.dev/datasets/ethereum-mainnet"
DEFAULT_RPC_URL = "https://mainnet.gateway.tenderly.co"
DATA_PATH = str(Path.cwd() / "data" / "uniswap_v3_pool_created")
Path(DATA_PATH).mkdir(parents=True, exist_ok=True)
WRITER_CHOICES = ["clickhouse", "delta_lake", "duckdb", "iceberg", "pyarrow"]
# Per-pipeline aliases for ingest's default "logs" table.
POOL_CREATED_LOGS_TABLE = "uniswap_v3_factory_pool_created_logs"
POOL_EVENTS_TABLE = "uniswap_v3_pool_logs"
# Output table from the factory PoolCreated decoder (stage 1).
POOL_CREATED_TABLE = "uniswap_v3_pool_created"


# Extract event signatures from ABI JSON files.
_FACTORY_ABI_JSON = (Path(__file__).parent / "uniswap-v3-factory-abi.json").read_text()
_FACTORY_EVENTS = {e.name: e for e in evm_abi_events(_FACTORY_ABI_JSON)}
POOL_CREATED_SIGNATURE = _FACTORY_EVENTS["PoolCreated"].signature

_POOL_ABI_JSON = (Path(__file__).parent / "uniswap-v3-pool-abi.json").read_text()
UNISWAP_V3_POOL_EVENT_SIGNATURES = [
    (
        e.name_snake_case,
        e.signature,
        f"uniswap_v3_pool_{e.name_snake_case}",
    )
    for e in evm_abi_events(_POOL_ABI_JSON)
]


def provider_url(
    provider: ingest.ProviderKind, rpc_url: Optional[str]
) -> Optional[str]:
    if provider == ingest.ProviderKind.HYPERSYNC:
        return DEFAULT_HYPERSYNC_URL
    if provider == ingest.ProviderKind.SQD:
        return DEFAULT_SQD_URL
    if provider == ingest.ProviderKind.RPC:
        return rpc_url or os.environ.get("RPC_URL", DEFAULT_RPC_URL)
    return rpc_url or os.environ.get("RPC_URL")


async def main(
    provider_kind: ingest.ProviderKind,
    from_block: int,
    to_block: Optional[int],
    rpc_url: Optional[str],
    database: str,
):
    # Stage 1: discover pools from Uniswap V3 factory events.
    await run_pool_created_pipeline(
        provider_kind=provider_kind,
        from_block=from_block,
        to_block=to_block,
        rpc_url=rpc_url,
        database=database,
    )

    # Read pool addresses produced by stage 1 and fan out the pool log indexing.
    pool_addresses = await load_pool_addresses(database)
    if not pool_addresses:
        print(
            "No pools found in uniswap_v3_pool_created. Skipping pool events pipeline."
        )
        return

    print(f"Indexing pool events for {len(pool_addresses)} pools")
    await run_pool_events_pipeline(
        provider_kind=provider_kind,
        from_block=from_block,
        to_block=to_block,
        rpc_url=rpc_url,
        database=database,
        pool_addresses=pool_addresses,
    )


async def load_pool_addresses(database: str) -> list[str]:
    # Load distinct pool addresses from the stage-1 output table.
    if database == "duckdb":
        import duckdb

        connection = duckdb.connect(database=f"{DATA_PATH}/duckdb.db")
        rows = connection.sql(
            f"SELECT DISTINCT pool FROM {POOL_CREATED_TABLE} WHERE pool IS NOT NULL"
        ).fetchall()
        connection.close()
        addresses = {row[0] for row in rows if row[0]}
        return sorted(addresses)

    if database == "pyarrow":
        import pyarrow.dataset as pa_dataset

        dataset = pa_dataset.dataset(
            f"{DATA_PATH}/pyarrow/{POOL_CREATED_TABLE}",
            format="parquet",
        )
        table = dataset.to_table(columns=["pool"])
        addresses = {value.as_py() for value in table["pool"] if value.as_py()}
        return sorted(addresses)

    if database == "delta_lake":
        from deltalake import DeltaTable

        table = DeltaTable(
            f"{DATA_PATH}/delta_lake/{POOL_CREATED_TABLE}"
        ).to_pyarrow_table(columns=["pool"])
        addresses = {value.as_py() for value in table["pool"] if value.as_py()}
        return sorted(addresses)

    raise ValueError(
        f"Pool loading for database '{database}' is not supported. Use one of: duckdb, pyarrow, delta_lake."
    )


def _pool_event_steps() -> list[cc.Step]:
    steps: list[cc.Step] = []

    # Decode each known Uniswap V3 pool event from the same raw pool logs table.
    for _, signature, output_table in UNISWAP_V3_POOL_EVENT_SIGNATURES:
        steps.append(
            cc.Step(
                kind=cc.StepKind.EVM_DECODE_EVENTS,
                config=cc.EvmDecodeEventsConfig(
                    event_signature=signature,
                    input_table=POOL_EVENTS_TABLE,
                    output_table=output_table,
                    allow_decode_fail=True,
                    filter_by_topic0=True,
                ),
            ),
        )
    steps.append(
        cc.Step(
            name="i256_to_i128",
            kind=cc.StepKind.CAST_BY_TYPE,
            config=cc.CastByTypeConfig(
                from_type=pa.decimal256(76, 0),
                to_type=pa.decimal128(38, 0),
                allow_cast_fail=True,
            ),
        ),
    )

    # Convert binary columns (addresses/hashes/topics) to readable hex.
    steps.append(
        cc.Step(
            kind=cc.StepKind.HEX_ENCODE,
            config=cc.HexEncodeConfig(),
        )
    )

    return steps


async def run_pool_created_pipeline(
    provider_kind: ingest.ProviderKind,
    from_block: int,
    to_block: Optional[int],
    rpc_url: Optional[str],
    database: str,
):
    # Query only factory PoolCreated logs in the requested block interval.
    url = provider_url(provider_kind, rpc_url)

    provider = ingest.ProviderConfig(
        kind=provider_kind,
        url=url,
        stop_on_head=True,
    )

    query = ingest.Query(
        kind=ingest.QueryKind.EVM,
        params=ingest.evm.Query(
            from_block=from_block,
            to_block=to_block,
            logs=[
                ingest.evm.LogRequest(
                    address=[UNISWAP_V3_FACTORY],
                    topic0=[_FACTORY_EVENTS["PoolCreated"].topic0],
                )
            ],
            fields=ingest.evm.Fields(
                log=ingest.evm.LogFields(
                    block_number=True,
                    block_hash=True,
                    transaction_hash=True,
                    log_index=True,
                    address=True,
                    topic0=True,
                    topic1=True,
                    topic2=True,
                    topic3=True,
                    data=True,
                ),
            ),
        ),
    )

    writer = await create_writer(database)

    pipeline = cc.Pipeline(
        provider=provider,
        query=query,
        writer=writer,
        table_aliases=cc.EvmTableAliases(logs=POOL_CREATED_LOGS_TABLE),
        steps=[
            # Decode the factory event to extract token0/token1/fee/tickSpacing/pool.
            cc.Step(
                kind=cc.StepKind.EVM_DECODE_EVENTS,
                config=cc.EvmDecodeEventsConfig(
                    event_signature=POOL_CREATED_SIGNATURE,
                    input_table=POOL_CREATED_LOGS_TABLE,
                    output_table=POOL_CREATED_TABLE,
                    allow_decode_fail=False,
                ),
            ),
            cc.Step(
                kind=cc.StepKind.HEX_ENCODE,
                config=cc.HexEncodeConfig(),
            ),
        ],
    )

    await run_pipeline(pipeline=pipeline)


async def run_pool_events_pipeline(
    provider_kind: ingest.ProviderKind,
    from_block: int,
    to_block: Optional[int],
    rpc_url: Optional[str],
    database: str,
    pool_addresses: list[str],
):
    # Re-query the same block interval, now filtered by discovered pool addresses only.
    url = provider_url(provider_kind, rpc_url)
    provider = ingest.ProviderConfig(
        kind=provider_kind,
        url=url,
        stop_on_head=True,
    )

    writer = await create_writer(database)

    print(
        f"Running pool events query for {len(pool_addresses)} addresses "
        f"(from_block={from_block}, to_block={to_block})"
    )
    query = ingest.Query(
        kind=ingest.QueryKind.EVM,
        params=ingest.evm.Query(
            from_block=from_block,
            to_block=to_block,
            logs=[
                ingest.evm.LogRequest(
                    # Intentionally no topic filter: index all events emitted by these pools.
                    address=pool_addresses,
                )
            ],
            fields=ingest.evm.Fields(
                log=ingest.evm.LogFields(
                    block_number=True,
                    block_hash=True,
                    transaction_hash=True,
                    log_index=True,
                    address=True,
                    topic0=True,
                    topic1=True,
                    topic2=True,
                    topic3=True,
                    data=True,
                ),
            ),
        ),
    )

    pipeline = cc.Pipeline(
        provider=provider,
        query=query,
        writer=writer,
        table_aliases=cc.EvmTableAliases(logs=POOL_EVENTS_TABLE),
        steps=_pool_event_steps(),
    )

    await run_pipeline(pipeline=pipeline)


async def create_writer(database: str) -> cc.Writer:
    # Reuse the same writer abstraction as other examples; each backend writes to DATA_PATH.
    if database == "pyarrow":
        return cc.Writer(
            kind=cc.WriterKind.PYARROW_DATASET,
            config=cc.PyArrowDatasetWriterConfig(base_dir=f"{DATA_PATH}/pyarrow"),
        )

    if database == "delta_lake":
        return cc.Writer(
            kind=cc.WriterKind.DELTA_LAKE,
            config=cc.DeltaLakeWriterConfig(
                data_uri=f"{DATA_PATH}/delta_lake",
            ),
        )

    if database == "duckdb":
        import duckdb

        connection = duckdb.connect(database=f"{DATA_PATH}/duckdb.db").cursor()
        return cc.Writer(
            kind=cc.WriterKind.DUCKDB,
            config=cc.DuckdbWriterConfig(connection=connection),
        )

    if database == "clickhouse":
        import clickhouse_connect

        client = await clickhouse_connect.get_async_client(
            host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
            port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
            username=os.environ.get("CLICKHOUSE_USER", "default"),
            password=os.environ.get("CLICKHOUSE_PASSWORD", "default"),
            database=os.environ.get("CLICKHOUSE_DATABASE", "default"),
            secure=os.environ.get("CLICKHOUSE_SECURE", "false").lower() == "true",
        )
        return cc.Writer(
            kind=cc.WriterKind.CLICKHOUSE,
            config=cc.ClickHouseWriterConfig(client=client),
        )

    if database == "iceberg":
        from pyiceberg.catalog import load_catalog

        iceberg_base_dir = Path(DATA_PATH) / "iceberg"
        warehouse_dir = iceberg_base_dir / "warehouse"
        iceberg_base_dir.mkdir(parents=True, exist_ok=True)
        warehouse_dir.mkdir(parents=True, exist_ok=True)

        warehouse_path = f"file://{warehouse_dir}"
        catalog = load_catalog(
            "local",
            type="sql",
            uri=f"sqlite:///{iceberg_base_dir / 'catalog.db'}",
            warehouse=warehouse_path,
        )
        return cc.Writer(
            kind=cc.WriterKind.ICEBERG,
            config=cc.IcebergWriterConfig(
                namespace=os.environ.get("ICEBERG_NAMESPACE", "default"),
                catalog=catalog,
                write_location=warehouse_path,
            ),
        )

    raise ValueError(
        f"Unknown database writer '{database}'. Expected one of {WRITER_CHOICES}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Index Uniswap V3 PoolCreated events")
    parser.add_argument(
        "--provider",
        choices=["rpc", "sqd", "hypersync"],
        default="rpc",
        help="Data provider to use",
    )
    parser.add_argument(
        "--from_block",
        type=int,
        default=12_369_621,
        help="Start block (defaults to Uniswap V3 deployment area)",
    )
    parser.add_argument(
        "--to_block",
        type=int,
        default=None,
        help="Inclusive end block (omit to stream from from_block to head)",
    )
    parser.add_argument(
        "--rpc_url",
        type=str,
        default=None,
        help="RPC URL (only used when --provider rpc)",
    )
    parser.add_argument(
        "--database",
        choices=WRITER_CHOICES,
        default="pyarrow",
        help="Database backend to use for the writer",
    )

    args = parser.parse_args()
    provider_kind = ingest.ProviderKind(args.provider)
    print(
        f"Running with provider: {provider_kind}, from_block: {args.from_block}, to_block: {args.to_block}, rpc_url: {args.rpc_url}, database: {args.database}"
    )
    asyncio.run(
        main(
            provider_kind,
            args.from_block,
            args.to_block,
            args.rpc_url,
            args.database,
        )
    )
