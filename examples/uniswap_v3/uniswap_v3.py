# =============================================================================
# Uniswap V3 Two-Stage Factory pattern
# =============================================================================
#
# This example demonstrates the "factory + children" indexing pattern, one of
# the most common patterns in crypto data engineering. Many DeFi protocols use
# a Factory contract that deploys child contracts (pools, vaults, markets, etc.)
# on-chain. To index them you need two stages:
#
#   1. Discover the children by reading events from the factory.
#   2. Index the events emitted by those children.
#
# This pattern applies to Uniswap (V2 and V3), Aave, Compound, Curve, and
# many other protocols that use factories to deploy per-pair or per-market
# contracts.
#
# In this example:
#
# Stage 1 — Pool Discovery:
#   Fetches "PoolCreated" events from the Uniswap V3 Factory contract to find
#   all pool addresses deployed in a given block range.
#
# Stage 2 — Pool Event Indexing:
#   Uses the discovered pool addresses to fetch and decode ALL events emitted
#   by those pools (swaps, mints, burns, etc.) in the same block range.
#
# Installation:
#   pip install tiders
#   # or with uv:
#   uv pip install tiders
#
# Usage:
#
#   uv run uniswap_v3.py --provider <hypersync|sqd|rpc> --from_block 12369621 --to_block 12370621
#       [--rpc_url URL]    \  # only needed with --provider rpc
#       [--database BACKEND]  # default: pyarrow. Options: pyarrow, duckdb, delta_lake, clickhouse, iceberg, postgresql
#
# Output is written to data/uniswap_v3/
#
# Note: This example is more complex than a real pipeline would be. It would be better to have 2 separate
# pipelines and an orchestration layer to run them sequentially, Also, it
# lets you choose the provider and database backend from the CLI. In practice,
# you would just hardcode the provider and writer you need — no argparse, no
# provider_url() helper, no create_writer() factory, no CLI. Those parts exist here
# only to showcase multiple Tiders backends in a single script.

import argparse
import asyncio
import os
from pathlib import Path
from typing import Optional

import pyarrow as pa
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

from tiders import config as cc  # noqa: E402
from tiders import run_pipeline  # noqa: E402
from tiders_core import evm_abi_events, ingest  # noqa: E402


# =============================================================================
# Constants
# =============================================================================

# The Uniswap V3 Factory contract. This is the "parent" in the factory pattern —
# it deploys pool contracts and emits a PoolCreated event for each one.
UNISWAP_V3_FACTORY = "0x1f98431c8ad98523631ae4a59f267346ea31f984"

# Default provider URLs for each data source.
DEFAULT_HYPERSYNC_URL = "https://eth.hypersync.xyz/"
DEFAULT_SQD_URL = "https://portal.sqd.dev/datasets/ethereum-mainnet"
DEFAULT_RPC_URL = "https://mainnet.gateway.tenderly.co"

# Output directory for all pipeline results.
DATA_PATH = str(Path.cwd() / "data")
Path(DATA_PATH).mkdir(parents=True, exist_ok=True)

WRITER_CHOICES = [
    "clickhouse",
    "delta_lake",
    "duckdb",
    "iceberg",
    "pyarrow",
    "postgresql",
]

# Table name aliases
POOL_CREATED_LOGS_TABLE = "uniswap_v3_factory_pool_created_logs"
POOL_EVENTS_TABLE = "uniswap_v3_pool_logs"
POOL_CREATED_TABLE = "uniswap_v3_pool_created"


# =============================================================================
# ABI Setup — Extract event signatures from JSON ABI files
# =============================================================================
# `evm_abi_events()` parses an ABI JSON and returns event descriptors.
# We build a dict keyed by event name for easy access, e.g.:
#   factory_events["PoolCreated"]["topic0"]   — for log filtering
#   factory_events["PoolCreated"]["signature"] — for decoding

_FACTORY_ABI_JSON = (Path(__file__).parent / "uniswap-v3-factory-abi.json").read_text()
factory_events = {
    ev.name: {
        "topic0": ev.topic0,
        "signature": ev.signature,
        "name_snake_case": ev.name_snake_case,
        "selector_signature": ev.selector_signature,
    }
    for ev in evm_abi_events(_FACTORY_ABI_JSON)
}

_POOL_ABI_JSON = (Path(__file__).parent / "uniswap-v3-pool-abi.json").read_text()
pool_events = {
    ev.name: {
        "topic0": ev.topic0,
        "signature": ev.signature,
        "name_snake_case": ev.name_snake_case,
        "selector_signature": ev.selector_signature,
    }
    for ev in evm_abi_events(_POOL_ABI_JSON)
}


# =============================================================================
# Provider Helper (only needed because this example supports multiple providers)
# =============================================================================


def create_provider(
    kind: ingest.ProviderKind, rpc_url: Optional[str]
) -> ingest.ProviderConfig:
    """Build a ProviderConfig for the given provider kind."""
    # `stop_on_head=True` means the pipeline will stop once it reaches the chain tip.
    if kind == ingest.ProviderKind.HYPERSYNC:
        # HyperSync requires a bearer token for authentication.
        return ingest.ProviderConfig(
            kind=kind,
            url=DEFAULT_HYPERSYNC_URL,
            bearer_token=os.environ.get("BEARER_TOKEN"),
            stop_on_head=True,
        )
    if kind == ingest.ProviderKind.SQD:
        return ingest.ProviderConfig(
            kind=kind,
            url=DEFAULT_SQD_URL,
            bearer_token=os.environ.get("BEARER_TOKEN"),
            stop_on_head=True,
        )
    # RPC or any other provider.
    url = rpc_url or os.environ.get("RPC_URL", DEFAULT_RPC_URL)
    return ingest.ProviderConfig(
        kind=kind,
        url=url,
        bearer_token=os.environ.get("BEARER_TOKEN"),
        stop_on_head=True,
    )


# =============================================================================
# Writer Factory Helper (only needed because this example supports multiple backends)
# =============================================================================
# In a real pipeline you would just create the one Writer you need inline.
# This factory exists only to let you try different backends from the CLI.
# The Writer is one of the four pipeline components — it controls where and how
# the transformed data is persisted.


async def create_writer(database: str) -> cc.Writer:
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

    if database == "postgresql":
        import psycopg

        host = os.environ.get("POSTGRES_HOST", "localhost")
        port = int(os.environ.get("POSTGRES_PORT", "5432"))
        user = os.environ.get("POSTGRES_USER", "postgres")
        password = os.environ.get("POSTGRES_PASSWORD", "secret")
        dbname = os.environ.get("POSTGRES_DB", "tiders")

        _conninfo = " ".join(
            [
                f"host={host}",
                f"port={port}",
                f"dbname={dbname}",
                f"user={user}",
                f"password={password}",
            ]
        )
        connection = await psycopg.AsyncConnection.connect(_conninfo, autocommit=False)

        return cc.Writer(
            kind=cc.WriterKind.POSTGRESQL,
            config=cc.PostgresqlWriterConfig(connection=connection),
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


# =============================================================================
# Stage 1 — Discover children from the Factory contract
# =============================================================================
# The first stage of the factory pattern: query the Factory for PoolCreated
# events to find all child contract (pool) addresses, token pairs, and fee tiers.


async def run_pool_created_pipeline(
    provider_kind: ingest.ProviderKind,
    from_block: int,
    to_block: Optional[int],
    rpc_url: Optional[str],
    database: str,
):

    provider = create_provider(provider_kind, rpc_url)

    query = ingest.Query(
        kind=ingest.QueryKind.EVM,
        params=ingest.evm.Query(
            from_block=from_block,
            to_block=to_block,
            logs=[
                ingest.evm.LogRequest(
                    address=[UNISWAP_V3_FACTORY],
                    topic0=[factory_events["PoolCreated"]["topic0"]],
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

    steps = [
        cc.Step(
            kind=cc.StepKind.EVM_DECODE_EVENTS,
            config=cc.EvmDecodeEventsConfig(
                event_signature=factory_events["PoolCreated"]["signature"],
                input_table=POOL_CREATED_LOGS_TABLE,
                output_table=POOL_CREATED_TABLE,
                allow_decode_fail=False,
            ),
        ),
        cc.Step(
            kind=cc.StepKind.HEX_ENCODE,
            config=cc.HexEncodeConfig(),
        ),
    ]

    # --- Assemble and run the pipeline ---
    # `table_aliases` renames the default "logs" table to our custom name so
    # the decode step can reference it by the aliased name.
    pipeline = cc.Pipeline(
        provider=provider,
        query=query,
        writer=writer,
        table_aliases=cc.EvmTableAliases(logs=POOL_CREATED_LOGS_TABLE),
        steps=steps,
    )

    await run_pipeline(pipeline=pipeline)


# =============================================================================
# Reading Stage 1 Results — Bridge between factory and children stages
# (only needed because this example supports multiple database backends)
# =============================================================================
# After Stage 1 completes, we read back the discovered child contract addresses
# from whatever storage backend was used. In a real pipeline you'd just read
# from your one database directly. This is regular Python — Tiders doesn't
# prescribe how you connect pipeline stages, so you can use any method to pass
# the factory output into the next pipeline.


async def load_pool_addresses(database: str) -> list[str]:
    """Load distinct pool addresses from the Stage 1 output table."""
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
        result = await client.query(
            f"SELECT DISTINCT pool FROM {POOL_CREATED_TABLE} WHERE pool IS NOT NULL"
        )
        addresses = {row[0] for row in result.result_rows if row[0]}
        return sorted(addresses)

    if database == "postgresql":
        import psycopg

        host = os.environ.get("POSTGRES_HOST", "localhost")
        port = int(os.environ.get("POSTGRES_PORT", "5432"))
        user = os.environ.get("POSTGRES_USER", "postgres")
        password = os.environ.get("POSTGRES_PASSWORD", "secret")
        dbname = os.environ.get("POSTGRES_DB", "tiders")

        _conninfo = " ".join(
            [
                f"host={host}",
                f"port={port}",
                f"dbname={dbname}",
                f"user={user}",
                f"password={password}",
            ]
        )
        async with await psycopg.AsyncConnection.connect(_conninfo) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SELECT DISTINCT pool FROM {POOL_CREATED_TABLE} WHERE pool IS NOT NULL"
                )
                rows = await cur.fetchall()
        addresses = {row[0] for row in rows if row[0]}
        return sorted(addresses)

    raise ValueError(
        f"Pool loading for database '{database}' is not supported. Use one of: duckdb, pyarrow, delta_lake, clickhouse, postgresql."
    )


# =============================================================================
# Stage 2 — Index events from the discovered child contracts
# =============================================================================
# The second stage of the factory pattern: now that we know the child contract
# addresses (pools), re-query the same block range filtered to those addresses.
# Fetch ALL events from those pools and decode each known event type (Swap,
# Mint, Burn, etc.) into its own table.


def _pool_event_steps() -> list[cc.Step]:
    """Build the transformation steps for decoding all pool event types all at once."""
    steps: list[cc.Step] = []

    # Create a decode step for each known event in the pool ABI.
    # Each event gets decoded from the shared raw logs table into its own
    # output table (e.g., "uniswap_v3_pool_swap", "uniswap_v3_pool_mint").
    # `filter_by_topic0=True` ensures only matching logs are decoded.
    # `allow_decode_fail=True` skips logs that don't match the expected format.
    for _, event in pool_events.items():
        output_table = f"uniswap_v3_pool_{event['name_snake_case']}"
        steps.append(
            cc.Step(
                kind=cc.StepKind.EVM_DECODE_EVENTS,
                config=cc.EvmDecodeEventsConfig(
                    event_signature=event["signature"],
                    input_table=POOL_EVENTS_TABLE,
                    output_table=output_table,
                    allow_decode_fail=True,
                    filter_by_topic0=True,
                ),
            ),
        )

    # Some Uniswap V3 events use int256 values (e.g., tick amounts) which are
    # stored as decimal256. Cast them down to decimal128 for broader compatibility.
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
    steps.append(
        cc.Step(
            name="join_blocks_data",
            kind=cc.StepKind.JOIN_BLOCK_DATA,
            config=cc.JoinBlockDataConfig(),
        )
    )

    # Convert binary columns (addresses, hashes, topics) to "0x..." hex strings.
    steps.append(
        cc.Step(
            kind=cc.StepKind.HEX_ENCODE,
            config=cc.HexEncodeConfig(),
        )
    )

    return steps


async def run_pool_events_pipeline(
    provider_kind: ingest.ProviderKind,
    from_block: int,
    to_block: Optional[int],
    rpc_url: Optional[str],
    database: str,
    pool_addresses: list[str],
):
    provider = create_provider(provider_kind, rpc_url)

    writer = await create_writer(database)

    print(
        f"Running pool events query for {len(pool_addresses)} addresses "
        f"(from_block={from_block}, to_block={to_block})"
    )

    # Query logs from the discovered pool addresses. No topic filter here —
    # we want ALL events from these pools, and the decode steps will sort them.
    query = ingest.Query(
        kind=ingest.QueryKind.EVM,
        params=ingest.evm.Query(
            from_block=from_block,
            to_block=to_block,
            logs=[ingest.evm.LogRequest(address=pool_addresses, include_blocks=True)],
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
                block=ingest.evm.BlockFields(
                    timestamp=True,
                    number=True,
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


# =============================================================================
# Orchestrator — Ties both stages together
# =============================================================================


async def main(
    provider_kind: ingest.ProviderKind,
    from_block: int,
    to_block: Optional[int],
    rpc_url: Optional[str],
    database: str,
):
    # Stage 1: Discover pools from Uniswap V3 Factory PoolCreated events.
    await run_pool_created_pipeline(
        provider_kind=provider_kind,
        from_block=from_block,
        to_block=to_block,
        rpc_url=rpc_url,
        database=database,
    )

    # Read the pool addresses written by Stage 1.
    pool_addresses = await load_pool_addresses(database)
    if not pool_addresses:
        print(
            "No pools found in uniswap_v3_pool_created. Skipping pool events pipeline."
        )
        return

    # Stage 2: Index all events from the discovered pools.
    print(f"Indexing pool events for {len(pool_addresses)} pools")
    await run_pool_events_pipeline(
        provider_kind=provider_kind,
        from_block=from_block,
        to_block=to_block,
        rpc_url=rpc_url,
        database=database,
        pool_addresses=pool_addresses,
    )


# =============================================================================
# CLI Entry Point
# =============================================================================

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
        default="duckdb",
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
