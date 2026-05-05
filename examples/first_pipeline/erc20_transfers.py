# ERC-20 Transfer Events Pipeline
# ================================
# This script fetches ERC-20 Transfer events from Ethereum and writes them to DuckDB.
#
# A tiders pipeline has 5 parts:
#   1. Contract  — (optional) load ABI to get event/function signatures and selectors
#   2. Provider  — where to fetch blockchain data from (RPC, HyperSync, SQD)
#   3. Query     — what data to fetch (block range, event filters, fields)
#   4. Steps     — transformations to apply (decode events, encode hex, etc.)
#   5. Writer    — where to store the results (DuckDB, ClickHouse, Parquet, etc.)
#
# Usage:
#   cd examples/first_pipeline
#   tiders abi # download the ABI for the ERC-20 contract and save it as erc20.abi.json
#   python erc20_transfers.py
#
# Verify the output by querying the DuckDB file using duckdb-cli:
#   duckdb data/transfers.duckdb
#   SHOW TABLES;
#   SELECT * FROM transfers;

import asyncio
from pathlib import Path
import pyarrow as pa

from tiders import run_pipeline
from tiders.config import (
    DuckdbWriterConfig,
    CheckpointConfig,
    EvmDecodeEventsConfig,
    CastByTypeConfig,
    HexEncodeConfig,
    Pipeline,
    Step,
    StepKind,
    Writer,
    WriterKind,
)
from tiders_core.ingest import ProviderConfig, ProviderKind, Query, QueryKind
from tiders_core.ingest import evm
from tiders_core import evm_abi_events, evm_abi_functions


# ---------------------------------------------------------------------------
# 1. Contract (optional, but very useful)
# ---------------------------------------------------------------------------
# Load the contract's ABI JSON file and parse it to extract event and function
# metadata.

erc20_address = "0xae78736Cd615f374D3085123A210448E74Fc6393"  # rETH token contract

erc20_abi_path = Path(
    "/home/yulesa/repos/tiders/examples/first_pipeline/erc20.abi.json"
)
erc20_abi_json = erc20_abi_path.read_text()

# Build a dict of events keyed by name, e.g. erc20_events["Transfer"]["topic0"]
erc20_events = {
    ev.name: {
        "topic0": ev.topic0,
        "signature": ev.signature,
        "name_snake_case": ev.name_snake_case,
        "selector_signature": ev.selector_signature,
    }
    for ev in evm_abi_events(erc20_abi_json)
}

# Build a dict of functions keyed by name, e.g. erc20_functions["approve"]["selector"]
erc20_functions = {
    fn.name: {
        "selector": fn.selector,
        "signature": fn.signature,
        "name_snake_case": fn.name_snake_case,
        "selector_signature": fn.selector_signature,
    }
    for fn in evm_abi_functions(erc20_abi_json)
}


# ---------------------------------------------------------------------------
# 2. Provider
# ---------------------------------------------------------------------------
# The provider defines where blockchain data is fetched from.
# Options: ProviderKind.RPC, ProviderKind.HYPERSYNC, ProviderKind.SQD
# RPC works with any standard Ethereum JSON-RPC endpoint.

provider = ProviderConfig(
    kind=ProviderKind.RPC,
    url="https://mainnet.gateway.tenderly.co",
)


# ---------------------------------------------------------------------------
# 3. Writer
# ---------------------------------------------------------------------------
# The writer defines where transformed data is stored.
# DuckDB creates a local database file. Other options include ClickHouse,
# Delta Lake, Iceberg, PostgreSQL, PyArrow Dataset (Parquet), and CSV.

writer = Writer(
    kind=WriterKind.DUCKDB,
    config=DuckdbWriterConfig(path="data/transfers.duckdb"),
)


# ---------------------------------------------------------------------------
# 4. Checkpoint (Optional)
# ---------------------------------------------------------------------------
# The checkpoint tells the pipeline where to resume from in case of interruptions.
# It reads the last processed block number from the specified table and column in the
# writer's database once at pipeline start and then overwrite the query's from_block 
# with that value + 1.

checkpoint = CheckpointConfig(
    table='transfers',
    column='block_number',
    writer_index=0
)


# ---------------------------------------------------------------------------
# 5. Query
# ---------------------------------------------------------------------------
# The query defines what data to fetch: block range, filters, and fields.
#
# - from_block / to_block: the block range to scan (100 blocks here for a quick test)
# - transactions/logs/traces: what to fetch. Filtering logs with topic0 equal to the
#   ERC-20 Transfer event topic0 (so any ERC-20 transfer, not just rETH)
# - fields: which columns to include in the output. Set each desired field to True.

query = Query(
    kind=QueryKind.EVM,
    params=evm.Query(
        from_block=18000000,
        to_block=18000100,
        logs=[evm.LogRequest(topic0=[erc20_events["Transfer"]["topic0"]])],
        fields=evm.Fields(
            log=evm.LogFields(
                log_index=True,
                transaction_hash=True,
                block_number=True,
                address=True,
                data=True,
                topic0=True,
                topic1=True,
                topic2=True,
                topic3=True,
            ),
        ),
    ),
)


# ---------------------------------------------------------------------------
# 6. Steps
# ---------------------------------------------------------------------------
# Steps are transformations applied to the raw data before writing.
# They run in order, each step's output feeding into the next.
#
# STEP 1: EVM_DECODE_EVENTS: decodes the raw log data (topic1..3 + data) into named
#   columns using the event signature.
#   - allow_decode_fail: if True, rows that fail to decode are kept (with nulls)
#   - hstack: if False, outputs only decoded columns; if True, append them to
#     the original raw log columns
#
# STEP 2: HEX_ENCODE: converts binary columns (addresses, hashes) to hex strings,
#   making them human-readable and compatible with databases like DuckDB.

steps = [
    Step(
        kind=StepKind.EVM_DECODE_EVENTS,
        config=EvmDecodeEventsConfig(
            event_signature=erc20_events["Transfer"]["signature"],
            allow_decode_fail=True,
            output_table="transfers",
            hstack=True,
        ),
    ),
    Step(
        kind=StepKind.CAST_BY_TYPE,
        config=CastByTypeConfig(
            from_type=pa.decimal256(76, 0),
            to_type=pa.decimal128(38, 0),
            allow_cast_fail=True,
        ),
    ),
    Step(
        kind=StepKind.HEX_ENCODE,
        config=HexEncodeConfig(),
    ),
]


# ---------------------------------------------------------------------------
# Assemble and run
# ---------------------------------------------------------------------------
# The Pipeline ties all parts together. run_pipeline() executes the full
# ingestion: fetch -> transform -> write.

pipeline = Pipeline(
    provider=provider,
    query=query,
    writer=writer,
    steps=steps,
    checkpoint=checkpoint,
)


if __name__ == "__main__":
    asyncio.run(run_pipeline(pipeline))
