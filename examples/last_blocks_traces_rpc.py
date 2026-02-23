# This example shows a simple pipeline that ingests traces for a range of blocks
# directly from an Ethereum RPC node.
#
# IMPORTANT: The provider must support block-level tracing:
#   - trace_block         → Erigon, Nethermind, Reth, Alchemy, QuickNode, Tenderly (authenticated)
#   - debug_traceBlockByNumber → Geth, Reth, Erigon, Alchemy, QuickNode, Infura, Tenderly (authenticated)
#
# Fetching traces per transaction is not supported — use a node with block-level
# trace support or switch to SQD Network / HyperSync.
#
# NOTE: The unauthenticated Tenderly public gateway (mainnet.gateway.tenderly.co)
# does NOT support trace methods. Use a project-specific authenticated URL:
#   https://mainnet.gateway.tenderly.co/<YOUR_ACCESS_KEY>
# or a provider that exposes trace APIs without authentication (e.g. a local Reth node).
#
# Cherry is published to PyPI as cherry-etl and cherry-core.
# To install it, run: pip install cherry-etl cherry-core
# Or with uv: uv pip install cherry-etl cherry-core

# You can run this script with:
# RPC_URL=https://mainnet.gateway.tenderly.co/<YOUR_ACCESS_KEY> uv run examples/last_blocks_traces_rpc.py

# After run, the parquet files are written to data/

import asyncio
import os
from pathlib import Path

from cherry_core import ingest
from cherry_etl import config as cc
from cherry_etl import run_pipeline

DATA_PATH = str(Path.cwd() / "data")
Path(DATA_PATH).mkdir(parents=True, exist_ok=True)

async def main():
    url = os.environ.get("RPC_URL", "https://mainnet.gateway.tenderly.co/6c6oefpBifI6lWPjYzklrk")

    provider = ingest.ProviderConfig(
        kind=ingest.ProviderKind.RPC,
        url=url,
        stop_on_head=True,
        batch_size=10,
        # Trace method: "trace_block" (Erigon/Nethermind/Reth, default) or
        # "debug_trace_block_by_number" (Geth/Reth/Erigon).
        trace_method="trace_block",
    )

    query = ingest.Query(
        kind=ingest.QueryKind.EVM,
        params=ingest.evm.Query(
            from_block=18_000_000,
            to_block=18_000_002,
            traces=[
                ingest.evm.TraceRequest(),
            ],
            fields=ingest.evm.Fields(
                trace=ingest.evm.TraceFields(
                    from_=True,
                    to=True,
                    call_type=True,
                    gas=True,
                    input=True,
                    init=True,
                    value=True,
                    author=True,
                    reward_type=True,
                    block_hash=True,
                    block_number=True,
                    address=True,
                    code=True,
                    gas_used=True,
                    output=True,
                    subtraces=True,
                    trace_address=True,
                    transaction_hash=True,
                    transaction_position=True,
                    type_=True,
                    error=True,
                    sighash=True,
                    action_address=True,
                    balance=True,
                    refund_address=True,
                ),
            ),
        ),
    )

    writer = cc.Writer(
        kind=cc.WriterKind.PYARROW_DATASET,
        config=cc.PyArrowDatasetWriterConfig(base_dir=DATA_PATH),
    )

    pipeline = cc.Pipeline(
        provider=provider,
        query=query,
        writer=writer,
        steps=[
            cc.Step(
                kind=cc.StepKind.HEX_ENCODE,
                config=cc.HexEncodeConfig(),
            ),
        ],
    )

    await run_pipeline(pipeline=pipeline)


if __name__ == "__main__":
    asyncio.run(main())
