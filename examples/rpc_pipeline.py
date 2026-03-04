# This example shows a simple pipeline that ingests the last 10 blocks
# directly from an Ethereum RPC node (e.g. a local node or Alchemy/Infura endpoint).
# Tiders is published to PyPI as tiders-etl and tiders-core.
# To install it, run: pip install tiders-etl tiders-core
# Or with uv: uv pip install tiders-etl tiders-core

# You can run this script with:
# uv run examples/rpc_pipeline.py
# uv run examples/rpc_pipeline.py https://your-rpc-url
# RPC_URL priority: command-line arg > env var > examples/.env > default fallback.

# After run, the parquet files are written to data/blocks/

import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")

from tiders_core import ingest
from tiders import config as cc
from tiders import run_pipeline

DATA_PATH = str(Path.cwd() / "data")
Path(DATA_PATH).mkdir(parents=True, exist_ok=True)

async def main():
    url = (
        sys.argv[1]
        if len(sys.argv) > 1
        else os.environ.get("RPC_URL", "https://mainnet.gateway.tenderly.co")
    )

    provider = ingest.ProviderConfig(
        kind=ingest.ProviderKind.RPC,
        url=url,
        stop_on_head=True,
        batch_size=10, 
    )

    query = ingest.Query(
        kind=ingest.QueryKind.EVM,
        params=ingest.evm.Query(
            from_block=18_000_000,
            to_block=18_000_100,
            transactions=[
                ingest.evm.TransactionRequest(
                    include_blocks=True,
                    include_logs=True,
                ),
            ],
            fields=ingest.evm.Fields(
                block=ingest.evm.BlockFields(
                    number=True,
                    hash=True,
                    parent_hash=True,
                    timestamp=True,
                    miner=True,
                    gas_limit=True,
                    gas_used=True,
                    base_fee_per_gas=True,
                    size=True,
                    withdrawals=True,
                ),
                transaction=ingest.evm.TransactionFields(
                    hash=True,
                    from_=True,
                    to=True,
                    value=True,
                    cumulative_gas_used=True,
                    effective_gas_price=True,
                    gas_used=True,
                    contract_address=True,
                ),
                log=ingest.evm.LogFields(
                    block_number=True,
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
