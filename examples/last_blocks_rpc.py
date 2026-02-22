# This example shows a simple pipeline that ingests the last 10 blocks
# directly from an Ethereum RPC node (e.g. a local node or Alchemy/Infura endpoint).
# Cherry is published to PyPI as cherry-etl and cherry-core.
# To install it, run: pip install cherry-etl cherry-core
# Or with uv: uv pip install cherry-etl cherry-core

# You can run this script with:
# RPC_URL=https://mainnet.gateway.tenderly.co uv run examples/last_blocks_rpc.py

# After run, the parquet files are written to data/blocks/

import asyncio
import os
from pathlib import Path

from cherry_core import ingest
from cherry_etl import config as cc
from cherry_etl import run_pipeline

DATA_PATH = str(Path.cwd() / "data")
Path(DATA_PATH).mkdir(parents=True, exist_ok=True)

async def main():
    url = os.environ.get("RPC_URL", "https://mainnet.gateway.tenderly.co")

    provider = ingest.ProviderConfig(
        kind=ingest.ProviderKind.RPC,
        url=url,
        stop_on_head=True,
        max_block_range=10,
    )

    query = ingest.Query(
        kind=ingest.QueryKind.EVM,
        params=ingest.evm.Query(
            from_block=18_000_000,
            to_block=18_000_010,
            transactions=[
                ingest.evm.TransactionRequest(
                    include_blocks=True,
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
                ),
                transaction=ingest.evm.TransactionFields(
                    hash=True,
                    from_=True,
                    to=True,
                    value=True,
                )
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
