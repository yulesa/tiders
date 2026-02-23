# This example shows a simple pipeline that ingests and decodes ERC-20 transfers
# directly from an Ethereum RPC node (e.g. a local node or Alchemy/Infura endpoint).
# Cherry is published to PyPI as cherry-etl and cherry-core.
# To install it, run: pip install cherry-etl cherry-core
# Or with uv: uv pip install cherry-etl cherry-core

# You can run this script with:
# RPC_URL=https://mainnet.gateway.tenderly.co uv run examples/erc20_rpc.py

# After run, the parquet files are written to data/transfers/

import asyncio
from pathlib import Path

import pyarrow as pa
from cherry_core import evm_signature_to_topic0, ingest
from cherry_etl import config as cc
from cherry_etl import run_pipeline

DATA_PATH = str(Path.cwd() / "data")
Path(DATA_PATH).mkdir(parents=True, exist_ok=True)

FROM_BLOCK = 22_000_000
TO_BLOCK = 22_001_000

TRANSFER_SIGNATURE = "Transfer(address indexed from, address indexed to, uint256 amount)"


async def main():
    provider = ingest.ProviderConfig(
        kind=ingest.ProviderKind.RPC,
        url="https://mainnet.gateway.tenderly.co",
        stop_on_head=True,
        batch_size=2000,
    )

    query = ingest.Query(
        kind=ingest.QueryKind.EVM,
        params=ingest.evm.Query(
            from_block=FROM_BLOCK,
            to_block=TO_BLOCK,
            logs=[
                ingest.evm.LogRequest(
                    topic0=[evm_signature_to_topic0(TRANSFER_SIGNATURE)],
                )
            ],
            fields=ingest.evm.Fields(
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
                kind=cc.StepKind.EVM_DECODE_EVENTS,
                config=cc.EvmDecodeEventsConfig(
                    event_signature=TRANSFER_SIGNATURE,
                    output_table="transfers",
                    allow_decode_fail=True,
                ),
            ),
            cc.Step(
                kind=cc.StepKind.HEX_ENCODE,
                config=cc.HexEncodeConfig(),
            ),
            cc.Step(
                name="i256_to_i128",
                kind=cc.StepKind.CAST_BY_TYPE,
                config=cc.CastByTypeConfig(
                    from_type=pa.decimal256(76, 0),
                    to_type=pa.decimal128(38, 0),
                    allow_cast_fail=True,
                ),
            ),
        ],
    )

    await run_pipeline(pipeline=pipeline)


if __name__ == "__main__":
    asyncio.run(main())
