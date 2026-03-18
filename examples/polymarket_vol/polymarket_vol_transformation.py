# Post-ingestion transformation — Polymarket hourly volume
#
# Aggregates OrdersMatched events from both CTFExchange and NegRiskCtfExchange
# into a single polymarket_hourly_volume table with USDC volume per hour.
#
# Prerequisites:
#   Both ingestion pipelines must complete first:
#     tiders start polymarket_CTFExchange.yaml
#     tiders start polymarket_NegRiskCtfExchange.yaml
#   (or run the equivalent Python scripts)
#
# Usage:
#   cd examples/polymarket_vol
#   python polymarket_vol_transformation.py

import os
import asyncio
import clickhouse_connect


async def main():
    client = await clickhouse_connect.get_async_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "default"),
        database=os.environ.get("CLICKHOUSE_DATABASE", "default"),
    )

    await client.command("DROP TABLE IF EXISTS polymarket_hourly_volume")

    await client.command("""
        CREATE TABLE polymarket_hourly_volume (
            time DateTime,
            volume Float64
        ) ENGINE = MergeTree()
        ORDER BY time
    """)

    await client.command("""
        INSERT INTO polymarket_hourly_volume
        WITH data AS (
            SELECT
                toStartOfHour(fromUnixTimestamp(toUInt32(timestamp))) AS time,
                SUM(if(makerAssetId = 0, makerAmountFilled, takerAmountFilled)) / 1e6 AS volume
            FROM polymarket_ctfexchange_ordersmatched
            GROUP BY 1

            UNION ALL

            SELECT
                toStartOfHour(fromUnixTimestamp(toUInt32(timestamp))) AS time,
                SUM(if(makerAssetId = 0, makerAmountFilled, takerAmountFilled)) / 1e6 AS volume
            FROM polymarket_negriskctfexchange_ordersmatched
            GROUP BY 1
        )
        SELECT
            time,
            SUM(volume) AS volume
        FROM data
        GROUP BY 1
        ORDER BY 1
    """)

    result = await client.query("SELECT * FROM polymarket_hourly_volume ORDER BY time")
    print(f"Inserted {result.row_count} rows into polymarket_hourly_volume")
    for row in result.result_rows:
        print(f"  {row[0]}  volume: {row[1]:,.2f}")


if __name__ == "__main__":
    asyncio.run(main())
