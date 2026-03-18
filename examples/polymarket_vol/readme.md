# Polymarket Hourly Volume

This example indexes Polymarket's two exchange contracts on Polygon — **CTFExchange** and **NegRiskCtfExchange** — and computes hourly trading volume from their `OrdersMatched` events.

## Overview

Polymarket uses two on-chain exchange contracts:

- **CTFExchange** (`0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e`) — standard binary-outcome markets.
- **NegRiskCtfExchange** (`0xC5d563A36AE78145C45a50134d48A1215220f80a`) — multiple-choice (neg-risk) markets.

The pipeline ingests events (OrderFilled, OrdersMatched, TokenRegistered, OrderCancelled, FeeCharged) from both contracts, decodes them, and writes the results to ClickHouse. A post-ingestion transformation then aggregates matched orders into an hourly volume table.

## Prerequisites

- A running ClickHouse instance
- `tiders` installed

## Setup

Copy the environment file and edit as needed:

```bash
cp .env.example .env
```

## Running the Example

Files are pre-configured with `from_block: 81053172` (2026-01-01 00:00:00) and `to_block: 81139570` (2026-01-02 23:59:59), 2 days worth of data. You can reduce the interval for a faster ingestion.

### Step 1: Ingest contract events

You can ingest data using either the YAML configs (no-code) or the Python scripts (code). Pick one approach.

**Option A — YAML (no-code):**

```bash
cd examples/polymarket_vol
tiders start polymarket_CTFExchange.yaml
tiders start polymarket_NegRiskCtfExchange.yaml
```

**Option B — Python scripts:**

```bash
cd examples/polymarket_vol
python polymarket_ctfexchange.py
python polymarket_neg_risk_ctf_exchange.py
```

Both options populate the same ClickHouse tables. The two pipelines are independent and can run in parallel.

### Step 2: Compute hourly volume

After **both** ingestion pipelines have completed, run the transformation script:

```bash
python polymarket_vol_transformation.py
```

This creates a `polymarket_hourly_volume` table in ClickHouse that aggregates USDC volume per hour across both exchange contracts.

## Files

| File | Description |
|------|-------------|
| `polymarket_CTFExchange.yaml` | YAML pipeline config for the CTFExchange contract |
| `polymarket_NegRiskCtfExchange.yaml` | YAML pipeline config for the NegRiskCtfExchange contract |
| `polymarket_ctfexchange.py` | Python pipeline script for the CTFExchange contract (generated from YAML) |
| `polymarket_neg_risk_ctf_exchange.py` | Python pipeline script for the NegRiskCtfExchange contract (generated from YAML) |
| `polymarket_vol_transformation.py` | Post-ingestion script that computes hourly volume |
| `polymarket_CTFExchange.abi.json` | ABI for the CTFExchange contract |
| `polymarket_NegRiskCtfExchange.abi.json` | ABI for the NegRiskCtfExchange contract |
| `.env.example` | Example environment variables |
