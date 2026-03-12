<img src="tiders_logo2.png" alt="Tiders" width="1000">

[![PyPI](https://img.shields.io/badge/PyPI-lightgreen?style=for-the-badge&logo=pypi&labelColor=white)](https://pypi.org/project/tiders/)
[![tiders-core](https://img.shields.io/badge/github-black?style=for-the-badge&logo=github)](https://github.com/yulesa/tiders-core)
[![tiders-rpc-client](https://img.shields.io/badge/github-black?style=for-the-badge&logo=github)](https://github.com/yulesa/tiders-rpc-client)
[![Documentation](https://img.shields.io/badge/documentation-blue?style=for-the-badge&logo=readthedocs)](https://yulesa.github.io/tiders-docs/)

Tiders is a python library for building blockchain data pipelines.

It is designed to make building production-ready blockchain data pipelines easy.

## Getting Started

See [getting started section](https://yulesa.github.io/tiders-docs/getting_started.html) of the docs.

## Two ways to use tiders

| Mode | How | When to use |
|---|---|---|
| **Python SDK** | Write a Python script, import `tiders` | Full control, custom logic, complex pipelines |
| **CLI (No-Code)** | Write a YAML config, run `tiders start` | Quick setup, no Python required, standard pipelines |

Both modes share the same pipeline engine.

## Installation

```bash
pip install tiders tiders-core
```

For the CLI (no-code mode):

```bash
pip install "tiders[cli]"
```

## Features

- High-level `datasets` API and flexible pipeline API.
- `High-performance`, `low-cost` and `uniform` data access. Ability to use advanced providers without platform lock-in.
- Included functionality to `decode`, `validate`, `transform` blockchain data. All implemented in `rust` for performance.
- Allow advance transformations using `polars`, `pyarrow`, `datafusion`, `pandas`.
- `Schema inference` automatically creates output tables.
- Keep datasets fresh with `continuous ingestion`.
- `Parallelized`, next batch of data is being fetched while your pre-processing function is running, while the database writes are being executed in parallel. Don't need to hand optimize anything.
- Included library of transformations.
- Included functionality to implement `crash-resistance`.

## Core Concepts

A pipeline is built from four components:

| Component | Description |
|---|---|
| `ProviderConfig` | Data source (HyperSync, SQD, or RPC) |
| `Query` | What data to fetch (block range, filters, field selection) |
| `Step` | Transformations to apply (decode, cast, encode, custom) |
| `Writer` | Output destination |

## Data providers

| Provider | Ethereum (EVM) | Solana (SVM) |
|---|---|---|
| [HyperSync](https://docs.envio.dev/docs/HyperSync/overview) | ✅ | ❌ |
| [SQD](https://docs.sqd.ai/) | ✅ | ✅ |
| RPC | ✅ | ❌ |

## Supported output formats

| Writer | Format |
|---|---|
| **DuckDB** | DuckDB database |
| **ClickHouse** | ClickHouse |
| **Iceberg** | Apache Iceberg |
| **Delta Lake** | Delta Lake |
| **PyArrow Dataset** | Parquet files |
| **PostgreSQL** | PostgreSQL |
| **CSV** | CSV files |

## Usage examples

- [Examples](examples)

## Logging

Python code uses the standard `logging` module of python, so it can be configured according to [python docs](https://docs.python.org/3/library/logging.html).

Set `RUST_LOG` environment variable according to [env_logger docs](https://docs.rs/env_logger/latest/env_logger/#enabling-logging) in order to see logs from rust modules.

To run an example with trace level logging for rust modules:
```
RUST_LOG=trace uv run examples/path/to/my/example
```

## Development

This repo uses `uv` for development. Clone all three projects side by side:

```bash
git clone https://github.com/yulesa/tiders.git
git clone https://github.com/yulesa/tiders-core.git
git clone https://github.com/yulesa/tiders-rpc-client.git
```

### Local development with tiders-core

Configure `tiders` to use your local `tiders-core` Python package by adding to `pyproject.toml`:

```toml
[tool.uv.sources]
tiders-core = { path = "../tiders-core/python", editable = true }
```

Then sync the environment:

```bash
cd tiders
uv sync
```

For full instructions including building `tiders-core` and `tiders-rpc-client` from source, see the [Development Setup](https://yulesa.github.io/tiders-docs/getting_started/development_setup.html) docs.

Core libraries we use for ingesting/decoding/validating/transforming blockchain data are implemented in [tiders-core](https://github.com/yulesa/tiders-core) repo.

## Acknowledgements

Tiders is a fork of [Cherry](https://github.com/steelcake/cherry) and [cherry-core](https://github.com/steelcake/cherry-core), a blockchain data pipeline framework built by the [SteelCake](https://github.com/steelcake) team. Cherry laid the architectural foundation that Tiders builds upon, and we're grateful for their work and the open-source spirit that made this continuation possible.

## License

Licensed under either of

 * Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license
   ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.