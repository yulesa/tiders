"""Tiders: a blockchain data ingestion and transformation pipeline framework.

Tiders provides a declarative pipeline system for streaming blockchain data (EVM and SVM
chains), applying transformation steps (decoding, casting, encoding), and writing
results to various storage backends (ClickHouse, Iceberg, Delta Lake, DuckDB,
PyArrow datasets).

Typical usage::

    import tiders

    pipeline = tiders.config.Pipeline(
        provider=provider_config,
        query=query,
        writer=writer,
        steps=steps,
    )
    await tiders.run_pipeline(pipeline, pipeline_name="my_pipeline")
"""

from . import config, utils
from .pipeline import run_pipeline

__all__ = ["config", "run_pipeline", "utils"]
