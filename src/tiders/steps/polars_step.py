"""Custom Polars transformation step."""

from typing import Dict

from ..config import PolarsStepConfig
import pyarrow as pa
import polars as pl


def execute(data: Dict[str, pa.Table], config: PolarsStepConfig) -> Dict[str, pa.Table]:
    """Run a user-supplied function using Polars DataFrames.

    Converts all PyArrow Tables to Polars DataFrames, invokes
    ``config.runner(dataframes, context)``, and converts the returned
    DataFrames back to PyArrow Tables.

    Args:
        data: A dictionary mapping table names to PyArrow Tables.
        config: A :class:`PolarsStepConfig` containing the runner callable and
            optional context.

    Returns:
        A new data dictionary with the transformed tables.
    """
    pl_data = {}

    for name, table in data.items():
        pl_data[name] = pl.from_arrow(table)

    out = config.runner(pl_data, config.context)

    pa_data = {}

    for name, table in out.items():
        pa_data[name] = table.to_arrow()

    return pa_data
