"""Custom Pandas transformation step."""

from typing import Dict

from ..config import PandasStepConfig
import pyarrow as pa

try:
    import pandas as pd
except ImportError:
    pd = None


def execute(data: Dict[str, pa.Table], config: PandasStepConfig) -> Dict[str, pa.Table]:
    """Run a user-supplied function using Pandas DataFrames.

    Converts all PyArrow Tables to Pandas DataFrames, invokes
    ``config.runner(dataframes, context)``, and converts the returned
    DataFrames back to PyArrow Tables.

    Args:
        data: A dictionary mapping table names to PyArrow Tables.
        config: A :class:`PandasStepConfig` containing the runner callable and
            optional context.

    Returns:
        A new data dictionary with the transformed tables.
    """
    if pd is None:
        raise ImportError(
            "Pandas step requires the pandas package. "
            "Install it with: pip install tiders[pandas]"
        )
    pd_data = {}

    for name, table in data.items():
        pd_data[name] = table.to_pandas()

    out = config.runner(pd_data, config.context)

    pa_data = {}

    for name, df in out.items():
        pa_data[name] = pa.Table.from_pandas(df)

    return pa_data
