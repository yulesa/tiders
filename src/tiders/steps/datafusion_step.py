"""Custom DataFusion transformation step."""

from typing import Dict

from ..config import DataFusionStepConfig
import pyarrow as pa

try:
    import datafusion
except ImportError:
    datafusion = None


def execute(
    data: Dict[str, pa.Table], config: DataFusionStepConfig
) -> Dict[str, pa.Table]:
    """Run a user-supplied function using a DataFusion session context.

    Converts all PyArrow Tables to DataFusion DataFrames within a fresh
    ``SessionContext``, invokes ``config.runner(ctx, dataframes, context)``,
    and converts the returned DataFrames back to PyArrow Tables.

    Args:
        data: A dictionary mapping table names to PyArrow Tables.
        config: A :class:`DataFusionStepConfig` containing the runner callable
            and optional context.

    Returns:
        A new data dictionary with the transformed tables.
    """
    if datafusion is None:
        raise ImportError(
            "DataFusion step requires the datafusion package. "
            "Install it with: pip install tiders[datafusion]"
        )
    df_data = {}

    ctx = datafusion.SessionContext()

    for name, table in data.items():
        df_data[name] = ctx.create_dataframe(
            name=name,
            partitions=[table.to_batches()],
            schema=table.schema,
        )

    out = config.runner(ctx, df_data, config.context)

    pa_data = {}

    for name, table in out.items():
        pa_data[name] = table.to_arrow_table()

    return pa_data
