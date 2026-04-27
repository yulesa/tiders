"""Step that removes columns from one or more tables."""

from typing import Dict

import pyarrow as pa

from ..config import DeleteColumnsConfig


def execute(
    data: Dict[str, pa.Table], config: DeleteColumnsConfig
) -> Dict[str, pa.Table]:
    """Drop the configured columns from the target tables."""
    out = dict(data)
    for table_name, columns in config.tables.items():
        out[table_name] = out[table_name].drop_columns(columns)
    return out
