"""Step that keeps only selected columns in one or more tables."""

from typing import Dict

import pyarrow as pa

from ..config import SelectColumnsConfig


def execute(
    data: Dict[str, pa.Table], config: SelectColumnsConfig
) -> Dict[str, pa.Table]:
    """Keep only the configured columns in the target tables."""
    out = dict(data)
    for table_name, columns in config.tables.items():
        out[table_name] = out[table_name].select(columns)
    return out
