"""Step that removes empty tables from the pipeline data dictionary."""

from typing import Dict

import pyarrow as pa

from ..config import DropEmptyTablesConfig


def execute(
    data: Dict[str, pa.Table], config: DropEmptyTablesConfig
) -> Dict[str, pa.Table]:
    """Drop empty tables from the configured subset or from all tables."""
    table_names = set(config.tables) if config.tables is not None else None
    out: Dict[str, pa.Table] = {}
    for table_name, table in data.items():
        should_check = table_names is None or table_name in table_names
        if should_check and table.num_rows == 0:
            continue
        out[table_name] = table
    return out
