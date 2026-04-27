"""Step that adds constant-value columns to one or more tables."""

from typing import Dict

import pyarrow as pa

from ..config import AddColumnsConfig


def execute(data: Dict[str, pa.Table], config: AddColumnsConfig) -> Dict[str, pa.Table]:
    """Add or replace constant-value columns in the target tables."""
    out = dict(data)
    for table_name, mappings in config.tables.items():
        table = out[table_name]
        for column_name, value in mappings.items():
            if column_name in table.column_names:
                table = table.drop_columns([column_name])
            table = table.append_column(
                column_name,
                pa.repeat(pa.scalar(value), size=table.num_rows),
            )
        out[table_name] = table
    return out
