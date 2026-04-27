"""Step that reorders columns in one or more tables."""

from typing import Dict

import pyarrow as pa

from ..config import ReorderColumnsConfig


def execute(
    data: Dict[str, pa.Table], config: ReorderColumnsConfig
) -> Dict[str, pa.Table]:
    """Apply the configured column ordering to the target tables.
    Columns not mentioned in the configuration will be placed after the leading columns,
    in their original order.
    """
    out = dict(data)
    for table_name, leading_columns in config.tables.items():
        table = out[table_name]
        trailing_columns = [
            column_name
            for column_name in table.column_names
            if column_name not in leading_columns
        ]
        out[table_name] = table.select([*leading_columns, *trailing_columns])
    return out
