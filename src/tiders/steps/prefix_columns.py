"""Step that prefixes selected columns in one or more tables."""

from typing import Dict

import pyarrow as pa

from ..config import PrefixColumnsConfig


def execute(
    data: Dict[str, pa.Table], config: PrefixColumnsConfig
) -> Dict[str, pa.Table]:
    """Prepend a prefix to the configured columns."""
    out = dict(data)
    for table_name, columns in config.tables.items():
        table = out[table_name]
        columns_to_rename = set(columns)
        out[table_name] = table.rename_columns(
            [
                f"{config.prefix}{column_name}"
                if column_name in columns_to_rename
                else column_name
                for column_name in table.column_names
            ]
        )
    return out
