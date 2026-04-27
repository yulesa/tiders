"""Step that suffixes selected columns in one or more tables."""

from typing import Dict

import pyarrow as pa

from ..config import SuffixColumnsConfig


def execute(
    data: Dict[str, pa.Table], config: SuffixColumnsConfig
) -> Dict[str, pa.Table]:
    """Append a suffix to the configured columns."""
    out = dict(data)
    for table_name, columns in config.tables.items():
        table = out[table_name]
        columns_to_rename = set(columns)
        out[table_name] = table.rename_columns(
            [
                f"{column_name}{config.suffix}"
                if column_name in columns_to_rename
                else column_name
                for column_name in table.column_names
            ]
        )
    return out
