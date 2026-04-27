"""Step that copies columns in one or more tables."""

from typing import Dict

import pyarrow as pa

from ..config import CopyColumnsConfig


def execute(
    data: Dict[str, pa.Table], config: CopyColumnsConfig
) -> Dict[str, pa.Table]:
    """Copy configured columns to new names in the target tables."""
    out = dict(data)
    for table_name, mappings in config.tables.items():
        table = out[table_name]
        for source_column, destination_column in mappings.items():
            if destination_column in table.column_names:
                table = table.drop_columns([destination_column])
            table = table.append_column(destination_column, table[source_column])
        out[table_name] = table
    return out
