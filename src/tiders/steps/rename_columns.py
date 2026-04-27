"""Step that renames columns in one or more tables."""

from typing import Dict

import pyarrow as pa

from ..config import RenameColumnsConfig


def execute(
    data: Dict[str, pa.Table], config: RenameColumnsConfig
) -> Dict[str, pa.Table]:
    """Rename the configured columns in the target tables."""
    out = dict(data)
    for table_name, mappings in config.tables.items():
        table = out[table_name]
        renamed_columns = [
            mappings.get(column_name, column_name) for column_name in table.column_names
        ]
        out[table_name] = table.rename_columns(renamed_columns)
    return out
