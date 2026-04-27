"""Step that renames tables in the pipeline data dictionary."""

from typing import Dict

import pyarrow as pa

from ..config import RenameTablesConfig


def execute(
    data: Dict[str, pa.Table], config: RenameTablesConfig
) -> Dict[str, pa.Table]:
    """Rename the configured tables and preserve iteration order."""
    out: Dict[str, pa.Table] = {}
    for table_name, table in data.items():
        out[config.mappings.get(table_name, table_name)] = table
    return out
