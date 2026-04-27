"""Step that suffixes selected table names."""

from typing import Dict

import pyarrow as pa

from ..config import SuffixTablesConfig


def execute(
    data: Dict[str, pa.Table], config: SuffixTablesConfig
) -> Dict[str, pa.Table]:
    """Append a suffix to the configured table names."""
    tables_to_rename = set(config.tables)
    out: Dict[str, pa.Table] = {}
    for table_name, table in data.items():
        out[
            f"{table_name}{config.suffix}"
            if table_name in tables_to_rename
            else table_name
        ] = table
    return out
