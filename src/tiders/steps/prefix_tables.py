"""Step that prefixes selected table names."""

from typing import Dict

import pyarrow as pa

from ..config import PrefixTablesConfig


def execute(
    data: Dict[str, pa.Table], config: PrefixTablesConfig
) -> Dict[str, pa.Table]:
    """Prepend a prefix to the configured table names."""
    tables_to_rename = set(config.tables)
    out: Dict[str, pa.Table] = {}
    for table_name, table in data.items():
        out[
            f"{config.prefix}{table_name}"
            if table_name in tables_to_rename
            else table_name
        ] = table
    return out
