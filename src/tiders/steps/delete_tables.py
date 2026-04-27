"""Step that removes tables from the pipeline data dictionary."""

from typing import Dict

import pyarrow as pa

from ..config import DeleteTablesConfig


def execute(
    data: Dict[str, pa.Table], config: DeleteTablesConfig
) -> Dict[str, pa.Table]:
    """Drop the configured tables from the pipeline data dictionary."""
    out = dict(data)
    for table_name in config.tables:
        out.pop(table_name, None)
    return out
