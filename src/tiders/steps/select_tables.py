"""Step that keeps only selected tables in the pipeline data dictionary."""

from typing import Dict

import pyarrow as pa

from ..config import SelectTablesConfig


def execute(
    data: Dict[str, pa.Table], config: SelectTablesConfig
) -> Dict[str, pa.Table]:
    """Keep only the configured tables."""
    return {table_name: data[table_name] for table_name in config.tables}
