"""Step that adds or replaces a ``chain_id`` column on every table."""

from typing import Dict

from ..config import SetChainIdConfig
import pyarrow as pa


def execute(data: Dict[str, pa.Table], config: SetChainIdConfig) -> Dict[str, pa.Table]:
    """Set a constant ``chain_id`` column (``UInt64``) on every table.

    If a ``chain_id`` column already exists it is dropped first, then a new
    column filled with ``config.chain_id`` is appended.

    Args:
        data: A dictionary mapping table names to PyArrow Tables.
        config: A :class:`SetChainIdConfig` with the chain identifier value.

    Returns:
        A new data dictionary with the ``chain_id`` column set on all tables.
    """
    out = {}

    for table_name, table in data.items():
        o = table
        for name in table.schema.names:
            if name == "chain_id":
                o = table.drop_columns("chain_id")
                break
        out[table_name] = o.append_column(
            "chain_id",
            pa.repeat(
                pa.scalar(config.chain_id, type=pa.uint64()), size=table.num_rows
            ),
        )

    return out
