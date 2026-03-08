"""Column-level type casting step."""

from typing import Dict
from copy import deepcopy

import pyarrow as pa
from tiders_core import cast, cast_schema
from ..config import CastConfig


def execute(data: Dict[str, pa.Table], config: CastConfig) -> Dict[str, pa.Table]:
    """Cast specific columns in a single table to new Arrow data types.

    Only the table matching ``config.table_name`` is affected. Each column
    listed in ``config.mappings`` is cast to its target type using the
    Rust-backed ``tiders_core.cast`` function. The table schema is updated
    accordingly via ``tiders_core.cast_schema``.

    Args:
        data: A dictionary mapping table names to PyArrow Tables.
        config: A :class:`CastConfig` specifying the target table and
            column-to-type mappings.

    Returns:
        A new data dictionary with the cast columns applied.
    """
    data = deepcopy(data)

    mappings = list(config.mappings.items())

    for table_name, table_data in data.items():
        if table_name != config.table_name:
            continue
        batches = table_data.to_batches()
        out_batches = []
        for batch in batches:
            out_batches.append(cast(mappings, batch, config.allow_cast_fail))

        new_schema = cast_schema(
            mappings,
            table_data.schema,
        )
        data[table_name] = pa.Table.from_batches(out_batches, schema=new_schema)

    return data
