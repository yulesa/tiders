"""Type-level casting step that converts all columns of a given type across all tables."""

from typing import Dict
from copy import deepcopy

import pyarrow as pa
from tiders_core import cast_by_type, cast_schema_by_type
from ..config import CastByTypeConfig


def execute(data: Dict[str, pa.Table], config: CastByTypeConfig) -> Dict[str, pa.Table]:
    """Cast every column matching ``config.from_type`` to ``config.to_type``.

    Applies the conversion across all tables in the data dictionary. The schema
    of each table is updated via ``tiders_core.cast_schema_by_type``.

    Args:
        data: A dictionary mapping table names to PyArrow Tables.
        config: A :class:`CastByTypeConfig` with source and target types.

    Returns:
        A new data dictionary with the casts applied.
    """
    data = deepcopy(data)

    for table_name, table_data in data.items():
        batches = table_data.to_batches()
        out_batches = []

        for batch in batches:
            out_batches.append(
                cast_by_type(
                    batch, config.from_type, config.to_type, config.allow_cast_fail
                )
            )

        new_schema = cast_schema_by_type(
            table_data.schema, config.from_type, config.to_type
        )
        data[table_name] = pa.Table.from_batches(out_batches, schema=new_schema)

    return data
