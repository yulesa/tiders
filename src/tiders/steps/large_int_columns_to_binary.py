"""Column-scoped large-integer-to-binary conversion step.

Converts named scale-0 ``Decimal128`` / ``Decimal256`` columns in a single
table to fixed-width big-endian two's-complement ``Binary`` (16 bytes and
32 bytes respectively) via the Rust-backed ``tiders_core.large_ints_to_binary``
function. Matches the wire format produced by ``evm_decode_events`` with
``large_int_as_binary=True``.
"""

from typing import Dict
from copy import deepcopy

import pyarrow as pa
from tiders_core import large_ints_to_binary

from tiders.config import LargeIntColumnsToBinaryConfig


def _convert_batch(batch: pa.RecordBatch, columns: list) -> pa.RecordBatch:
    """Convert the selected ``columns`` of ``batch`` to binary, in place.

    The selected columns are converted via ``large_ints_to_binary`` and spliced
    back into their original positions; all other columns are left untouched.
    """
    sub_batch = batch.select(columns)
    converted = large_ints_to_binary(sub_batch)

    converted_by_name = {
        name: converted.column(i) for i, name in enumerate(converted.schema.names)
    }
    converted_fields = {
        name: converted.schema.field(i)
        for i, name in enumerate(converted.schema.names)
    }

    arrays = []
    fields = []
    for i, name in enumerate(batch.schema.names):
        if name in converted_by_name:
            arrays.append(converted_by_name[name])
            fields.append(converted_fields[name])
        else:
            arrays.append(batch.column(i))
            fields.append(batch.schema.field(i))

    return pa.RecordBatch.from_arrays(arrays, schema=pa.schema(fields))


def execute(
    data: Dict[str, pa.Table], config: LargeIntColumnsToBinaryConfig
) -> Dict[str, pa.Table]:
    """Convert specific large-integer columns to binary in a single table.

    Only the table matching ``config.table_name`` is affected. Each column
    listed in ``config.columns`` must be a scale-0 ``Decimal128`` or
    ``Decimal256``; it is converted to fixed-width big-endian two's-complement
    ``Binary``. Other columns are left untouched.

    The conversion is delegated to ``tiders_core.large_ints_to_binary`` by
    building a sub-batch of just the selected columns, casting it, and
    splicing the results back into the original batch.

    Args:
        data: A dictionary mapping table names to PyArrow Tables.
        config: A :class:`LargeIntColumnsToBinaryConfig` specifying the target
            table and the columns to convert.

    Returns:
        A new data dictionary with the converted columns applied.
    """
    data = deepcopy(data)

    table = data.get(config.table_name)
    if table is None:
        return data

    columns = list(config.columns)
    if not columns:
        return data

    out_batches = [_convert_batch(batch, columns) for batch in table.to_batches()]

    # A zero-row table yields no batches, so derive the converted output schema
    # from an explicit empty batch. This keeps the binary column types
    # consistent with non-empty tables instead of falling back to the original
    # Decimal schema (or crashing in pa.Table.from_batches with an empty list).
    empty_batch = pa.record_batch(
        [pa.array([], type=field.type) for field in table.schema],
        schema=table.schema,
    )
    out_schema = _convert_batch(empty_batch, columns).schema

    data[config.table_name] = pa.Table.from_batches(out_batches, schema=out_schema)

    return data
