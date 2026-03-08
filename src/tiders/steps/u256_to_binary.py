"""U256-to-binary conversion step.

Converts ``Decimal256`` columns to a fixed-size binary representation, handling
both top-level columns (via the Rust ``u256_to_binary`` function) and nested
columns inside structs and lists (via a recursive Python fallback).
"""

from typing import Dict
from copy import deepcopy

from tiders_core import u256_to_binary, u256_column_to_binary
import pyarrow as pa

from tiders.config import U256ToBinaryConfig


def _convert_array(arr: pa.Array) -> pa.Array:
    """Recursively convert any remaining Decimal256 arrays the Rust function missed.

    The Rust-level ``u256_to_binary`` handles top-level columns, but may not
    traverse into nested types (e.g. ``List<Struct<Decimal256>>``). This
    function walks the Arrow type tree and converts any ``Decimal256`` arrays
    it finds using ``u256_column_to_binary``.

    Args:
        arr: A PyArrow Array that may contain Decimal256 values at any nesting
            level.

    Returns:
        The array with all Decimal256 sub-arrays replaced by their binary
        equivalents.
    """
    if pa.types.is_decimal256(arr.type):
        return u256_column_to_binary(arr)
    if pa.types.is_struct(arr.type):
        new_fields = []
        new_arrays = []
        for i in range(arr.type.num_fields):
            f = arr.type.field(i)
            child = _convert_array(arr.field(i))
            new_fields.append(pa.field(f.name, child.type))
            new_arrays.append(child)
        return pa.StructArray.from_arrays(
            new_arrays, fields=new_fields, mask=arr.is_null()
        )
    if pa.types.is_list(arr.type):
        converted_values = _convert_array(arr.values)
        new_type = pa.list_(pa.field(arr.type.value_field.name, converted_values.type))
        return pa.ListArray.from_arrays(
            arr.offsets, converted_values, mask=arr.is_null(), type=new_type
        )
    if pa.types.is_large_list(arr.type):
        converted_values = _convert_array(arr.values)
        new_type = pa.large_list(
            pa.field(arr.type.value_field.name, converted_values.type)
        )
        return pa.LargeListArray.from_arrays(
            arr.offsets, converted_values, mask=arr.is_null(), type=new_type
        )
    return arr


def _fix_nested(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Post-process a batch to convert any remaining nested Decimal256 columns.

    Applies :func:`_convert_array` to every column in the batch and rebuilds
    the schema to reflect the converted types.

    Args:
        batch: A PyArrow RecordBatch that has already been processed by the
            Rust ``u256_to_binary`` function.

    Returns:
        A new RecordBatch with all Decimal256 columns (including nested ones)
        converted to binary.
    """
    new_columns = []
    new_fields = []
    for i, name in enumerate(batch.schema.names):
        col = batch.column(i)
        fixed = _convert_array(col)
        new_columns.append(fixed)
        new_fields.append(pa.field(name, fixed.type))
    return pa.RecordBatch.from_arrays(new_columns, schema=pa.schema(new_fields))


def execute(
    data: Dict[str, pa.Table], config: U256ToBinaryConfig
) -> Dict[str, pa.Table]:
    """Convert Decimal256 columns to binary across the specified tables.

    First applies the Rust ``u256_to_binary`` for top-level columns, then runs
    :func:`_fix_nested` to handle any nested Decimal256 values that the Rust
    function does not reach.

    Args:
        data: A dictionary mapping table names to PyArrow Tables.
        config: A :class:`U256ToBinaryConfig` controlling which tables to
            process.

    Returns:
        A new data dictionary with Decimal256 columns converted to binary.
    """
    data = deepcopy(data)

    table_names = data.keys() if config.tables is None else config.tables

    for table_name in table_names:
        table = data[table_name]
        batches = table.to_batches()
        out_batches = []

        for batch in batches:
            out_batches.append(_fix_nested(u256_to_binary(batch)))

        data[table_name] = pa.Table.from_batches(out_batches)

    return data
