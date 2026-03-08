"""Arrow schema and table utility functions used by transformation steps."""

import pyarrow as pa
from copy import deepcopy


def _replace_type(
    dt: pa.DataType, from_type: pa.DataType, to_type: pa.DataType
) -> pa.DataType:
    """Recursively replace occurrences of ``from_type`` with ``to_type`` in a data type.

    Traverses struct, list, and large-list types to find and replace all
    occurrences of ``from_type``.

    Args:
        dt: The Arrow data type to inspect.
        from_type: The data type to look for.
        to_type: The replacement data type.

    Returns:
        The (possibly transformed) data type.
    """
    if dt == from_type:
        return to_type
    if pa.types.is_struct(dt):
        new_fields = [
            pa.field(
                dt.field(i).name, _replace_type(dt.field(i).type, from_type, to_type)
            )
            for i in range(dt.num_fields)
        ]
        return pa.struct(new_fields)
    if pa.types.is_list(dt):
        value_field = dt.value_field
        new_value_type = _replace_type(value_field.type, from_type, to_type)
        return pa.list_(pa.field(value_field.name, new_value_type))
    if pa.types.is_large_list(dt):
        value_field = dt.value_field
        new_value_type = _replace_type(value_field.type, from_type, to_type)
        return pa.large_list(pa.field(value_field.name, new_value_type))
    return dt


def arrow_schema_cast_by_type(
    schema: pa.Schema, from_type: pa.DataType, to_type: pa.DataType
) -> pa.Schema:
    """Return a copy of ``schema`` with every ``from_type`` field replaced by ``to_type``.

    Args:
        schema: The source Arrow schema.
        from_type: The data type to match.
        to_type: The replacement data type.

    Returns:
        A new schema with the substitutions applied.
    """
    schema = deepcopy(schema)

    for i, name in enumerate(schema.names):
        dt = _replace_type(schema.field(i).type, from_type, to_type)
        schema = schema.set(i, pa.field(name, dt))

    return schema


def arrow_schema_binary_to_string(schema: pa.Schema) -> pa.Schema:
    """Convert all binary and large-binary fields in a schema to string types.

    This is a convenience wrapper around :func:`arrow_schema_cast_by_type` that
    maps ``binary -> string`` and ``large_binary -> large_string``.

    Args:
        schema: The source Arrow schema.

    Returns:
        A new schema with binary fields converted to string fields.
    """
    return arrow_schema_cast_by_type(
        arrow_schema_cast_by_type(schema, pa.binary(), pa.string()),
        pa.large_binary(),
        pa.large_string(),
    )


def arrow_table_to_batch(table: pa.Table) -> pa.RecordBatch:
    """Collapse a multi-chunk PyArrow Table into a single RecordBatch.

    Each column's chunks are combined via ``combine_chunks()`` before
    constructing the batch.

    Args:
        table: A PyArrow Table (potentially with multiple chunks per column).

    Returns:
        A single ``RecordBatch`` containing all rows.
    """
    arrays = []
    for col in table.columns:
        arrays.append(col.combine_chunks())

    return pa.RecordBatch.from_arrays(arrays, names=table.column_names)
