import pyarrow as pa
from copy import deepcopy


def _replace_type(dt: pa.DataType, from_type: pa.DataType, to_type: pa.DataType) -> pa.DataType:
    if dt == from_type:
        return to_type
    if pa.types.is_struct(dt):
        new_fields = [
            pa.field(dt.field(i).name, _replace_type(dt.field(i).type, from_type, to_type))
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
    schema = deepcopy(schema)

    for i, name in enumerate(schema.names):
        dt = _replace_type(schema.field(i).type, from_type, to_type)
        schema = schema.set(i, pa.field(name, dt))

    return schema


def arrow_schema_binary_to_string(schema: pa.Schema):
    return arrow_schema_cast_by_type(
        arrow_schema_cast_by_type(schema, pa.binary(), pa.string()),
        pa.large_binary(),
        pa.large_string(),
    )


def arrow_table_to_batch(table: pa.Table) -> pa.RecordBatch:
    arrays = []
    for col in table.columns:
        arrays.append(col.combine_chunks())

    return pa.RecordBatch.from_arrays(arrays, names=table.column_names)
