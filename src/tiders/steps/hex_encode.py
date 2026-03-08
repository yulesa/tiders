"""Hex-encoding step for converting binary columns to hex strings."""

from typing import Dict
from copy import deepcopy

from ..config import HexEncodeConfig
from tiders_core import hex_encode, prefix_hex_encode
import pyarrow as pa


def execute(data: Dict[str, pa.Table], config: HexEncodeConfig) -> Dict[str, pa.Table]:
    """Encode binary columns in the specified tables as hexadecimal strings.

    Uses ``prefix_hex_encode`` (``0x``-prefixed) or ``hex_encode`` (raw)
    depending on ``config.prefixed``. When a table has no batches, an empty
    table with the correctly encoded schema is produced.

    Args:
        data: A dictionary mapping table names to PyArrow Tables.
        config: A :class:`HexEncodeConfig` controlling which tables to process
            and whether to ``0x``-prefix the output.

    Returns:
        A new data dictionary with hex-encoded columns.
    """
    data = deepcopy(data)

    decode_fn = prefix_hex_encode if config.prefixed else hex_encode

    table_names = data.keys() if config.tables is None else config.tables

    for table_name in table_names:
        table = data[table_name]
        batches = table.to_batches()
        out_batches = []

        for batch in batches:
            out_batches.append(decode_fn(batch))

        if not out_batches:
            # Empty table: build an empty batch from the input schema so we can
            # derive the encoded schema and create an empty output table safely.
            empty_batch = pa.RecordBatch.from_arrays(
                [pa.array([], type=field.type) for field in table.schema],
                schema=table.schema,
            )
            encoded_empty = decode_fn(empty_batch)
            data[table_name] = pa.Table.from_batches([], schema=encoded_empty.schema)
            continue

        data[table_name] = pa.Table.from_batches(out_batches)

    return data
