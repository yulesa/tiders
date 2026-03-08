"""Base58-encoding step for converting binary columns to Base58 strings."""

from typing import Dict
from copy import deepcopy

from ..config import Base58EncodeConfig
from tiders_core import base58_encode
import pyarrow as pa


def execute(
    data: Dict[str, pa.Table], config: Base58EncodeConfig
) -> Dict[str, pa.Table]:
    """Encode binary columns in the specified tables as Base58 strings.

    Iterates over the selected tables (or all tables when
    ``config.tables`` is ``None``) and converts each record batch's binary
    columns to their Base58 representation using the Rust-backed
    ``tiders_core.base58_encode`` function.

    Args:
        data: A dictionary mapping table names to PyArrow Tables.
        config: A :class:`Base58EncodeConfig` controlling which tables to process.

    Returns:
        A new data dictionary with Base58-encoded columns.
    """
    data = deepcopy(data)

    table_names = data.keys() if config.tables is None else config.tables

    for table_name in table_names:
        table = data[table_name]
        batches = table.to_batches()
        out_batches = []

        for batch in batches:
            out_batches.append(base58_encode(batch))

        data[table_name] = pa.Table.from_batches(out_batches)

    return data
