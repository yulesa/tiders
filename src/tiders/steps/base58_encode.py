from typing import Dict
from copy import deepcopy

from ..config import Base58EncodeConfig
from tiders_core import base58_encode
import pyarrow as pa


def execute(
    data: Dict[str, pa.Table], config: Base58EncodeConfig
) -> Dict[str, pa.Table]:
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
