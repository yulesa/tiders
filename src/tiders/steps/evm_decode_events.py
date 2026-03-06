from typing import Dict
from copy import deepcopy

from tiders_core import evm_decode_events, evm_event_signature_to_arrow_schema
from ..config import EvmDecodeEventsConfig
import pyarrow as pa


def execute(
    data: Dict[str, pa.Table], config: EvmDecodeEventsConfig
) -> Dict[str, pa.Table]:
    data = deepcopy(data)

    input_table = data[config.input_table]
    input_batches = input_table.to_batches()

    output_batches = []

    for batch in input_batches:
        output_batches.append(
            evm_decode_events(
                config.event_signature,
                batch,
                config.allow_decode_fail,
                config.filter_by_topic0,
                config.hstack,
            )
        )

    # When hstack is enabled, the output schema includes both decoded and
    # original input columns, so we derive it from the batches themselves.
    if config.hstack and output_batches:
        schema = output_batches[0].schema
    else:
        schema = evm_event_signature_to_arrow_schema(config.event_signature)

    output_table = pa.Table.from_batches(output_batches, schema=schema)

    data[config.output_table] = output_table

    return data
