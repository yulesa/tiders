"""EVM event log decoding step."""

from typing import Dict
from copy import deepcopy

from tiders_core import evm_decode_events, evm_event_signature_to_arrow_schema
from ..config import EvmDecodeEventsConfig
import pyarrow as pa


def execute(
    data: Dict[str, pa.Table], config: EvmDecodeEventsConfig
) -> Dict[str, pa.Table]:
    """Decode raw EVM log entries into structured columns.

    Reads from ``config.input_table``, decodes each batch using the provided
    event signature, and writes the result to ``config.output_table``. When
    ``config.hstack`` is ``True``, the decoded columns are horizontally stacked
    with the original log columns.

    Args:
        data: A dictionary mapping table names to PyArrow Tables.
        config: An :class:`EvmDecodeEventsConfig` with the event signature and
            decoding options.

    Returns:
        A new data dictionary containing the decoded output table.
    """
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

    decoded_schema = evm_event_signature_to_arrow_schema(config.event_signature)
    if config.hstack:
        schema = pa.schema(list(decoded_schema) + list(input_table.schema))
    else:
        schema = decoded_schema

    output_table = pa.Table.from_batches(output_batches, schema=schema)

    data[config.output_table] = output_table

    return data
