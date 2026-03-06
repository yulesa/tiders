from typing import Dict
from copy import deepcopy

from tiders_core import svm_decode_instructions, instruction_signature_to_arrow_schema
from ..config import SvmDecodeInstructionsConfig
import pyarrow as pa


def execute(
    data: Dict[str, pa.Table], config: SvmDecodeInstructionsConfig
) -> Dict[str, pa.Table]:
    data = deepcopy(data)

    input_table = data[config.input_table]
    input_batches = input_table.to_batches()

    output_batches = []

    for batch in input_batches:
        output_batches.append(
            svm_decode_instructions(
                config.instruction_signature,
                batch,
                config.allow_decode_fail,
                config.filter_by_discriminator,
                config.hstack,
            )
        )

    decoded_schema = instruction_signature_to_arrow_schema(config.instruction_signature)
    if config.hstack:
        schema = pa.schema(list(decoded_schema) + list(input_table.schema))
    else:
        schema = decoded_schema

    output_table = pa.Table.from_batches(output_batches, schema=schema)

    data[config.output_table] = output_table

    return data
