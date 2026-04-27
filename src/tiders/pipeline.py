"""Pipeline execution engine.

This module implements the core runtime loop: it streams data from a blockchain
provider, applies table aliases, runs transformation steps, and pushes results
to a configured writer backend.
"""

import asyncio
import logging
from dataclasses import asdict
from enum import Enum

from .config import (
    Base58EncodeConfig,
    CastByTypeConfig,
    CastConfig,
    PolarsStepConfig,
    PandasStepConfig,
    DataFusionStepConfig,
    EvmDecodeEventsConfig,
    HexEncodeConfig,
    Pipeline,
    EvmTableAliases,
    SvmTableAliases,
    SetChainIdConfig,
    DeleteTablesConfig,
    DeleteColumnsConfig,
    RenameTablesConfig,
    RenameColumnsConfig,
    SelectTablesConfig,
    SelectColumnsConfig,
    ReorderColumnsConfig,
    AddColumnsConfig,
    CopyColumnsConfig,
    PrefixColumnsConfig,
    SuffixColumnsConfig,
    PrefixTablesConfig,
    SuffixTablesConfig,
    DropEmptyTablesConfig,
    JoinBlockDataConfig,
    JoinSvmTransactionDataConfig,
    JoinEvmTransactionDataConfig,
    Step,
    StepKind,
    U256ToBinaryConfig,
    SvmDecodeInstructionsConfig,
    SvmDecodeLogsConfig,
)
from typing import Dict, List, Optional
from tiders_core.ingest import start_stream
from tiders_core.ingest import QueryKind
import pyarrow as pa
from .writers.writer import create_writer
from . import steps as step_def
from copy import deepcopy

logger = logging.getLogger(__name__)


def _apply_table_aliases(
    query_kind: QueryKind | str,
    data: Dict[str, pa.RecordBatch],
    aliases: Optional[EvmTableAliases | SvmTableAliases],
) -> Dict[str, pa.RecordBatch]:
    """Rename tables from their default ingested names to user-defined aliases.

    Args:
        query_kind: The kind of blockchain query (EVM or SVM), used to select
            the correct alias mapping.
        data: A dictionary mapping table names to PyArrow RecordBatches.
        aliases: An :class:`EvmTableAliases` or :class:`SvmTableAliases`
            instance, or ``None`` to skip aliasing.

    Returns:
        A new dictionary with renamed keys. Tables without an alias keep their
        original name.

    Raises:
        ValueError: If two source tables resolve to the same alias.
    """
    if aliases is None:
        return data

    kind = query_kind.value if isinstance(query_kind, Enum) else query_kind
    if kind == QueryKind.EVM.value and isinstance(aliases, EvmTableAliases):
        alias_map = asdict(aliases)
    elif kind == QueryKind.SVM.value and isinstance(aliases, SvmTableAliases):
        alias_map = asdict(aliases)
    else:
        return data

    out: Dict[str, pa.RecordBatch] = {}
    for table_name, table_batch in data.items():
        resolved_name = alias_map.get(table_name) or table_name
        if resolved_name in out:
            raise ValueError(
                f"table alias collision: multiple source tables resolved to '{resolved_name}'"
            )
        out[resolved_name] = table_batch

    return out


def process_steps(
    data: Dict[str, pa.Table],
    steps: List[Step],
) -> Dict[str, pa.Table]:
    """Apply an ordered list of transformation steps to the data tables.

    Each step receives the full data dictionary and returns a (possibly
    modified) copy. Steps are executed sequentially in the order they appear in
    ``steps``.

    Args:
        data: A dictionary mapping table names to PyArrow Tables.
        steps: The ordered list of :class:`Step` objects to execute.

    Returns:
        The transformed data dictionary after all steps have run.

    Raises:
        Exception: If a step has an unknown :attr:`StepKind`.
    """
    logger.debug("Processing pipeline steps")

    data = deepcopy(data)

    for step in steps:
        logger.debug(f"running step kind: {step.kind} name: {step.name}")

        if step.kind == StepKind.EVM_DECODE_EVENTS:
            assert isinstance(step.config, EvmDecodeEventsConfig)
            data = step_def.evm_decode_events.execute(data, step.config)
        elif step.kind == StepKind.SVM_DECODE_INSTRUCTIONS:
            assert isinstance(step.config, SvmDecodeInstructionsConfig)
            data = step_def.svm_decode_instructions.execute(data, step.config)
        elif step.kind == StepKind.SVM_DECODE_LOGS:
            assert isinstance(step.config, SvmDecodeLogsConfig)
            data = step_def.svm_decode_logs.execute(data, step.config)
        elif step.kind == StepKind.CAST:
            assert isinstance(step.config, CastConfig)
            data = step_def.cast.execute(data, step.config)
        elif step.kind == StepKind.HEX_ENCODE:
            assert isinstance(step.config, HexEncodeConfig)
            data = step_def.hex_encode.execute(data, step.config)
        elif step.kind == StepKind.U256_TO_BINARY:
            assert isinstance(step.config, U256ToBinaryConfig)
            data = step_def.u256_to_binary.execute(data, step.config)
        elif step.kind == StepKind.BASE58_ENCODE:
            assert isinstance(step.config, Base58EncodeConfig)
            data = step_def.base58_encode.execute(data, step.config)
        elif step.kind == StepKind.CAST_BY_TYPE:
            assert isinstance(step.config, CastByTypeConfig)
            data = step_def.cast_by_type.execute(data, step.config)
        elif step.kind == StepKind.DATAFUSION:
            from .steps import datafusion_step

            assert isinstance(step.config, DataFusionStepConfig)
            data = datafusion_step.execute(data, step.config)
        elif step.kind == StepKind.POLARS:
            from .steps import polars_step

            assert isinstance(step.config, PolarsStepConfig)
            data = polars_step.execute(data, step.config)
        elif step.kind == StepKind.PANDAS:
            from .steps import pandas_step

            assert isinstance(step.config, PandasStepConfig)
            data = pandas_step.execute(data, step.config)
        elif step.kind == StepKind.SET_CHAIN_ID:
            assert isinstance(step.config, SetChainIdConfig)
            data = step_def.set_chain_id.execute(data, step.config)
        elif step.kind == StepKind.DELETE_TABLES:
            assert isinstance(step.config, DeleteTablesConfig)
            data = step_def.delete_tables.execute(data, step.config)
        elif step.kind == StepKind.DELETE_COLUMNS:
            assert isinstance(step.config, DeleteColumnsConfig)
            data = step_def.delete_columns.execute(data, step.config)
        elif step.kind == StepKind.RENAME_TABLES:
            assert isinstance(step.config, RenameTablesConfig)
            data = step_def.rename_tables.execute(data, step.config)
        elif step.kind == StepKind.RENAME_COLUMNS:
            assert isinstance(step.config, RenameColumnsConfig)
            data = step_def.rename_columns.execute(data, step.config)
        elif step.kind == StepKind.SELECT_TABLES:
            assert isinstance(step.config, SelectTablesConfig)
            data = step_def.select_tables.execute(data, step.config)
        elif step.kind == StepKind.SELECT_COLUMNS:
            assert isinstance(step.config, SelectColumnsConfig)
            data = step_def.select_columns.execute(data, step.config)
        elif step.kind == StepKind.REORDER_COLUMNS:
            assert isinstance(step.config, ReorderColumnsConfig)
            data = step_def.reorder_columns.execute(data, step.config)
        elif step.kind == StepKind.ADD_COLUMNS:
            assert isinstance(step.config, AddColumnsConfig)
            data = step_def.add_columns.execute(data, step.config)
        elif step.kind == StepKind.COPY_COLUMNS:
            assert isinstance(step.config, CopyColumnsConfig)
            data = step_def.copy_columns.execute(data, step.config)
        elif step.kind == StepKind.PREFIX_COLUMNS:
            assert isinstance(step.config, PrefixColumnsConfig)
            data = step_def.prefix_columns.execute(data, step.config)
        elif step.kind == StepKind.SUFFIX_COLUMNS:
            assert isinstance(step.config, SuffixColumnsConfig)
            data = step_def.suffix_columns.execute(data, step.config)
        elif step.kind == StepKind.PREFIX_TABLES:
            assert isinstance(step.config, PrefixTablesConfig)
            data = step_def.prefix_tables.execute(data, step.config)
        elif step.kind == StepKind.SUFFIX_TABLES:
            assert isinstance(step.config, SuffixTablesConfig)
            data = step_def.suffix_tables.execute(data, step.config)
        elif step.kind == StepKind.DROP_EMPTY_TABLES:
            assert isinstance(step.config, DropEmptyTablesConfig)
            data = step_def.drop_empty_tables.execute(data, step.config)
        elif step.kind == StepKind.JOIN_BLOCK_DATA:
            assert isinstance(step.config, JoinBlockDataConfig)
            data = step_def.join_block_data.execute(data, step.config)
        elif step.kind == StepKind.JOIN_SVM_TRANSACTION_DATA:
            assert isinstance(step.config, JoinSvmTransactionDataConfig)
            data = step_def.join_svm_transaction_data.execute(data, step.config)
        elif step.kind == StepKind.JOIN_EVM_TRANSACTION_DATA:
            assert isinstance(step.config, JoinEvmTransactionDataConfig)
            data = step_def.join_evm_transaction_data.execute(data, step.config)
        else:
            raise Exception(f"Unknown step kind: {step.kind}")

    return data


def merge_data(data: list[Dict[str, pa.Table]]) -> Dict[str, pa.Table]:
    """Concatenate multiple data dictionaries into a single dictionary.

    All dictionaries must contain exactly the same set of table names. Tables
    with the same name are vertically concatenated using
    ``pyarrow.concat_tables``.

    Args:
        data: A list of data dictionaries (each mapping table names to PyArrow
            Tables).

    Returns:
        A single merged dictionary with one concatenated table per name.

    Raises:
        AssertionError: If the dictionaries do not share the same set of keys.
    """
    keys = list(data[0].keys())
    keys.sort()

    for i in range(1, len(data)):
        other_keys = list(data[i].keys())
        other_keys.sort()

        assert keys == other_keys

    out = {}

    for key in keys:
        tables = []
        for d in data:
            tables.append(d[key])

        out[key] = pa.concat_tables(tables)

    return out


async def run_pipeline(pipeline: Pipeline, pipeline_name: Optional[str] = None):
    """Execute a full pipeline: ingest, transform, and write data.

    This coroutine opens a streaming connection to the configured blockchain
    data provider, and for each batch of data:

    1. Applies table aliases (if configured).
    2. Converts record batches to tables.
    3. Runs all transformation steps (in a background thread).
    4. Pushes the processed tables to the configured writer.

    The loop runs until the stream is exhausted (``stream.next()`` returns
    ``None``).

    Args:
        pipeline: The fully configured :class:`Pipeline` object.
        pipeline_name: An optional name used in log messages for identification.
    """
    logger.info(f"Running pipeline: {pipeline_name}")
    logger.debug(f"Pipeline config: {pipeline}")

    stream = start_stream(pipeline.provider, pipeline.query)

    writers = (
        [create_writer(w) for w in pipeline.writer]
        if isinstance(pipeline.writer, list)
        else [create_writer(pipeline.writer)]
    )

    while True:
        data = await stream.next()
        if data is None:
            break

        logger.debug("Received data from ingest")

        data = _apply_table_aliases(
            pipeline.query.kind,
            data,
            pipeline.table_aliases,
        )

        tables = {}

        for table_name, table_batch in data.items():
            tables[table_name] = pa.Table.from_batches([table_batch])

        processed = await asyncio.to_thread(process_steps, tables, pipeline.steps)

        logger.debug("Pushing data to writer")

        await asyncio.gather(*[w.push_data(processed) for w in writers])


__all__ = ["run_pipeline"]
