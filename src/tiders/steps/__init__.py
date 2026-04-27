"""Transformation step modules.

Each sub-module exposes an ``execute(data, config)`` function that receives the
full data dictionary and a step-specific config, and returns a transformed data
dictionary.
"""

from . import (
    evm_decode_events,
    cast,
    hex_encode,
    util,
    cast_by_type,
    base58_encode,
    u256_to_binary,
    svm_decode_instructions,
    svm_decode_logs,
    set_chain_id,
    delete_tables,
    delete_columns,
    rename_tables,
    rename_columns,
    select_tables,
    select_columns,
    reorder_columns,
    add_columns,
    copy_columns,
    prefix_columns,
    suffix_columns,
    prefix_tables,
    suffix_tables,
    drop_empty_tables,
    join_block_data,
    join_svm_transaction_data,
    join_evm_transaction_data,
)

_LAZY_MODULES = {"polars_step", "pandas_step", "datafusion_step"}

__all__ = [
    "evm_decode_events",
    "cast",
    "hex_encode",
    "util",
    "cast_by_type",
    "base58_encode",
    "u256_to_binary",
    "svm_decode_instructions",
    "svm_decode_logs",
    "set_chain_id",
    "delete_tables",
    "delete_columns",
    "rename_tables",
    "rename_columns",
    "select_tables",
    "select_columns",
    "reorder_columns",
    "add_columns",
    "copy_columns",
    "prefix_columns",
    "suffix_columns",
    "prefix_tables",
    "suffix_tables",
    "drop_empty_tables",
    "join_block_data",
    "join_svm_transaction_data",
    "join_evm_transaction_data",
]


def __getattr__(name: str):
    if name in _LAZY_MODULES:
        import importlib

        mod = importlib.import_module(f".{name}", __name__)
        globals()[name] = mod
        return mod
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
