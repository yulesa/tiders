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
)

_LAZY_MODULES = {"polars_step", "datafusion_step"}

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
    "polars_step",
    "datafusion_step",
]


def __getattr__(name: str):
    if name in _LAZY_MODULES:
        import importlib

        mod = importlib.import_module(f".{name}", __name__)
        globals()[name] = mod
        return mod
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
