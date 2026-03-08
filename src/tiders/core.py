"""Re-exports all symbols from the ``tiders_core`` Rust extension module.

This module provides direct access to the low-level Rust-backed functions
(e.g. ``evm_decode_events``, ``hex_encode``, ``cast``, ``base58_encode``,
``u256_to_binary``, ``svm_decode_instructions``, etc.) without requiring
users to import ``tiders_core`` directly.
"""

from tiders_core import *  # noqa: F403  # pyright: ignore[reportWildcardImportFromLibrary]
