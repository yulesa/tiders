"""Utility helpers for working with Solana/SVM blockchain data."""

from hashlib import sha256


def svm_anchor_discriminator(name: str, namespace: str = "global") -> bytes:
    """Compute the 8-byte Anchor discriminator for an instruction or account.

    The discriminator is the first 8 bytes of ``SHA-256("<namespace>:<name>")``.
    Anchor programs use this value to identify which instruction handler should
    process a given transaction instruction.

    See: https://github.com/solana-foundation/anchor/blob/master/lang/syn/src/codegen/program/common.rs

    Args:
        name: The instruction or account name (e.g. ``"initialize"``).
        namespace: The namespace prefix. Defaults to ``"global"`` which is the
            standard Anchor namespace for instructions.

    Returns:
        An 8-byte ``bytes`` object containing the discriminator.

    Example::

        disc = svm_anchor_discriminator("initialize")
        # Use disc to filter or match Anchor instructions.
    """
    preimage = f"{namespace}:{name}".encode("utf-8")
    hash_bytes = sha256(preimage).digest()[:8]

    return hash_bytes


__all__ = ["svm_anchor_discriminator"]
