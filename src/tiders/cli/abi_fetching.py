"""ABI fetching from Sourcify and Etherscan APIs.

Functions
---------
- ``fetch_abi_single``       — fetch and save ABI for a single contract address
- ``fetch_abis_from_yaml``   — fetch and save ABIs for all contracts in a YAML file
- ``fetch_abi_sourcify``     — fetch ABI from Sourcify v2 API
- ``fetch_abi_etherscan``    — fetch ABI from Etherscan-compatible API
- ``fetch_abi``              — orchestrate fetching with fallback between
- ``_update_yaml_abi_paths`` — helper to update YAML file with new ABI paths after fetching
- ``resolve_chain_id``       — helper to resolve chain ID from string input (e.g. "ethereum" -> 1)
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from pathlib import Path
from typing import Optional

logger = logging.getLogger("tiders.cli")

_REQUEST_TIMEOUT = 30  # seconds


# ---------------------------------------------------------------------------
# Higher-level fetching for the CLI command
# ---------------------------------------------------------------------------


def fetch_abi_single(
    address: str,
    chain_id: int,
    source: str,
    etherscan_api_key: Optional[str],
    output: Optional[str],
) -> Path:
    """Fetch and save ABI for a single contract address.

    Args:
        address: The contract address (0x-prefixed).
        chain_id: The chain ID.
        source: Primary source — ``"sourcify"`` or ``"etherscan"``.
        etherscan_api_key: API key for Etherscan (may be ``None``).
        output: Optional output file path. Defaults to ``{address}.abi.json`` in cwd.

    Returns:
        The path where the ABI was saved.

    Raises:
        RuntimeError: If fetching fails from both sources.
    """
    abi_json = fetch_abi(chain_id, address, source, etherscan_api_key)

    out_path = (
        Path(output) if output is not None else Path.cwd() / f"{address}.abi.json"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(abi_json, encoding="utf-8")
    return out_path


def fetch_abis_from_yaml(
    yaml_path: Path,
    cli_chain_id: int,
    source: str,
    etherscan_api_key: Optional[str],
    output: Optional[str],
    env_file: Optional[str],
) -> list[tuple[str, Path]]:
    """Fetch and save ABIs for all contracts declared in a YAML file.

    Iterates through contracts; skips those that already have an ABI file.

    Args:
        yaml_path: Resolved path to the YAML file.
        cli_chain_id: Default chain ID from CLI (used when contract has no chain_id set).
        source: Primary source — ``"sourcify"`` or ``"etherscan"``.
        etherscan_api_key: API key for Etherscan (may be ``None``).
        output: Optional output directory path. Defaults to the YAML directory.
        env_file: Optional path to a .env file override.

    Returns:
        List of ``(contract_name, saved_path)`` tuples for successfully fetched ABIs.

    Raises:
        ValueError: If the YAML has no ``contracts`` section or has config errors.
    """
    from .env import load_and_substitute
    from .tiders_yaml_parser import YamlConfigError, parse_contracts

    # Inline YAML loading to avoid circular imports with main.py helpers
    try:
        import yaml
    except ImportError:
        raise ValueError("PyYAML is required for CLI mode: pip install tiders[cli]")

    with open(yaml_path) as f:
        raw_yaml = yaml.safe_load(f)
    if not isinstance(raw_yaml, dict):
        raise ValueError(
            f"{yaml_path}: YAML root must be a mapping, got {type(raw_yaml).__name__}"
        )

    yaml_dir = yaml_path.parent

    # Apply env-file override if provided
    if env_file is not None:
        raw_yaml["environment_path"] = str(Path(env_file).resolve())

    # Substitute env vars (addresses may use ${VAR} patterns)
    raw_yaml = load_and_substitute(yaml_path, raw_yaml)

    if "contracts" not in raw_yaml:
        raise ValueError(f"No 'contracts' section found in {yaml_path}.")

    try:
        contracts = parse_contracts(raw_yaml["contracts"], yaml_dir)
    except YamlConfigError as exc:
        raise ValueError(f"Config error: {exc}") from exc

    # Determine output directory
    out_dir = Path(output) if output is not None else yaml_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    saved: list[tuple[str, Path]] = []

    for contract in contracts.values():
        if not contract.address:
            logger.warning(
                f"Contract '{contract.name}' has no address, skipping ABI fetch"
            )
            continue

        # Skip if ABI already exists via abi key
        if contract.abi_path is not None and Path(contract.abi_path).is_file():
            logger.info(
                f"Contract '{contract.name}' already has ABI at {contract.abi_path}, skipping"
            )
            continue

        expected_path = out_dir / f"{contract.name}.abi.json"
        if expected_path.is_file():
            logger.info(
                f"ABI already exists at {expected_path}, skipping '{contract.name}'"
            )
            continue

        # Determine chain_id: use contract's chain_id if set, else CLI default
        chain_id = contract.chain_id
        if chain_id is None:
            logger.warning(
                f"Contract '{contract.name}': chain_id not specified, "
                f"defaulting to chain_id={cli_chain_id}"
            )
            chain_id = cli_chain_id

        try:
            abi_json = fetch_abi(chain_id, contract.address, source, etherscan_api_key)
        except RuntimeError as exc:
            logger.error(f"Failed to fetch ABI for '{contract.name}': {exc}")
            continue

        expected_path.write_text(abi_json, encoding="utf-8")
        saved.append((contract.name, expected_path))

    # Update the YAML file with abi paths for newly fetched ABIs
    if saved:
        _update_yaml_abi_paths(yaml_path, yaml_dir, saved)

    return saved


def fetch_abi(
    chain_id: int,
    address: str,
    source: str = "sourcify",
    etherscan_api_key: Optional[str] = None,
) -> str:
    """Fetch a contract ABI, trying ``source`` first then falling back.

    Args:
        chain_id: The chain ID.
        address: The contract address (0x-prefixed).
        source: Primary source — ``"sourcify"`` or ``"etherscan"``.
        etherscan_api_key: API key for Etherscan (required for Etherscan calls).

    Returns:
        The ABI as a pretty-printed JSON string.

    Raises:
        RuntimeError: If both sources fail.
    """

    def _fetch_sourcify():
        return fetch_abi_sourcify(chain_id, address)

    def _fetch_etherscan():
        assert etherscan_api_key is not None
        return fetch_abi_etherscan(chain_id, address, etherscan_api_key)

    def _warn_no_etherscan_key():
        logger.warning(
            "ETHERSCAN_API_KEY not set in environment variables, skipping Etherscan"
        )
        return None

    if source == "sourcify":
        primary_name, fallback_name = "Sourcify", "Etherscan"
        primary_fn = _fetch_sourcify
        if etherscan_api_key:
            fallback_fn = _fetch_etherscan
        else:
            fallback_fn = _warn_no_etherscan_key
    else:
        primary_name, fallback_name = "Etherscan", "Sourcify"
        if etherscan_api_key:
            primary_fn = _fetch_etherscan
        else:
            primary_fn = _warn_no_etherscan_key
        fallback_fn = _fetch_sourcify

    result = primary_fn()
    if result is not None:
        logger.info(
            f"Successfully fetched ABI for {address} on chain {chain_id} from {primary_name}"
        )
        return result

    logger.warning(
        f"{primary_name} failed for {address} on chain {chain_id}, "
        f"trying {fallback_name}"
    )

    result = fallback_fn()
    if result is not None:
        logger.info(
            f"Successfully fetched ABI for {address} on chain {chain_id} from {fallback_name}"
        )
        return result

    raise RuntimeError(
        f"Failed to fetch ABI for {address} on chain {chain_id}: "
        f"both {primary_name} and {fallback_name} failed"
    )


def fetch_abi_sourcify(chain_id: int, address: str) -> str | None:
    """Fetch a contract ABI from the Sourcify v2 API.

    Args:
        chain_id: The chain ID (e.g. 1 for Ethereum mainnet).
        address: The contract address (0x-prefixed).

    Returns:
        The ABI as a JSON string, or ``None`` if the fetch failed.
    """
    url = f"https://sourcify.dev/server/v2/contract/{chain_id}/{address}?fields=abi"
    try:
        with urllib.request.urlopen(url, timeout=_REQUEST_TIMEOUT) as resp:
            data = json.loads(resp.read().decode())
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError) as e:
        logger.warning(f"Sourcify fetch failed for {address} on chain {chain_id}: {e}")
        return None
    except Exception as e:
        logger.warning(f"Sourcify fetch failed for {address} on chain {chain_id}: {e}")
        return None

    # Sourcify v2 returns the ABI at the top-level "abi" key
    abi = data.get("abi")
    if abi is None:
        logger.warning(
            f"Sourcify response for {address} on chain {chain_id} has no 'abi' key"
        )
        return None

    if not isinstance(abi, list):
        logger.warning(f"Sourcify ABI for {address} on chain {chain_id} is not a list")
        return None

    return json.dumps(abi, indent=2)


_ETHERSCAN_V2_BASE = "https://api.etherscan.io/v2/api"


def fetch_abi_etherscan(chain_id: int, address: str, api_key: str) -> str | None:
    """Fetch a contract ABI from the Etherscan v2 API.

    Uses the unified v2 endpoint which supports all chains via the
    ``chainid`` query parameter.

    Args:
        chain_id: The chain ID.
        address: The contract address (0x-prefixed).
        api_key: The Etherscan API key.

    Returns:
        The ABI as a JSON string, or ``None`` if the fetch failed.
    """
    url = (
        f"{_ETHERSCAN_V2_BASE}"
        f"?chainid={chain_id}"
        f"&module=contract&action=getabi"
        f"&address={address}&apikey={api_key}"
    )
    try:
        with urllib.request.urlopen(url, timeout=_REQUEST_TIMEOUT) as resp:
            data = json.loads(resp.read().decode())
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError) as e:
        logger.warning(f"Etherscan fetch failed for {address} on chain {chain_id}: {e}")
        return None
    except Exception as e:
        logger.warning(f"Etherscan fetch failed for {address} on chain {chain_id}: {e}")
        return None

    if data.get("status") != "1" or "result" not in data:
        msg = data.get("result", data.get("message", "unknown error"))
        logger.warning(f"Etherscan API error for {address} on chain {chain_id}: {msg}")
        return None

    # Etherscan returns the ABI as a JSON string in "result"
    abi_str = data["result"]

    # Validate it parses as a list
    try:
        abi = json.loads(abi_str)
        if not isinstance(abi, list):
            logger.warning(
                f"Etherscan ABI for {address} on chain {chain_id} is not a list"
            )
            return None
    except json.JSONDecodeError:
        logger.warning(
            f"Etherscan ABI for {address} on chain {chain_id} is not valid JSON"
        )
        return None

    return json.dumps(abi, indent=2)


def _update_yaml_abi_paths(
    yaml_path: Path,
    yaml_dir: Path,
    saved: list[tuple[str, Path]],
) -> None:
    """Update the YAML config file, adding the abi path for each fetched contract.

    Uses plain text manipulation to preserve formatting and comments.
    For each saved contract, finds its ``- name: X`` block and inserts
    an ``abi:`` line after the ``address:`` line.
    """
    import re

    yaml_text = yaml_path.read_text(encoding="utf-8")

    for name, abi_path in saved:
        # Compute path relative to yaml dir for portability
        try:
            rel_path = "./" + str(abi_path.relative_to(yaml_dir))
        except ValueError:
            rel_path = str(abi_path)

        # Match: "- name: <name>" followed by indented keys, capturing
        # the indent and address line so we can insert abi: after it.
        pattern = re.compile(
            rf"(- name:\s*{re.escape(name)}\s*\n"
            rf"(?:[ \t]+\w+:.*\n)*?)"
            rf"([ \t]+)(address:.*\n)",
            re.MULTILINE,
        )

        match = pattern.search(yaml_text)
        if not match:
            logger.warning(
                f"Could not locate contract '{name}' in {yaml_path} to update abi path"
            )
            continue

        indent = match.group(2)
        insert_pos = match.end(3)
        yaml_text = (
            yaml_text[:insert_pos]
            + f"{indent}abi: {rel_path}\n"
            + yaml_text[insert_pos:]
        )

    yaml_path.write_text(yaml_text, encoding="utf-8")


# Chain name to chain ID mapping
CHAIN_NAME_TO_ID: dict[str, int] = {
    "mainnet": 1,
    "ethereum": 1,
    "ethereum-mainnet": 1,
    "bnb": 56,
    "base": 8453,
    "arbitrum": 42161,
    "polygon": 137,
    "scroll": 534352,
    "unichain": 130,
}


def resolve_chain_id(value: str | int) -> int:
    """Resolve a chain ID from a name or numeric value.

    Accepts an integer, a numeric string (e.g. ``"1"``), or a chain name
    (e.g. ``"ethereum"``).

    Raises:
        ValueError: If the value is not a known chain name or valid integer.
    """
    if isinstance(value, int):
        return value
    value_lower = str(value).strip().lower()
    if value_lower in CHAIN_NAME_TO_ID:
        return CHAIN_NAME_TO_ID[value_lower]
    try:
        return int(value_lower)
    except ValueError:
        valid = sorted(CHAIN_NAME_TO_ID.keys())
        raise ValueError(
            f"Unknown chain '{value}'. Use a chain ID number or one of: {valid}"
        )
