"""ABI fetching from Sourcify and Etherscan APIs.

Functions
---------
- ``fetch_abi_single``    — fetch and save ABI for a single contract address
- ``fetch_abis_from_yaml`` — fetch and save ABIs for all contracts in a YAML file
- ``fetch_abi_sourcify``  — fetch ABI from Sourcify v2 API
- ``fetch_abi_etherscan`` — fetch ABI from Etherscan-compatible API
- ``fetch_abi``           — orchestrate fetching with fallback between sources
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger("tiders.cli")

_REQUEST_TIMEOUT = 30  # 


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

    out_path = Path(output) if output is not None else Path.cwd() / f"{address}.abi.json"
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
        cli_chain_id: Default chain ID from CLI (used when contract has no network).
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

        # Determine chain_id: use contract's network if set, else CLI default
        chain_id = contract.network
        if chain_id is None:
            logger.warning(
                f"Contract '{contract.name}': network not specified, "
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

    return saved




def fetch_abi_sourcify(chain_id: int, address: str) -> str | None:
    """Fetch a contract ABI from the Sourcify v2 API.

    Args:
        chain_id: The chain ID (e.g. 1 for Ethereum mainnet).
        address: The contract address (0x-prefixed).

    Returns:
        The ABI as a JSON string, or ``None`` if the fetch failed.
    """
    url = f"https://sourcify.dev/server/v2/contract/{chain_id}/{address}"
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
        logger.warning(
            f"Sourcify ABI for {address} on chain {chain_id} is not a list"
        )
        return None

    return json.dumps(abi, indent=2)


# Etherscan-compatible API base URLs by chain ID
_ETHERSCAN_API_URLS: dict[int, str] = {
    1: "https://api.etherscan.io",
    5: "https://api-goerli.etherscan.io",
    11155111: "https://api-sepolia.etherscan.io",
    10: "https://api-optimistic.etherscan.io",
    42161: "https://api.arbiscan.io",
    137: "https://api.polygonscan.com",
    8453: "https://api.basescan.org",
    56: "https://api.bscscan.com",
    43114: "https://api.snowtrace.io",
    250: "https://api.ftmscan.com",
}

def fetch_abi_etherscan(
    chain_id: int, address: str, api_key: str
) -> str | None:
    """Fetch a contract ABI from an Etherscan-compatible API.

    Args:
        chain_id: The chain ID.
        address: The contract address (0x-prefixed).
        api_key: The Etherscan API key.

    Returns:
        The ABI as a JSON string, or ``None`` if the fetch failed.
    """
    base_url = _ETHERSCAN_API_URLS.get(chain_id)
    if base_url is None:
        logger.warning(
            f"No known Etherscan API URL for chain_id={chain_id}, skipping Etherscan"
        )
        return None

    url = (
        f"{base_url}/api"
        f"?module=contract&action=getabi"
        f"&address={address}&apikey={api_key}"
    )
    try:
        with urllib.request.urlopen(url, timeout=_REQUEST_TIMEOUT) as resp:
            data = json.loads(resp.read().decode())
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError) as e:
        logger.warning(
            f"Etherscan fetch failed for {address} on chain {chain_id}: {e}"
        )
        return None
    except Exception as e:
        logger.warning(
            f"Etherscan fetch failed for {address} on chain {chain_id}: {e}"
        )
        return None

    if data.get("status") != "1" or "result" not in data:
        msg = data.get("result", data.get("message", "unknown error"))
        logger.warning(
            f"Etherscan API error for {address} on chain {chain_id}: {msg}"
        )
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
    if source == "sourcify":
        primary_name, fallback_name = "Sourcify", "Etherscan"
        primary_fn = lambda: fetch_abi_sourcify(chain_id, address)
        fallback_fn = lambda: (
            fetch_abi_etherscan(chain_id, address, etherscan_api_key)
            if etherscan_api_key
            else _skip_etherscan_no_key()
        )
    else:
        primary_name, fallback_name = "Etherscan", "Sourcify"
        if not etherscan_api_key:
            logger.warning(
                "ETHERSCAN_API_KEY not set, falling back to Sourcify"
            )
            result = fetch_abi_sourcify(chain_id, address)
            if result is not None:
                return result
            raise RuntimeError(
                f"Failed to fetch ABI for {address} on chain {chain_id}: "
                f"ETHERSCAN_API_KEY not set and Sourcify failed"
            )
        primary_fn = lambda: fetch_abi_etherscan(chain_id, address, etherscan_api_key)
        fallback_fn = lambda: fetch_abi_sourcify(chain_id, address)

    result = primary_fn()
    if result is not None:
        return result

    logger.warning(
        f"{primary_name} failed for {address} on chain {chain_id}, "
        f"trying {fallback_name}"
    )

    result = fallback_fn()
    if result is not None:
        return result

    raise RuntimeError(
        f"Failed to fetch ABI for {address} on chain {chain_id}: "
        f"both {primary_name} and {fallback_name} failed"
    )


def _skip_etherscan_no_key() -> None:
    """Log a warning that Etherscan is skipped due to missing API key."""
    logger.warning("ETHERSCAN_API_KEY not set, skipping Etherscan fallback")
    return None