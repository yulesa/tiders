"""YAML config parser for tiders CLI.

Parses the provider, contracts, query, and fields sections of a tiders YAML
config file into the existing tiders dataclasses.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any

from tiders_core import evm_abi_events, evm_abi_functions, evm_signature_to_topic0
from tiders_core.ingest import ProviderConfig, ProviderKind, Query, QueryKind
from tiders_core.ingest import evm, svm


class YamlConfigError(Exception):
    """Raised when the YAML config contains invalid or missing values.

    Attributes:
        path: Dot-separated path to the problematic key in the YAML
            (e.g. ``"query.logs[0].topic0"``).
        message: Human-readable description of the problem.
    """

    def __init__(self, message: str, path: str = ""):
        self.path = path
        self.message = message
        if path:
            super().__init__(f"{path}: {message}")
        else:
            super().__init__(message)


# ---------------------------------------------------------------------------
# Contract resolution
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class ContractInfo:
    """Resolved contract metadata from ABI + address."""

    name: str
    address: str
    events: dict[str, dict[str, str]]  # event_name -> {topic0, signature}
    functions: dict[str, dict[str, str]]  # func_name -> {selector, signature}


def load_contracts(
    contracts_list: list[dict[str, Any]], yaml_dir: Path
) -> dict[str, ContractInfo]:
    """Parse the ``contracts`` YAML section into a lookup table.

    Each contract entry has a ``name`` and ``details`` list. The first detail
    with an ``abi`` path is used to extract events/functions.
    """
    result: dict[str, ContractInfo] = {}
    for i, contract in enumerate(contracts_list):
        ctx = f"contracts[{i}]"
        if "name" not in contract:
            raise YamlConfigError("Missing required key 'name'.", ctx)
        name = contract["name"]
        ctx = f"contracts[{i}] ({name})"

        details = contract.get("details")
        if not details:
            raise YamlConfigError(
                "Missing or empty 'details' list. Each contract needs at least "
                "one detail entry with an address and optionally an ABI path.",
                ctx,
            )

        detail = details[0]
        address = detail.get("address", "")

        events: dict[str, dict[str, str]] = {}
        functions: dict[str, dict[str, str]] = {}

        abi_path_str = detail.get("abi")
        if abi_path_str is not None:
            abi_path = Path(abi_path_str)
            if not abi_path.is_absolute():
                abi_path = yaml_dir / abi_path
            if not abi_path.is_file():
                raise YamlConfigError(
                    f"ABI file not found: {abi_path}. Check that the path is "
                    f"correct relative to the YAML config directory ({yaml_dir}).",
                    f"{ctx}.details[0].abi",
                )
            abi_json = abi_path.read_text()

            try:
                for ev in evm_abi_events(abi_json):
                    events[ev.name] = {
                        "topic0": ev.topic0,
                        "signature": ev.signature,
                    }
                for fn in evm_abi_functions(abi_json):
                    functions[fn.name] = {
                        "selector": fn.selector,
                        "signature": fn.signature,
                    }
            except Exception as e:
                raise YamlConfigError(
                    f"Failed to parse ABI file {abi_path}: {e}",
                    f"{ctx}.details[0].abi",
                ) from e

        result[name] = ContractInfo(
            name=name, address=address, events=events, functions=functions
        )
    return result


def _resolve_ref(value: str, contracts: dict[str, ContractInfo]) -> str:
    """Resolve a ``ContractName.property`` reference to its concrete value.

    Supported patterns:
    - ``ContractName.address``
    - ``ContractName.Events.EventName.topic0``
    - ``ContractName.Events.EventName.signature``
    - ``ContractName.Functions.FuncName.selector``
    - ``ContractName.Functions.FuncName.signature``
    """
    parts = value.split(".")
    if len(parts) < 2 or parts[0] not in contracts:
        return value

    contract = contracts[parts[0]]

    if len(parts) == 2 and parts[1] == "address":
        return contract.address

    if len(parts) == 4:
        category, item_name, prop = parts[1], parts[2], parts[3]
        if category == "Events":
            if item_name not in contract.events:
                raise YamlConfigError(
                    f"Event '{item_name}' not found in contract '{contract.name}'. "
                    f"Available events: {sorted(contract.events.keys())}",
                    value,
                )
            if prop not in contract.events[item_name]:
                raise YamlConfigError(
                    f"Property '{prop}' not available for event '{item_name}'. "
                    f"Available: {sorted(contract.events[item_name].keys())}",
                    value,
                )
            return contract.events[item_name][prop]
        if category == "Functions":
            if item_name not in contract.functions:
                raise YamlConfigError(
                    f"Function '{item_name}' not found in contract '{contract.name}'. "
                    f"Available functions: {sorted(contract.functions.keys())}",
                    value,
                )
            if prop not in contract.functions[item_name]:
                raise YamlConfigError(
                    f"Property '{prop}' not available for function '{item_name}'. "
                    f"Available: {sorted(contract.functions[item_name].keys())}",
                    value,
                )
            return contract.functions[item_name][prop]

    raise YamlConfigError(
        f"Invalid contract reference '{value}'. Expected one of: "
        f"'{parts[0]}.address', '{parts[0]}.Events.<EventName>.<prop>', "
        f"'{parts[0]}.Functions.<FuncName>.<prop>'.",
        value,
    )


def resolve_contract_refs(obj: Any, contracts: dict[str, ContractInfo]) -> Any:
    """Recursively resolve contract references in the parsed YAML."""
    if isinstance(obj, str):
        return _resolve_ref(obj, contracts)
    if isinstance(obj, dict):
        return {k: resolve_contract_refs(v, contracts) for k, v in obj.items()}
    if isinstance(obj, list):
        return [resolve_contract_refs(item, contracts) for item in obj]
    return obj


# ---------------------------------------------------------------------------
# Provider parsing
# ---------------------------------------------------------------------------

def parse_provider(raw: dict[str, Any]) -> ProviderConfig:
    """Parse the ``provider`` YAML section into a ProviderConfig."""
    raw = dict(raw)
    if "kind" not in raw:
        raise YamlConfigError(
            "Missing required key 'kind'. Must be one of: "
            f"{[e.value for e in ProviderKind]}.",
            "provider",
        )
    kind_str = raw.pop("kind")
    try:
        kind = ProviderKind(kind_str)
    except ValueError:
        raise YamlConfigError(
            f"Unknown provider kind '{kind_str}'. Must be one of: "
            f"{[e.value for e in ProviderKind]}.",
            "provider.kind",
        )

    valid_fields = {f.name for f in dataclasses.fields(ProviderConfig)} - {"kind"}
    unknown = set(raw.keys()) - valid_fields
    if unknown:
        raise YamlConfigError(
            f"Unknown provider options: {sorted(unknown)}. "
            f"Valid options: {sorted(valid_fields)}.",
            "provider",
        )

    return ProviderConfig(kind=kind, **raw)


# ---------------------------------------------------------------------------
# Fields parsing
# ---------------------------------------------------------------------------

def _fields_from_list(
    field_names: list[str], fields_cls: type, path: str
) -> Any:
    """Create a fields dataclass from a list of field names, setting each to True."""
    valid = {f.name for f in dataclasses.fields(fields_cls)}
    kwargs = {}
    for name in field_names:
        if name not in valid:
            raise YamlConfigError(
                f"Unknown field '{name}' for {fields_cls.__name__}. "
                f"Valid fields: {sorted(valid)}.",
                path,
            )
        kwargs[name] = True
    return fields_cls(**kwargs)


def _fields_from_dict(
    field_dict: dict[str, bool], fields_cls: type, path: str
) -> Any:
    """Create a fields dataclass from a dict of {field_name: true/false}."""
    valid = {f.name for f in dataclasses.fields(fields_cls)}
    kwargs = {}
    for name, val in field_dict.items():
        if name not in valid:
            raise YamlConfigError(
                f"Unknown field '{name}' for {fields_cls.__name__}. "
                f"Valid fields: {sorted(valid)}.",
                path,
            )
        kwargs[name] = bool(val)
    return fields_cls(**kwargs)


def _parse_field_selector(raw: list | dict, fields_cls: type, path: str) -> Any:
    """Parse a field selector that can be either a list or a dict."""
    if isinstance(raw, list):
        return _fields_from_list(raw, fields_cls, path)
    if isinstance(raw, dict):
        return _fields_from_dict(raw, fields_cls, path)
    raise YamlConfigError(
        f"Fields must be a list of names or a dict of {{name: true/false}}, "
        f"got {type(raw).__name__}.",
        path,
    )


# ---------------------------------------------------------------------------
# EVM query parsing
# ---------------------------------------------------------------------------

_EVM_FIELD_MAP = {
    "block": evm.BlockFields,
    "transaction": evm.TransactionFields,
    "log": evm.LogFields,
    "trace": evm.TraceFields,
}


def _parse_evm_request(
    raw: dict[str, Any], request_cls: type, path: str
) -> Any:
    """Parse a single EVM request (log, transaction, or trace) from YAML."""
    valid = {f.name for f in dataclasses.fields(request_cls)}
    unknown = set(raw.keys()) - valid
    if unknown:
        raise YamlConfigError(
            f"Unknown keys for {request_cls.__name__}: {sorted(unknown)}. "
            f"Valid keys: {sorted(valid)}.",
            path,
        )

    kwargs = {}
    for f in dataclasses.fields(request_cls):
        if f.name not in raw:
            continue
        val = raw[f.name]
        # List fields: wrap single values in a list
        if f.default_factory is list and not isinstance(val, list):
            val = [val]
        kwargs[f.name] = val
    return request_cls(**kwargs)


def _is_hex_hash(s: str) -> bool:
    """Check if a string looks like a 0x-prefixed hex hash."""
    return s.startswith("0x") and len(s) == 66


def _resolve_topic0(value: str) -> str:
    """Accept either a raw hex hash or an event signature, return topic0 hash."""
    if _is_hex_hash(value):
        return value
    return evm_signature_to_topic0(value)


def _preprocess_evm_log_request(raw: dict[str, Any], path: str) -> dict[str, Any]:
    """Pre-process an EVM log request: resolve topic0 from event signatures."""
    raw = dict(raw)
    if "topic0" in raw:
        val = raw["topic0"]
        if isinstance(val, list):
            raw["topic0"] = [_resolve_topic0(v) for v in val]
        elif isinstance(val, str):
            raw["topic0"] = [_resolve_topic0(val)]
    return raw


def parse_evm_query(raw: dict[str, Any]) -> evm.Query:
    """Parse the EVM query section from YAML into an evm.Query."""
    kwargs: dict[str, Any] = {}

    if "from_block" in raw:
        kwargs["from_block"] = raw["from_block"]
    if "to_block" in raw:
        kwargs["to_block"] = raw["to_block"]
    if "include_all_blocks" in raw:
        kwargs["include_all_blocks"] = raw["include_all_blocks"]

    # Parse fields
    if "fields" in raw:
        fields_raw = raw["fields"]
        fields_kwargs = {}
        for key, cls in _EVM_FIELD_MAP.items():
            if key in fields_raw:
                fields_kwargs[key] = _parse_field_selector(
                    fields_raw[key], cls, f"query.fields.{key}"
                )
        unknown_field_keys = set(fields_raw.keys()) - set(_EVM_FIELD_MAP.keys())
        if unknown_field_keys:
            raise YamlConfigError(
                f"Unknown field categories: {sorted(unknown_field_keys)}. "
                f"Valid categories: {sorted(_EVM_FIELD_MAP.keys())}.",
                "query.fields",
            )
        kwargs["fields"] = evm.Fields(**fields_kwargs)

    # Parse request lists (logs, transactions, traces)
    if "logs" in raw:
        log_requests = []
        for i, lr in enumerate(raw["logs"]):
            lr = _preprocess_evm_log_request(lr, f"query.logs[{i}]")
            log_requests.append(
                _parse_evm_request(lr, evm.LogRequest, f"query.logs[{i}]")
            )
        kwargs["logs"] = log_requests

    if "transactions" in raw:
        tx_requests = []
        for i, tr in enumerate(raw["transactions"]):
            tx_requests.append(
                _parse_evm_request(tr, evm.TransactionRequest, f"query.transactions[{i}]")
            )
        kwargs["transactions"] = tx_requests

    if "traces" in raw:
        trace_requests = []
        for i, tr in enumerate(raw["traces"]):
            trace_requests.append(
                _parse_evm_request(tr, evm.TraceRequest, f"query.traces[{i}]")
            )
        kwargs["traces"] = trace_requests

    # Validate no unknown top-level query keys
    known_keys = {
        "from_block", "to_block", "include_all_blocks",
        "fields", "logs", "transactions", "traces",
    }
    unknown = set(raw.keys()) - known_keys
    if unknown:
        raise YamlConfigError(
            f"Unknown EVM query keys: {sorted(unknown)}. "
            f"Valid keys: {sorted(known_keys)}.",
            "query",
        )

    return evm.Query(**kwargs)


# ---------------------------------------------------------------------------
# SVM query parsing
# ---------------------------------------------------------------------------

_SVM_FIELD_MAP = {
    "instruction": svm.InstructionFields,
    "transaction": svm.TransactionFields,
    "log": svm.LogFields,
    "balance": svm.BalanceFields,
    "token_balance": svm.TokenBalanceFields,
    "reward": svm.RewardFields,
    "block": svm.BlockFields,
}

_SVM_REQUEST_TYPES = {
    "instructions": svm.InstructionRequest,
    "transactions": svm.TransactionRequest,
    "logs": svm.LogRequest,
    "balances": svm.BalanceRequest,
    "token_balances": svm.TokenBalanceRequest,
    "rewards": svm.RewardRequest,
}


def _parse_svm_request(
    raw: dict[str, Any], request_cls: type, path: str
) -> Any:
    """Parse a single SVM request from YAML."""
    valid = {f.name for f in dataclasses.fields(request_cls)}
    unknown = set(raw.keys()) - valid
    if unknown:
        raise YamlConfigError(
            f"Unknown keys for {request_cls.__name__}: {sorted(unknown)}. "
            f"Valid keys: {sorted(valid)}.",
            path,
        )

    kwargs = {}
    for f in dataclasses.fields(request_cls):
        if f.name not in raw:
            continue
        val = raw[f.name]
        # List fields: wrap single values in a list
        if f.default_factory is list and not isinstance(val, list):
            val = [val]
        # Handle LogKind enum conversion
        if f.name == "kind" and request_cls is svm.LogRequest:
            if isinstance(val, list):
                val = [
                    svm.LogKind(v) if isinstance(v, str) else v for v in val
                ]
        kwargs[f.name] = val
    return request_cls(**kwargs)


def parse_svm_query(raw: dict[str, Any]) -> svm.Query:
    """Parse the SVM query section from YAML into a svm.Query."""
    kwargs: dict[str, Any] = {}

    if "from_block" in raw:
        kwargs["from_block"] = raw["from_block"]
    if "to_block" in raw:
        kwargs["to_block"] = raw["to_block"]
    if "include_all_blocks" in raw:
        kwargs["include_all_blocks"] = raw["include_all_blocks"]

    # Parse fields
    if "fields" in raw:
        fields_raw = raw["fields"]
        fields_kwargs = {}
        for key, cls in _SVM_FIELD_MAP.items():
            if key in fields_raw:
                fields_kwargs[key] = _parse_field_selector(
                    fields_raw[key], cls, f"query.fields.{key}"
                )
        unknown_field_keys = set(fields_raw.keys()) - set(_SVM_FIELD_MAP.keys())
        if unknown_field_keys:
            raise YamlConfigError(
                f"Unknown field categories: {sorted(unknown_field_keys)}. "
                f"Valid categories: {sorted(_SVM_FIELD_MAP.keys())}.",
                "query.fields",
            )
        kwargs["fields"] = svm.Fields(**fields_kwargs)

    # Parse request lists
    for key, cls in _SVM_REQUEST_TYPES.items():
        if key in raw:
            requests = []
            for i, r in enumerate(raw[key]):
                requests.append(
                    _parse_svm_request(r, cls, f"query.{key}[{i}]")
                )
            kwargs[key] = requests

    # Validate no unknown top-level query keys
    known_keys = {
        "from_block", "to_block", "include_all_blocks", "fields",
        *_SVM_REQUEST_TYPES.keys(),
    }
    unknown = set(raw.keys()) - known_keys
    if unknown:
        raise YamlConfigError(
            f"Unknown SVM query keys: {sorted(unknown)}. "
            f"Valid keys: {sorted(known_keys)}.",
            "query",
        )

    return svm.Query(**kwargs)


# ---------------------------------------------------------------------------
# Top-level query parsing
# ---------------------------------------------------------------------------

def parse_query(raw: dict[str, Any]) -> Query:
    """Parse the ``query`` YAML section into a Query dataclass."""
    raw = dict(raw)
    if "kind" not in raw:
        raise YamlConfigError(
            "Missing required key 'kind'. Must be one of: "
            f"{[e.value for e in QueryKind]}.",
            "query",
        )
    kind_str = raw.pop("kind")
    try:
        kind = QueryKind(kind_str)
    except ValueError:
        raise YamlConfigError(
            f"Unknown query kind '{kind_str}'. Must be one of: "
            f"{[e.value for e in QueryKind]}.",
            "query.kind",
        )

    if kind == QueryKind.EVM:
        params = parse_evm_query(raw)
    elif kind == QueryKind.SVM:
        params = parse_svm_query(raw)
    else:
        raise YamlConfigError(f"Unsupported query kind: {kind_str}", "query.kind")

    return Query(kind=kind, params=params)


def parse_provider_and_query(
    raw_config: dict[str, Any], yaml_dir: Path
) -> tuple[ProviderConfig, Query]:
    """Parse provider, contracts, and query sections from a raw YAML config.

    This is the main entry point for Commit 3's functionality:
    1. Load and resolve contracts (ABIs, addresses, events, functions)
    2. Substitute contract references throughout the config
    3. Parse provider → ProviderConfig
    4. Parse query → Query (EVM or SVM)
    """
    # Load contracts if present
    contracts: dict[str, ContractInfo] = {}
    if "contracts" in raw_config:
        contracts = load_contracts(raw_config["contracts"], yaml_dir)

    # Resolve contract references in provider and query sections
    provider_raw = raw_config.get("provider")
    query_raw = raw_config.get("query")

    if provider_raw is None:
        raise YamlConfigError("Missing required 'provider' section in config.")
    if query_raw is None:
        raise YamlConfigError("Missing required 'query' section in config.")

    if contracts:
        provider_raw = resolve_contract_refs(provider_raw, contracts)
        query_raw = resolve_contract_refs(query_raw, contracts)

    provider = parse_provider(dict(provider_raw))
    query = parse_query(dict(query_raw))

    return provider, query
