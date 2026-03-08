"""YAML config parser for tiders CLI.

Parses the provider, contracts, query, fields, and steps sections of a tiders
YAML config file into the existing tiders dataclasses.
"""

from __future__ import annotations

import dataclasses
import importlib.util
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pyarrow as pa

from tiders_core import evm_abi_events, evm_abi_functions, evm_signature_to_topic0
from tiders_core.ingest import ProviderConfig, ProviderKind, Query, QueryKind
from tiders_core.ingest import evm, svm
from tiders_core.svm_decode import (
    Array,
    DynType,
    Enum,
    Field,
    FixedArray,
    InstructionSignature,
    LogSignature,
    Option,
    ParamInput,
    Struct,
    Variant,
)

from tiders.config import (
    Base58EncodeConfig,
    CastByTypeConfig,
    CastConfig,
    DataFusionStepConfig,
    EvmDecodeEventsConfig,
    GlaciersEventsConfig,
    HexEncodeConfig,
    PolarsStepConfig,
    SetChainIdConfig,
    Step,
    StepKind,
    SvmDecodeInstructionsConfig,
    SvmDecodeLogsConfig,
    U256ToBinaryConfig,
)


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


# ---------------------------------------------------------------------------
# PyArrow type string parsing
# ---------------------------------------------------------------------------

# Matches type strings like "decimal128(38,0)", "decimal256(76,0)", "int32", etc.
_PARAMETERIZED_TYPE_RE = re.compile(r"^(\w+)\((.+)\)$")

_SIMPLE_PA_TYPES: dict[str, pa.DataType] = {
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "uint8": pa.uint8(),
    "uint16": pa.uint16(),
    "uint32": pa.uint32(),
    "uint64": pa.uint64(),
    "float16": pa.float16(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "string": pa.string(),
    "utf8": pa.utf8(),
    "large_string": pa.large_string(),
    "large_utf8": pa.large_utf8(),
    "binary": pa.binary(),
    "large_binary": pa.large_binary(),
    "bool": pa.bool_(),
    "boolean": pa.bool_(),
    "date32": pa.date32(),
    "date64": pa.date64(),
    "null": pa.null(),
}


def parse_pa_type(type_str: str, path: str) -> pa.DataType:
    """Parse a type string like ``"decimal128(38,0)"`` into a ``pa.DataType``.

    Supported formats:
    - Simple: ``"int32"``, ``"string"``, ``"float64"``, ``"binary"``, etc.
    - Decimal: ``"decimal128(precision,scale)"``, ``"decimal256(precision,scale)"``
    """
    type_str = type_str.strip()

    # Try simple types first
    if type_str in _SIMPLE_PA_TYPES:
        return _SIMPLE_PA_TYPES[type_str]

    # Try parameterized types
    m = _PARAMETERIZED_TYPE_RE.match(type_str)
    if m:
        type_name = m.group(1)
        params_str = m.group(2)

        if type_name == "decimal128":
            try:
                parts = [int(p.strip()) for p in params_str.split(",")]
                if len(parts) != 2:
                    raise ValueError("expected 2 params")
                return pa.decimal128(parts[0], parts[1])
            except (ValueError, TypeError) as e:
                raise YamlConfigError(
                    f"Invalid decimal128 parameters '{params_str}'. "
                    f"Expected 'decimal128(precision,scale)', e.g. 'decimal128(38,0)'. "
                    f"Error: {e}",
                    path,
                ) from e

        if type_name == "decimal256":
            try:
                parts = [int(p.strip()) for p in params_str.split(",")]
                if len(parts) != 2:
                    raise ValueError("expected 2 params")
                return pa.decimal256(parts[0], parts[1])
            except (ValueError, TypeError) as e:
                raise YamlConfigError(
                    f"Invalid decimal256 parameters '{params_str}'. "
                    f"Expected 'decimal256(precision,scale)', e.g. 'decimal256(76,0)'. "
                    f"Error: {e}",
                    path,
                ) from e

    raise YamlConfigError(
        f"Unknown type '{type_str}'. Supported simple types: "
        f"{sorted(_SIMPLE_PA_TYPES.keys())}. "
        f"Parameterized types: decimal128(p,s), decimal256(p,s).",
        path,
    )


# ---------------------------------------------------------------------------
# SVM DynType parsing (for instruction/log signatures)
# ---------------------------------------------------------------------------

_PRIMITIVE_DYNTYPE_MAP: dict[str, str] = {
    "i8": "i8", "i16": "i16", "i32": "i32", "i64": "i64", "i128": "i128",
    "u8": "u8", "u16": "u16", "u32": "u32", "u64": "u64", "u128": "u128",
    "bool": "bool",
}


def _parse_dyntype(raw: Any, path: str) -> Any:
    """Parse a DynType from a YAML value (string or dict).

    Supported formats:
    - Primitive string: ``"u64"``, ``"bool"``, ``"i128"``, etc.
    - Dict with ``type`` key for complex types:
      - ``{type: array, element: u8}``
      - ``{type: fixed_array, element: u8, size: 32}``
      - ``{type: option, element: u64}``
      - ``{type: struct, fields: [{name: x, type: u64}, ...]}``
      - ``{type: enum, variants: [{name: A, type: u32}, {name: B}]}``
    """
    if isinstance(raw, str):
        if raw in _PRIMITIVE_DYNTYPE_MAP:
            return _PRIMITIVE_DYNTYPE_MAP[raw]
        raise YamlConfigError(
            f"Unknown DynType primitive '{raw}'. "
            f"Valid primitives: {sorted(_PRIMITIVE_DYNTYPE_MAP.keys())}.",
            path,
        )

    if not isinstance(raw, dict):
        raise YamlConfigError(
            f"DynType must be a string (primitive) or a dict (complex type), "
            f"got {type(raw).__name__}.",
            path,
        )

    if "type" not in raw:
        raise YamlConfigError(
            "Complex DynType dict must have a 'type' key. "
            "Supported: array, fixed_array, option, struct, enum.",
            path,
        )

    type_name = raw["type"]

    if type_name == "array":
        if "element" not in raw:
            raise YamlConfigError(
                "Array type requires an 'element' key.", path
            )
        return Array(element_type=_parse_dyntype(raw["element"], f"{path}.element"))

    if type_name == "fixed_array":
        if "element" not in raw:
            raise YamlConfigError(
                "FixedArray type requires an 'element' key.", path
            )
        if "size" not in raw:
            raise YamlConfigError(
                "FixedArray type requires a 'size' key.", path
            )
        return FixedArray(
            element_type=_parse_dyntype(raw["element"], f"{path}.element"),
            size=int(raw["size"]),
        )

    if type_name == "option":
        if "element" not in raw:
            raise YamlConfigError(
                "Option type requires an 'element' key.", path
            )
        return Option(element_type=_parse_dyntype(raw["element"], f"{path}.element"))

    if type_name == "struct":
        if "fields" not in raw:
            raise YamlConfigError(
                "Struct type requires a 'fields' list.", path
            )
        fields = []
        for j, f in enumerate(raw["fields"]):
            fp = f"{path}.fields[{j}]"
            if "name" not in f:
                raise YamlConfigError("Struct field must have a 'name'.", fp)
            if "type" not in f:
                raise YamlConfigError(
                    f"Struct field '{f['name']}' must have a 'type'.", fp
                )
            fields.append(
                Field(name=f["name"], element_type=_parse_dyntype(f["type"], fp))
            )
        return Struct(fields=fields)

    if type_name == "enum":
        if "variants" not in raw:
            raise YamlConfigError(
                "Enum type requires a 'variants' list.", path
            )
        variants = []
        for j, v in enumerate(raw["variants"]):
            vp = f"{path}.variants[{j}]"
            if "name" not in v:
                raise YamlConfigError("Enum variant must have a 'name'.", vp)
            elem = None
            if "type" in v:
                elem = _parse_dyntype(v["type"], vp)
            variants.append(Variant(name=v["name"], element_type=elem))
        return Enum(variants=variants)

    raise YamlConfigError(
        f"Unknown complex DynType '{type_name}'. "
        f"Supported: array, fixed_array, option, struct, enum.",
        path,
    )


def _parse_param_inputs(
    params_raw: list[dict[str, Any]], path: str
) -> list[ParamInput]:
    """Parse a list of parameter definitions into ParamInput objects."""
    result = []
    for i, p in enumerate(params_raw):
        pp = f"{path}[{i}]"
        if "name" not in p:
            raise YamlConfigError("Parameter must have a 'name'.", pp)
        if "type" not in p:
            raise YamlConfigError(
                f"Parameter '{p['name']}' must have a 'type'.", pp
            )
        result.append(
            ParamInput(name=p["name"], param_type=_parse_dyntype(p["type"], pp))
        )
    return result


# ---------------------------------------------------------------------------
# SQL step runner builder
# ---------------------------------------------------------------------------

def _build_sql_runner(
    queries: list[str],
) -> Any:
    """Build a DataFusion runner function that executes a list of SQL queries.

    Each SQL query is executed against the session context. The last query's
    result for each table name is kept. Tables referenced in FROM clauses are
    automatically registered.
    """
    def sql_runner(ctx: Any, tables: dict, context: Any) -> dict:
        result = dict(tables)
        for sql in queries:
            df = ctx.sql(sql)
            # Use the table alias from the SQL if available;
            # otherwise use "sql_result"
            result_name = _extract_table_name_from_sql(sql) or "sql_result"
            result[result_name] = df
        return result

    return sql_runner


def _extract_table_name_from_sql(sql: str) -> Optional[str]:
    """Try to extract a destination table name from CREATE TABLE ... AS or
    just return None for plain SELECT queries."""
    # Match: CREATE TABLE <name> AS ...
    m = re.match(
        r"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(\w+)\s+AS\s+",
        sql,
        re.IGNORECASE,
    )
    if m:
        return m.group(1)
    return None


# ---------------------------------------------------------------------------
# python_file step loader
# ---------------------------------------------------------------------------

def _load_python_file_step(
    raw: dict[str, Any], path: str, yaml_dir: Path
) -> Step:
    """Load a step from a Python file, importing the function by name."""
    if "file" not in raw:
        raise YamlConfigError(
            "python_file step requires a 'file' key pointing to a .py file.",
            path,
        )
    if "function" not in raw:
        raise YamlConfigError(
            "python_file step requires a 'function' key naming the callable.",
            path,
        )

    file_path = Path(raw["file"])
    if not file_path.is_absolute():
        file_path = yaml_dir / file_path
    if not file_path.is_file():
        raise YamlConfigError(
            f"Python file not found: {file_path}. Check the path relative to "
            f"the YAML config directory ({yaml_dir}).",
            f"{path}.file",
        )

    func_name = raw["function"]
    step_type = raw.get("step_type", "datafusion")

    # Import the module from file path
    module_name = f"_tiders_yaml_user_module_{file_path.stem}"
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise YamlConfigError(
            f"Could not load Python module from {file_path}.", f"{path}.file"
        )
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as e:
        raise YamlConfigError(
            f"Error loading Python file {file_path}: {e}", f"{path}.file"
        ) from e

    if not hasattr(module, func_name):
        available = [
            n for n in dir(module) if not n.startswith("_") and callable(getattr(module, n))
        ]
        raise YamlConfigError(
            f"Function '{func_name}' not found in {file_path}. "
            f"Available callables: {available}.",
            f"{path}.function",
        )

    func = getattr(module, func_name)
    context = raw.get("context")

    if step_type == "polars":
        return Step(
            kind=StepKind.POLARS,
            config=PolarsStepConfig(runner=func, context=context),
            name=raw.get("name"),
        )
    elif step_type == "datafusion":
        return Step(
            kind=StepKind.DATAFUSION,
            config=DataFusionStepConfig(runner=func, context=context),
            name=raw.get("name"),
        )
    else:
        raise YamlConfigError(
            f"Unknown step_type '{step_type}'. Must be 'polars' or 'datafusion'.",
            f"{path}.step_type",
        )


# ---------------------------------------------------------------------------
# Steps parsing
# ---------------------------------------------------------------------------

def _parse_step(
    raw: dict[str, Any], index: int, yaml_dir: Path
) -> Step:
    """Parse a single step from its YAML dict representation."""
    path = f"steps[{index}]"

    if "kind" not in raw:
        raise YamlConfigError(
            "Missing required key 'kind'. Each step must specify its type.",
            path,
        )
    kind_str = raw["kind"]
    config_raw = raw.get("config", {})
    step_name = raw.get("name")

    # --- YAML-only step kinds (not in StepKind enum) ---

    if kind_str == "sql":
        if "queries" not in config_raw:
            raise YamlConfigError(
                "SQL step requires 'config.queries': a list of SQL strings.",
                f"{path}.config",
            )
        queries = config_raw["queries"]
        if isinstance(queries, str):
            queries = [queries]
        if not isinstance(queries, list):
            raise YamlConfigError(
                "'config.queries' must be a string or list of SQL strings.",
                f"{path}.config.queries",
            )
        runner = _build_sql_runner(queries)
        return Step(
            kind=StepKind.DATAFUSION,
            config=DataFusionStepConfig(runner=runner),
            name=step_name or "sql",
        )

    if kind_str == "python_file":
        return _load_python_file_step(
            {**config_raw, "name": step_name}, path, yaml_dir
        )

    # --- Standard step kinds ---

    try:
        kind = StepKind(kind_str)
    except ValueError:
        valid = [e.value for e in StepKind] + ["sql", "python_file"]
        raise YamlConfigError(
            f"Unknown step kind '{kind_str}'. Must be one of: {valid}.",
            f"{path}.kind",
        )

    # Reject polars/datafusion in YAML mode
    if kind == StepKind.POLARS:
        raise YamlConfigError(
            "The 'polars' step kind cannot be used directly in YAML mode "
            "because it requires a Python callable. Use 'python_file' to "
            "reference a .py file, or 'sql' for SQL-based transforms.",
            f"{path}.kind",
        )
    if kind == StepKind.DATAFUSION:
        raise YamlConfigError(
            "The 'datafusion' step kind cannot be used directly in YAML mode "
            "because it requires a Python callable. Use 'python_file' to "
            "reference a .py file, or 'sql' for SQL-based transforms.",
            f"{path}.kind",
        )

    config = _parse_step_config(kind, config_raw, path)
    return Step(kind=kind, config=config, name=step_name)


def _parse_step_config(
    kind: StepKind, raw: dict[str, Any], path: str
) -> Any:
    """Parse a step's config dict into the appropriate config dataclass."""
    cfg_path = f"{path}.config"

    if kind == StepKind.EVM_DECODE_EVENTS:
        if "event_signature" not in raw:
            raise YamlConfigError(
                "evm_decode_events requires 'config.event_signature'.",
                cfg_path,
            )
        return EvmDecodeEventsConfig(
            event_signature=raw["event_signature"],
            allow_decode_fail=raw.get("allow_decode_fail", False),
            filter_by_topic0=raw.get("filter_by_topic0", False),
            input_table=raw.get("input_table", "logs"),
            output_table=raw.get("output_table", "decoded_logs"),
            hstack=raw.get("hstack", True),
        )

    if kind == StepKind.CAST_BY_TYPE:
        if "from_type" not in raw:
            raise YamlConfigError(
                "cast_by_type requires 'config.from_type' (e.g. 'decimal256(76,0)').",
                cfg_path,
            )
        if "to_type" not in raw:
            raise YamlConfigError(
                "cast_by_type requires 'config.to_type' (e.g. 'decimal128(38,0)').",
                cfg_path,
            )
        return CastByTypeConfig(
            from_type=parse_pa_type(raw["from_type"], f"{cfg_path}.from_type"),
            to_type=parse_pa_type(raw["to_type"], f"{cfg_path}.to_type"),
            allow_cast_fail=raw.get("allow_cast_fail", False),
        )

    if kind == StepKind.CAST:
        if "table_name" not in raw:
            raise YamlConfigError(
                "cast requires 'config.table_name'.", cfg_path
            )
        if "mappings" not in raw:
            raise YamlConfigError(
                "cast requires 'config.mappings': a dict of "
                "{column_name: type_string}.",
                cfg_path,
            )
        mappings = {}
        for col, type_str in raw["mappings"].items():
            mappings[col] = parse_pa_type(
                type_str, f"{cfg_path}.mappings.{col}"
            )
        return CastConfig(
            table_name=raw["table_name"],
            mappings=mappings,
            allow_cast_fail=raw.get("allow_cast_fail", False),
        )

    if kind == StepKind.HEX_ENCODE:
        return HexEncodeConfig(
            tables=raw.get("tables"),
            prefixed=raw.get("prefixed", True),
        )

    if kind == StepKind.BASE58_ENCODE:
        return Base58EncodeConfig(tables=raw.get("tables"))

    if kind == StepKind.U256_TO_BINARY:
        return U256ToBinaryConfig(tables=raw.get("tables"))

    if kind == StepKind.SET_CHAIN_ID:
        if "chain_id" not in raw:
            raise YamlConfigError(
                "set_chain_id requires 'config.chain_id' (integer).",
                cfg_path,
            )
        return SetChainIdConfig(chain_id=int(raw["chain_id"]))

    if kind == StepKind.SVM_DECODE_INSTRUCTIONS:
        return _parse_svm_decode_instructions_config(raw, cfg_path)

    if kind == StepKind.SVM_DECODE_LOGS:
        return _parse_svm_decode_logs_config(raw, cfg_path)

    if kind == StepKind.GLACIERS_EVENTS:
        if "abi_db_path" not in raw:
            raise YamlConfigError(
                "glaciers_events requires 'config.abi_db_path'.", cfg_path
            )
        return GlaciersEventsConfig(
            abi_db_path=raw["abi_db_path"],
            decoder_type=raw.get("decoder_type", "log"),
            input_table=raw.get("input_table", "logs"),
            output_table=raw.get("output_table", "decoded_logs"),
        )

    # Fallback for step kinds that don't need config parsing
    raise YamlConfigError(
        f"Step kind '{kind.value}' is not yet supported in YAML mode.",
        f"{path}.kind",
    )


def _parse_svm_decode_instructions_config(
    raw: dict[str, Any], path: str
) -> SvmDecodeInstructionsConfig:
    """Parse svm_decode_instructions config including the instruction signature."""
    if "instruction_signature" not in raw:
        raise YamlConfigError(
            "svm_decode_instructions requires 'config.instruction_signature' "
            "with 'discriminator', 'params', and 'accounts_names'.",
            path,
        )
    sig_raw = raw["instruction_signature"]
    sig_path = f"{path}.instruction_signature"

    if "discriminator" not in sig_raw:
        raise YamlConfigError(
            "instruction_signature requires a 'discriminator' (hex string or bytes).",
            sig_path,
        )
    if "params" not in sig_raw:
        raise YamlConfigError(
            "instruction_signature requires a 'params' list.",
            sig_path,
        )
    if "accounts_names" not in sig_raw:
        raise YamlConfigError(
            "instruction_signature requires an 'accounts_names' list.",
            sig_path,
        )

    params = _parse_param_inputs(sig_raw["params"], f"{sig_path}.params")
    sig = InstructionSignature(
        discriminator=sig_raw["discriminator"],
        params=params,
        accounts_names=sig_raw["accounts_names"],
    )

    return SvmDecodeInstructionsConfig(
        instruction_signature=sig,
        allow_decode_fail=raw.get("allow_decode_fail", False),
        filter_by_discriminator=raw.get("filter_by_discriminator", False),
        input_table=raw.get("input_table", "instructions"),
        output_table=raw.get("output_table", "decoded_instructions"),
        hstack=raw.get("hstack", True),
    )


def _parse_svm_decode_logs_config(
    raw: dict[str, Any], path: str
) -> SvmDecodeLogsConfig:
    """Parse svm_decode_logs config including the log signature."""
    if "log_signature" not in raw:
        raise YamlConfigError(
            "svm_decode_logs requires 'config.log_signature' with 'params'.",
            path,
        )
    sig_raw = raw["log_signature"]
    sig_path = f"{path}.log_signature"

    if "params" not in sig_raw:
        raise YamlConfigError(
            "log_signature requires a 'params' list.", sig_path
        )

    params = _parse_param_inputs(sig_raw["params"], f"{sig_path}.params")
    sig = LogSignature(params=params)

    return SvmDecodeLogsConfig(
        log_signature=sig,
        allow_decode_fail=raw.get("allow_decode_fail", False),
        input_table=raw.get("input_table", "logs"),
        output_table=raw.get("output_table", "decoded_logs"),
        hstack=raw.get("hstack", True),
    )


def parse_steps(
    steps_raw: list[dict[str, Any]], yaml_dir: Path
) -> list[Step]:
    """Parse the ``steps`` YAML section into a list of Step dataclasses."""
    if not isinstance(steps_raw, list):
        raise YamlConfigError(
            "'steps' must be a list of step definitions.",
            "steps",
        )
    return [_parse_step(s, i, yaml_dir) for i, s in enumerate(steps_raw)]
