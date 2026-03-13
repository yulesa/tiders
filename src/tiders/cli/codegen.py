"""Code generation for tiders pipelines.

Receive the tiders objects of the pipeline and emits an equivalent inline Python script
that constructs and runs the same pipeline using the tiders Python SDK.

Top-level
---------
- ``generate``              — assembles the full Python source file
- ``collect_imports``       — determines the set of import lines needed

Contract code generation
------------------------
- ``_contracts_to_code``     — generates contract variable definitions (ABI loading or inlined dicts)

Serialization
-------------
- ``to_code``               — recursive function that take tiders object → Python code string
- ``_pa_type_to_code``      — helper function that handles pyarrow DataType objects
- ``_dataclass_to_code``    — helper function that serializes a Python dataclass, skipping defaults
- ``_sig_obj_to_code``      — helper function that serializes a non-dataclass object via its __init__ signature
- ``_equals_default``       — helper function that checks if a value equals its default
- ``_qualified_class_name`` — helper function that returns the Python expression for a class name

SQL / Python file steps
-----------------------
- ``_sql_step_to_runner_def``       — generates a runner function for a SQL step
- ``_python_file_step_to_source``   — extracts source code from a python_file step runner
- ``_runner_step_type``             — returns the config class name for a sql/python_file step

PyArrow detection
-----------------
- ``_has_pa_types``           — checks if any field in an object tree is a pyarrow DataType
- ``_has_pa_types_in_steps``  — checks if any step contains pyarrow types
"""

from __future__ import annotations

import dataclasses
import inspect
import re
import textwrap
from enum import Enum
from pathlib import Path
from typing import Any

import pyarrow as pa

_INDENT = "    "


# ---------------------------------------------------------------------------
# Main generator
# ---------------------------------------------------------------------------


def generate(
    project: Any,
    contracts: dict[str, Any],
    provider: Any,
    query: Any,
    steps: list[Any],
    writer: Any,
    table_aliases: Any,
    raw_steps: list[dict[str, Any]],
    env_map: dict[str, str],
    yaml_path: Path,
) -> str:
    """Generate a complete Python script from parsed tiders objects.

    Args:
        project: ProjectInfo (from parse_tiders_yaml).
        provider: ProviderConfig.
        query: Query (tiders_core).
        steps: list[Step] (parsed).
        writer: Writer or list[Writer].
        table_aliases: EvmTableAliases | SvmTableAliases | None.
        raw_steps: Only necessary for 'sql' steps. Used to extract the SQL query that get's hided behind the runner function.
        env_map: Mapping of env var names to resolved values.
        yaml_path: Path to the original YAML file.
        contracts: Mapping of contract name to ContractInfo, if any.

    Returns:
        A string containing the complete generated Python source.
    """
    from tiders_core.ingest import QueryKind

    has_contracts = bool(contracts)
    has_abi_paths = has_contracts and any(c.abi_path for c in contracts.values())
    has_env_vars = bool(env_map)
    has_evm = query.kind == QueryKind.EVM
    has_svm = query.kind == QueryKind.SVM
    has_pa = _has_pa_types_in_steps(steps)

    # --- Build import block ---
    import_lines = collect_imports(
        has_abi_paths=has_abi_paths,
        steps=steps,
        writer=writer,
        table_aliases=table_aliases,
        has_env_vars=has_env_vars,
        has_pa_types=has_pa,
        has_evm=has_evm,
        has_svm=has_svm,
    )
    
    # --- contracts ---
    if has_contracts:
        contract_lines = _contracts_to_code(contracts)

    # --- Serialize provider and queries component ---
    provider_code = to_code(provider, env_map, indent=0)
    query_code = to_code(query, env_map, indent=0)

    # --- Serialize steps component ---
    step_func_defs: list[str] = [] # For runner functions defined by sql / python_file steps
    step_codes: list[str] = []
    for step, raw_step in zip(steps, raw_steps):
        kind_str = raw_step.get("kind", "")
        step_name = raw_step.get("name") or kind_str

        if kind_str not in ("sql", "python_file"):
            step_codes.append(to_code(step, env_map, indent=0))
            continue
        if kind_str == "sql":
            queries = raw_step.get("config", {}).get("queries", [])
            if isinstance(queries, str):
                queries = [queries]
            func_name, func_src = _sql_step_to_runner_def(step_name, queries)
            step_func_defs.append(func_src)
        elif kind_str == "python_file":
            func_name, func_src = _python_file_step_to_source(step)
            step_func_defs.append(func_src)

        step_kind_code = to_code(step.kind, env_map)
        step_type = _runner_step_type(raw_step)
        step_codes.append(
            f"Step(\n"
            f"    kind={step_kind_code},\n"
            f"    config={step_type}(runner={func_name}),\n"
            f"    name={repr(step.name or step_name)},\n"
            f")"
        )

    # --- Serialize writers ---
    if isinstance(writer, list):
        writer_code = (
            "[\n    "
            + ",\n    ".join(to_code(w, env_map, indent=1) for w in writer)
            + ",\n]"
        )
    else:
        writer_code = to_code(writer, env_map, indent=0)
    
    # --- Serialize table alias ---
    table_aliases_code = to_code(table_aliases, env_map, indent=0) if table_aliases else None

    # --- Assemble the file ---
    lines: list[str] = []

    # Header comment
    lines.append(f"# Generated by tiders codegen from {yaml_path.name}")
    lines.append(f"# Project: {project.name} — {project.description}")
    lines.append("")

    # Imports
    lines.extend(import_lines)
    lines.append("")
    lines.append("")

    # Helper functions (SQL runners, python_file runners)
    if step_func_defs:
        for func_src in step_func_defs:
            lines.append(func_src)
            lines.append("")
        lines.append("")
    
    if has_contracts:
        lines.extend(contract_lines)
        lines.append("")

    # provider
    lines.append(f"provider = {provider_code}")
    lines.append("")

    # query
    lines.append(f"query = {query_code}")
    lines.append("")

    # steps
    if step_codes:
        lines.append("steps = [")
        for sc in step_codes:
            # Indent each step by one level
            indented = textwrap.indent(sc, _INDENT)
            lines.append(f"{indented},")
        lines.append("]")
    else:
        lines.append("steps = []")
    lines.append("")

    # writer
    if isinstance(writer, list):
        lines.append(f"writer = {writer_code}")
    else:
        lines.append(f"writer = {writer_code}")
    lines.append("")

    # table_aliases
    if table_aliases_code:
        lines.append(f"table_aliases = {table_aliases_code}")
        lines.append("")

    # pipeline
    pipeline_args = [
        "    provider=provider,",
        "    query=query,",
        "    writer=writer,",
        "    steps=steps,",
    ]
    if table_aliases:
        pipeline_args.append("    table_aliases=table_aliases,")

    lines.append("pipeline = Pipeline(")
    lines.extend(pipeline_args)
    lines.append(")")
    lines.append("")
    lines.append("")

    # Entry point
    lines.append('if __name__ == "__main__":')
    lines.append("    asyncio.run(run_pipeline(pipeline))")
    lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Collect Imports
# ---------------------------------------------------------------------------

# Classes that live in tiders.config
_TIDERS_CONFIG_CLASSES = {
    "Pipeline",
    "Step",
    "StepKind",
    "Writer",
    "WriterKind",
    "EvmDecodeEventsConfig",
    "SvmDecodeInstructionsConfig",
    "SvmDecodeLogsConfig",
    "CastConfig",
    "CastByTypeConfig",
    "HexEncodeConfig",
    "U256ToBinaryConfig",
    "Base58EncodeConfig",
    "HexEncodeConfig",
    "SetChainIdConfig",
    "JoinBlockDataConfig",
    "JoinEvmTransactionDataConfig",
    "JoinSvmTransactionDataConfig",
    "PolarsStepConfig",
    "PandasStepConfig",
    "DataFusionStepConfig",
    "ClickHouseWriterConfig",
    "ClickHouseSkipIndex",
    "IcebergWriterConfig",
    "DeltaLakeWriterConfig",
    "PyArrowDatasetWriterConfig",
    "DuckdbWriterConfig",
    "PostgresqlWriterConfig",
    "CsvWriterConfig",
    "EvmTableAliases",
    "SvmTableAliases",
}


def collect_imports(
    steps: list[Any],
    writer: Any,
    table_aliases: Any,
    has_env_vars: bool,
    has_pa_types: bool,
    has_evm: bool,
    has_svm: bool,
    has_abi_paths: bool,
) -> list[str]:
    """Build the sorted import block for the generated file."""
    # --- stdlib ---
    import_lines: list[str] = ["import asyncio"]
    # --- # os ---
    if has_env_vars:
        import_lines.append("import os")
    # --- pathlib ---
    if has_abi_paths:
        import_lines.append("from pathlib import Path")
    # --- pyarrow ---
    if has_pa_types:
        import_lines.append("import pyarrow as pa")

    # --- Collect tiders.config imports ---
    # Always include
    config_names: set[str] = {"Pipeline", "Step", "StepKind", "Writer", "WriterKind"}

    def _scan_config_names(obj: Any) -> None:
        # If obj is None, skip.
        if obj is None:
            return
        cls_name = type(obj).__name__
        # If obj's class name is in _TIDERS_CONFIG_CLASSES, add to config_names.
        if cls_name in _TIDERS_CONFIG_CLASSES:
            config_names.add(cls_name)
        # If obj is a dataclass, recurse into each field's value.
        if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
            for f in dataclasses.fields(obj):
                _scan_config_names(getattr(obj, f.name))
        # If obj is a list, recurse into each item.
        elif isinstance(obj, list):
            for item in obj:
                _scan_config_names(item)

    # Step classes
    for step in steps:
        _scan_config_names(step)

    # Writer classes
    for w in (writer if isinstance(writer, list) else [writer]):
        _scan_config_names(w)

    # Table_aliases
    _scan_config_names(table_aliases)

    sorted_config = sorted(config_names)

    # --- from tiders ---

    # Always include
    tiders_imports: list[str] = ["from tiders import run_pipeline"]
    tiders_imports.append(
        f"from tiders.config import {', '.join(sorted_config)}"
    )

    # --- tiders_core.ingest ---
    core_names = {"ProviderConfig", "ProviderKind", "Query", "QueryKind"}
    tiders_imports.append(
        f"from tiders_core.ingest import {', '.join(sorted(core_names))}"
    )
    if has_evm:
        tiders_imports.append("from tiders_core.ingest import evm")
    if has_svm:
        tiders_imports.append("from tiders_core.ingest import svm")

    # --- tiders_core ABI helpers ---
    if has_abi_paths:
        tiders_imports.append(
            "from tiders_core import evm_abi_events, evm_abi_functions"
        )

    # --- svm_decode imports if needed ---
    svm_decode_names: set[str] = set()
    for step in steps:
        if dataclasses.is_dataclass(step) and not isinstance(step, type):
            cfg = step.config
            cfg_cls = type(cfg).__name__
            if cfg_cls == "SvmDecodeInstructionsConfig":
                # InstructionSignature and its nested types
                svm_decode_names.add("InstructionSignature")
            elif cfg_cls == "SvmDecodeLogsConfig":
                svm_decode_names.add("LogSignature")
    if svm_decode_names:
        tiders_imports.append(
            f"from tiders_core.svm_decode import {', '.join(sorted(svm_decode_names))}"
        )

    import_lines.append("")
    import_lines.extend(tiders_imports)
    return import_lines


# ---------------------------------------------------------------------------
# Contract code generation
# ---------------------------------------------------------------------------


def _contracts_to_code(contracts: dict[str, Any]) -> list[str]:
    """Generate Python code lines for contract definitions.

    For contracts with an ABI path, generates code that loads the ABI file
    and parses events/functions using ``evm_abi_events`` / ``evm_abi_functions``.
    For contracts without an ABI path, inlines the resolved event/function data
    as dict literals.
    """
    lines: list[str] = []

    for name, contract in contracts.items():
        if contract.abi_path:
            lines.append(f"# Contract: {name}")
            lines.append(f"{name}_abi_path = Path({repr(contract.abi_path)})")
            lines.append(f"{name}_abi_json = {name}_abi_path.read_text()")
            lines.append(
                f"{name}_events = {{"
                f"ev.name: {{'topic0': ev.topic0, 'signature': ev.signature}} "
                f"for ev in evm_abi_events({name}_abi_json)}}"
            )
            lines.append(
                f"{name}_functions = {{"
                f"fn.name: {{'selector': fn.selector, 'signature': fn.signature}} "
                f"for fn in evm_abi_functions({name}_abi_json)}}"
            )
        else:
            lines.append(f"# Contract: {name}")
            lines.append(f"{name}_events = {repr(contract.events)}")
            lines.append(f"{name}_functions = {repr(contract.functions)}")

        if contract.address:
            lines.append(f"{name}_address = {repr(contract.address)}")

        lines.append("")

    return lines


# ---------------------------------------------------------------------------
# Serializer: object → Python code string
# ---------------------------------------------------------------------------


def to_code(obj: Any, env_map: dict[str, str], indent: int = 0) -> str:
    """Recursively convert a tiders object into its Python source representation.

    i.e.:
    Input:
    ```
        obj = Step(
            kind=StepKind.EVM_DECODE_EVENTS,
            config=EvmDecodeEventsConfig(
                event_signature="Transfer(address indexed,address indexed,uint256)",
                filter_by_topic0=True,
            ),
            name="decode_transfers",
        )
        env_map = {"RPC_URL": "https://eth.example.com"}
    ```

    Output String:
        Step(
            kind=StepKind.EVM_DECODE_EVENTS,
            config=EvmDecodeEventsConfig(
                event_signature='Transfer(address indexed,address indexed,uint256)',
                filter_by_topic0=True,
            ),
            name='decode_transfers',
        )

    Args:
        obj: The object to serialize.
        env_map: Mapping ``{var_name: resolved_value}`` for env var back-substitution.
        indent: Current indentation depth (each level adds ``_INDENT``).

    Returns:
        A Python expression string.
    """
    pad = _INDENT * indent
    inner_pad = _INDENT * (indent + 1)

    # --- None ---
    if obj is None:
        return "None"

    # --- bool (must come before int) ---
    if isinstance(obj, bool):
        return "True" if obj else "False"

    # --- Enum (must come before str, since str-based enums pass isinstance str check) ---
    if isinstance(obj, Enum):
        cls_name = type(obj).__name__
        return f"{cls_name}.{obj.name}"

    # --- int / float ---
    if isinstance(obj, (int, float)):
        return repr(obj)

    # --- str: check env var map first ---
    if isinstance(obj, str):
        # Reverse-lookup: if this value came from an env var, emit os.environ.get(...)
        for var_name, resolved in env_map.items():
            if obj == resolved:
                return f"os.environ.get({repr(var_name)})"
        return repr(obj)

    # --- pyarrow DataType ---
    if isinstance(obj, pa.DataType):
        return _pa_type_to_code(obj)

    # --- list ---
    if isinstance(obj, list):
        if not obj:
            return "[]"
        items = [to_code(item, env_map, indent + 1) for item in obj]
        if len(items) == 1 and "\n" not in items[0]:
            return f"[{items[0]}]"
        inner = (",\n" + inner_pad).join(items)
        return f"[\n{inner_pad}{inner},\n{pad}]"

    # --- dict ---
    if isinstance(obj, dict):
        if not obj:
            return "{}"
        pairs = [
            f"{repr(k)}: {to_code(v, env_map, indent + 1)}" for k, v in obj.items()
        ]
        if len(pairs) <= 2 and all("\n" not in p for p in pairs):
            return "{" + ", ".join(pairs) + "}"
        inner = (",\n" + inner_pad).join(pairs)
        return f"{{\n{inner_pad}{inner},\n{pad}}}"

    # --- dataclass ---
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return _dataclass_to_code(obj, env_map, indent)

    # --- Rust / pyo3 objects (ProviderConfig, Query, evm.Query, etc.) ---
    # These are not dataclasses but have inspectable signatures.
    cls = type(obj)
    try:
        sig = inspect.signature(cls)
    except (ValueError, TypeError):
        return repr(obj)

    return _sig_obj_to_code(obj, cls, sig, env_map, indent)


def _pa_type_to_code(t: pa.DataType) -> str:
    """Convert a pyarrow DataType to its ``pa.<func>(...)`` constructor expression."""
    s = str(t)  # e.g. "decimal128(38, 0)", "int64", "utf8"
    # Strip spaces inside parentheses for a clean call
    # e.g. "decimal128(38, 0)" -> "pa.decimal128(38, 0)"
    return f"pa.{s}"


def _dataclass_to_code(obj: Any, env_map: dict[str, str], indent: int) -> str:
    """Serialize a Python dataclass, skipping fields that equal their default."""
    pad = _INDENT * indent
    inner_pad = _INDENT * (indent + 1)
    cls_name = _qualified_class_name(type(obj))

    parts: list[str] = []
    for f in dataclasses.fields(obj):
        value = getattr(obj, f.name)
        # Determine default
        if f.default is not dataclasses.MISSING:
            default = f.default
        elif f.default_factory is not dataclasses.MISSING:  # type: ignore[misc]
            default = f.default_factory()  # type: ignore[misc]
        else:
            default = dataclasses.MISSING

        if default is not dataclasses.MISSING and _equals_default(value, default):
            continue

        code = to_code(value, env_map, indent + 1)
        parts.append(f"{f.name}={code}")

    if not parts:
        return f"{cls_name}()"
    if len(parts) == 1 and "\n" not in parts[0]:
        return f"{cls_name}({parts[0]})"
    inner = (",\n" + inner_pad).join(parts)
    return f"{cls_name}(\n{inner_pad}{inner},\n{pad})"


def _sig_obj_to_code(
    obj: Any,
    cls: type,
    sig: inspect.Signature,
    env_map: dict[str, str],
    indent: int,
) -> str:
    """Serialize a non-dataclass object using its ``__init__`` signature."""
    pad = _INDENT * indent
    inner_pad = _INDENT * (indent + 1)

    # Use the module-qualified name if the class lives in a submodule (e.g. evm.Query)
    cls_name = _qualified_class_name(cls)

    parts: list[str] = []
    for param_name, param in sig.parameters.items():
        if param_name in ("self", "args", "kwargs"):
            continue

        value = getattr(obj, param_name, None)

        # Check default
        if param.default is not inspect.Parameter.empty:
            default = param.default
            if _equals_default(value, default):
                continue

        code = to_code(value, env_map, indent + 1)
        parts.append(f"{param_name}={code}")

    if not parts:
        return f"{cls_name}()"
    if len(parts) == 1 and "\n" not in parts[0]:
        return f"{cls_name}({parts[0]})"
    inner = (",\n" + inner_pad).join(parts)
    return f"{cls_name}(\n{inner_pad}{inner},\n{pad})"


def _equals_default(value: Any, default: Any) -> bool:
    """Return True if ``value`` equals ``default``, handling list/dict factories."""
    try:
        if isinstance(value, list) and isinstance(default, list):
            return value == default
        if isinstance(value, dict) and isinstance(default, dict):
            return value == default
        return bool(value == default)
    except Exception:
        return False


def _qualified_class_name(cls: type) -> str:
    """Return the Python expression used to refer to a class in generated code.

    For classes in ``tiders_core.ingest.evm`` / ``svm``, use the ``evm.Foo``
    / ``svm.Foo`` qualified form since we import those modules directly.
    """
    module = getattr(cls, "__module__", "") or ""
    name = cls.__qualname__

    if "ingest.evm" in module or module.endswith(".evm"):
        return f"evm.{name}"
    if "ingest.svm" in module or module.endswith(".svm"):
        return f"svm.{name}"
    return name


# ---------------------------------------------------------------------------
# SQL step runner code generation
# ---------------------------------------------------------------------------


def _sql_step_to_runner_def(step_name: str, queries: list[str]) -> tuple[str, str]:
    """Return (runner_func_name, function_source) for a SQL step."""
    safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", step_name)
    func_name = f"{safe_name}_runner"

    body_lines = [
        f"def {func_name}(ctx, tables, context):",
    ]
    for q in queries:
        q_stripped = q.strip()
        body_lines.append(f"    ctx.sql({repr(q_stripped)})")
    # Return empty dict to preserve existing tables (DataFusion approach)
    body_lines.append("    return {}")

    return func_name, "\n".join(body_lines)


# ---------------------------------------------------------------------------
# Python file step: extract function source
# ---------------------------------------------------------------------------


def _python_file_step_to_source(step: Any) -> tuple[str, str]:
    """Return (func_name, source_code) by inspecting the loaded runner.

    The runner is stored in ``step.config.runner``.
    """
    runner = step.config.runner
    func_name = runner.__name__
    try:
        source = inspect.getsource(runner)
        # Dedent in case the function is indented inside a class or another function
        source = textwrap.dedent(source)
    except (OSError, TypeError):
        source = f"# Could not extract source for '{func_name}'. Define it manually.\ndef {func_name}(ctx, tables, context):\n    raise NotImplementedError\n"
    return func_name, source


def _runner_step_type(raw_step: dict[str, Any]) -> str:
    """Return the config class name for a sql / python_file step."""
    kind_str = raw_step.get("kind", "sql")
    if kind_str == "sql":
        return "DataFusionStepConfig"
    # python_file: look at step_type field
    step_type = raw_step.get("config", {}).get("step_type", "datafusion")
    if step_type == "polars":
        return "PolarsStepConfig"
    if step_type == "pandas":
        return "PandasStepConfig"
    return "DataFusionStepConfig"


# ---------------------------------------------------------------------------
# Has-pyarrow-type detection
# ---------------------------------------------------------------------------


def _has_pa_types(obj: Any) -> bool:
    """Return True if any field in the object tree is a pyarrow DataType."""
    if isinstance(obj, pa.DataType):
        return True
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return any(_has_pa_types(getattr(obj, f.name)) for f in dataclasses.fields(obj))
    if isinstance(obj, list):
        return any(_has_pa_types(item) for item in obj)
    if isinstance(obj, dict):
        return any(_has_pa_types(v) for v in obj.values())
    return False


def _has_pa_types_in_steps(steps: list[Any]) -> bool:
    return any(_has_pa_types(step) for step in steps)
