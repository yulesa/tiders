"""CLI entry point for tiders.

Provides the ``tiders`` console command with subcommands:

- ``tiders start [YAML_PATH]``   — run a pipeline from a YAML file.
- ``tiders codegen [YAML_PATH]`` — generate a Python script from a YAML file.
- ``tiders --help``                — show version and available commands.

Command implementations
-----------------------
- ``start``    — resolve YAML, build Pipeline, run with asyncio
- ``codegen``  — resolve YAML, generate equivalent Python script, write to file

Resolving the tiders YAML
--------------
- ``_resolve_tiders_yaml``   — resolve path, load YAML, substitute env vars, parse into tiders objects
- ``_auto_discover_yaml``  — find tiders.yaml/yml in cwd if no path given
- ``_is_tiders_yaml``      — check if a YAML file has ``provider:`` + ``query:`` keys
- ``_load_yaml``             — load and validate a YAML file as a dict
- ``_collect_env_var_names`` — scan raw YAML for ``${VAR}`` patterns and collect var names
- ``_collect_env_var_names_recursive`` — recursive helper for scanning YAML structures
- ``_collect_env_vars``      — build a {var_name: value} map from os.environ for a set of variable names (after .env loading)

Helpers
--------------
- ``_get_version``           — read package version from importlib.metadata
- ``_setup_logging``         — configure structured logging (timestamps + levels)
- ``_project_name_to_filename`` — converts project name to camelCase filename

"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import re
from pathlib import Path
from typing import Optional, Any

import click

from .abi_fetching import fetch_abi_single, fetch_abis_from_yaml
from .codegen import generate
from .env import load_and_substitute
from .tiders_yaml_parser import YamlConfigError, parse_tiders_yaml

from .env import ENV_VAR_PATTERN


# ---------------------------------------------------------------------------
# YAML file loading and parsing
# ---------------------------------------------------------------------------


def _resolve_tiders_yaml(
    yaml_path: Optional[str],
    env_file: Optional[str],
    *,
    is_codegen: bool = False,
) -> tuple:
    """Create all tiders objects from a YAML file.

    Resolves the YAML path, loads the file, applies env-file override,
    substitutes environment variables, and parses all pipeline sections.

    Args:
        yaml_path: Optional explicit path to the YAML file.
        env_file: Optional path to a .env file override.
        is_codegen: When ``True``, also returns the raw steps list and env var
            map (needed by ``codegen``).

    Returns:
        ``(yaml_resolved_path, project, provider, query, steps, writer, table_aliases)``
        plus ``(raw_steps, env_map, contracts)`` appended when ``is_codegen=True``.
    """
    if yaml_path is not None:
        yaml_resolved_path = Path(yaml_path)
        if not yaml_resolved_path.is_file():
            raise click.ClickException(f"YAML file not found: {yaml_resolved_path}")
    else:
        yaml_resolved_path = _auto_discover_yaml()

    yaml_resolved_path = yaml_resolved_path.resolve()
    raw_yaml = _load_yaml(yaml_resolved_path)

    if env_file is not None:
        raw_yaml["environment_path"] = str(Path(env_file).resolve())

    # Collect env var names before substitution replaces ${VAR} patterns
    env_var_names = _collect_env_var_names(raw_yaml) if is_codegen else set()

    try:
        raw_yaml = load_and_substitute(yaml_resolved_path, raw_yaml)
    except (ValueError, ImportError) as exc:
        raise click.ClickException(str(exc))

    try:
        yaml_dir = yaml_resolved_path.parent
        project, provider, query, steps, writer, table_aliases, contracts = (
            parse_tiders_yaml(raw_yaml, yaml_dir)
        )
    except YamlConfigError as exc:
        raise click.ClickException(f"Config error: {exc}")
    except KeyError as exc:
        raise click.ClickException(f"Missing required YAML section: {exc}")

    result = (
        yaml_resolved_path,
        project,
        provider,
        query,
        steps,
        writer,
        table_aliases,
    )

    if is_codegen:
        raw_steps: list[dict] = raw_yaml.get("steps", [])
        if not isinstance(raw_steps, list):
            raw_steps = []
        env_map = _collect_env_vars(env_var_names)
        return result + (raw_steps, env_map, contracts)
    return result


# ---------------------------------------------------------------------------
# Auto-discovering YAML files in the current directory
# ---------------------------------------------------------------------------


def _auto_discover_yaml() -> Path:
    """Search the current directory for a tiders YAML file.

    Checks for tiders.yaml / tiders.yml first, then falls back to scanning all
    .yaml / .yml files for ones containing ``provider:`` and ``query:`` keys.

    Returns:
        The path to the discovered YAML file.

    Raises:
        click.ClickException: If zero or multiple YAML files are found.
    """
    cwd = Path.cwd()

    # Check well-known names first
    for name in ("tiders.yaml", "tiders.yml"):
        candidate = cwd / name
        if candidate.is_file():
            return candidate

    # Scan all YAML files in current directory
    all_yamls = set(cwd.glob("*.yaml")) | set(cwd.glob("*.yml"))
    candidates = [p for p in sorted(all_yamls) if _is_tiders_yaml(p)]

    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) == 0:
        raise click.ClickException(
            "No tiders YAML found in current directory. "
            "Create a tiders.yaml or pass a path: tiders start <pipeline.yaml>"
        )
    names = [c.name for c in candidates]
    raise click.ClickException(
        f"Multiple tiders YAML files found: {names}. "
        f"Please specify which one to use: tiders start <pipeline.yaml>"
    )


def _is_tiders_yaml(path: Path) -> bool:
    """Check if a YAML file looks like a tiders pipeline (has provider: and query: keys)."""
    try:
        data = _load_yaml(path)
        return "provider" in data and "query" in data
    except Exception:
        return False


def _load_yaml(path: Path) -> dict:
    """Load a YAML file and return the parsed dict."""
    try:
        import yaml
    except ImportError:
        raise click.ClickException(
            "PyYAML is required for CLI mode: pip install tiders[cli]"
        )
    with open(path) as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise click.ClickException(
            f"{path}: YAML root must be a mapping, got {type(data).__name__}"
        )
    return data


# ---------------------------------------------------------------------------
# Env var collection (run on raw YAML *before* substitution)
# ---------------------------------------------------------------------------


def _collect_env_var_names(obj: Any) -> set[str]:
    """Scan a raw YAML structure for ``${VAR}`` patterns and return the variable names."""
    names: set[str] = set()
    _collect_env_var_names_recursive(obj, names)
    return names


def _collect_env_var_names_recursive(obj: Any, names: set[str]) -> None:
    if isinstance(obj, str):
        for match in ENV_VAR_PATTERN.finditer(obj):
            names.add(match.group(1))
    elif isinstance(obj, dict):
        for v in obj.values():
            _collect_env_var_names_recursive(v, names)
    elif isinstance(obj, list):
        for item in obj:
            _collect_env_var_names_recursive(item, names)


def _collect_env_vars(var_names: set[str]) -> dict[str, str]:
    """Build a ``{var_name: resolved_value}`` map from ``os.environ``.

    Call this *after* loading the .env file so that values loaded from .env
    are included.  Variables missing from ``os.environ`` are silently skipped
    (they will have already caused an error during substitution).
    """
    import os

    return {name: os.environ[name] for name in var_names if name in os.environ}


# ---------------------------------------------------------------------------
# Helper functions for CLI commands
# ---------------------------------------------------------------------------


def _setup_logging() -> None:
    """Configure structured logging with timestamps and level prefixes."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _get_version() -> str:
    """Return the tiders package version."""
    try:
        from importlib.metadata import version

        return version("tiders")
    except Exception:
        return "dev"


def _project_name_to_filename(name: str) -> str:
    """Convert a project name to a snake_case Python filename.

    Examples:
        "rETH_transfer"        → "reth_transfer.py"
        "reth_transfer_nocode" → "reth_transfer_nocode.py"
        "MyPipeline"           → "my_pipeline.py"
    """
    # Insert underscore before uppercase letters that follow a lowercase letter or digit
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    # Replace non-alphanumeric chars with underscores
    s = re.sub(r"[^a-zA-Z0-9]+", "_", s)
    # Collapse multiple underscores and strip leading/trailing
    s = re.sub(r"_+", "_", s).strip("_").lower()
    if not s:
        return "pipeline.py"
    return f"{s}.py"


# ---------------------------------------------------------------------------
# CLI command implementations
# ---------------------------------------------------------------------------


@click.group()
@click.version_option(version=_get_version(), prog_name="tiders")
def main() -> None:
    """Tiders — blockchain data pipeline framework."""


@main.command()
@click.argument("yaml_path", required=False, type=click.Path(exists=False))
@click.option(
    "--from-block", type=int, default=None, help="Override the starting block number."
)
@click.option(
    "--to-block", type=int, default=None, help="Override the ending block number."
)
@click.option(
    "--env-file",
    type=click.Path(exists=True, dir_okay=False),
    default=None,
    help="Path to .env file (overrides default discovery).",
)
def start(
    yaml_path: Optional[str],
    from_block: Optional[int],
    to_block: Optional[int],
    env_file: Optional[str],
) -> None:
    """Run a pipeline from a YAML file.

    If YAML_PATH is omitted, searches the current directory for a tiders
    pipeline (tiders.yaml, tiders.yml, or any .yaml/.yml with provider: and
    query: keys).
    """
    _setup_logging()
    logger = logging.getLogger("tiders.cli")

    yaml_resolved_path, project, provider, query, steps, writer, table_aliases = (
        _resolve_tiders_yaml(yaml_path, env_file)
    )
    logger.info(f"Using YAML: {yaml_resolved_path}")

    if from_block is not None:
        query.params.from_block = from_block
    if to_block is not None:
        query.params.to_block = to_block

    from tiders.config import Pipeline
    from tiders.pipeline import run_pipeline

    pipeline = Pipeline(
        provider=provider,
        query=query,
        writer=writer,
        steps=steps,
        table_aliases=table_aliases,
    )

    logger.info(f"Starting {project.name} pipeline...")

    try:
        asyncio.run(run_pipeline(pipeline, pipeline_name=yaml_resolved_path.stem))
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user.")
        sys.exit(130)
    except Exception as exc:
        logger.error(f"Pipeline failed: {exc}", exc_info=True)
        sys.exit(1)

    logger.info("Pipeline completed successfully.")


@main.command()
@click.argument("yaml_path", required=False, type=click.Path(exists=False))
@click.option(
    "--output",
    "-o",
    type=click.Path(dir_okay=False),
    default=None,
    help="Output file path. Defaults to <ProjectName>.py in the current directory.",
)
@click.option(
    "--env-file",
    type=click.Path(exists=True, dir_okay=False),
    default=None,
    help="Path to .env file (overrides default discovery).",
)
def codegen(
    yaml_path: Optional[str],
    output: Optional[str],
    env_file: Optional[str],
) -> None:
    """Generate a Python script from a YAML pipeline file.

    Reads the YAML file (same discovery rules as ``tiders start``) and
    writes an equivalent inline Python script that constructs and runs the
    same pipeline using the tiders Python SDK.

    If OUTPUT is omitted, the file is saved as ``<project_name>.py`` in the
    current working directory (project name converted to snake_case from the
    YAML ``project.name`` field).
    """
    (
        yaml_resolved_path,
        project,
        provider,
        query,
        steps,
        writer,
        table_aliases,
        raw_steps,
        env_map,
        contracts,
    ) = _resolve_tiders_yaml(yaml_path, env_file, is_codegen=True)

    try:
        code = generate(
            project=project,
            contracts=contracts,
            provider=provider,
            query=query,
            steps=steps,
            writer=writer,
            table_aliases=table_aliases,
            raw_steps=raw_steps,
            env_map=env_map,
            yaml_path=yaml_resolved_path,
        )
    except Exception as exc:
        raise click.ClickException(f"Code generation failed: {exc}")

    out_path = (
        Path(output)
        if output is not None
        else Path.cwd() / _project_name_to_filename(project.name)
    )
    out_path.write_text(code, encoding="utf-8")
    click.echo(f"Generated: {out_path}")


@main.command()
@click.option(
    "--output",
    "-o",
    type=click.Path(dir_okay=True),
    default=None,
    help="Output path. In single-address mode: file path. In YAML mode: directory.",
)
@click.option(
    "--address",
    type=str,
    default=None,
    help="Contract address to fetch ABI for (single-address mode).",
)
@click.option(
    "--chain-id",
    type=int,
    default=1,
    help="Chain ID of the contract (default: 1, Ethereum mainnet).",
)
@click.option(
    "--env-file",
    type=click.Path(exists=True, dir_okay=False),
    default=None,
    help="Path to .env file (overrides default discovery).",
)
@click.option(
    "--yaml-path",
    type=click.Path(exists=True, dir_okay=False),
    default=None,
    help="Path to YAML file with contract declarations.",
)
@click.option(
    "--source",
    type=click.Choice(["sourcify", "etherscan"]),
    default="sourcify",
    help="ABI source to try first (default: sourcify). Falls back to the other.",
)
def abi(
    output: Optional[str],
    address: Optional[str],
    chain_id: int,
    env_file: Optional[str],
    yaml_path: Optional[str],
    source: str,
) -> None:
    """Fetch contract ABIs from Sourcify or Etherscan.

    Three usage modes:

    \b
    1. Single address:  tiders abi --address 0x... [--chain-id 1]
    2. From YAML file:  tiders abi --yaml-path pipeline.yaml
    3. Auto-discover:   tiders abi  (finds tiders.yaml in cwd)

    In YAML mode, iterates through declared contracts and fetches missing ABIs.
    """
    _setup_logging()
    logger = logging.getLogger("tiders.cli")

    if address is not None and yaml_path is not None:
        raise click.UsageError(
            "--address and --yaml-path are mutually exclusive. "
            "Use --address for a single contract or --yaml-path for YAML mode."
        )

    # Load .env file: explicit path, or default to .env in cwd
    from .env import load_env_file

    if env_file is not None:
        load_env_file(Path(env_file))
    else:
        load_env_file(Path.cwd() / ".env")

    etherscan_api_key = os.environ.get("ETHERSCAN_API_KEY")

    # Mode 1: single address
    if address is not None:
        try:
            saved_path = fetch_abi_single(
                address, chain_id, source, etherscan_api_key, output
            )
        except RuntimeError as exc:
            raise click.ClickException(str(exc))
        click.echo(f"Saved ABI: {saved_path}")
        return

    # Mode 2 & 3: YAML-based
    if yaml_path is not None:
        yaml_resolved_path = Path(yaml_path).resolve()
    else:
        yaml_resolved_path = _auto_discover_yaml().resolve()

    logger.info(f"Using YAML: {yaml_resolved_path}")

    try:
        saved = fetch_abis_from_yaml(
            yaml_resolved_path, chain_id, source, etherscan_api_key, output, env_file
        )
    except ValueError as exc:
        raise click.ClickException(str(exc))

    for name, path in saved:
        click.echo(f"Saved ABI for '{name}': {path}")

    if not saved:
        click.echo("No new ABIs to fetch (all contracts already have ABIs).")
