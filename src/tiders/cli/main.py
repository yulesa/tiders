"""CLI entry point for tiders.

Provides the ``tiders`` console command with subcommands:

- ``tiders start [CONFIG_PATH]`` — run a pipeline from a YAML config file.
- ``tiders --help`` — show version and available commands.

Execution flow for ``tiders start``
------------------------------------
1. ``main``                — click group entry point, handles --version/--help
2. ``start``               — resolve config path, parse, build pipeline, run it
3.   ``_setup_logging``    — configure structured logging (timestamps + levels)
4.   ``_auto_discover_config`` — find tiders.yaml/yml in cwd if no path given
5.     ``_is_tiders_config``   — check if a YAML file has provider: + query: keys
6.       ``_load_yaml``        — load and validate a YAML file as a dict
7.   ``_get_version``      — read package version from importlib.metadata
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path
from typing import Optional

import click

from .env import load_and_substitute
from .tiders_yaml_parser import (
    YamlConfigError,
    parse_tiders_yaml,
)


def _setup_logging() -> None:
    """Configure structured logging with timestamps and level prefixes."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


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


def _is_tiders_config(path: Path) -> bool:
    """Check if a YAML file looks like a tiders config (has provider: and query: keys)."""
    try:
        data = _load_yaml(path)
        return "provider" in data and "query" in data
    except Exception:
        return False


def _auto_discover_config() -> Path:
    """Search the current directory for a tiders YAML config file.

    Checks for tiders.yaml / tiders.yml first, then falls back to scanning all
    .yaml / .yml files for ones containing ``provider:`` and ``query:`` keys.

    Returns:
        The path to the discovered config file.

    Raises:
        click.ClickException: If zero or multiple configs are found.
    """
    cwd = Path.cwd()

    # Check well-known names first
    for name in ("tiders.yaml", "tiders.yml"):
        candidate = cwd / name
        if candidate.is_file():
            return candidate

    # Scan all YAML files in current directory
    all_yamls = set(cwd.glob("*.yaml")) | set(cwd.glob("*.yml"))
    candidates = [p for p in sorted(all_yamls) if _is_tiders_config(p)]

    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) == 0:
        raise click.ClickException(
            "No tiders config found in current directory. "
            "Create a tiders.yaml or pass a path: tiders start <config.yaml>"
        )
    names = [c.name for c in candidates]
    raise click.ClickException(
        f"Multiple tiders configs found: {names}. "
        f"Please specify which one to use: tiders start <config.yaml>"
    )


def _get_version() -> str:
    """Return the tiders package version."""
    try:
        from importlib.metadata import version

        return version("tiders")
    except Exception:
        return "dev"


@click.group()
@click.version_option(version=_get_version(), prog_name="tiders")
def main() -> None:
    """Tiders — blockchain data pipeline framework."""


@main.command()
@click.argument("config_path", required=False, type=click.Path(exists=False))
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
    config_path: Optional[str],
    from_block: Optional[int],
    to_block: Optional[int],
    env_file: Optional[str],
) -> None:
    """Run a pipeline from a YAML config file.

    If CONFIG_PATH is omitted, searches the current directory for a tiders
    config (tiders.yaml, tiders.yml, or any .yaml/.yml with provider: and
    query: keys).
    """
    _setup_logging()
    logger = logging.getLogger("tiders.cli")

    # Resolve config path
    if config_path is not None:
        yaml_path = Path(config_path)
        if not yaml_path.is_file():
            raise click.ClickException(f"Config file not found: {config_path}")
    else:
        yaml_path = _auto_discover_config()

    yaml_path = yaml_path.resolve()
    logger.info(f"Using config: {yaml_path}")

    # Load and parse YAML
    raw_config = _load_yaml(yaml_path)

    # Handle --env-file override
    if env_file is not None:
        raw_config["environment_path"] = str(Path(env_file).resolve())

    # Env substitution
    try:
        raw_config = load_and_substitute(yaml_path, raw_config)
    except (ValueError, ImportError) as exc:
        raise click.ClickException(str(exc))

    # Parse all sections
    try:
        yaml_dir = yaml_path.parent
        project, provider, query, steps, writer, table_aliases, _contracts = (
            parse_tiders_yaml(raw_config, yaml_dir)
        )

        # Apply --from-block / --to-block overrides
        if from_block is not None:
            query.params.from_block = from_block
        if to_block is not None:
            query.params.to_block = to_block
    except YamlConfigError as exc:
        raise click.ClickException(f"Config error: {exc}")
    except KeyError as exc:
        raise click.ClickException(f"Missing required config section: {exc}")

    # Build pipeline
    from tiders.config import Pipeline

    pipeline = Pipeline(
        provider=provider,
        query=query,
        writer=writer,
        steps=steps,
        table_aliases=table_aliases,
    )

    logger.info(f"Starting {project.name} pipeline...")

    # Run
    from tiders.pipeline import run_pipeline

    try:
        asyncio.run(run_pipeline(pipeline, pipeline_name=yaml_path.stem))
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user.")
        sys.exit(130)
    except Exception as exc:
        logger.error(f"Pipeline failed: {exc}", exc_info=True)
        sys.exit(1)

    logger.info("Pipeline completed successfully.")
