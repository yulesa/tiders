from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

ENV_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")


def load_env_file(env_path: Path) -> None:
    """Load a .env file into os.environ using python-dotenv."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        raise ImportError(
            "python-dotenv is required for CLI mode: pip install tiders[cli]"
        )
    if env_path.is_file():
        load_dotenv(env_path, override=True)


def resolve_env_path(yaml_dir: Path, raw_config: dict[str, Any]) -> Path:
    """Determine the .env file path from config or default to yaml_dir/.env."""
    env_path_str = raw_config.pop("environment_path", None)
    if env_path_str is not None:
        path = Path(env_path_str)
        if not path.is_absolute():
            path = yaml_dir / path
        return path
    return yaml_dir / ".env"


def _substitute_string(value: str) -> str:
    """Replace ${VAR} patterns in a string with environment variable values."""

    def replacer(match: re.Match) -> str:
        var_name = match.group(1)
        env_value = os.environ.get(var_name)
        if env_value is None:
            raise ValueError(
                f"Environment variable '{var_name}' is not defined. "
                f"Set it in your .env file or shell environment."
            )
        return env_value

    return ENV_VAR_PATTERN.sub(replacer, value)


def substitute_env_vars(obj: Any) -> Any:
    """Recursively walk a parsed YAML structure and substitute ${VAR} patterns."""
    if isinstance(obj, str):
        return _substitute_string(obj)
    if isinstance(obj, dict):
        return {k: substitute_env_vars(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [substitute_env_vars(item) for item in obj]
    return obj


def load_and_substitute(yaml_path: Path, raw_config: dict[str, Any]) -> dict[str, Any]:
    """Load .env and substitute env vars in the parsed YAML config.

    This is the main entry point for Commit 2's functionality:
    1. Resolve the .env file path (from environment_path key or default)
    2. Load the .env file into os.environ
    3. Recursively substitute ${VAR} patterns in all string values
    """
    yaml_dir = yaml_path.parent.resolve()
    env_path = resolve_env_path(yaml_dir, raw_config)
    load_env_file(env_path)
    return substitute_env_vars(raw_config)
