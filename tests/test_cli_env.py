from __future__ import annotations

import pytest

from tiders.cli.env import (
    load_and_substitute,
    resolve_env_path,
    substitute_env_vars,
)


class TestSubstituteEnvVars:
    def test_plain_string_unchanged(self):
        assert substitute_env_vars("hello world") == "hello world"

    def test_single_var(self, monkeypatch):
        monkeypatch.setenv("MY_VAR", "replaced")
        assert substitute_env_vars("${MY_VAR}") == "replaced"

    def test_var_in_middle_of_string(self, monkeypatch):
        monkeypatch.setenv("HOST", "localhost")
        assert substitute_env_vars("http://${HOST}:8080") == "http://localhost:8080"

    def test_multiple_vars(self, monkeypatch):
        monkeypatch.setenv("USER", "admin")
        monkeypatch.setenv("PASS", "secret")
        result = substitute_env_vars("${USER}:${PASS}")
        assert result == "admin:secret"

    def test_undefined_var_raises(self, monkeypatch):
        monkeypatch.delenv("UNDEFINED_VAR_XYZ", raising=False)
        with pytest.raises(ValueError, match="UNDEFINED_VAR_XYZ"):
            substitute_env_vars("${UNDEFINED_VAR_XYZ}")

    def test_nested_dict(self, monkeypatch):
        monkeypatch.setenv("DB_PATH", "/data/my.db")
        config = {"writer": {"config": {"path": "${DB_PATH}"}}}
        result = substitute_env_vars(config)
        assert result == {"writer": {"config": {"path": "/data/my.db"}}}

    def test_list(self, monkeypatch):
        monkeypatch.setenv("ADDR", "0xabc")
        config = ["${ADDR}", "plain"]
        result = substitute_env_vars(config)
        assert result == ["0xabc", "plain"]

    def test_non_string_values_unchanged(self):
        config = {"number": 42, "flag": True, "nothing": None}
        assert substitute_env_vars(config) == config


class TestResolveEnvPath:
    def test_default_env_path(self, tmp_path):
        config = {}
        result = resolve_env_path(tmp_path, config)
        assert result == tmp_path / ".env"

    def test_custom_env_path_absolute(self, tmp_path):
        custom = tmp_path / "custom.env"
        config = {"environment_path": str(custom)}
        result = resolve_env_path(tmp_path, config)
        assert result == custom

    def test_custom_env_path_relative(self, tmp_path):
        config = {"environment_path": "envs/prod.env"}
        result = resolve_env_path(tmp_path, config)
        assert result == tmp_path / "envs" / "prod.env"

    def test_environment_path_removed_from_config(self, tmp_path):
        config = {"environment_path": "foo.env", "other": "keep"}
        resolve_env_path(tmp_path, config)
        assert "environment_path" not in config
        assert config["other"] == "keep"


class TestLoadAndSubstitute:
    def test_full_flow(self, tmp_path, monkeypatch):
        # Create .env file
        env_file = tmp_path / ".env"
        env_file.write_text("RPC_URL=https://eth.example.com\n")

        # Create a fake yaml path
        yaml_path = tmp_path / "tiders.yaml"
        yaml_path.touch()

        config = {
            "provider": {"url": "${RPC_URL}"},
            "query": {"from_block": 100},
        }

        result = load_and_substitute(yaml_path, config)
        assert result["provider"]["url"] == "https://eth.example.com"
        assert result["query"]["from_block"] == 100

    def test_custom_env_path(self, tmp_path, monkeypatch):
        env_file = tmp_path / "custom" / "my.env"
        env_file.parent.mkdir()
        env_file.write_text("SECRET=42\n")

        yaml_path = tmp_path / "tiders.yaml"
        yaml_path.touch()

        config = {
            "environment_path": "custom/my.env",
            "value": "${SECRET}",
        }

        result = load_and_substitute(yaml_path, config)
        assert result["value"] == "42"
        assert "environment_path" not in result

    def test_missing_env_file_no_error(self, tmp_path, monkeypatch):
        """If .env doesn't exist, that's fine — just don't load it."""
        yaml_path = tmp_path / "tiders.yaml"
        yaml_path.touch()

        monkeypatch.setenv("ALREADY_SET", "yes")
        config = {"val": "${ALREADY_SET}"}

        result = load_and_substitute(yaml_path, config)
        assert result["val"] == "yes"
