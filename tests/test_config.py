from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from experimenttracker.cli import initialize
from experimenttracker.config import GatewayConfig, QuotaConfig, load_config


@pytest.mark.parametrize(
    "url",
    [
        "http://gateway.test",
        "https://user:password@gateway.test",
        "https://gateway.test/api",
        "https://gateway.test?token=secret",
        "https://gateway.test#secret",
        "https://",
    ],
)
def test_gateway_accepts_only_a_credential_free_https_origin(url):
    with pytest.raises(ValidationError):
        GatewayConfig(url=url, credential_file=Path("/tmp/token"))


def test_credentials_are_explicit_and_recovery_preserves_hysteresis():
    with pytest.raises(ValidationError):
        GatewayConfig(url="https://gateway.test", credential_file=Path("relative-token"))
    with pytest.raises(ValidationError):
        QuotaConfig(five_hour_min_remaining=35, recovery_remaining=(30, 15))
    with pytest.raises(ValidationError):
        QuotaConfig(recovery_remaining=(101, 15))
    assert GatewayConfig(url="https://gateway.test", credential_file=Path("/tmp/token")).url


@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
def test_executor_job_limit_requires_a_positive_integer(limit):
    from experimenttracker.config import SlurmConfig

    with pytest.raises(ValidationError):
        SlurmConfig(max_concurrent_jobs=limit)


def test_executor_job_limit_can_be_unbounded():
    from experimenttracker.config import SlurmConfig

    assert SlurmConfig(max_concurrent_jobs=None).max_concurrent_jobs is None


def test_old_generated_recipe_config_loads_but_other_unknown_keys_are_rejected(tmp_path):
    config = {
        "schema_version": 1,
        "state_dir": str(tmp_path / "state"),
        "job_output_root": str(tmp_path / "outputs"),
        "backup_dir": str(tmp_path / "backups"),
        "recipe_dir": "/obsolete/recipes/toy_compare",
        "web_dir": str(tmp_path / "web"),
        "api_token_file": str(tmp_path / "token"),
    }
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(config))

    loaded = load_config(path)

    assert loaded.web_dir == Path(config["web_dir"])
    assert not hasattr(loaded, "recipe_dir")
    config["unrecognized"] = "still rejected"
    path.write_text(yaml.safe_dump(config))
    with pytest.raises(ValidationError, match="unrecognized"):
        load_config(path)


def test_initialize_writes_recipe_free_config(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "config.yaml"

    initialize(path, "demo", None)

    assert "recipe_dir" not in yaml.safe_load(path.read_text())
