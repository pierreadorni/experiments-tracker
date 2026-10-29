import json
from types import SimpleNamespace

from experimenttracker.doctor import diagnose


def test_preflight_preserves_absent_state_and_never_echoes_credentials(config):
    config.api_token_file.chmod(0o600)
    config.web_dir.mkdir()
    (config.web_dir / "index.html").write_text("built")
    result = diagnose(config)
    assert result["ok"]
    assert not config.state_dir.exists() and not config.backup_dir.exists()
    assert config.api_token_file.read_text() not in json.dumps(result)
    config.api_token_file.chmod(0o644)
    result = diagnose(config)
    assert not result["ok"]
    assert next(item for item in result["checks"] if item["name"] == "api_credential")["ok"] is False
    assert config.api_token_file.stat().st_mode & 0o777 == 0o644


def test_live_preflight_only_inspects_filesystem_and_runtime_version(config, monkeypatch):
    config.mode = "live"
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        return SimpleNamespace(stdout="nfs\n" if command[0] == "stat" else "codex-cli 0.156.1\n")

    monkeypatch.setattr("experimenttracker.doctor.subprocess.run", run)
    monkeypatch.setattr("experimenttracker.doctor.shutil.which", lambda name: "/bin/" + name)
    result = diagnose(config)
    assert calls == [["stat", "-f", "-c", "%T", str(config.state_dir.parent)], ["codex", "--version"]]
    assert next(item for item in result["checks"] if item["name"] == "state_storage")["ok"] is False
    assert not config.state_dir.exists()
