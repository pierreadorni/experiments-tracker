"""The opt-in C4 demo runs through the installed CLI entry point."""

import json
import os
import subprocess
import sys
from pathlib import Path


def test_cli_contract4_demo_completes_in_isolated_temporary_state(tmp_path):
    config = tmp_path / "demo.yaml"
    config.write_text(
        "\n".join(
            [
                "schema_version: 1",
                "mode: demo",
                f"state_dir: {tmp_path / 'configured-state'}",
                f"job_output_root: {tmp_path / 'configured-outputs'}",
                f"backup_dir: {tmp_path / 'configured-backups'}",
                f"web_dir: {tmp_path}",
                f"api_token_file: {tmp_path / 'token'}",
                f"projects_root: {tmp_path / 'configured-projects'}",
                "gateway:",
                "  url: https://gateway.example",
                f"  credential_file: {tmp_path / 'missing-gateway-credential'}",
            ]
        )
        + "\n"
    )
    environment = os.environ.copy()
    source = str((Path(__file__).resolve().parents[1] / "src").resolve())
    environment["PYTHONPATH"] = os.pathsep.join(filter(None, (source, environment.get("PYTHONPATH"))))
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "experimenttracker.cli",
            "--config",
            str(config),
            "demo",
            "--contract",
            "4",
            "--max-seconds",
            "60",
        ],
        capture_output=True,
        text=True,
        timeout=70,
        env=environment,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    receipt = json.loads(completed.stdout.strip().splitlines()[-1])
    assert receipt == {
        "contract": 4,
        "mode": "demo",
        "phase": "complete",
        "report_state": "ready",
        "report_title": "Synthetic contract-4 demo report",
        "synthetic_only": True,
    }
    assert not (tmp_path / "configured-state").exists()
    assert not (tmp_path / "configured-outputs").exists()
    assert not (tmp_path / "configured-backups").exists()


def test_cli_contract4_demo_refuses_live_config_before_state_creation(tmp_path):
    config = tmp_path / "live.yaml"
    config.write_text(
        "\n".join(
            [
                "schema_version: 1",
                "mode: live",
                f"state_dir: {tmp_path / 'live-state'}",
                f"job_output_root: {tmp_path / 'live-outputs'}",
                f"backup_dir: {tmp_path / 'live-backups'}",
                f"web_dir: {tmp_path}",
                f"api_token_file: {tmp_path / 'token'}",
                f"projects_root: {tmp_path / 'projects'}",
            ]
        )
        + "\n"
    )
    environment = os.environ.copy()
    source = str((Path(__file__).resolve().parents[1] / "src").resolve())
    environment["PYTHONPATH"] = os.pathsep.join(filter(None, (source, environment.get("PYTHONPATH"))))
    completed = subprocess.run(
        [sys.executable, "-m", "experimenttracker.cli", "--config", str(config), "demo", "--contract", "4"],
        capture_output=True,
        text=True,
        timeout=20,
        env=environment,
        check=False,
    )
    assert completed.returncode == 1
    assert "refuses a live configuration" in completed.stdout
    assert not (tmp_path / "live-state").exists()
    assert not (tmp_path / "live-outputs").exists()
