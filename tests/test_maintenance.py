import contextlib
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
import yaml
from fastapi.testclient import TestClient

from experimenttracker.api import create_app
from experimenttracker.maintenance import (
    InstallCommand,
    Maintenance,
    ReleaseUpdate,
    StepUpdate,
    blocked,
    blockers,
    checked_backup,
    identities,
    release_root,
    software_files,
    verify_release,
)
from experimenttracker.store import Conflict, Store, encode

ROOT = Path(__file__).resolve().parents[1]
COMMIT = "a" * 40


def release(config, commit=COMMIT, *, runnable=False):
    root = release_root(config, commit)
    root.mkdir(parents=True)
    for name in (
        "pyproject.toml",
        "uv.lock",
        "web/package-lock.json",
        "gateway/package-lock.json",
        "tools/codex/package-lock.json",
    ):
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, target)
    (root / "web/dist").mkdir(parents=True)
    (root / "web/dist/index.html").write_text("<html>Synthetic maintenance fixture</html>")
    (root / ".venv/bin").mkdir(parents=True)
    (root / ".venv/bin/python").symlink_to(sys.executable)
    shutil.copyfile(Path(sys.prefix) / "pyvenv.cfg", root / ".venv/pyvenv.cfg")
    (root / ".venv/lib").symlink_to(Path(sys.prefix) / "lib", target_is_directory=True)
    if runnable:
        shutil.copytree(ROOT / "src", root / "src", ignore=shutil.ignore_patterns("__pycache__"))
        shutil.copytree(ROOT / "recipes", root / "recipes")
    else:
        (root / "src").mkdir()
        (root / "src/example.py").write_text("pass\n")
    receipt = {
        "commit_id": commit,
        "checks": "make check passed",
        "files": software_files(root),
        "tested_at": 1,
    }
    (root / "release.json").write_text(encode(receipt))
    return root


def ready(config, store):
    release(config)
    manager = Maintenance(config, store, config_path=Path("/synthetic/config.yaml"))
    manager.register(ReleaseUpdate(commit_id=COMMIT, summary="Synthetic update", state="ready"))
    return manager


def test_install_idempotency_conflicts_and_dispatch_gate(config, runtime):
    store, _, _, workflow = runtime
    manager = ready(config, store)
    command = InstallCommand(request_id=uuid4(), release_id=COMMIT)
    assert not blocked(store)
    first = manager.install(command)
    assert first == manager.install(command)
    assert blocked(store)
    with pytest.raises(Conflict, match="immutable"):
        manager.install(command.model_copy(update={"release_id": "b" * 40}))
    with pytest.raises(Conflict, match="already in progress"):
        manager.install(command.model_copy(update={"request_id": uuid4()}))
    assert blockers(workflow) == []
    assert store.one("SELECT COUNT(*) AS n FROM maintenance")["n"] == 1


def test_receipt_detects_changed_software_and_is_immutable(config, runtime):
    store, *_ = runtime
    manager = ready(config, store)
    with pytest.raises(Conflict, match="immutable"):
        manager.register(
            ReleaseUpdate(commit_id=COMMIT, summary="Overwrite", state="testing", preparer_pid=os.getpid())
        )
    (release_root(config, COMMIT) / "src/example.py").write_text("changed\n")
    with pytest.raises(ValueError, match="changed"):
        verify_release(config, COMMIT)
    with pytest.raises(ValueError, match="invalid release"):
        release_root(config, "../untrusted")


def test_failure_after_shutdown_keeps_dispatch_held(config, runtime):
    store, *_ = runtime
    manager = ready(config, store)
    command = InstallCommand(request_id=uuid4(), release_id=COMMIT)
    manager.install(command)
    for state in ("backing_up", "verifying_backup", "restarting", "starting", "verifying"):
        manager.step(StepUpdate(request_id=command.request_id, state=state))
    manager.step(
        StepUpdate(request_id=command.request_id, state="failed", error="Synthetic recovery failure")
    )
    assert manager.status()["operation"]["state"] == "blocked"
    assert blocked(store)
    with pytest.raises(Conflict):
        manager.step(StepUpdate(request_id=command.request_id, state="complete"))


def test_failure_before_shutdown_releases_dispatch_and_backup_is_private(config, runtime):
    store, *_ = runtime
    manager = ready(config, store)
    command = InstallCommand(request_id=uuid4(), release_id=COMMIT)
    manager.install(command)
    manager.step(StepUpdate(request_id=command.request_id, state="failed", error="Synthetic backup failure"))
    assert not blocked(store)
    before = identities(store)
    checked_backup(store, config.backup_dir)
    reader = Store.read_only(config.backup_dir / "workflow.sqlite")
    try:
        assert identities(reader) == before
    finally:
        reader.close()
    assert (config.backup_dir / "workflow.sqlite").stat().st_mode & 0o777 == 0o600


@pytest.mark.asyncio
async def test_maintenance_observes_but_never_advances_new_work(config, runtime, monkeypatch):
    store, _, _, workflow = runtime
    manager = ready(config, store)
    manager.install(InstallCommand(request_id=uuid4(), release_id=COMMIT))
    calls = []

    async def observe():
        calls.append("observe")

    async def forbidden(*args, **kwargs):
        pytest.fail("maintenance dispatched new work")

    monkeypatch.setattr(workflow, "observe_jobs", observe)
    monkeypatch.setattr(workflow, "advance", forbidden)
    monkeypatch.setattr(workflow.conversation, "advance", forbidden)
    monkeypatch.setattr(workflow.supervisor, "advance", forbidden)
    await workflow.tick(force_observe=True)
    assert calls == ["observe"]
    await workflow.close()


def test_maintenance_api_requires_auth_and_explicit_tested_release(config, runtime):
    store, _, _, workflow = runtime
    release(config)
    app = create_app(config, components=(None, store, workflow))
    with TestClient(app) as client:
        assert client.get("/api/maintenance").status_code == 401
        headers = {"Authorization": "Bearer " + "t" * 40}
        assert (
            client.post(
                "/api/maintenance/install",
                headers=headers,
                json={
                    "request_id": str(uuid4()),
                    "release_id": COMMIT,
                    "shell": "unsafe",
                },
            ).status_code
            == 422
        )
        assert (
            client.post(
                "/api/maintenance/install",
                headers=headers,
                json={
                    "request_id": str(uuid4()),
                    "release_id": COMMIT,
                },
            ).status_code
            == 409
        )
        assert (
            client.post(
                "/api/maintenance/releases",
                headers=headers,
                json={
                    "commit_id": COMMIT,
                    "summary": "Synthetic release",
                    "state": "ready",
                },
            ).status_code
            == 200
        )
        status = client.get("/api/maintenance", headers=headers).json()
        assert not status["available"]
        assert status["release"]["state"] == "ready"
        assert str(config.state_dir) not in json.dumps(status)


def test_coordinator_unknown_launch_is_not_retried(config, runtime, monkeypatch):
    store, *_ = runtime
    manager = ready(config, store)
    command = InstallCommand(request_id=uuid4(), release_id=COMMIT)
    manager.install(command)
    with store.tx() as db:
        db.execute("UPDATE maintenance SET detail=?", (encode({"coordinator_launch": True}),))
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **k: pytest.fail("uncertain coordinator retried"))
    manager.launch()
    assert manager.status()["operation"]["state"] == "blocked"


def wait_for(call, *, seconds=120):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            result = call()
            if result:
                return result
        except (httpx.HTTPError, KeyError):
            pass
        time.sleep(0.1)
    raise AssertionError("synthetic maintenance condition timed out")


def test_real_demo_process_handover_without_model_or_scheduler(config, tmp_path):
    # Both owners and their coordinator are real processes with isolated demo state.
    # This establishes process handover, not live Slurm/account/phone acceptance.
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        config.port = sock.getsockname()[1]
    config_path = tmp_path / "demo.yaml"
    config_path.write_text(yaml.safe_dump(json.loads(config.model_dump_json())))
    config_path.chmod(0o600)
    root = release(config, runnable=True)
    seed = Store(config.state_dir / "workflow.sqlite")
    eid = seed.command(
        uuid4(), "create", {"brief": "Synthetic preserved history", "recipe_id": "toy_compare_v1"}
    )["experiment_id"]
    seed.update(eid, phase="complete", pause_agent=1, pause_compute=1)
    with seed.tx() as db:
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,state,job_id,script_hash,resources,snapshot_hash,created_at) VALUES(?,?,'train_linear','complete','synthetic-job','source','{}','snapshot',0)",
            (str(uuid4()), eid),
        )
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,input,created_at,updated_at) VALUES(?,?,'prepare',1,'completed','synthetic-thread','synthetic-turn','{}',0,0)",
            (str(uuid4()), eid),
        )
    baseline = identities(seed)
    seed.close()
    log = (tmp_path / "initial-owner.log").open("w")
    env = {**os.environ, "PYTHONPATH": str(ROOT / "src")}
    initial = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "experimenttracker.cli",
            "--config",
            str(config_path),
            "serve",
            "--max-seconds",
            "180",
        ],
        cwd=ROOT,
        env=env,
        stdout=log,
        stderr=log,
    )
    replacement = None
    request_id = str(uuid4())
    try:
        with httpx.Client(
            base_url=f"http://127.0.0.1:{config.port}",
            headers={
                "Authorization": "Bearer " + config.api_token_file.read_text().strip(),
            },
            timeout=3,
        ) as client:
            wait_for(lambda: client.get("/api/maintenance/internal").is_success)
            result = client.post(
                "/api/maintenance/releases",
                json={
                    "commit_id": COMMIT,
                    "summary": "Synthetic process handover",
                    "state": "ready",
                },
            )
            assert result.status_code == 200, result.text
            result = client.post(
                "/api/maintenance/install", json={"request_id": request_id, "release_id": COMMIT}
            )
            assert result.status_code == 202, result.text

            def complete():
                from experimenttracker.maintenance import process_identity

                directory = config.state_dir / "maintenance" / request_id
                launch = directory / "launch.json"
                if launch.exists() and not process_identity(json.loads(launch.read_text())["pid"]):
                    pytest.fail("Replacement exited: " + (directory / "executor.log").read_text())
                response = client.get("/api/maintenance/internal")
                response.raise_for_status()
                value = response.json()
                if value["maintenance"]["operation"]["state"] in ("failed", "blocked"):
                    directory = config.state_dir / "maintenance" / request_id
                    pytest.fail(
                        (directory / "coordinator.log").read_text()
                        + "\n"
                        + (directory / "executor.log").read_text()
                        if (directory / "executor.log").exists()
                        else (directory / "coordinator.log").read_text()
                    )
                return value if value["maintenance"]["operation"]["state"] == "complete" else None

            outcome = wait_for(complete)
            assert outcome["maintenance"]["current_commit"] == COMMIT
            assert outcome["last_tick"] >= outcome["started_at"]
            assert client.get("/").text == (root / "web/dist/index.html").read_text()
            backup = config.backup_dir / "maintenance" / request_id / "workflow.sqlite"
            assert backup.is_file() and backup.stat().st_mode & 0o777 == 0o600
            assert outcome["identities"] == baseline
            assert len(client.get("/api/experiments").json()["experiments"]) == 1
            initial.wait(timeout=10)
            replacement = json.loads(
                (config.state_dir / "maintenance" / request_id / "launch.json").read_text()
            )["pid"]
    finally:
        if replacement is None:
            launch = config.state_dir / "maintenance" / request_id / "launch.json"
            if launch.exists():
                replacement = json.loads(launch.read_text())["pid"]
        if replacement:
            with contextlib.suppress(ProcessLookupError):
                os.kill(replacement, signal.SIGTERM)
            time.sleep(0.5)
        if initial.poll() is None:
            initial.terminate()
        initial.wait(timeout=15)
        log.close()


@pytest.mark.asyncio
async def test_gateway_lost_ack_adopts_saved_install_and_rejection(config, runtime):
    from experimenttracker.config import GatewayConfig
    from experimenttracker.gateway_client import GatewayClient

    store, *_ = runtime
    manager = ready(config, store)
    credential = config.api_token_file.parent / "executor-token"
    credential.write_text("e" * 40)
    credential.chmod(0o600)
    config.gateway = GatewayConfig(url="https://synthetic.test", credential_file=credential)
    first_id, second_id = str(uuid4()), str(uuid4())
    command = {"request_id": first_id, "release_id": COMMIT}
    outcomes = []
    lose_ack = True

    def transport(request):
        nonlocal lose_ack
        if request.url.path.endswith("maintenance-commands"):
            if request.method == "GET":
                return httpx.Response(200, json={"schema_version": 1, "commands": [command]})
            value = json.loads(request.content)
            outcomes.append(value)
            if lose_ack:
                lose_ack = False
                raise httpx.ReadError("synthetic lost ACK", request=request)
            return httpx.Response(200, json={"schema_version": 1, "accepted": value["request_id"]})
        assert str(config.state_dir) not in request.content.decode()
        return httpx.Response(200, json={"schema_version": 1, "accepted": True})

    bridge = GatewayClient(config, store, transport=httpx.MockTransport(transport))
    bridge.maintenance_provider = lambda: manager
    try:
        with pytest.raises(httpx.ReadError):
            await bridge.maintenance()
        await bridge.maintenance()
        assert outcomes[0] == outcomes[1]
        assert store.one("SELECT COUNT(*) AS n FROM maintenance")["n"] == 1
        command = {"request_id": second_id, "release_id": COMMIT}
        await bridge.maintenance()
        assert outcomes[-1]["state"] == "rejected"
        manager.step(StepUpdate(request_id=first_id, state="failed", error="synthetic pre-stop failure"))
        await bridge.maintenance()
        assert outcomes[-1] == outcomes[-2]
        assert store.one("SELECT COUNT(*) AS n FROM maintenance")["n"] == 1
    finally:
        await bridge.close()


def test_dead_release_preparer_never_stays_ready_or_testing(config, runtime, monkeypatch):
    store, *_ = runtime
    manager = Maintenance(config, store)
    manager.register(
        ReleaseUpdate(
            commit_id=COMMIT, summary="Synthetic preparation", state="testing", preparer_pid=os.getpid()
        )
    )
    monkeypatch.setattr("experimenttracker.maintenance.process_identity", lambda pid: None)
    manager.launch()
    assert manager.status()["release"]["state"] == "failed"
    assert "Preparation stopped" in manager.status()["release"]["error"]
