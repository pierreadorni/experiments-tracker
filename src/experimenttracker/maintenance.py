"""Tested software releases and a one-shot, inference-free executor handover.

Only the owner writes SQLite while serving. The coordinator uses authenticated
loopback RPCs until shutdown, then takes the same ownership lock for offline work.
Neither browser commands nor gateway projections supply executable paths.
"""

import argparse
import contextlib
import fcntl
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tarfile
import time
from pathlib import Path
from uuid import UUID

import httpx
from pydantic import Field

from .models import StrictModel
from .store import Conflict, Store, encode

COMMIT = r"^[0-9a-f]{40}$"
ACTIVE = "state NOT IN ('complete','failed','cancelled')"
STEPS = ("waiting", "backing_up", "verifying_backup", "restarting", "starting", "verifying")


class InstallCommand(StrictModel):
    schema_version: int = Field(default=1, ge=1, le=1)
    request_id: UUID
    release_id: str = Field(pattern=COMMIT)


class ReleaseUpdate(StrictModel):
    commit_id: str = Field(pattern=COMMIT)
    summary: str = Field(min_length=1, max_length=2000)
    state: str = Field(pattern=r"^(preparing|testing|ready|failed)$")
    preparer_pid: int | None = Field(default=None, ge=1)


class StepUpdate(StrictModel):
    request_id: UUID
    state: str = Field(
        pattern=r"^(waiting|backing_up|verifying_backup|restarting|starting|verifying|complete|failed)$"
    )
    error: str | None = Field(default=None, max_length=1000)


def blocked(store):
    return bool(store.one(f"SELECT 1 FROM maintenance WHERE {ACTIVE}"))


def release_root(config, commit):
    # commit must come from a validated model or saved database row.
    if len(commit) != 40 or any(c not in "0123456789abcdef" for c in commit):
        raise ValueError("invalid release identity")
    return config.state_dir / "releases" / commit


def software_files(root):
    """Bounded software-only receipt; never reads project/research directories."""
    paths = []
    for name in ("src", "recipes", "web/dist", "gateway/dist"):
        directory = root / name
        if directory.exists():
            paths.extend(p for p in directory.rglob("*") if p.is_file() and "__pycache__" not in p.parts)
    paths.extend(
        root / name
        for name in (
            "pyproject.toml",
            "uv.lock",
            "web/package-lock.json",
            "gateway/package-lock.json",
            "tools/codex/package-lock.json",
        )
    )
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)}


def verify_release(config, commit):
    root = release_root(config, commit)
    receipt = json.loads((root / "release.json").read_text())
    if receipt["commit_id"] != commit or receipt["checks"] != "make check passed":
        raise ValueError("release has no matching successful check receipt")
    if receipt["files"] != software_files(root):
        raise ValueError("tested release software changed")
    if not (root / ".venv/bin/python").is_file() or not (root / "web/dist/index.html").is_file():
        raise ValueError("release runtime or frontend missing")
    return receipt


def identities(store):
    return {
        "jobs": store.rows("SELECT intent_id,job_id FROM jobs ORDER BY intent_id"),
        "actions": store.rows("SELECT id,thread_id,turn_id FROM agent_actions ORDER BY id"),
        "project_turns": store.rows("SELECT id,thread_id,turn_id FROM project_turns ORDER BY id"),
        "pauses": store.rows("SELECT id,pause_agent,pause_compute FROM experiments ORDER BY id"),
    }


def blockers(workflow):
    store = workflow.store
    reasons = []
    for label, query in (
        (
            "agent work is active or uncertain",
            "SELECT 1 FROM agent_actions WHERE state IN ('dispatching','running','uncertain')",
        ),
        (
            "project work is active or uncertain",
            "SELECT 1 FROM project_turns WHERE state IN ('dispatching','running','uncertain')",
        ),
        (
            "an input or approval request is unresolved",
            "SELECT 1 FROM experiments WHERE pending_request IS NOT NULL",
        ),
        ("scheduler submission needs reconciliation", "SELECT 1 FROM jobs WHERE state='submitting'"),
        (
            "a transfer is unfinished",
            "SELECT 1 FROM execution_transfers WHERE state NOT IN ('complete','failed','cancelled')",
        ),
        ("restart steering is pending", "SELECT 1 FROM steering WHERE mode='restart' AND state='pending'"),
    ):
        if store.one(query):
            reasons.append(label)
    if any(lane.agent_task and not lane.agent_task.done() for lane in workflow.pool_owner.lanes):
        reasons.append("an agent task is settling")
    supervisor = workflow.pool_owner.supervisor
    if workflow.submission_lock.locked():
        reasons.append("a scheduler admission is settling")
    if (
        any(not task.done() for task, _ in workflow.output_checks.values())
        or any(not task.done() for task in workflow.evidence_checks.values())
        or any(not task.done() for task in workflow.cleanup_tasks.values())
    ):
        reasons.append("filesystem work is settling")
    if supervisor.task and not supervisor.task.done():
        reasons.append("project work is settling")
    return reasons


class Maintenance:
    def __init__(self, config, store, *, config_path=None, current=None):
        self.config, self.store = config, store
        self.config_path = config_path
        self.current = current or os.environ.get("ET_RELEASE_COMMIT")
        self.boot_id = str(os.getpid()) + ":" + str(time.time_ns())
        self.config_identity = (
            hashlib.sha256(config_path.read_bytes()).hexdigest()
            if config_path and config_path.exists()
            else None
        )

    def status(self, workflow=None):
        release = self.store.one("SELECT * FROM releases ORDER BY updated_at DESC LIMIT 1")
        operation = self.store.one("SELECT * FROM maintenance ORDER BY created_at DESC LIMIT 1")
        return {
            "current_commit": self.current,
            "available": bool(self.config_path),
            "release": {k: release[k] for k in ("commit_id", "summary", "state", "error")}
            if release
            else None,
            "operation": {
                k: operation[k] for k in ("request_id", "release_id", "state", "error", "updated_at")
            }
            if operation
            else None,
            "blockers": blockers(workflow) if workflow else [],
            "running_jobs": self.store.one("SELECT COUNT(*) AS n FROM jobs WHERE state='submitted'")["n"],
            "observed_at": time.time(),
        }

    def register(self, update):
        receipt = verify_release(self.config, update.commit_id) if update.state == "ready" else None
        if update.state in ("preparing", "testing"):
            if not update.preparer_pid or not process_identity(update.preparer_pid):
                raise ValueError("preparation requires an existing local process identity")
            receipt = {
                "preparer_pid": update.preparer_pid,
                "process_identity": process_identity(update.preparer_pid),
            }
        with self.store.tx() as db:
            prior = db.execute("SELECT * FROM releases WHERE commit_id=?", (update.commit_id,)).fetchone()
            if prior and prior["state"] == "ready":
                if update.state != "ready" or prior["receipt"] != encode(receipt):
                    raise Conflict("a tested release is immutable; prepare another commit")
                return
            db.execute(
                "INSERT INTO releases VALUES(?,?,?,?,?,?) ON CONFLICT(commit_id) DO UPDATE SET "
                "summary=excluded.summary,state=excluded.state,receipt=excluded.receipt,error=excluded.error,updated_at=excluded.updated_at",
                (
                    update.commit_id,
                    update.summary,
                    update.state,
                    encode(receipt) if receipt else None,
                    "Release preparation failed; inspect the protected preparation log."
                    if update.state == "failed"
                    else None,
                    time.time(),
                ),
            )

    def install(self, command):
        identity = str(command.request_id)
        with self.store.tx() as db:
            prior = db.execute("SELECT * FROM maintenance WHERE request_id=?", (identity,)).fetchone()
            if prior:
                if prior["release_id"] != command.release_id:
                    raise Conflict("maintenance request identity is immutable")
                return {
                    "schema_version": 1,
                    **{k: prior[k] for k in ("request_id", "release_id", "state", "error")},
                }
            if not self.config_path:
                raise Conflict(
                    "maintenance launcher is not configured; bootstrap with the documented serve command"
                )
            if self.current == command.release_id:
                raise Conflict("this release is already running")
            release = db.execute("SELECT * FROM releases WHERE commit_id=?", (command.release_id,)).fetchone()
            if not release or release["state"] != "ready":
                raise Conflict("release is not tested and ready")
            if db.execute(f"SELECT 1 FROM maintenance WHERE {ACTIVE}").fetchone():
                raise Conflict("an installation is already in progress")
            now = time.time()
            db.execute(
                "INSERT INTO maintenance(request_id,release_id,state,created_at,updated_at) VALUES(?,?,?,?,?)",
                (identity, command.release_id, "waiting", now, now),
            )
        return {
            "schema_version": 1,
            "request_id": identity,
            "release_id": command.release_id,
            "state": "waiting",
            "error": None,
        }

    def step(self, update):
        row = self.store.one("SELECT * FROM maintenance WHERE request_id=?", (str(update.request_id),))
        if not row:
            raise Conflict("unknown maintenance request")
        old, new = row["state"], update.state
        if new == "failed" and old in ("restarting", "starting", "verifying"):
            new = "blocked"
        allowed = (
            old == new
            or new in ("failed", "blocked")
            or (old in STEPS and new in STEPS and STEPS.index(new) == STEPS.index(old) + 1)
            or (old == "verifying" and new == "complete")
        )
        if old in ("complete", "failed", "blocked", "cancelled") or not allowed:
            raise Conflict("invalid maintenance transition")
        with self.store.tx() as db:
            db.execute(
                "UPDATE maintenance SET state=?,error=?,updated_at=? WHERE request_id=?",
                (new, update.error, time.time(), str(update.request_id)),
            )

    def launch(self):
        """Persist launch intent before Popen; an uncertain launch is never retried."""
        for release in self.store.rows("SELECT * FROM releases WHERE state IN ('preparing','testing')"):
            preparer = json.loads(release["receipt"] or "{}")
            pid = preparer.get("preparer_pid")
            if not pid or process_identity(pid) != preparer.get("process_identity"):
                with self.store.tx() as db:
                    db.execute(
                        "UPDATE releases SET state='failed',error=?,updated_at=? WHERE commit_id=?",
                        (
                            "Preparation stopped before a successful test receipt. Inspect the protected preparation log.",
                            time.time(),
                            release["commit_id"],
                        ),
                    )
        row = self.store.one(f"SELECT * FROM maintenance WHERE {ACTIVE}")
        if not row or row["state"] == "blocked" or not self.config_path:
            return
        detail = json.loads(row["detail"])
        if detail.get("coordinator_launch"):
            pid = detail.get("pid")
            if not pid or process_identity(pid) != detail.get("process_identity"):
                with self.store.tx() as db:
                    db.execute(
                        "UPDATE maintenance SET state='blocked',error=?,updated_at=? WHERE request_id=?",
                        (
                            "Maintenance coordinator outcome is unknown. Inspect the saved request before recovery.",
                            time.time(),
                            row["request_id"],
                        ),
                    )
            return
        if row["state"] != "waiting":
            return
        directory = self.config.state_dir / "maintenance" / row["request_id"]
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        with self.store.tx() as db:
            db.execute(
                "UPDATE maintenance SET detail=? WHERE request_id=?",
                (encode({"coordinator_launch": True}), row["request_id"]),
            )
        try:
            with (directory / "coordinator.log").open("ab") as log:
                os.chmod(log.name, 0o600)
                child = subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "experimenttracker.maintenance",
                        "run",
                        "--config",
                        str(self.config_path),
                        "--request",
                        row["request_id"],
                    ],
                    stdin=subprocess.DEVNULL,
                    stdout=log,
                    stderr=log,
                    start_new_session=True,
                )
            with self.store.tx() as db:
                db.execute(
                    "UPDATE maintenance SET detail=? WHERE request_id=?",
                    (
                        encode(
                            {
                                "coordinator_launch": True,
                                "pid": child.pid,
                                "process_identity": process_identity(child.pid),
                            }
                        ),
                        row["request_id"],
                    ),
                )
        except OSError:
            self.step(
                StepUpdate(
                    request_id=row["request_id"],
                    state="failed",
                    error="Could not start maintenance coordinator; executor remains running.",
                )
            )


def process_identity(pid):
    try:
        value = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return None if value[0] == "Z" else value[19]
    except (OSError, IndexError):
        return None


def publish_progress(config, status):
    """Independent outbound progress path, available while the executor is stopped."""
    if not config.gateway.url:
        return
    headers = {"Authorization": "Bearer " + config.gateway.credential_file.read_text().strip()}
    if config.gateway.sites_credential_file:
        headers["OAI-Sites-Authorization"] = (
            "Bearer " + config.gateway.sites_credential_file.read_text().strip()
        )
    try:
        response = httpx.post(
            config.gateway.url.rstrip("/") + "/api/executor/maintenance",
            headers=headers,
            json=status,
            timeout=15,
            follow_redirects=False,
        )
        response.raise_for_status()
    except httpx.HTTPError:
        # SQLite remains authoritative; the resumed bridge republishes the outcome.
        print("Gateway progress delivery unavailable; local maintenance state retained.", flush=True)


def connect(config):
    return httpx.Client(
        base_url=f"http://127.0.0.1:{config.port}",
        headers={"Authorization": "Bearer " + config.api_token_file.read_text().strip()},
        timeout=30,
    )


def rpc(client, method, path, **kwargs):
    response = client.request(method, "/api/maintenance" + path, **kwargs)
    response.raise_for_status()
    return response.json()


def checked_backup(store, destination):
    store.backup(destination)
    verify_backup(destination / "workflow.sqlite")


def verify_backup(path):
    path.chmod(0o600)
    with contextlib.closing(sqlite3.connect(path)) as db:
        if (
            db.execute("PRAGMA integrity_check").fetchall() != [("ok",)]
            or db.execute("PRAGMA foreign_key_check").fetchall()
        ):
            raise ValueError("backup integrity or foreign-key check failed")


def prepare(config, config_path, commit, summary, repository):
    commit = subprocess.check_output(
        ["git", "-C", str(repository), "rev-parse", "--verify", commit + "^{commit}"], text=True
    ).strip()
    directory = config.state_dir / "release-preparation"
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (directory / (commit + ".lock")).open("a+") as guard:
        fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
        root = release_root(config, commit)
        if root.exists() and not (root / "release.json").exists():
            root.rename(root.with_name(commit + ".incomplete-" + str(time.time_ns())))
        _prepare(config, config_path, commit, summary, repository)


def _prepare(config, config_path, commit, summary, repository):
    commit = subprocess.check_output(
        ["git", "-C", str(repository), "rev-parse", "--verify", commit + "^{commit}"], text=True
    ).strip()
    root = release_root(config, commit)
    with connect(config) as client:

        def publish(state):
            rpc(
                client,
                "POST",
                "/releases",
                json={"commit_id": commit, "summary": summary, "state": state, "preparer_pid": os.getpid()},
            )

        if root.exists():
            verify_release(config, commit)
            publish("ready")
            return
        publish("preparing")
        root.mkdir(parents=True, mode=0o700)
        try:
            archive = root / "source.tar"
            subprocess.run(
                ["git", "-C", str(repository), "archive", "--format=tar", "--output", str(archive), commit],
                check=True,
            )
            with tarfile.open(archive) as tar:
                # Committed source only. Refuse links, traversal and special files.
                for member in tar.getmembers():
                    if (
                        member.issym()
                        or member.islnk()
                        or not (member.isfile() or member.isdir())
                        or not (root / member.name).resolve().is_relative_to(root)
                    ):
                        raise ValueError("unsafe release source entry")
                tar.extractall(root, filter="data")
            archive.unlink()
            with (root / "prepare.log").open("ab") as log:
                os.chmod(log.name, 0o600)
                for command in (
                    ["uv", "sync", "--locked"],
                    ["npm", "ci", "--prefix", "web"],
                    ["npm", "ci", "--prefix", "gateway"],
                    ["npm", "ci", "--prefix", "tools/codex"],
                ):
                    subprocess.run(command, cwd=root, stdout=log, stderr=log, check=True)
                publish("testing")
                subprocess.run(["make", "check"], cwd=root, stdout=log, stderr=log, check=True)
            receipt = {
                "commit_id": commit,
                "checks": "make check passed",
                "files": software_files(root),
                "tested_at": time.time(),
            }
            (root / "release.json").write_text(encode(receipt))
            (root / "release.json").chmod(0o400)
            publish("ready")
        except BaseException:
            with contextlib.suppress(Exception):
                publish("failed")
            raise
    print(f"Tested release ready: {commit}", flush=True)


def run(config, config_path, request_id):
    """One-shot coordinator. No inference, forced kills, retries of process starts or restores."""
    from .cli import OwnershipLock

    directory = config.state_dir / "maintenance" / request_id
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (directory / "coordinator.lock").open("a+") as guard:
        fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with connect(config) as client:
            progress = None

            def step(state, error=None):
                nonlocal progress
                rpc(client, "POST", "/step", json={"request_id": request_id, "state": state, "error": error})
                progress = rpc(client, "GET", "")
                publish_progress(config, {k: v for k, v in progress.items() if k != "schema_version"})

            try:
                current = rpc(client, "GET", "/internal")
                op = current["maintenance"]["operation"]
                if not op or op["request_id"] != request_id or op["state"] != "waiting":
                    raise ValueError(
                        "coordinator requires the exact waiting request; inspect saved outcome before recovery"
                    )
                release = op["release_id"]
                config_identity = hashlib.sha256(config_path.read_bytes()).hexdigest()
                if current["config_identity"] != config_identity:
                    raise ValueError("configuration changed since owner startup")
                verify_release(config, release)
                deadline = time.monotonic() + 86400
                while current["maintenance"]["blockers"]:
                    if time.monotonic() > deadline:
                        raise TimeoutError(
                            "safe boundary not reached within 24 hours; executor remains running"
                        )
                    time.sleep(5)
                    current = rpc(client, "GET", "/internal")
                    publish_progress(config, current["maintenance"])
                step("backing_up")
                reader = Store.read_only(config.state_dir / "workflow.sqlite")
                try:
                    # A protected pre-migration snapshot; original execution state stays authoritative.
                    reader.backup(config.backup_dir / "maintenance" / request_id)
                finally:
                    reader.close()
                step("verifying_backup")
                backup = config.backup_dir / "maintenance" / request_id / "workflow.sqlite"
                verify_backup(backup)
                if hashlib.sha256(config_path.read_bytes()).hexdigest() != config_identity:
                    raise ValueError("configuration changed while preparing restart")
                step("restarting")
                old_boot = current["boot_id"]
                rpc(client, "POST", "/stop", json={"request_id": request_id, "release_id": release})
                deadline = time.monotonic() + 300
                while True:
                    try:
                        lock = OwnershipLock(config.state_dir)
                        break
                    except RuntimeError:
                        if time.monotonic() > deadline:
                            raise TimeoutError(
                                "graceful shutdown did not release ownership; no forced termination attempted"
                            ) from None
                        time.sleep(1)
                try:
                    store = Store(config.state_dir / "workflow.sqlite")
                    try:
                        baseline = identities(store)
                        (directory / "identities.json").write_text(encode(baseline))
                        (directory / "identities.json").chmod(0o600)
                        root = release_root(config, release)
                        if hashlib.sha256(config_path.read_bytes()).hexdigest() != config_identity:
                            raise ValueError("configuration changed during shutdown")
                        receipt = verify_release(config, release)
                        saved = store.one("SELECT receipt FROM releases WHERE commit_id=?", (release,))
                        if not saved or saved["receipt"] != encode(receipt):
                            raise ValueError("tested receipt no longer matches the registered release")
                        # Persist the uncertain launch boundary before releasing the single owner lock.
                        with store.tx() as db:
                            db.execute(
                                "UPDATE maintenance SET state='starting',updated_at=? WHERE request_id=? AND state='restarting'",
                                (time.time(), request_id),
                            )
                    finally:
                        store.close()
                finally:
                    lock.close()
                progress["operation"]["state"] = "starting"
                progress["observed_at"] = time.time()
                publish_progress(config, {k: v for k, v in progress.items() if k != "schema_version"})
                with (directory / "executor.log").open("ab") as log:
                    os.chmod(log.name, 0o600)
                    env = {**os.environ, "ET_RELEASE_COMMIT": release, "PYTHONPATH": str(root / "src")}
                    child = subprocess.Popen(
                        [
                            str(root / ".venv/bin/python"),
                            "-m",
                            "experimenttracker.cli",
                            "--config",
                            str(config_path),
                            "serve",
                            "--release",
                            release,
                        ],
                        cwd=root,
                        env=env,
                        stdin=subprocess.DEVNULL,
                        stdout=log,
                        stderr=log,
                        start_new_session=True,
                    )
                (directory / "launch.json").write_text(encode({"pid": child.pid, "release_id": release}))
                (directory / "launch.json").chmod(0o600)
                deadline = time.monotonic() + 300
                while True:
                    try:
                        new = rpc(client, "GET", "/internal")
                        if new["boot_id"] != old_boot and new["maintenance"]["current_commit"] == release:
                            break
                    except (httpx.HTTPError, ValueError):
                        pass
                    if time.monotonic() > deadline or child.poll() is not None:
                        raise TimeoutError(
                            "replacement executor did not become available; inspect saved launch, do not retry blindly"
                        )
                    time.sleep(1)
                step("verifying")
                while not new["last_tick"] or new["last_tick"] < new["started_at"] or not new["agent_ready"]:
                    if time.monotonic() > deadline:
                        raise TimeoutError("replacement did not finish a healthy executor cycle")
                    time.sleep(1)
                    new = rpc(client, "GET", "/internal")
                if new["executor_error"] or new["identities"] != baseline:
                    raise ValueError("recovery check failed; dispatch remains held for operator inspection")
                step("complete")
            except Exception:
                # If the owner is offline, mutate only after taking its exact lock.
                try:
                    lock = OwnershipLock(config.state_dir)
                except RuntimeError:
                    lock = None
                if lock:
                    try:
                        store = Store(config.state_dir / "workflow.sqlite")
                        try:
                            row = store.one("SELECT state FROM maintenance WHERE request_id=?", (request_id,))
                            if row and row["state"] in STEPS:
                                with store.tx() as db:
                                    db.execute(
                                        "UPDATE maintenance SET state='blocked',error=?,updated_at=? WHERE request_id=?",
                                        (
                                            "Executor handover stopped. Inspect the saved launch before recovery.",
                                            time.time(),
                                            request_id,
                                        ),
                                    )
                            progress = Maintenance(config, store).status()
                            publish_progress(config, progress)
                        finally:
                            store.close()
                    finally:
                        lock.close()
                # Keep diagnostics private, never put raw exceptions/URLs into published status.
                with contextlib.suppress(Exception):
                    step(
                        "failed",
                        "Maintenance stopped. Inspect the protected coordinator log before recovery.",
                    )
                raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["prepare", "run"])
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--request", type=UUID)
    parser.add_argument("--commit", default="HEAD")
    parser.add_argument("--detach", action="store_true", help="prepare in a one-shot background process")
    parser.add_argument("--summary", default="Tested application update")
    parser.add_argument("--repository", type=Path, default=Path.cwd())
    args = parser.parse_args()
    from .config import load_config

    config_path = args.config.resolve()
    config = load_config(config_path)
    if args.action == "prepare" and args.detach:
        commit = subprocess.check_output(
            ["git", "-C", str(args.repository.resolve()), "rev-parse", "--verify", args.commit + "^{commit}"],
            text=True,
        ).strip()
        directory = config.state_dir / "release-preparation"
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        with connect(config) as client:
            rpc(
                client,
                "POST",
                "/releases",
                json={
                    "commit_id": commit,
                    "summary": args.summary,
                    "state": "preparing",
                    "preparer_pid": os.getpid(),
                },
            )
        with (directory / (commit + ".log")).open("ab") as log:
            os.chmod(log.name, 0o600)
            child = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "experimenttracker.maintenance",
                    "prepare",
                    "--config",
                    str(config_path),
                    "--commit",
                    commit,
                    "--summary",
                    args.summary,
                    "--repository",
                    str(args.repository.resolve()),
                ],
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=log,
                start_new_session=True,
            )
        print(encode({"state": "preparing", "commit_id": commit, "pid": child.pid}))
    elif args.action == "prepare":
        prepare(config, config_path, args.commit, args.summary, args.repository.resolve())
    elif args.request:
        run(config, config_path, str(args.request))
    else:
        parser.error("run requires --request")


if __name__ == "__main__":
    main()
