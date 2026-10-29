import argparse
import asyncio
import fcntl
import hashlib
import json
import logging
import os
import secrets
import subprocess
import time
from pathlib import Path
from uuid import UUID, uuid4
from zipfile import BadZipFile

import uvicorn
import yaml

from .codex_client import CodexAgent, DemoAgent
from .config import load_config
from .projects import Projects
from .slurm import DemoScheduler, SlurmScheduler
from .store import Store
from .workflow import Workflow


class OwnershipLock:
    def __init__(self, directory: Path):
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.file = (directory / "owner.lock").open("a+")
        try:
            fcntl.flock(self.file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.file.close()
            raise RuntimeError("another ExperimentTracker process owns this state directory") from None
        self.file.seek(0)
        self.file.truncate()
        self.file.write(str(os.getpid()))
        self.file.flush()

    def close(self):
        self.file.close()


def initialize(path: Path, mode: str, state_dir: str | None):
    if path.exists():
        raise ValueError("configuration exists; edit it rather than overwriting")
    root = Path.cwd().resolve()
    local = root / ".local"
    local.mkdir(exist_ok=True, mode=0o700)
    token_path = local / "api-token"
    if not token_path.exists():
        token_path.write_text(secrets.token_urlsafe(32))
    token_path.chmod(0o600)
    tag = hashlib.sha256(str(root).encode()).hexdigest()[:8]
    state = state_dir or (
        f"/var/tmp/experimenttracker-{os.getuid()}"
        if mode == "live"
        else f"/tmp/experimenttracker-{os.getuid()}-{tag}"
    )
    config = {
        "schema_version": 1,
        "executor_id": "acacia",
        "mode": mode,
        "state_dir": state,
        "job_output_root": str(local / f"{mode}-outputs"),
        "projects_root": str(local / "projects"),
        "backup_dir": str(local / f"{mode}-backups"),
        "recipe_dir": str(root / "recipes/toy_compare"),
        "web_dir": str(root / "web/dist"),
        "api_token_file": str(token_path),
        "scheduler_seconds": 0.2 if mode == "demo" else 10,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(config, sort_keys=False))
    path.chmod(0o600)
    load_config(path)
    print(f"Created {path}; API token stored privately in {token_path}")


def runtime(config):
    lock = OwnershipLock(config.state_dir)
    try:
        filesystem = subprocess.run(
            ["stat", "-f", "-c", "%T", str(config.state_dir)],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout.strip()
        if filesystem in ("nfs", "nfs4", "cifs", "smb2"):
            raise ValueError("SQLite state must be on local storage; use online backups for shared storage")
        store = Store(config.state_dir / "workflow.sqlite")
    except BaseException:
        lock.close()
        raise
    scheduler = (
        DemoScheduler(config.state_dir / "demo-scheduler")
        if config.mode == "demo"
        else SlurmScheduler(config.slurm)
    )
    agent = DemoAgent() if config.mode == "demo" else CodexAgent(config.codex, config.state_dir)
    return lock, store, Workflow(config, store, scheduler, agent)


async def serve_http(config, app, max_seconds):
    server = uvicorn.Server(
        uvicorn.Config(app, host=config.host, port=config.port, log_level="info", access_log=False)
    )

    async def stop_at_deadline():
        await asyncio.sleep(max_seconds)
        server.should_exit = True

    timer = asyncio.create_task(stop_at_deadline()) if max_seconds is not None else None
    try:
        await server.serve()
    finally:
        if timer:
            timer.cancel()


async def demo(config, brief, max_seconds):
    lock, store, workflow = runtime(config)
    try:
        result = store.command(uuid4(), "create", {"brief": brief, "recipe_id": "toy_compare_v1"})
        eid = result["experiment_id"]
        print(f"Experiment: {eid}; mode: {config.mode}", flush=True)
        started = time.monotonic()
        previous = None
        while time.monotonic() - started < max_seconds:
            await workflow.tick()
            exp = store.experiment(eid)
            summary = (exp["phase"], exp["hold_reason"], exp["error"])
            if summary != previous:
                print(json.dumps({"phase": summary[0], "hold": summary[1], "error": summary[2]}), flush=True)
                previous = summary
            if exp["phase"] in ("complete", "failed"):
                print(f"Artifacts: {workflow.root(eid)}", flush=True)
                return 0 if exp["phase"] == "complete" else 1
            await asyncio.sleep(min(config.scheduler_seconds, 1))
        print("Bounded demo ended; durable state and existing jobs are preserved.")
        return 2
    finally:
        await workflow.close()
        store.close()
        lock.close()


async def reconcile(config, action_id, jobs, outputs=None, agent_output=None, turn_id=None):
    lock, store, workflow = runtime(config)
    try:
        if action_id:
            await workflow.agent.start()
            print(json.dumps(await workflow.reconcile_action(str(action_id), turn_id=turn_id), indent=2))
        if jobs:
            await workflow.observe_jobs()
            print(json.dumps(store.rows("SELECT * FROM jobs ORDER BY created_at"), indent=2))
        if outputs:
            print(json.dumps(await workflow.reconcile_outputs(str(outputs)), indent=2))
        if agent_output:
            await workflow.agent.start()
            print(json.dumps(await workflow.reconcile_agent_output(str(agent_output)), indent=2))
    finally:
        await workflow.close()
        store.close()
        lock.close()


def main():
    parser = argparse.ArgumentParser(description="ExperimentTracker · brief-driven Slurm CPU/GPU experiments")
    parser.add_argument("--config", type=Path, default=Path("config.local.yaml"))
    sub = parser.add_subparsers(dest="command", required=True)
    init = sub.add_parser("init")
    init.add_argument("--mode", choices=["demo", "live"], default="demo")
    init.add_argument("--state-dir")
    run = sub.add_parser("demo", help="submit one bounded synthetic experiment")
    run.add_argument(
        "--brief", default="Compare linear and quadratic regression on the fixed shared synthetic dataset."
    )
    run.add_argument("--max-seconds", type=int, default=900)
    serve = sub.add_parser("serve", help="run the single owning daemon and loopback API")
    serve.add_argument("--max-seconds", type=int, default=None)
    present = sub.add_parser(
        "present", help="queue a versioned report from saved evidence; no training or model calls"
    )
    present.add_argument("experiment_id", type=UUID)
    present.add_argument("--document", type=Path, required=True)
    present.add_argument("--request-id", type=UUID)
    sub.add_parser("status")
    sub.add_parser("doctor", help="read-only startup preflight; no jobs, inference or state mutation")
    inspect = sub.add_parser("inspect", help="read a detailed saved experiment without mutation")
    inspect.add_argument("experiment_id", type=UUID)
    verify = sub.add_parser(
        "verify", help="read-only publication checks; historical contracts retain their saved audit semantics"
    )
    verify.add_argument("experiment_id", type=UUID)
    bundle = sub.add_parser("verify-bundle", help="check a downloaded ZIP without extracting or executing it")
    bundle.add_argument("archive", type=Path)
    recover = sub.add_parser("reconcile", help="observe saved identities; never launch jobs or turns")
    choices = recover.add_mutually_exclusive_group(required=True)
    choices.add_argument("--action-id", type=UUID)
    choices.add_argument("--jobs", action="store_true")
    choices.add_argument("--outputs", type=UUID, metavar="EXPERIMENT_UUID")
    choices.add_argument("--agent-output", type=UUID, metavar="EXPERIMENT_UUID")
    recover.add_argument(
        "--turn-id", help="recover a lost response using an exact completed turn with matching input"
    )
    backup = sub.add_parser("backup")
    backup.add_argument("--destination", type=Path)
    args = parser.parse_args()
    if args.command == "reconcile" and args.turn_id and not args.action_id:
        parser.error("--turn-id requires --action-id")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    if args.command == "init":
        initialize(args.config, args.mode, args.state_dir)
        return
    if args.command == "verify-bundle":
        from .audit import verify_bundle

        try:
            print(json.dumps({"ok": True, **verify_bundle(args.archive)}, indent=2))
        except (ValueError, KeyError, OSError, BadZipFile) as exc:
            print(json.dumps({"ok": False, "error": str(exc)}))
            raise SystemExit(1) from None
        return
    config = load_config(args.config)
    if args.command == "doctor":
        from .doctor import diagnose

        result = diagnose(config)
        print(json.dumps(result, indent=2))
        raise SystemExit(0 if result["ok"] else 1)
    if args.command == "demo":
        raise SystemExit(asyncio.run(demo(config, args.brief, args.max_seconds)))
    if args.command == "serve":
        from .api import create_app

        if args.max_seconds is not None and args.max_seconds < 1:
            raise ValueError("max-seconds must be positive")
        app = create_app(config)
        asyncio.run(serve_http(config, app, args.max_seconds))
    elif args.command == "reconcile":
        asyncio.run(
            reconcile(config, args.action_id, args.jobs, args.outputs, args.agent_output, args.turn_id)
        )
    elif args.command == "inspect":
        reader = Store.read_only(config.state_dir / "workflow.sqlite")
        try:
            print(json.dumps(reader.projection(str(args.experiment_id)), indent=2))
        finally:
            reader.close()
    elif args.command == "verify":
        from .audit import verify_experiment

        reader = Store.read_only(config.state_dir / "workflow.sqlite")
        try:
            if reader.rows("PRAGMA integrity_check") != [{"integrity_check": "ok"}] or reader.rows(
                "PRAGMA foreign_key_check"
            ):
                raise ValueError("database integrity check failed")
            print(
                json.dumps(
                    {
                        "ok": True,
                        **verify_experiment(
                            reader,
                            str(args.experiment_id),
                            Projects(config, reader).root(str(args.experiment_id)),
                        ),
                    },
                    indent=2,
                )
            )
        except (ValueError, KeyError, OSError) as exc:
            print(json.dumps({"ok": False, "error": str(exc)}))
            raise SystemExit(1) from None
        finally:
            reader.close()
    elif args.command == "present":
        import httpx

        from .models import PresentCommand

        payload = PresentCommand(
            request_id=args.request_id or uuid4(), document=json.loads(args.document.read_text())
        )
        # The running single owner applies the command; CLI never opens a second writer.
        response = httpx.post(
            f"http://{config.host}:{config.port}/api/experiments/{args.experiment_id}/reports",
            headers={"Authorization": "Bearer " + config.api_token_file.read_text().strip()},
            json=payload.model_dump(mode="json"),
            timeout=30,
        )
        response.raise_for_status()
        print(json.dumps(response.json(), indent=2))
    elif args.command == "status":
        # Read-only inspection works while the daemon owns the write lock.
        import sqlite3

        connection = sqlite3.connect(f"file:{config.state_dir / 'workflow.sqlite'}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        print(
            json.dumps(
                [
                    dict(r)
                    for r in connection.execute(
                        "SELECT id,phase,hold_reason,updated_at,error FROM experiments ORDER BY created_at"
                    )
                ],
                indent=2,
            )
        )
        connection.close()
    elif args.command == "backup":
        # Online backup through a separate reader does not mutate authoritative state.
        destination = args.destination or config.backup_dir
        source = config.state_dir / "workflow.sqlite"
        if (destination / "workflow.sqlite").resolve() == source.resolve():
            raise ValueError("backup destination must differ from authoritative state")
        reader = Store.read_only(source)
        try:
            reader.backup(destination)
        finally:
            reader.close()
        print(f"Backup: {destination / 'workflow.sqlite'}")


if __name__ == "__main__":
    main()
