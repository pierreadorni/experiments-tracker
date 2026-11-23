"""Ephemeral loopback browser acceptance; synthetic adapters, never Slurm/Codex.

Run from the isolated validation checkout with STATE_DIR and WEB_DIR arguments.
The fixture-only repair endpoint exercises production repair against DemoScheduler
under live-policy validation temporarily; this is not live integration evidence.
"""

import secrets
import sys
from pathlib import Path

import uvicorn
from fastapi import HTTPException, Request
from test_iteration import IterationAgent

from experimenttracker.api import create_app
from experimenttracker.cli import OwnershipLock
from experimenttracker.config import Config
from experimenttracker.slurm import DemoScheduler
from experimenttracker.store import Store
from experimenttracker.workflow import Workflow


def main():
    folder, web = map(Path, sys.argv[1:3])
    folder.mkdir(parents=True, exist_ok=True, mode=0o700)
    token = folder / "token"
    token.write_text(secrets.token_urlsafe(40))
    token.chmod(0o600)
    config = Config(
        mode="demo",
        state_dir=folder / "state",
        job_output_root=folder / "outputs",
        backup_dir=folder / "backups",
        api_token_file=token,
        web_dir=web,
        scheduler_seconds=0.1,
        port=8879,
    )
    lock = OwnershipLock(config.state_dir)
    store = Store(config.state_dir / "workflow.sqlite")
    agent = IterationAgent(fail_plot=True)
    workflow = Workflow(config, store, DemoScheduler(config.state_dir / "scheduler"), agent)
    app = create_app(config, components=(lock, store, workflow))

    @app.post("/fixture/repair")
    async def repair(request: Request):
        if not secrets.compare_digest(
            request.headers.get("authorization", ""), "Bearer " + token.read_text().strip()
        ):
            raise HTTPException(401)
        agent.fail_plot = False
        original = config.mode
        try:
            config.mode = "live"
            await workflow.repair_failed_experiments()
        finally:
            config.mode = original
        return {"synthetic": True}

    @app.post("/fixture/autopilot-handoff")
    async def autopilot_handoff(request: Request):
        if not secrets.compare_digest(
            request.headers.get("authorization", ""), "Bearer " + token.read_text().strip()
        ):
            raise HTTPException(401)
        import time
        from uuid import uuid4

        from experimenttracker.autopilot_models import AutopilotPolicy
        from experimenttracker.autopilot_tools import UpdateDocument

        service = workflow.research.service
        now = time.time()
        view = service.view("test-project")
        if not view["run"]:
            policy = AutopilotPolicy(
                subject="Synthetic browser handoff fixture",
                scope="Infrastructure only; no real research",
                publication_goal="Software acceptance",
                venue="Synthetic fixture",
                start_at=now,
                handoff_at=now + 604800,
                submission_at=now + 1209600,
                weekly_gpu_hours=0,
                anchor_at=now,
            )
            service.command(
                "test-project",
                {
                    "request_id": str(uuid4()),
                    "kind": "configure",
                    "expected_revision": 0,
                    "policy": policy.model_dump(),
                },
            )
            run = service.run("test-project")
            with store.tx() as db:
                request_id = uuid4()
                service.update_document(
                    db,
                    run,
                    str(request_id),
                    UpdateDocument(
                        request_id=request_id,
                        name="paper.tex",
                        base_version=0,
                        text="Synthetic browser source package. TODO: real research is not performed.",
                    ),
                    "synthetic-fixture",
                )
                service.emit(db, run["id"], "synthetic_handoff_fixture")
        return {"synthetic": True, "draft_only": True}

    # create_app mounts its static fallback last; keep the fixture endpoint ahead of it.
    fixture_routes = [
        route for route in app.router.routes if getattr(route, "path", "").startswith("/fixture/")
    ]
    app.router.routes[:] = fixture_routes + [
        route for route in app.router.routes if route not in fixture_routes
    ]
    uvicorn.run(app, host="127.0.0.1", port=config.port, log_level="warning")


if __name__ == "__main__":
    main()
