"""Temporary loopback C4 fixture: real FastAPI, SQLite and inert fake scheduler/agent."""

import argparse
import os
import secrets
from pathlib import Path
from uuid import uuid4

import uvicorn

from experimenttracker.api import create_app
from experimenttracker.codex_client import DemoAgent
from experimenttracker.config import Config, ProjectConfig
from experimenttracker.research.checkpoints import SaveCheckpoint, save_checkpoint
from experimenttracker.research.owner_tool import AskOwnerQuestion, ask_owner
from experimenttracker.research.session import first_turn
from experimenttracker.slurm import DemoScheduler
from experimenttracker.store import Store
from experimenttracker.workflow import Workflow
from experimenttracker.workspaces import initialize_project


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--port", type=int, default=8879)
    args = parser.parse_args()
    root = args.root.resolve()
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.stat().st_uid != os.getuid() or root.stat().st_mode & 0o077 or any(root.iterdir()):
        parser.error("--root must be an empty private directory owned by the current user")
    token_file = root / "api-token"
    with open(token_file, "x", opener=lambda path, flags: os.open(path, flags, 0o600)) as token:
        token.write(secrets.token_urlsafe(32))
    checkout = root / "project"
    checkout.mkdir()
    initialize_project(checkout)
    project = ProjectConfig(id="test-project", name="C4 acceptance", checkout=checkout, research_loop=True)
    config = Config(
        mode="demo",
        projects=[project],
        state_dir=root / "state",
        job_output_root=root / "outputs",
        backup_dir=root / "backups",
        web_dir=Path(__file__).resolve().parents[1] / "dist",
        api_token_file=token_file,
        port=args.port,
        scheduler_seconds=0.2,
    )
    store = Store(config.state_dir / "workflow.sqlite")
    scheduler = DemoScheduler(config.state_dir / "scheduler")
    agent = DemoAgent()
    workflow = Workflow(config, store, scheduler, agent)
    eid = store.command(
        uuid4(),
        "create",
        {
            "brief": "Synthetic C4 approval UI acceptance.",
            "recipe_id": "agent_experiment_v1",
            "project_id": project.id,
            "execution_contract": 4,
        },
    )["experiment_id"]
    assert store.experiment(eid)["contract"]["execution_contract"] == 4
    action = first_turn(store, eid)
    if action is None:
        raise RuntimeError(f"failed to seed contract-4 research turn for {eid}")
    with store.tx() as db:
        before = dict(db.execute("SELECT * FROM agent_actions WHERE id=?", (action["id"],)).fetchone())
        db.execute(
            "UPDATE agent_actions SET state='running',thread_id='synthetic-c4-thread',"
            "turn_id='synthetic-c4-turn',attempts=1 WHERE id=?",
            (action["id"],),
        )
        db.execute("UPDATE experiments SET thread_id='synthetic-c4-thread' WHERE id=?", (eid,))
        from experimenttracker.core import action_obligations

        action_obligations.sync_action(db, action["id"], before=before)
    action = store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))
    approval = AskOwnerQuestion(
        request_id="f37a3b18-1e2e-4641-90eb-fba7e0a5e69b",
        question="May this exact objective proceed?",
        kind="approval",
        objective="Run the bounded synthetic CPU comparison only.",
    )
    ask_owner(store, action, approval)
    save_checkpoint(
        store,
        action,
        SaveCheckpoint(
            request_id="39ef2ba2-6a90-422a-89c6-82998c280761",
            markdown="# PRIVATE CHECKPOINT: never show this content",
        ),
    )
    (root / "experiment-id").write_text(eid)
    app = create_app(config, components=(None, store, workflow))
    uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
