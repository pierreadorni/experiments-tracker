"""T4.1g-2: one supported flagged create reaches its exact first C7 turn (C7)."""

from pathlib import Path
from uuid import uuid4

import pytest
from test_projects import register
from test_research_dispatch import ScriptedAgent, scripted

from experimenttracker.config import ProjectConfig
from experimenttracker.engine import gitworkspace
from experimenttracker.research.session import first_turn
from experimenttracker.store import Store
from experimenttracker.workspaces import WorkspaceUnavailable, git, initialize_project

BRIEF = "Compare two toy optimizers under one fixed compute budget."


def research_project(workflow, store, tmp_path):
    """A registered Git project whose owner declaration opts into contract 4."""
    source = register(workflow.config, store, tmp_path)
    initialize_project(source)
    (source / "shared.py").write_text("value = 1\n")
    git(source, "add", "--", "shared.py")
    git(source, "-c", "user.name=Test", "-c", "user.email=test@localhost", "commit", "-m", "Source")
    workflow.config.projects[-1].research_loop = True
    workflow.projects.reload()
    return source


def create(store):
    return store.command(
        uuid4(),
        "create",
        {"brief": BRIEF, "recipe_id": "agent_experiment_v1", "project_id": "anyup-rs"},
    )["experiment_id"]


def research_actions(store, eid):
    return store.rows(
        "SELECT * FROM agent_actions WHERE experiment_id=? AND phase='research' ORDER BY created_at,rowid",
        (eid,),
    )


def test_reload_keeps_the_owner_flag_for_a_saved_project_row(config, runtime, tmp_path):
    store, _, _, workflow = runtime
    source = tmp_path / "declared"
    source.mkdir()
    initialize_project(source)
    project = ProjectConfig(id="anyup-rs", name="anyup-rs", checkout=source, research_loop=True)
    config.projects.append(project)
    info = source.stat()
    with store.tx() as db:
        db.execute(
            "INSERT INTO projects VALUES(?,?,?,?,?,?,?)",
            (project.id, project.name, str(source), info.st_dev, info.st_ino, 0, 0.0),
        )

    workflow.projects.reload()

    assert store.projects["anyup-rs"].research_loop is True


@pytest.mark.asyncio
async def test_flagged_create_runs_one_first_turn_on_the_named_worktree(
    config, runtime, tmp_path, monkeypatch
):
    store, _, _, workflow = runtime
    research_project(workflow, store, tmp_path)
    monkeypatch.setattr(gitworkspace, "_today", lambda: "20260101")
    eid = create(store)
    assert store.projection(eid)["contract"]["execution_contract"] == 4
    assert store.rows("SELECT * FROM stages WHERE experiment_id=?", (eid,)) == []
    assert workflow.workspaces.branch(eid) is None

    agent = scripted(
        workflow,
        ScriptedAgent(result={"status": "continue", "summary": "Measured.", "next": "Compare."}),
    )

    # The shared control pass queues and prepares the turn, then offers it to the pool.
    await workflow.step_control_finish()
    branch = workflow.workspaces.branch(eid)
    assert branch is not None
    worktree = Path(branch["worktree"])
    (worktree / "notes.md").write_text("first measurement\n")
    workflow.agent_pool.admit()
    task = workflow.agent_pool.primary.agent_task
    assert task is not None
    await task
    workflow.agent_pool.primary.agent_task = None

    [action] = research_actions(store, eid)
    assert (action["state"], action["thread_id"], action["turn_id"]) == (
        "completed",
        "thread-1",
        "turn-1",
    )
    assert agent.sessions == [(None, worktree, True)]
    assert store.experiment(eid)["thread_id"] == "thread-1"
    assert store.rows("SELECT * FROM stages WHERE experiment_id=?", (eid,)) == []
    assert git(worktree, "show", "HEAD:notes.md").decode() == "first measurement\n"
    assert "Research turn " + action["id"] in git(worktree, "log", "-1", "--format=%s").decode()

    # A completed first turn is never replaced by another turn or worktree.
    await workflow.prepare_research_turns()
    assert len(research_actions(store, eid)) == 1

    store.close()
    reopened = Store(config.state_dir / "workflow.sqlite")
    try:
        assert first_turn(reopened, eid) is None
        assert len(research_actions(reopened, eid)) == 1
    finally:
        reopened.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("gate", ["paused", "cancelled", "deleted", "maintenance"])
async def test_preparation_queues_nothing_behind_a_gate(runtime, tmp_path, monkeypatch, gate):
    store, _, _, workflow = runtime
    research_project(workflow, store, tmp_path)
    eid = create(store)
    if gate == "paused":
        store.update(eid, pause_agent=True)
    elif gate == "cancelled":
        store.update(eid, phase="cancelled")
    elif gate == "deleted":
        with store.tx() as db:
            db.execute("UPDATE experiments SET deleted_at=? WHERE id=?", (1.0, eid))
    else:
        monkeypatch.setattr("experimenttracker.workflow.blocked", lambda _store: True)

    await workflow.prepare_research_turns()

    assert research_actions(store, eid) == []
    assert workflow.workspaces.branch(eid) is None


@pytest.mark.asyncio
async def test_one_workspace_failure_holds_only_that_experiment(runtime, tmp_path, monkeypatch):
    store, _, _, workflow = runtime
    research_project(workflow, store, tmp_path)
    failed, prepared = create(store), create(store)
    real = workflow.workspaces.ensure_branch

    def ensure_branch(eid):
        if eid == failed:
            raise WorkspaceUnavailable("synthetic worktree failure")
        return real(eid)

    monkeypatch.setattr(workflow.workspaces, "ensure_branch", ensure_branch)

    await workflow.prepare_research_turns()

    assert [row["state"] for row in research_actions(store, failed)] == ["pending"]
    assert store.experiment(failed)["hold_reason"] == "workspace_unavailable"
    assert workflow.workspaces.branch(failed) is None
    assert [row["state"] for row in research_actions(store, prepared)] == ["pending"]
    assert workflow.workspaces.branch(prepared) is not None
