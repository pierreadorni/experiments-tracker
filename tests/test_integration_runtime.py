"""Real Git and SQLite prove the periodic integration driver's saved boundaries (T3.4c-3).

Each case queues a branch request through the store command and drives the runtime once
against a real repository and the real SQLite queue. The driver commits only the exact
saved branch worktree of a terminal complete experiment with no in-flight agent turn or
job, runs the owner's check argv in the candidate worktree, reverifies the saved scope and
pinned head before the fast-forward, and settles the branch row with the finished
request. Every ineligible case stays claimed with an explicit saved status.
"""

import json
import sys
from pathlib import Path
from uuid import uuid4

from test_branch_workspaces import create, git_project

from experimenttracker.config import IntegrationConfig
from experimenttracker.engine import gitworkspace, integration, integration_runtime
from experimenttracker.engine import integration_queue as queue
from experimenttracker.workspaces import WorkspaceUnavailable, git

CHECK_CWD = [
    sys.executable,
    "-c",
    "import pathlib,sys; pathlib.Path(sys.argv[1]).write_text(str(pathlib.Path.cwd()))",
]


def commit_of(repo, ref="HEAD"):
    return git(repo, "rev-parse", ref).decode().strip()


def branch_row(workflow, eid):
    return workflow.workspaces.branch(eid)


def saved(store, request_id):
    with store.tx() as db:
        return queue.saved(db, request_id)


def events(store, eid):
    rows = store.rows(
        "SELECT kind,payload FROM events WHERE experiment_id=? AND kind LIKE 'integration%' ORDER BY rowid",
        (eid,),
    )
    return [(row["kind"], json.loads(row["payload"])) for row in rows]


def settings(store, *, check_command=None, require_approval=False):
    """Declare the owner's integration settings for the saved test project."""
    project = store.projects["anyup-rs"]
    store.projects[project.id] = project.model_copy(
        update={
            "check_command": check_command,
            "integration": IntegrationConfig(require_approval=require_approval),
        }
    )


async def queued_branch(runtime, tmp_path, monkeypatch, *, edit="value = 2\n", complete=True, **declared):
    """A real contract-3 experiment whose editable branch worktree has a queued request."""
    store, _, _, workflow = runtime
    source = git_project(workflow, store, tmp_path)
    if declared:
        settings(store, **declared)
    monkeypatch.setattr(gitworkspace, "_today", lambda: "20260101")
    eid = create(store)
    assert await workflow.prepare_workspace(store.experiment(eid))
    if edit is not None:
        (Path(branch_row(workflow, eid)["worktree"]) / "shared.py").write_text(edit)
    request_id = str(uuid4())
    store.command(request_id, "integrate", {}, eid=eid)
    if complete:
        store.update(eid, phase="complete", hold_reason=None)
    return source, store, workflow, eid, request_id


async def test_a_completed_merge_commits_checks_and_settles_the_saved_branch(runtime, tmp_path, monkeypatch):
    marker = tmp_path / "check-cwd.txt"
    source, store, workflow, eid, request_id = await queued_branch(
        runtime, tmp_path, monkeypatch, check_command=[*CHECK_CWD, str(marker)]
    )
    base_before = commit_of(source, "main")

    result = integration_runtime.run(workflow.workspaces)[0]

    merged, candidate = commit_of(source, "main"), result["candidate"]
    assert result["outcome"] == "applied"
    # The owner's check ran inside the exact candidate worktree the fast-forward applied.
    assert marker.read_text() == candidate["worktree"]
    assert merged == candidate["candidate_commit"] != base_before
    assert git(source, "rev-list", "--parents", "-n", "1", merged).decode().split()[1] == base_before
    assert (source / "shared.py").read_text() == "value = 2\n"
    view, row = saved(store, request_id), branch_row(workflow, eid)
    assert (view["stage"], view["state"]) == ("applied", "satisfied")
    assert candidate["head_commit"] == commit_of(Path(row["worktree"]))
    assert (row["state"], row["merged_commit"]) == ("merged", merged)
    # One settled row and one event describe exactly the applied merge.
    recorded = events(store, eid)
    assert [kind for kind, _ in recorded] == ["integration_applied"]
    assert recorded[0][1]["commit"] == merged and recorded[0][1]["head_commit"] == candidate["head_commit"]
    # The candidate left no integration worktree behind, so the next request can prepare.
    assert not (source / ".worktrees" / "_integration").exists()


async def test_a_failed_project_check_never_merges(runtime, tmp_path, monkeypatch):
    source, store, workflow, eid, request_id = await queued_branch(
        runtime, tmp_path, monkeypatch, check_command=[sys.executable, "-c", "raise SystemExit(3)"]
    )
    base_before = commit_of(source, "main")

    assert integration_runtime.run(workflow.workspaces)[0]["outcome"] == "check_failed"
    assert saved(store, request_id)["stage"] == "check_failed"
    assert commit_of(source, "main") == base_before
    assert branch_row(workflow, eid)["state"] == "active"
    # The saved failure waits for the experiment's agent; a later pass re-applies nothing.
    assert integration_runtime.run(workflow.workspaces)[0]["outcome"] == "waiting"
    assert commit_of(source, "main") == base_before


async def test_an_uncertain_apply_is_not_replayed_and_never_settles_the_row(runtime, tmp_path, monkeypatch):
    source, store, workflow, eid, request_id = await queued_branch(runtime, tmp_path, monkeypatch)
    calls = []

    def applied_then_lost(repo, candidate):
        calls.append(candidate["candidate_commit"])
        integration.apply(repo, candidate)
        raise WorkspaceUnavailable("lost response")

    first = integration_runtime.run(workflow.workspaces, apply=applied_then_lost)[0]

    assert first["outcome"] == "waiting"
    view = saved(store, request_id)
    assert view["stage"] == "ready" and view["status"].startswith("apply outcome unknown")
    assert commit_of(source, "main") == view["candidate"]["candidate_commit"]
    # The branch row stays exactly as it was: apply settled nothing without queue.finish.
    row = branch_row(workflow, eid)
    assert (row["state"], row["merged_commit"]) == ("active", None)
    assert events(store, eid) == []

    def never(*_):
        raise AssertionError("an uncertain apply must never be repeated")

    assert integration_runtime.run(workflow.workspaces, apply=never)[0]["outcome"] == "waiting"
    assert len(calls) == 1
