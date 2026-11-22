"""Real Git and SQLite prove the periodic integration driver's saved boundaries (T3.4c-3).

Each case queues a branch request through the store command and drives the runtime once
against a real repository and the real SQLite queue. The driver commits only the exact
saved branch worktree of a terminal complete experiment with no in-flight agent turn or
job, runs the owner's check argv in the candidate worktree, reverifies the saved scope and
pinned head before the fast-forward, and settles the branch row with the finished
request; anything ineligible stays claimed with an explicit saved status.
"""

import json
import sys
from pathlib import Path
from uuid import uuid4

import pytest
from test_branch_workspaces import create, git_project

from experimenttracker.config import IntegrationConfig
from experimenttracker.engine import gitworkspace, integration, integration_runtime
from experimenttracker.engine import integration_queue as queue
from experimenttracker.store import encode
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


@pytest.mark.usefixtures("contract_three_fixture")
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


@pytest.mark.usefixtures("contract_three_fixture")
async def test_a_merge_waits_for_terminal_completion(runtime, tmp_path, monkeypatch):
    source, store, workflow, eid, request_id = await queued_branch(
        runtime, tmp_path, monkeypatch, complete=False
    )
    base_before = commit_of(source, "main")

    result = integration_runtime.run(workflow.workspaces)[0]

    # A live run still commits its branch, so nothing is committed or prepared yet.
    assert (result["outcome"], result["stage"]) == ("waiting", "claimed")
    assert saved(store, request_id)["status"] == integration_runtime.UNFINISHED
    assert commit_of(source, "main") == base_before
    assert branch_row(workflow, eid)["head_commit"] == base_before

    store.update(eid, phase="complete", hold_reason=None)
    assert integration_runtime.run(workflow.workspaces)[0]["outcome"] == "applied"
    assert (source / "shared.py").read_text() == "value = 2\n"


@pytest.mark.parametrize(
    ("insert", "settle"),
    [
        pytest.param(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,"
            "attempts,created_at,updated_at) VALUES('a1',?,'agent:prepare',1,'running','{}',0,0,0)",
            "UPDATE agent_actions SET state='completed'",
            id="agent",
        ),
        pytest.param(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,script_hash,resources,"
            "snapshot_hash,created_at) VALUES('j1',?,'prepare',1,'submitted','h','{}','s',0)",
            "UPDATE jobs SET state='complete'",
            id="job",
        ),
    ],
)
@pytest.mark.usefixtures("contract_three_fixture")
async def test_in_flight_work_defers_the_commit_until_the_branch_is_idle(
    runtime, tmp_path, monkeypatch, insert, settle
):
    source, store, workflow, eid, request_id = await queued_branch(runtime, tmp_path, monkeypatch)
    base_before = commit_of(source, "main")
    with store.tx() as db:
        db.execute(insert, (eid,))

    assert integration_runtime.run(workflow.workspaces)[0]["outcome"] == "waiting"
    assert saved(store, request_id)["status"] == integration_runtime.WAITING
    assert commit_of(source, "main") == base_before
    # The uncommitted branch edit and untouched base are the exact saved evidence.
    assert branch_row(workflow, eid)["head_commit"] == base_before
    assert not (source / ".worktrees" / "_integration").exists()

    with store.tx() as db:
        db.execute(settle)
    assert integration_runtime.run(workflow.workspaces)[0]["outcome"] == "applied"
    assert (source / "shared.py").read_text() == "value = 2\n"


@pytest.mark.usefixtures("contract_three_fixture")
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


def test_project_check_is_argv_only_and_time_bounded(tmp_path):
    passed, summary = integration_runtime.project_check(
        tmp_path, [sys.executable, "-c", "print('one', 'two'); raise SystemExit(4)"], 30
    )
    assert passed is False and summary == "one two"
    assert integration_runtime.project_check(tmp_path, [sys.executable, "-c", "pass"], 30)[0] is True
    with pytest.raises(TimeoutError):
        integration_runtime.project_check(
            tmp_path, [sys.executable, "-c", "import time; time.sleep(30)"], 0.2
        )
    with pytest.raises(OSError):
        integration_runtime.project_check(tmp_path, ["definitely-not-a-command"], 30)


@pytest.mark.usefixtures("contract_three_fixture")
async def test_a_dont_merge_flag_blocks_even_a_ready_request(runtime, tmp_path, monkeypatch):
    source, store, workflow, eid, request_id = await queued_branch(runtime, tmp_path, monkeypatch)
    with store.tx() as db:
        queue.claim(db, str(source.resolve()))
        queue.checkpoint(db, request_id, "ready")
    contract = store.experiment(eid)["contract"]
    with store.tx() as db:
        db.execute(
            "UPDATE experiments SET contract=? WHERE id=?", (encode({**contract, "dont_merge": True}), eid)
        )
    base_before = commit_of(source, "main")

    result = integration_runtime.run(workflow.workspaces)[0]

    assert (result["outcome"], result["stage"]) == ("blocked", "ready")
    assert saved(store, request_id)["status"].startswith(integration_runtime.MANUAL)
    assert commit_of(source, "main") == base_before
    assert events(store, eid) == []


@pytest.mark.parametrize("drift", ["branch", "worktree", "project", "descendant", "deleted", "empty"])
@pytest.mark.usefixtures("contract_three_fixture")
async def test_an_ineligible_request_is_saved_for_manual_inspection_without_git_work(
    runtime, tmp_path, monkeypatch, drift
):
    source, store, workflow, eid, request_id = await queued_branch(
        runtime, tmp_path, monkeypatch, edit=None if drift == "empty" else "value = 2\n"
    )
    base_before, row = commit_of(source, "main"), branch_row(workflow, eid)
    expected = {
        "branch": "does not match the request",
        "worktree": "worktree",
        "project": "no longer this repository",
        "descendant": "not its saved commit",
        "deleted": "can no longer be integrated",
        "empty": "no commits outside the base branch",
    }[drift]
    if drift == "branch":
        with store.tx() as db:
            db.execute(
                "UPDATE experiment_branches SET branch='exp/20260101-other' WHERE experiment_id=?", (eid,)
            )
    elif drift == "worktree":
        git(source, "worktree", "remove", "--force", "--force", row["worktree"])
    elif drift == "project":
        project = store.projects["anyup-rs"]
        store.projects[project.id] = project.model_copy(update={"checkout": tmp_path / "elsewhere"})
    elif drift == "descendant":
        # The backend never committed this edit, so its head is not the saved one.
        worktree = Path(row["worktree"])
        (worktree / "shared.py").write_text("later\n")
        git(worktree, "-c", "user.name=T", "-c", "user.email=t@localhost", "commit", "-am", "later")
    elif drift == "deleted":
        store.command(uuid4(), "delete", {}, eid=eid)

    result = integration_runtime.run(workflow.workspaces)[0]

    assert (result["outcome"], result["stage"]) == ("blocked", "claimed")
    assert expected in saved(store, request_id)["status"]
    # No prepare intent is saved, so the one integration worktree stays free.
    assert saved(store, request_id)["candidate"] is None
    assert commit_of(source, "main") == base_before
    assert events(store, eid) == []
    assert not (source / ".worktrees" / "_integration").exists()


@pytest.mark.usefixtures("contract_three_fixture")
async def test_required_approval_waits_without_a_saved_owner_receipt(runtime, tmp_path, monkeypatch):
    source, store, workflow, eid, request_id = await queued_branch(
        runtime, tmp_path, monkeypatch, require_approval=True
    )
    base_before = commit_of(source, "main")

    assert integration_runtime.run(workflow.workspaces)[0]["outcome"] == "awaiting_approval"
    assert saved(store, request_id)["stage"] == "awaiting_approval"
    assert commit_of(source, "main") == base_before
    # The periodic driver never approves; the exact candidate waits for the owner receipt.
    assert integration_runtime.run(workflow.workspaces)[0]["outcome"] == "waiting"
    assert commit_of(source, "main") == base_before


@pytest.mark.usefixtures("contract_three_fixture")
async def test_a_ready_candidate_waits_while_the_experiment_is_busy_again(runtime, tmp_path, monkeypatch):
    source, store, workflow, eid, request_id = await queued_branch(
        runtime, tmp_path, monkeypatch, require_approval=True
    )
    assert integration_runtime.run(workflow.workspaces)[0]["outcome"] == "awaiting_approval"
    candidate, base_before = saved(store, request_id)["candidate"], commit_of(source, "main")
    with store.tx() as db:
        # The deferred owner approval command saves this exact receipt.
        queue.checkpoint(db, request_id, "ready")
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,script_hash,resources,"
            "snapshot_hash,created_at) VALUES('j1',?,'prepare',1,'submitted','h','{}','s',0)",
            (eid,),
        )

    assert integration_runtime.run(workflow.workspaces)[0]["outcome"] == "waiting"
    assert saved(store, request_id)["status"] == integration_runtime.WAITING
    assert commit_of(source, "main") == base_before

    with store.tx() as db:
        db.execute("UPDATE jobs SET state='complete'")
    result = integration_runtime.run(workflow.workspaces)[0]
    assert result["outcome"] == "applied"
    # The pinned candidate is applied; nothing re-prepares it for the new head.
    assert result["candidate"]["candidate_commit"] == candidate["candidate_commit"]
    assert commit_of(source, "main") == candidate["candidate_commit"]


@pytest.mark.parametrize("change", ["head", "dont_merge"])
@pytest.mark.usefixtures("contract_three_fixture")
async def test_a_scope_change_during_the_check_is_not_fast_forwarded(runtime, tmp_path, monkeypatch, change):
    source, store, workflow, eid, request_id = await queued_branch(
        runtime, tmp_path, monkeypatch, check_command=[sys.executable, "-c", "pass"]
    )
    base_before = commit_of(source, "main")

    def owner_changed_the_scope(cwd, argv, timeout):
        with store.tx() as db:
            if change == "head":
                db.execute(
                    "UPDATE experiment_branches SET head_commit=? WHERE experiment_id=?", ("f" * 40, eid)
                )
            else:
                contract = store.experiment(eid)["contract"]
                db.execute(
                    "UPDATE experiments SET contract=? WHERE id=?",
                    (encode({**contract, "dont_merge": True}), eid),
                )
        return True, "the saved scope changed during the check"

    result = integration_runtime.run(workflow.workspaces, check=owner_changed_the_scope)[0]

    # The check passed, but the fast-forward is refused: the pinned head is reverified.
    assert (result["outcome"], result["stage"]) == ("waiting", "ready")
    assert saved(store, request_id)["status"].startswith(integration_runtime.MANUAL)
    assert commit_of(source, "main") == base_before
    assert (source / "shared.py").read_text() == "value = 1\n"


@pytest.mark.usefixtures("contract_three_fixture")
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


@pytest.mark.usefixtures("contract_three_fixture")
async def test_reverting_a_settled_merge_restores_the_branch_row(runtime, tmp_path, monkeypatch):
    source, store, workflow, eid, request_id = await queued_branch(runtime, tmp_path, monkeypatch)
    assert integration_runtime.run(workflow.workspaces)[0]["outcome"] == "applied"
    merged = commit_of(source, "main")
    revert_id = str(uuid4())
    store.command(revert_id, "revert_integration", {}, eid=eid)

    result = integration_runtime.run(workflow.workspaces)[0]

    assert result["outcome"] == "applied"
    assert (source / "shared.py").read_text() == "value = 1\n"
    row = branch_row(workflow, eid)
    assert (row["state"], row["merged_commit"]) == ("active", None)
    assert [kind for kind, _ in events(store, eid)] == ["integration_applied", "integration_reverted"]
    assert saved(store, revert_id)["stage"] == "applied"
    assert saved(store, revert_id)["candidate"]["head_commit"] == merged
    # The reverted integration leaves the branch eligible for a later request.
    again = str(uuid4())
    store.command(again, "integrate", {}, eid=eid)
    assert saved(store, again)["stage"] == "queued"


@pytest.mark.usefixtures("contract_three_fixture")
async def test_one_driver_per_repository_drains_the_queue_in_order(runtime, tmp_path, monkeypatch):
    store, _, _, workflow = runtime
    source = git_project(workflow, store, tmp_path)
    monkeypatch.setattr(gitworkspace, "_today", lambda: "20260101")
    requests, names = [], []
    for eid in (create(store), create(store)):
        assert await workflow.prepare_workspace(store.experiment(eid))
        names.append(f"{eid}.txt")
        (Path(branch_row(workflow, eid)["worktree"]) / names[-1]).write_text("work\n")
        requests.append(str(uuid4()))
        store.command(requests[-1], "integrate", {}, eid=eid)
        store.update(eid, phase="complete", hold_reason=None)

    first_pass = integration_runtime.run(workflow.workspaces)

    # One repository, one driver: the second request stays queued for the next pass.
    assert len(first_pass) == 1 and first_pass[0]["outcome"] == "applied"
    assert sorted(saved(store, request_id)["stage"] for request_id in requests) == ["applied", "queued"]
    assert integration_runtime.run(workflow.workspaces)[0]["outcome"] == "applied"
    assert [saved(store, request_id)["stage"] for request_id in requests] == ["applied", "applied"]
    assert all((source / name).exists() for name in names)
