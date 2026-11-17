"""New Git-backed contract-3 experiments edit their own named branch worktree (T3.3)."""

import asyncio
import json
import shutil
from pathlib import Path
from uuid import uuid4

import pytest
from helpers import IterationAgent, approve_plan, until
from test_projects import register

from experimenttracker import clock, early_repair
from experimenttracker.engine import gitworkspace
from experimenttracker.store import encode
from experimenttracker.workflow import Workflow
from experimenttracker.workspaces import WorkspaceUnavailable, git, initialize_project


def git_project(workflow, store, tmp_path):
    source = register(workflow.config, store, tmp_path)
    initialize_project(source)
    (source / "shared.py").write_text("value = 1\n")
    git(source, "add", "--", "shared.py")
    git(source, "-c", "user.name=Test", "-c", "user.email=test@localhost", "commit", "-m", "Source")
    workflow.projects.reload()
    return source


def create(store, **extra):
    return store.command(
        uuid4(),
        "create",
        {
            "brief": "Test an entirely new method and show its frontier.",
            "recipe_id": "agent_experiment_v1",
            "project_id": "anyup-rs",
            **extra,
        },
    )["experiment_id"]


async def branch_experiment(runtime, tmp_path, monkeypatch):
    store, _, _, workflow = runtime
    source = git_project(workflow, store, tmp_path)
    monkeypatch.setattr(gitworkspace, "_today", lambda: "20260101")
    eid = create(store)
    assert await workflow.prepare_workspace(store.experiment(eid))
    return source, workflow.workspaces.branch(eid)


async def test_new_git_experiment_creates_its_named_branch_before_dispatch(runtime, tmp_path, monkeypatch):
    store, _, _, workflow = runtime
    source = git_project(workflow, store, tmp_path)
    monkeypatch.setattr(gitworkspace, "_today", lambda: "20260101")
    eid = create(store)
    assert store.experiment(eid)["contract"]["branch_workspace"] is True
    # Dirty main shows what the new branch must not inherit.
    (source / "shared.py").write_text("value = 2\n")
    (source / "untracked.py").write_text("dirty\n")
    status = git(source, "status", "--porcelain", "-z")

    assert await workflow.prepare_workspace(store.experiment(eid))
    row = workflow.workspaces.branch(eid)
    worktree = Path(row["worktree"])
    assert row["branch"] == f"exp/20260101-{eid}"
    assert worktree == source / ".worktrees" / eid
    assert (worktree / "shared.py").read_text() == "value = 1\n"
    assert git(worktree, "rev-parse", "HEAD").decode().strip() == row["base_commit"]
    assert git(source, "rev-parse", "--abbrev-ref", "HEAD").decode().strip() == "main"
    assert git(source, "status", "--porcelain", "-z") == status
    context = workflow.project_context(store.experiment(eid))
    assert context["checkout"] == str(worktree)
    assert context["isolation"] == "worktree"
    assert context["source_checkout"] == str(source.resolve())
    kinds = {event["kind"] for event in store.rows("SELECT kind FROM events WHERE experiment_id=?", (eid,))}
    assert {"workspace_isolation_queued", "workspace_creation_intent", "workspace_isolated"} <= kinds


async def test_preparation_confirms_the_exact_branch_without_resetting_it(runtime, tmp_path, monkeypatch):
    store, _, _, workflow = runtime
    _, row = await branch_experiment(runtime, tmp_path, monkeypatch)
    worktree = Path(row["worktree"])
    (worktree / "shared.py").write_text("agent edit\n")

    assert await workflow.prepare_workspace(store.experiment(row["experiment_id"]))
    assert (worktree / "shared.py").read_text() == "agent edit\n"
    assert workflow.workspaces.checkout(row["experiment_id"]) == str(worktree)


async def test_lost_creation_response_reconciles_to_the_saved_identity(runtime, tmp_path, monkeypatch):
    store, _, _, workflow = runtime
    git_project(workflow, store, tmp_path)
    monkeypatch.setattr(gitworkspace, "_today", lambda: "20260101")
    eid = create(store)
    original = gitworkspace.git

    def lost_response(folder, *args):
        result = original(folder, *args)
        if args[:2] == ("worktree", "add"):
            raise WorkspaceUnavailable("saved creation response lost")
        return result

    monkeypatch.setattr(gitworkspace, "git", lost_response)
    with pytest.raises(WorkspaceUnavailable, match="response lost"):
        workflow.workspaces.ensure_branch(eid)
    row = workflow.workspaces.branch(eid)
    assert (Path(row["worktree"]) / "shared.py").read_text() == "value = 1\n"

    assert workflow.workspaces.ensure_branch(eid) == row["worktree"]
    assert workflow.workspaces.branch_checkout(eid) == row["worktree"]


async def test_missing_and_foreign_worktrees_are_refused_not_replaced(runtime, tmp_path, monkeypatch):
    store, _, _, workflow = runtime
    source, row = await branch_experiment(runtime, tmp_path, monkeypatch)
    eid, worktree = row["experiment_id"], Path(row["worktree"])
    git(source, "worktree", "unlock", str(worktree))
    await asyncio.to_thread(shutil.move, str(worktree), str(worktree.with_name("saved-original")))
    with pytest.raises(WorkspaceUnavailable, match="missing"):
        workflow.workspaces.ensure_branch(eid)
    with pytest.raises(FileNotFoundError):
        (worktree / "shared.py").read_text()

    git(source, "worktree", "prune")
    git(source, "worktree", "add", "-b", "foreign", str(worktree), "main")
    with pytest.raises(WorkspaceUnavailable, match="exact experiment branch"):
        workflow.workspaces.ensure_branch(eid)
    assert git(worktree, "rev-parse", "--abbrev-ref", "HEAD").decode().strip() == "foreign"
    git(source, "worktree", "remove", str(worktree))
    git(source, "branch", "-D", row["branch"])
    with pytest.raises(WorkspaceUnavailable, match="exact experiment branch"):
        workflow.workspaces.ensure_branch(eid)
    assert not await asyncio.to_thread(worktree.exists)


async def test_saved_plan_creates_exactly_its_base_after_main_moves(runtime, tmp_path, monkeypatch):
    store, _, _, workflow = runtime
    source = git_project(workflow, store, tmp_path)
    monkeypatch.setattr(gitworkspace, "_today", lambda: "20260101")
    eid = create(store)
    planned = workflow.workspaces.record_branch(eid)
    (source / "shared.py").write_text("value = 3\n")
    git(source, "-c", "user.name=Test", "-c", "user.email=test@localhost", "commit", "-am", "Advance main")

    assert await workflow.prepare_workspace(store.experiment(eid))
    row = workflow.workspaces.branch(eid)
    assert row["base_commit"] == planned["base_commit"]
    assert (Path(row["worktree"]) / "shared.py").read_text() == "value = 1\n"


async def test_busy_project_does_not_hold_a_new_named_branch(runtime, tmp_path, monkeypatch):
    store, _, _, workflow = runtime
    _, row = await branch_experiment(runtime, tmp_path, monkeypatch)
    first = row["experiment_id"]
    store.update(first, phase="training")
    store.db.execute(
        "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,attempts,created_at,updated_at) "
        "VALUES(?,?,'prepare',1,'completed','{}',1,?,?)",
        (str(uuid4()), first, clock.now(), clock.now()),
    )

    second = create(store)
    assert await workflow.prepare_workspace(store.experiment(second))
    assert store.experiment(second)["hold_reason"] is None
    other = workflow.workspaces.branch(second)
    assert (other["worktree"], other["branch"]) != (row["worktree"], row["branch"])


async def test_repair_keeps_project_edits_in_the_branch_worktree(runtime, tmp_path, monkeypatch):
    store, _, _, workflow = runtime
    _, row = await branch_experiment(runtime, tmp_path, monkeypatch)
    eid, worktree = row["experiment_id"], Path(row["worktree"])
    work = workflow.working_directory(store.experiment(eid))
    work.mkdir(parents=True)
    (work / "recipe.py").write_text("print('synthetic')\n")
    plan = {"files": ["recipe.py"], "project_files": ["shared.py"], "stages": []}
    store.update(
        eid,
        snapshot={"snapshot_hash": "synthetic", "plan": plan},
        phase="training",
        hold_reason="compute_failed",
        error="synthetic failure",
    )
    store.db.execute(
        "INSERT INTO jobs(intent_id,experiment_id,stage_id,state,script_hash,resources,snapshot_hash,created_at) "
        "VALUES(?,?,'prepare','failed','hash','{}','snapshot',?)",
        (str(uuid4()), eid, clock.now()),
    )

    await early_repair.queue(workflow, store.experiment(eid))
    exp = store.experiment(eid)
    assert exp["contract"]["early_repair"]["state"] == "preparing"
    assert exp["contract"]["working_directory"] != str(worktree)
    # No candidate project copy: this repair edits files in the branch it commits.
    assert "repair_project" not in exp["contract"]
    assert workflow.project_context(exp)["checkout"] == str(worktree)


async def test_non_git_project_keeps_the_shared_checkout(runtime, tmp_path):
    store, _, _, workflow = runtime
    source = register(workflow.config, store, tmp_path)
    (source / "shared.py").write_text("value = 1\n")
    workflow.projects.reload()
    eid = create(store)
    assert "branch_workspace" not in store.experiment(eid)["contract"]

    assert await workflow.prepare_workspace(store.experiment(eid))
    assert workflow.workspaces.branch(eid) is None
    assert workflow.workspaces.checkout(eid) == str(source)
    assert workflow.project_context(store.experiment(eid))["isolation"] == "shared"


async def test_repository_without_main_keeps_the_shared_checkout(runtime, tmp_path):
    store, _, _, workflow = runtime
    source = register(workflow.config, store, tmp_path)
    (source / "shared.py").write_text("value = 1\n")
    git(source, "init", "--initial-branch=trunk")
    git(source, "add", "--", "shared.py")
    git(source, "-c", "user.name=Test", "-c", "user.email=test@localhost", "commit", "-m", "Source")
    workflow.projects.reload()
    eid = create(store)
    assert "branch_workspace" not in store.experiment(eid)["contract"]

    assert await workflow.prepare_workspace(store.experiment(eid))
    assert workflow.workspaces.branch(eid) is None


async def test_explicit_shared_workspace_is_not_marked_for_a_branch(runtime, tmp_path):
    store, _, _, workflow = runtime
    git_project(workflow, store, tmp_path)
    eid = create(store, workspace_mode="shared")
    assert "branch_workspace" not in store.experiment(eid)["contract"]
    assert await workflow.prepare_workspace(store.experiment(eid))
    assert workflow.workspaces.branch(eid) is None


async def test_named_branch_experiment_runs_from_its_own_worktree(runtime, tmp_path, monkeypatch):
    class WorktreeAgent(IterationAgent):
        async def begin(self, thread_id, prompt, schema, *, workspace=None, project_workspace=None, **kwargs):
            payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
            turn = await super().begin(
                thread_id, prompt, schema, workspace=workspace, project_workspace=project_workspace, **kwargs
            )
            if payload["phase"] == "prepare":
                # The agent edits the branch worktree; attempts copy the declared source from it.
                (project_workspace / "shared.py").write_text("value = 4\n")
                self.results[(thread_id, turn)]["project_files"] = ["shared.py"]
            return turn

    store, scheduler, _, old = runtime
    source = git_project(old, store, tmp_path)
    workflow = Workflow(old.config, store, scheduler, WorktreeAgent())
    monkeypatch.setattr(gitworkspace, "_today", lambda: "20260101")
    eid = create(store)
    try:
        await approve_plan(workflow, store, eid)
        await until(workflow, store, eid, lambda e: e["phase"] == "training")
        row = workflow.workspaces.branch(eid)
        assert (Path(row["worktree"]) / "shared.py").read_text() == "value = 4\n"
        jobs = store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
        assert jobs
        for job in jobs:
            attempt = Path(job["execution_root"])
            assert attempt.is_relative_to(workflow.projects.base_root(eid))
            assert (attempt / "snapshot/recipe.py").is_file()
            assert (attempt / "snapshot/project/shared.py").read_text() == "value = 4\n"
        # The main checkout keeps its committed source and is never edited or committed to.
        assert git(source, "status", "--porcelain", "-z") == b""
        head = git(row["worktree"], "rev-parse", "HEAD").decode().strip()
        assert head != row["base_commit"]
        assert row["head_commit"] == head
        assert git(row["worktree"], "show", f"{head}:shared.py").decode() == "value = 4\n"
        for job in jobs:
            manifest = json.loads(job["execution_manifest"])
            assert manifest["commit"] == head
            assert manifest["source_context"]["git_commit"] == head
        for job in jobs:
            assert git(row["repo"], "rev-parse", f"refs/et/jobs/{job['intent_id']}").decode().strip() == head
    finally:
        await workflow.close()


async def test_accepted_implementation_commit_records_head_and_reports_skipped(
    runtime, tmp_path, monkeypatch
):
    class SkippingAgent(IterationAgent):
        async def begin(self, thread_id, prompt, schema, *, workspace=None, project_workspace=None, **kwargs):
            payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
            turn = await super().begin(
                thread_id, prompt, schema, workspace=workspace, project_workspace=project_workspace, **kwargs
            )
            if payload["phase"] == "prepare":
                (project_workspace / "shared.py").write_text("value = 4\n")
                (project_workspace / ".ssh").mkdir(exist_ok=True)
                (project_workspace / ".ssh" / "id").write_text("private\n")
                self.results[(thread_id, turn)]["project_files"] = ["shared.py"]
            return turn

    store, scheduler, _, old = runtime
    source = git_project(old, store, tmp_path)
    workflow = Workflow(old.config, store, scheduler, SkippingAgent())
    monkeypatch.setattr(gitworkspace, "_today", lambda: "20260101")
    eid = create(store)
    try:
        await approve_plan(workflow, store, eid)
        await until(workflow, store, eid, lambda e: e["phase"] == "training")
        row = workflow.workspaces.branch(eid)
        worktree = Path(row["worktree"])
        head = git(worktree, "rev-parse", "HEAD").decode().strip()
        assert head != row["base_commit"]
        assert row["head_commit"] == head
        assert git(worktree, "show", f"{head}:shared.py").decode() == "value = 4\n"
        with pytest.raises(WorkspaceUnavailable):
            git(worktree, "cat-file", "-e", f"{head}:.ssh/id")
        committed = [
            json.loads(event["payload"])
            for event in store.rows(
                "SELECT payload FROM events WHERE experiment_id=? AND kind='workspace_committed'", (eid,)
            )
        ]
        assert committed == [{"kind": "implementation", "commit": head}]
        skipped = [
            json.loads(event["payload"])
            for event in store.rows(
                "SELECT payload FROM events WHERE experiment_id=? AND kind='workspace_commit_skipped'",
                (eid,),
            )
        ]
        assert skipped == [
            {
                "kind": "implementation",
                "commit": head,
                "skipped": [".ssh/id"],
                "skipped_count": 1,
            }
        ]
        # The user's main checkout is never edited or committed to by the backend.
        assert git(source, "status", "--porcelain", "-z") == b""
    finally:
        await workflow.close()


async def test_branch_commit_uses_the_saved_tree_and_reconciles_a_lost_response(
    runtime, tmp_path, monkeypatch
):
    store, _, _, workflow = runtime
    _, row = await branch_experiment(runtime, tmp_path, monkeypatch)
    eid, worktree = row["experiment_id"], Path(row["worktree"])
    # A repair candidate is a copy for editing, never the tree this commit names.
    candidate = workflow.projects.base_root(eid) / "repair-candidates" / "2" / "project"
    candidate.mkdir(parents=True)
    (candidate / "shared.py").write_text("repair candidate\n")
    work = workflow.projects.base_root(eid) / "repair-candidates" / "2" / "work"
    work.mkdir(parents=True)
    (work / "shared.py").write_text("candidate work\n")
    contract = store.experiment(eid)["contract"]
    contract["working_directory"] = str(work)
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))
    assert workflow.working_directory(store.experiment(eid)) == work

    (worktree / "shared.py").write_text("branch work\n")
    message = "Job sources train revision 1"
    # Simulate a lost backend commit response: Git changed state, the saved head did not.
    lost, _ = gitworkspace.commit(worktree, message)
    assert lost != row["head_commit"]
    # Later uncommitted work must survive adoption instead of being swept into a commit.
    (worktree / "later.py").write_text("later work\n")
    outcome = workflow.workspaces.commit_branch(eid, message)
    assert outcome["commit"] == lost
    assert outcome["skipped"] == []
    assert (worktree / "later.py").read_text() == "later work\n"
    assert git(worktree, "status", "--porcelain", "-z") == b"?? later.py\0"
    with pytest.raises(WorkspaceUnavailable):
        git(worktree, "cat-file", "-e", f"{lost}:later.py")
    assert store.one("SELECT head_commit FROM experiment_branches WHERE experiment_id=?", (eid,)) == {
        "head_commit": lost
    }
    assert workflow.workspaces.branch_checkout(eid) == str(worktree)
    assert git(worktree, "show", f"{lost}:shared.py").decode() == "branch work\n"
    assert (candidate / "shared.py").read_text() == "repair candidate\n"
    assert (work / "shared.py").read_text() == "candidate work\n"

    # An unrelated external commit is never adopted as this request's commit.
    (worktree / "shared.py").write_text("unrelated work\n")
    gitworkspace.commit(worktree, "Unrelated change")
    with pytest.raises(WorkspaceUnavailable, match="saved commit"):
        workflow.workspaces.commit_branch(eid, message)
    assert store.one("SELECT head_commit FROM experiment_branches WHERE experiment_id=?", (eid,)) == {
        "head_commit": lost
    }


async def test_lost_response_adoption_requires_the_backend_author(runtime, tmp_path, monkeypatch):
    store, _, _, workflow = runtime
    _, row = await branch_experiment(runtime, tmp_path, monkeypatch)
    eid, worktree = row["experiment_id"], Path(row["worktree"])
    message = "Job sources train revision 1"
    (worktree / "shared.py").write_text("human work\n")
    git(worktree, "add", "--", "shared.py")
    git(
        worktree,
        "-c",
        "user.name=Human",
        "-c",
        "user.email=human@example.com",
        "-c",
        "commit.gpgsign=false",
        "commit",
        "--no-verify",
        "-m",
        message,
    )
    with pytest.raises(WorkspaceUnavailable, match="saved commit"):
        workflow.workspaces.commit_branch(eid, message)
    assert store.one("SELECT head_commit FROM experiment_branches WHERE experiment_id=?", (eid,)) == {
        "head_commit": row["head_commit"]
    }


async def test_agent_and_scheduler_jobs_record_the_exact_committed_sources(runtime, tmp_path, monkeypatch):
    from test_iteration_agent_stages import MixedAgent

    class BranchMixedAgent(MixedAgent):
        async def begin(self, thread_id, prompt, schema, *, workspace=None, project_workspace=None, **kwargs):
            payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
            turn = await super().begin(
                thread_id, prompt, schema, workspace=workspace, project_workspace=project_workspace, **kwargs
            )
            if payload["phase"] == "prepare":
                (project_workspace / "shared.py").write_text("value = 5\n")
                value = self.results[(thread_id, turn)]
                value["project_files"] = ["shared.py"]
            return turn

    store, scheduler, _, old = runtime
    git_project(old, store, tmp_path)
    agent = BranchMixedAgent()
    workflow = Workflow(old.config, store, scheduler, agent)
    monkeypatch.setattr(gitworkspace, "_today", lambda: "20260101")
    eid = create(store)
    agent.wf, agent.store, agent.eid, agent.scheduler = workflow, store, eid, scheduler
    try:
        await approve_plan(workflow, store, eid)
        await until(workflow, store, eid, lambda e: e["phase"] == "complete")
        row = workflow.workspaces.branch(eid)
        head = row["head_commit"]
        agent_row = store.one(
            "SELECT * FROM jobs WHERE experiment_id=? AND json_extract(execution_manifest, '$.executor') IS 'agent'",
            (eid,),
        )
        agent_manifest = json.loads(agent_row["execution_manifest"])
        assert agent_manifest["commit"] == head
        assert agent_manifest["source_context"]["git_commit"] == head
        assert (
            git(row["repo"], "rev-parse", f"refs/et/jobs/{agent_row['intent_id']}").decode().strip() == head
        )
        jobs = store.rows(
            "SELECT * FROM jobs WHERE experiment_id=? AND json_extract(execution_manifest, '$.executor') IS NOT 'agent'",
            (eid,),
        )
        assert len(jobs) == 3
        for job in jobs:
            manifest = json.loads(job["execution_manifest"])
            assert manifest["commit"] == head
            assert manifest["source_context"]["git_commit"] == head
            assert git(row["repo"], "rev-parse", f"refs/et/jobs/{job['intent_id']}").decode().strip() == head
            attempt = Path(job["execution_root"])
            assert (
                git(row["worktree"], "show", f"{head}:shared.py").decode()
                == (attempt / "snapshot/project/shared.py").read_text()
            )
        assert git(row["worktree"], "show", f"{head}:shared.py").decode() == "value = 5\n"
    finally:
        await workflow.close()
