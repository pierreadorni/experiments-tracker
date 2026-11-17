"""Scripted proof of one durable integrator turn for one exact saved conflict (T3.5c)."""

import json
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from experimenttracker.codex_client import NeedsAttention
from experimenttracker.engine import integration_queue as queue
from experimenttracker.engine.gitworkspace import commit, create
from experimenttracker.engine.integration import prepare
from experimenttracker.engine.integration_resolver import PHASE_PREFIX, saved_input
from experimenttracker.workspaces import git


def quota_snapshot(used=0):
    return {
        "ordinaryUsageAllowed": True,
        "rateLimitsByLimitId": {
            "codex": {
                "primary": {"windowDurationMins": 300, "usedPercent": used},
                "secondary": {"windowDurationMins": 10080, "usedPercent": used},
            }
        },
    }


class ScriptedAgent:
    is_demo = False
    available = True
    resolved_model = "scripted"
    start_count = 0
    models = [{"id": "scripted", "reasoning_efforts": ["low", "medium"], "default_effort": "low"}]

    def __init__(self, edit=None, result=None, fail=None):
        self.edit, self.result, self.fail = edit, result, fail
        self.calls = []
        self.sessions = []
        self.turns = []
        self.read_turn_result = None

    async def quota(self):
        return quota_snapshot()

    async def session(self, thread_id, workspace, *, writable=False, **kwargs):
        self.sessions.append((thread_id, Path(workspace), writable))
        return thread_id or "thread-1", "scripted"

    async def begin(self, thread_id, prompt, schema, *, workspace=None, effort="low", **kwargs):
        self.calls.append(("begin", thread_id, prompt, schema, Path(workspace), effort))
        assert set(schema["properties"]) == {"summary"}
        if self.edit is not None:
            self.edit(Path(workspace))
        return "turn-1"

    async def finish(self, thread_id, turn_id):
        self.calls.append(("finish", thread_id, turn_id))
        if self.fail is not None:
            raise self.fail
        return self.result

    async def read_thread(self, thread_id):
        return self.turns

    async def read_turn(self, thread_id, turn_id, **kwargs):
        return self.read_turn_result


def make_repo(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir(parents=True)
    git(repo, "init", "--initial-branch=main")
    git(repo, "config", "user.name", "Test")
    git(repo, "config", "user.email", "test@localhost")
    (repo / "tracked.txt").write_text("base\n")
    git(repo, "add", "--", "tracked.txt")
    git(repo, "commit", "-m", "Source")
    return repo


def conflicted(tmp_path):
    repo = make_repo(tmp_path)
    worktree = Path(create(repo, "demo")["worktree"])
    (worktree / "tracked.txt").write_text("experiment\n")
    head_commit = commit(worktree, "Experiment work")[0]
    (repo / "tracked.txt").write_text("main\n")
    git(repo, "add", "--", "tracked.txt")
    git(repo, "commit", "-m", "Move main")
    return repo, prepare(repo, "conflict", head_commit)


def head(repo, ref="HEAD"):
    return git(repo, "rev-parse", ref).decode().strip()


def save_conflict(store, repo, candidate, *, eid=None, request_id="r1"):
    if eid is None:
        eid = store.command(
            uuid4(),
            "create",
            {"brief": "Synthetic integration conflict resolution check.", "recipe_id": "agent_experiment_v1"},
        )["experiment_id"]
    store.update(eid, phase="complete")
    with store.tx() as db:
        db.execute(
            "INSERT INTO experiment_branches VALUES(?,?,?,?,?,?,'active',NULL,0,0)",
            (
                eid,
                str(repo),
                "exp/demo",
                candidate["worktree"],
                candidate["base_commit"],
                candidate["head_commit"],
            ),
        )
        queue.enqueue(
            db,
            repo,
            request_id,
            {"kind": "merge", "experiment_id": eid, "branch": "exp/demo", "base": "main"},
            experiment_id=eid,
            project_id="test-project",
        )
        queue.claim(db, repo)
        queue.checkpoint(db, request_id, "conflict", candidate=candidate, status="integration conflict")
    return eid


def scripted(workflow, agent):
    workflow.agent_pool.primary.agent = agent
    workflow.agent_pool.primary.agent_ready = True
    return agent


def saved(store, request_id="r1"):
    with store.tx() as db:
        return queue.saved(db, request_id)


def action(store, request_id="r1"):
    return store.one(
        "SELECT * FROM agent_actions WHERE phase=? ORDER BY rowid DESC LIMIT 1",
        (PHASE_PREFIX + request_id,),
    )


async def dispatch_one(workflow):
    await workflow.integration_resolver.advance()
    workflow.agent_pool.admit()
    task = workflow.agent_pool.primary.agent_task
    assert task is not None
    await task
    workflow.agent_pool.primary.agent_task = None


@pytest.mark.asyncio
async def test_one_integrator_turn_resolves_the_exact_conflict_and_returns_to_the_gate(runtime, tmp_path):
    store, _, _, workflow = runtime
    repo, candidate = conflicted(tmp_path)
    eid = save_conflict(store, repo, candidate)
    base = head(repo)

    agent = scripted(
        workflow,
        ScriptedAgent(
            edit=lambda worktree: (worktree / "tracked.txt").write_text("resolved by integrator\n"),
            result={"summary": "kept the base edit and the experiment edit"},
        ),
    )
    await dispatch_one(workflow)

    view = saved(store)
    row = action(store)
    assert (view["stage"], view["state"]) == ("claimed", "open")
    ready = view["candidate"]
    assert (ready["state"], ready["conflicts"], ready["key"]) == ("ready", [], candidate["key"])
    assert ready["head_commit"] == candidate["head_commit"]
    assert (row["state"], json.loads(row["result"])) == ("completed", agent.result)
    assert agent.sessions == [(None, Path(candidate["worktree"]), True)]
    assert (Path(candidate["worktree"]) / "tracked.txt").read_text() == "resolved by integrator\n"
    assert head(repo) == base
    assert head(repo, view["candidate"]["ref"]) == view["candidate"]["candidate_commit"]
    assert saved_input(row)["candidate"] == candidate
    assert len(store.rows("SELECT * FROM agent_actions WHERE phase=?", (PHASE_PREFIX + "r1",))) == 1
    events = store.rows(
        "SELECT kind FROM events WHERE experiment_id=? AND kind IN "
        "('integration_resolution_queued','agent_dispatch_intent','agent_turn_started') ORDER BY seq",
        (eid,),
    )
    assert [event["kind"] for event in events] == [
        "integration_resolution_queued",
        "agent_dispatch_intent",
        "agent_turn_started",
    ]

    # The accepted candidate is the same exact saved identity; the existing gate owns
    # the project check, approval and fast-forward from here.
    from experimenttracker.engine import integration_worker as worker

    applied = worker.drive(
        store,
        "r1",
        project={"base": "main", "check_command": None, "check_timeout": 30, "require_approval": False},
        check=lambda *_: pytest.fail("no check configured"),
    )
    assert applied["outcome"] == "applied"
    assert head(repo) == view["candidate"]["candidate_commit"]


@pytest.mark.asyncio
async def test_quota_denial_never_starts_the_integrator_turn(runtime, tmp_path, monkeypatch):
    store, _, _, workflow = runtime
    repo, candidate = conflicted(tmp_path)
    save_conflict(store, repo, candidate)
    agent = scripted(workflow, ScriptedAgent(result={"summary": "unused"}))

    monkeypatch.setattr(workflow.agent_pool, "quota", lambda **_kwargs: SimpleNamespace(admitted=False))
    await workflow.integration_resolver.advance()
    workflow.agent_pool.admit()
    task = workflow.agent_pool.primary.agent_task
    assert task is not None
    await task
    workflow.agent_pool.primary.agent_task = None

    row = action(store)
    assert (row["state"], row["thread_id"], row["turn_id"]) == ("pending", None, None)
    assert agent.calls == [] and agent.sessions == []
    assert saved(store)["stage"] == "conflict"


@pytest.mark.asyncio
async def test_a_leftover_conflict_marker_is_refused_without_a_second_turn(runtime, tmp_path):
    store, _, _, workflow = runtime
    repo, candidate = conflicted(tmp_path)
    save_conflict(store, repo, candidate)
    base = head(repo)
    scripted(
        workflow,
        ScriptedAgent(
            edit=lambda worktree: (worktree / "tracked.txt").write_text(
                "<<<<<<< HEAD\nmain\n=======\nexperiment\n>>>>>>> exp/demo\n"
            ),
            result={"summary": "not actually resolved"},
        ),
    )
    await dispatch_one(workflow)

    view = saved(store)
    row = action(store)
    assert view["stage"] == "conflict"
    assert "conflict markers" in row["error"]
    assert head(repo) == base
    assert not git(repo, "for-each-ref", "--format=%(refname)", candidate["ref"]).decode().strip()

    # A terminal action is never replayed automatically for the same saved conflict.
    await workflow.integration_resolver.advance()
    workflow.agent_pool.admit()
    assert workflow.agent_pool.primary.agent_task is None
    assert action(store)["id"] == row["id"]


@pytest.mark.asyncio
async def test_uncertain_integrator_turn_is_read_back_not_resent(runtime, tmp_path):
    store, _, _, workflow = runtime
    repo, candidate = conflicted(tmp_path)
    save_conflict(store, repo, candidate)
    base = head(repo)
    agent = scripted(
        workflow,
        ScriptedAgent(
            edit=lambda worktree: (worktree / "tracked.txt").write_text("resolved after restart\n"),
            fail=NeedsAttention("lost"),
        ),
    )
    await dispatch_one(workflow)

    row = action(store)
    assert row["state"] == "uncertain" and row["turn_id"] == "turn-1"
    event = store.one(
        "SELECT payload FROM events WHERE experiment_id=? AND kind='agent_dispatch_intent'",
        (row["experiment_id"],),
    )
    prompt_hash = json.loads(event["payload"])["prompt_sha256"]
    agent.turns = [{"id": "turn-1", "status": "completed", "prompt_sha256": prompt_hash}]
    agent.read_turn_result = {"summary": "recovered exact completed turn"}
    workflow.agent_ready = True

    await workflow.resolver.resolve()

    view = saved(store)
    assert view["stage"] == "claimed"
    assert action(store)["state"] == "completed"
    assert head(repo) == base
    assert len([call for call in agent.calls if call[0] == "begin"]) == 1
    # The exact completed turn was read back; no replacement turn was sent.
    assert [call[0] for call in agent.calls].count("begin") == 1
    assert head(repo, view["candidate"]["ref"]) == view["candidate"]["candidate_commit"]


@pytest.mark.asyncio
async def test_unknown_conflict_input_is_not_queued(runtime, tmp_path):
    store, _, _, workflow = runtime
    repo, candidate = conflicted(tmp_path)
    eid = save_conflict(store, repo, candidate)
    # The worktree no longer holds the saved unmerged paths.
    worktree = Path(candidate["worktree"])
    (worktree / "tracked.txt").write_text("already resolved by owner\n")
    git(worktree, "add", "--", "tracked.txt")

    await workflow.integration_resolver.advance()

    assert action(store) is None
    view = saved(store)
    assert view["stage"] == "conflict"
    assert "manual inspection" in view["status"]
    assert workflow.agent_pool.primary.agent_task is None
    assert store.experiment(eid)["phase"] == "complete"
