"""Scripted boundary proof of the inert C7 research dispatch handoff (T4.1e)."""

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from test_projects import register

from experimenttracker.codex_client import NeedsAttention
from experimenttracker.engine import gitworkspace
from experimenttracker.research.checkpoints import SaveCheckpoint, save_checkpoint
from experimenttracker.research.dispatch import PHASE, ResearchDispatch
from experimenttracker.research.prompt import MAX_PROMPT_BYTES
from experimenttracker.research.session import first_turn, follow_up_turn
from experimenttracker.workspaces import git, initialize_project

BRIEF = "Compare two toy optimizers under one fixed compute budget."


def quota_snapshot(used=0):
    codex = {
        "primary": {"windowDurationMins": 300, "usedPercent": used},
        "secondary": {"windowDurationMins": 10080, "usedPercent": used},
    }
    return {"ordinaryUsageAllowed": True, "rateLimitsByLimitId": {"codex": codex}}


class ScriptedAgent:
    """Fake managed runtime: no model, scheduler or network call leaves this process."""

    is_demo = False
    available = True
    resolved_model = "scripted"
    start_count = 0
    models = [{"id": "scripted", "reasoning_efforts": ["low"], "default_effort": "low"}]

    def __init__(self, result=None, begin_fail=None, session_effect=None, session_thread=None):
        self.result, self.begin_fail = result, begin_fail
        self.session_effect, self.session_thread = session_effect, session_thread
        self.sessions, self.calls = [], []

    async def quota(self):
        return quota_snapshot()

    async def close(self):
        pass

    async def session(self, thread_id, workspace, *, writable=False, **kwargs):
        self.sessions.append((thread_id, Path(workspace), writable))
        if self.session_effect is not None:
            self.session_effect()
        return self.session_thread or thread_id or "thread-1", "scripted"

    async def begin(self, thread_id, prompt, schema, *, workspace=None, effort="low", **kwargs):
        self.calls.append(("begin", thread_id, prompt, Path(workspace)))
        assert set(schema["properties"]) == {"status", "summary", "next", "integrate"}
        if self.begin_fail is not None:
            raise self.begin_fail
        return "turn-1"

    async def finish(self, thread_id, turn_id):
        self.calls.append(("finish", thread_id, turn_id))
        return self.result


def git_project(workflow, store, tmp_path):
    source = register(workflow.config, store, tmp_path)
    initialize_project(source)
    (source / "shared.py").write_text("value = 1\n")
    git(source, "add", "--", "shared.py")
    git(source, "-c", "user.name=Test", "-c", "user.email=test@localhost", "commit", "-m", "Source")
    workflow.projects.reload()
    return source


def research_setup(runtime, tmp_path, monkeypatch):
    """Save one manually migrated contract 4 experiment with its named branch worktree."""
    store, _, _, workflow = runtime
    source = git_project(workflow, store, tmp_path)
    monkeypatch.setattr(gitworkspace, "_today", lambda: "20260101")
    eid = store.command(
        uuid4(),
        "create",
        {"brief": BRIEF, "recipe_id": "agent_experiment_v1", "project_id": "anyup-rs"},
    )["experiment_id"]
    contract = store.experiment(eid)["contract"]
    contract["execution_contract"] = 4
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (json.dumps(contract), eid))
    workflow.workspaces.ensure_branch(eid)
    action = first_turn(store, eid)
    return source, eid, workflow.workspaces.branch(eid), action


def scripted(workflow, agent):
    workflow.agent_pool.primary.agent = agent
    workflow.agent_pool.primary.agent_ready = True
    return agent


async def run_pass(workflow, dispatcher):
    await dispatcher.advance()
    workflow.agent_pool.admit()
    task = workflow.agent_pool.primary.agent_task
    if task is not None:
        await task
        workflow.agent_pool.primary.agent_task = None


def saved_action(store, action_id):
    return store.one("SELECT * FROM agent_actions WHERE id=?", (action_id,))


def events(store, eid, kinds):
    return store.rows(
        f"SELECT kind,payload FROM events WHERE experiment_id=? AND kind IN ({','.join('?' * len(kinds))}) "
        "ORDER BY seq",
        (eid, *kinds),
    )


def completion(recorded):
    async def complete(action, result):
        recorded.append((action, result))

    return complete


@pytest.mark.asyncio
async def test_pending_research_row_runs_once_on_the_saved_named_worktree(runtime, tmp_path, monkeypatch):
    store, _, _, workflow = runtime
    _, eid, branch, action = research_setup(runtime, tmp_path, monkeypatch)
    agent = scripted(
        workflow,
        ScriptedAgent(result={"status": "continue", "summary": "measured", "next": "compare"}),
    )
    completed = []

    await run_pass(workflow, ResearchDispatch(workflow, completion(completed)))

    row = saved_action(store, action["id"])
    assert (row["state"], row["thread_id"], row["turn_id"], row["attempts"]) == (
        "running",
        "thread-1",
        "turn-1",
        1,
    )
    assert agent.sessions == [(None, Path(branch["worktree"]), True)]
    assert [call[0] for call in agent.calls] == ["begin", "finish"]
    assert agent.calls[0][1:3] == ("thread-1", action["input"])
    assert store.experiment(eid)["thread_id"] == "thread-1"
    assert completed == [(row, agent.result)]

    rows = events(store, eid, ["agent_dispatch_intent", "agent_turn_started"])
    assert [row["kind"] for row in rows] == ["agent_dispatch_intent", "agent_turn_started"]
    intent = json.loads(rows[0]["payload"])
    assert intent == {
        "action_id": action["id"],
        "phase": PHASE,
        "thread_id": "thread-1",
        "prompt_sha256": hashlib.sha256(action["input"].encode()).hexdigest(),
    }
    assert json.loads(rows[1]["payload"]) == {"action_id": action["id"], "turn_id": "turn-1"}


@pytest.mark.asyncio
async def test_only_the_saved_experiment_thread_is_resumed(runtime, tmp_path, monkeypatch):
    store, _, _, workflow = runtime
    _, eid, branch, action = research_setup(runtime, tmp_path, monkeypatch)
    with store.tx() as db:
        db.execute("UPDATE experiments SET thread_id=? WHERE id=?", ("thread-9", eid))
        db.execute("UPDATE agent_actions SET thread_id=? WHERE id=?", ("thread-9", action["id"]))
    agent = scripted(workflow, ScriptedAgent(result={"status": "wait", "summary": "s", "next": "n"}))

    await run_pass(workflow, ResearchDispatch(workflow, completion([])))

    assert agent.sessions == [("thread-9", Path(branch["worktree"]), True)]
    assert agent.calls[0][1] == "thread-9"
    assert saved_action(store, action["id"])["thread_id"] == "thread-9"


@pytest.mark.asyncio
async def test_follow_up_dispatch_seeds_the_latest_checkpoint_without_persisting_the_prompt(
    runtime, tmp_path, monkeypatch
):
    store, _, _, workflow = runtime
    _, eid, _, action = research_setup(runtime, tmp_path, monkeypatch)
    agent = scripted(workflow, ScriptedAgent())
    dispatcher = ResearchDispatch(workflow, completion([]))
    await run_pass(workflow, dispatcher)
    running = saved_action(store, action["id"])
    for markdown in ("# old private notebook", "# latest private notebook\n" + "界" * 1300):
        save_checkpoint(store, running, SaveCheckpoint(request_id=uuid4(), markdown=markdown))
    workflow._action_state(action["id"], "completed", result={"status": "continue"})
    next_action = follow_up_turn(store, eid)
    assert next_action is not None and "private notebook" not in next_action["input"]
    # Simulate a lost start reply that quotes the prompt: the exact receipt must
    # still match the runtime input, but the exception cannot publish the Markdown.
    agent.begin_fail = RuntimeError(markdown)
    await run_pass(workflow, dispatcher)

    sent_prompt = agent.calls[-1][2]
    assert f"ACTION {next_action['id']}" in sent_prompt
    assert "# latest private notebook" in sent_prompt and "# old private notebook" not in sent_prompt
    assert len(sent_prompt.encode()) < MAX_PROMPT_BYTES
    saved = saved_action(store, next_action["id"])
    assert saved["state"] == "uncertain"
    assert saved["input"] == next_action["input"] and "private notebook" not in saved["error"]
    assert workflow.dispatch_prompt_hash(saved) == hashlib.sha256(sent_prompt.encode()).hexdigest()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("last_tokens", "usage_turn", "has_checkpoint", "rotate"),
    [
        (800, "turn-1", True, True),
        (799, "turn-1", True, False),
        (900, "other-turn", True, False),
        (900, "turn-1", False, False),
    ],
)
async def test_follow_up_rotates_only_with_exact_context_pressure_and_checkpoint(
    runtime, tmp_path, monkeypatch, last_tokens, usage_turn, has_checkpoint, rotate
):
    store, _, _, workflow = runtime
    _, eid, branch, first = research_setup(runtime, tmp_path, monkeypatch)
    agent = scripted(workflow, ScriptedAgent())
    dispatcher = ResearchDispatch(workflow, completion([]))
    await run_pass(workflow, dispatcher)
    first_running = saved_action(store, first["id"])
    notebook = "# Private rotation notebook"
    if has_checkpoint:
        save_checkpoint(store, first_running, SaveCheckpoint(request_id=uuid4(), markdown=notebook))
    workflow._action_state(first["id"], "completed", result={"status": "continue"})
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_usage(action_id,thread_id,turn_id,payload,observed_at) VALUES(?,?,?,?,0)",
            (
                first["id"],
                "thread-1",
                usage_turn,
                json.dumps(
                    {
                        "model_context_window": 1000,
                        "last_request": {"totalTokens": last_tokens},
                        "thread_total": {"totalTokens": 100_000},
                    }
                ),
            ),
        )
    follow_up = follow_up_turn(store, eid)
    agent.session_thread = "thread-2" if rotate else None
    agent.begin_fail = RuntimeError("lost start reply")

    await run_pass(workflow, dispatcher)
    await run_pass(workflow, dispatcher)

    thread = "thread-2" if rotate else "thread-1"
    assert len(agent.sessions) == 2
    assert agent.sessions[-1] == (None if rotate else "thread-1", Path(branch["worktree"]), True)
    assert store.experiment(eid)["thread_id"] == thread
    assert saved_action(store, first["id"])["thread_id"] == "thread-1"
    saved = saved_action(store, follow_up["id"])
    assert (saved["state"], saved["thread_id"], saved["attempts"]) == ("uncertain", thread, 1)
    _, sent_thread, prompt, _ = agent.calls[-1]
    assert sent_thread == thread and (notebook in prompt) == has_checkpoint
    assert notebook not in saved["input"] and notebook not in saved["error"]
    receipt = json.loads(events(store, eid, ["agent_dispatch_intent"])[-1]["payload"])
    assert receipt.get("rotated_from_thread_id") == ("thread-1" if rotate else None)
    assert receipt["prompt_sha256"] == hashlib.sha256(prompt.encode()).hexdigest()


@pytest.mark.asyncio
@pytest.mark.parametrize("gate", ["paused", "cancelled", "maintenance", "quota"])
async def test_no_work_starts_behind_a_gate(runtime, tmp_path, monkeypatch, gate):
    store, _, _, workflow = runtime
    _, eid, _, action = research_setup(runtime, tmp_path, monkeypatch)
    agent = scripted(workflow, ScriptedAgent(result={"status": "continue", "summary": "s", "next": "n"}))
    if gate == "paused":
        store.update(eid, pause_agent=True)
    elif gate == "cancelled":
        store.update(eid, phase="cancelled")
    elif gate == "maintenance":
        monkeypatch.setattr("experimenttracker.research.dispatch.blocked", lambda _store: True)
    else:
        monkeypatch.setattr(workflow.agent_pool, "quota", lambda **_kwargs: SimpleNamespace(admitted=False))

    await run_pass(workflow, ResearchDispatch(workflow, completion([])))

    assert agent.sessions == [] and agent.calls == []
    saved = saved_action(store, action["id"])
    assert (saved["state"], saved["thread_id"], saved["turn_id"]) == ("pending", None, None)
    assert events(store, eid, ["agent_dispatch_intent", "agent_turn_started"]) == []


@pytest.mark.asyncio
async def test_a_missing_named_worktree_never_falls_back_to_the_shared_checkout(
    runtime, tmp_path, monkeypatch
):
    store, _, _, workflow = runtime
    source, eid, _, action = research_setup(runtime, tmp_path, monkeypatch)
    agent = scripted(workflow, ScriptedAgent(result={"status": "continue", "summary": "s", "next": "n"}))
    with store.tx() as db:
        db.execute("DELETE FROM experiment_branches WHERE experiment_id=?", (eid,))
    # The shared checkout is still resolvable, so only the named-worktree rule refuses.
    assert workflow.workspaces.checkout(eid) == str(source)

    await run_pass(workflow, ResearchDispatch(workflow, completion([])))

    assert agent.sessions == [] and agent.calls == []
    saved = saved_action(store, action["id"])
    assert saved["state"] == "pending"
    assert "branch identity" in saved["error"]
    assert events(store, eid, ["agent_dispatch_intent", "agent_turn_started"]) == []


@pytest.mark.asyncio
async def test_a_lost_start_response_stays_uncertain_and_is_never_retried(runtime, tmp_path, monkeypatch):
    store, _, _, workflow = runtime
    _, eid, _, action = research_setup(runtime, tmp_path, monkeypatch)
    agent = scripted(
        workflow,
        ScriptedAgent(begin_fail=NeedsAttention("turn/start response timed out")),
    )

    await run_pass(workflow, ResearchDispatch(workflow, completion([])))

    saved = saved_action(store, action["id"])
    assert (saved["state"], saved["thread_id"], saved["turn_id"], saved["attempts"]) == (
        "uncertain",
        "thread-1",
        None,
        1,
    )
    assert "timed out" in saved["error"]
    rows = events(store, eid, ["agent_dispatch_intent", "agent_turn_started"])
    assert [row["kind"] for row in rows] == ["agent_dispatch_intent"]
    intent = json.loads(rows[0]["payload"])
    assert intent["prompt_sha256"] == hashlib.sha256(action["input"].encode()).hexdigest()
    assert workflow.agent_pool.turn_capacity() == 0

    await ResearchDispatch(workflow, completion([])).advance()
    workflow.agent_pool.admit()
    assert workflow.agent_pool.primary.agent_task is None
    assert [call[0] for call in agent.calls].count("begin") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["thread", "worktree", "pause", "stale"])
async def test_a_fault_after_an_await_never_reaches_a_dispatch(runtime, tmp_path, monkeypatch, fault):
    store, _, _, workflow = runtime
    _, eid, _, action = research_setup(runtime, tmp_path, monkeypatch)
    if fault == "thread":
        with store.tx() as db:
            db.execute("UPDATE experiments SET thread_id=? WHERE id=?", ("thread-9", eid))
            db.execute("UPDATE agent_actions SET thread_id=? WHERE id=?", ("thread-9", action["id"]))
    real = workflow.workspaces.branch_checkout

    def supersede():
        with store.tx() as db:
            db.execute("UPDATE agent_actions SET state='superseded' WHERE id=?", (action["id"],))

    def checkout(target):
        if fault == "pause":
            store.update(target, pause_agent=True)
        elif fault == "stale":
            supersede()
        return real(target)

    def remove_branch():
        with store.tx() as db:
            db.execute("DELETE FROM experiment_branches WHERE experiment_id=?", (eid,))

    monkeypatch.setattr(workflow.workspaces, "branch_checkout", checkout)
    agent = scripted(
        workflow,
        ScriptedAgent(
            session_effect=remove_branch if fault == "worktree" else None,
            session_thread="other-thread" if fault == "thread" else None,
        ),
    )

    await run_pass(workflow, ResearchDispatch(workflow, completion([])))

    saved = saved_action(store, action["id"])
    assert agent.calls == []
    assert events(store, eid, ["agent_dispatch_intent", "agent_turn_started"]) == []
    assert saved["state"] == ("superseded" if fault == "stale" else "pending")
    if fault in ("thread", "worktree"):
        assert len(agent.sessions) == 1 and saved["error"]
    else:
        assert agent.sessions == []
