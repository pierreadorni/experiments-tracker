"""C7 readback routing: live, resolver and operator turns settle through one path (T4.1f-2)."""

import hashlib
import json
from uuid import uuid4

import pytest
from test_research_dispatch import ScriptedAgent, research_setup, saved_action, scripted
from test_research_settlement import events as commit_events
from test_research_settlement import setup as running_setup
from test_research_settlement import turn as turn_result

from experimenttracker import clock
from experimenttracker.codex_client import NeedsAttention
from experimenttracker.core import action_obligations


class ReadbackAgent(ScriptedAgent):
    """A scripted runtime that also serves saved thread and turn reads."""

    def __init__(self, *, thread=None, turns=None, **kwargs):
        super().__init__(**kwargs)
        self.thread = thread or []
        self.turns = turns or {}
        self.reads = []

    async def read_thread(self, thread_id):
        self.reads.append(("thread", thread_id))
        return self.thread

    async def read_turn(self, thread_id, turn_id, *, expected_prompt_hash=None):
        self.reads.append(("turn", thread_id, turn_id, expected_prompt_hash))
        if expected_prompt_hash is not None:
            saved_hash = next(
                (row["prompt_sha256"] for row in self.thread if row["id"] == turn_id), None
            )
            if saved_hash != expected_prompt_hash:
                raise NeedsAttention("saved turn input does not match the exact dispatched prompt")
        return self.turns[(thread_id, turn_id)]


def intent(store, eid, action, thread="thread-1"):
    """Save the exact dispatch intent a research turn records before it starts."""
    prompt_hash = hashlib.sha256(action["input"].encode()).hexdigest()
    with store.tx() as db:
        store.emit(
            db,
            eid,
            "agent_dispatch_intent",
            {
                "action_id": action["id"],
                "phase": "research",
                "thread_id": thread,
                "prompt_sha256": prompt_hash,
            },
        )
    return prompt_hash


def unsaved(store, action):
    """Leave the saved action uncertain with no turn identity, as a lost start does."""
    with store.tx() as db:
        before = dict(db.execute("SELECT * FROM agent_actions WHERE id=?", (action["id"],)).fetchone())
        db.execute(
            "UPDATE agent_actions SET state='uncertain',turn_id=NULL,attempts=1 WHERE id=?",
            (action["id"],),
        )
        action_obligations.sync_action(db, action["id"], before=before)
    return saved_action(store, action["id"])


def kinds(store, eid, *wanted):
    return [
        row["kind"]
        for row in store.rows(
            f"SELECT kind FROM events WHERE experiment_id=? AND kind IN ({','.join('?' * len(wanted))}) "
            "ORDER BY seq",
            (eid, *wanted),
        )
    ]


@pytest.mark.asyncio
async def test_control_finish_offers_a_saved_research_row_to_the_settlement_callback(
    runtime, tmp_path, monkeypatch
):
    store, _, _, workflow = runtime
    _, eid, _, action = research_setup(runtime, tmp_path, monkeypatch)
    agent = scripted(
        workflow, ReadbackAgent(result={"status": "continue", "summary": "measured", "next": "compare"})
    )

    await workflow.step_control_finish()
    workflow.agent_pool.admit()
    task = workflow.agent_pool.primary.agent_task
    assert task is not None
    await task
    workflow.agent_pool.primary.agent_task = None

    row = saved_action(store, action["id"])
    assert (row["state"], row["thread_id"], row["turn_id"]) == ("completed", "thread-1", "turn-1")
    assert [call[0] for call in agent.calls] == ["begin", "finish"]
    assert kinds(store, eid, "workspace_commit_intent", "workspace_commit_settled") == [
        "workspace_commit_intent",
        "workspace_commit_settled",
    ]
    assert store.one("SELECT state FROM obligations WHERE target=? ORDER BY rowid DESC", (action["id"],))[
        "state"
    ] == "satisfied"


@pytest.mark.asyncio
async def test_operator_readback_uses_the_read_slot_and_settles_one_commit(runtime, tmp_path, monkeypatch):
    store, eid, workflow, action = running_setup(runtime, tmp_path, monkeypatch)
    agent = scripted(
        workflow,
        ReadbackAgent(turns={("thread-1", "turn-1"): turn_result("continue")}),
    )

    receipt = await workflow.reconcile_action(action["id"])

    assert receipt == {
        "action_id": action["id"],
        "thread_id": "thread-1",
        "turn_id": "turn-1",
        "state": "completed",
    }
    assert saved_action(store, action["id"])["state"] == "completed"
    assert agent.reads == [("turn", "thread-1", "turn-1", None)]
    assert kinds(store, eid, "workspace_commit_intent", "workspace_commit_settled") == [
        "workspace_commit_intent",
        "workspace_commit_settled",
    ]


@pytest.mark.asyncio
async def test_a_replayed_completed_research_turn_returns_the_same_receipt_without_a_read(
    runtime, tmp_path, monkeypatch
):
    store, _, workflow, action = running_setup(runtime, tmp_path, monkeypatch)
    with store.tx() as db:
        db.execute("UPDATE agent_actions SET state='completed' WHERE id=?", (action["id"],))
    agent = scripted(workflow, ReadbackAgent())

    def refuse(*_args, **_kwargs):
        raise AssertionError("a completed exact turn must not be read again")

    monkeypatch.setattr(agent, "read_turn", refuse)
    first = await workflow.reconcile_action(action["id"], turn_id="turn-1")
    second = await workflow.reconcile_action(action["id"], turn_id="turn-1")

    assert first == second
    assert first["state"] == "completed"
    assert agent.reads == []
    assert commit_events(store, action["experiment_id"]) == []


@pytest.mark.asyncio
async def test_a_missing_research_turn_adopts_only_the_saved_dispatch_hash(runtime, tmp_path, monkeypatch):
    store, eid, workflow, action = running_setup(runtime, tmp_path, monkeypatch)
    expected = unsaved(store, action)
    prompt_hash = intent(store, eid, expected)
    agent = scripted(
        workflow,
        ReadbackAgent(
            thread=[{"id": "turn-9", "status": "completed", "prompt_sha256": prompt_hash}],
            turns={("thread-1", "turn-9"): turn_result("continue")},
        ),
    )

    receipt = await workflow.reconcile_action(expected["id"], turn_id="turn-9")

    assert (receipt["state"], receipt["turn_id"]) == ("completed", "turn-9")
    assert saved_action(store, expected["id"])["turn_id"] == "turn-9"
    assert agent.reads == [("turn", "thread-1", "turn-9", prompt_hash)]


@pytest.mark.asyncio
async def test_a_prior_turn_with_identical_scientific_input_is_never_accepted(
    runtime, tmp_path, monkeypatch
):
    store, eid, workflow, action = running_setup(runtime, tmp_path, monkeypatch)
    expected = unsaved(store, action)
    prompt_hash = intent(store, eid, expected)
    older = hashlib.sha256(b"older action with the same brief").hexdigest()
    scripted(
        workflow,
        ReadbackAgent(
            thread=[{"id": "turn-old", "status": "completed", "prompt_sha256": older}],
            turns={("thread-1", "turn-old"): turn_result("continue")},
        ),
    )
    assert prompt_hash != older

    with pytest.raises(NeedsAttention, match="does not match"):
        await workflow.reconcile_action(expected["id"], turn_id="turn-old")

    saved = saved_action(store, expected["id"])
    assert (saved["state"], saved["turn_id"]) == ("uncertain", None)
    assert commit_events(store, eid) == []


@pytest.mark.asyncio
async def test_a_turn_already_owned_by_another_action_is_refused_before_a_read(
    runtime, tmp_path, monkeypatch
):
    store, eid, workflow, action = running_setup(runtime, tmp_path, monkeypatch)
    expected = unsaved(store, action)
    intent(store, eid, expected)
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,"
            "thread_id,turn_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (str(uuid4()), eid, "research", 0, "completed", "other", "thread-1", "turn-9", 0, 0),
        )
    agent = scripted(workflow, ReadbackAgent())

    with pytest.raises(NeedsAttention, match="already belongs"):
        await workflow.reconcile_action(expected["id"], turn_id="turn-9")

    assert agent.reads == []
    assert saved_action(store, expected["id"])["state"] == "uncertain"


@pytest.mark.asyncio
async def test_the_resolver_adopts_a_completed_research_turn_through_settlement(
    runtime, tmp_path, monkeypatch
):
    store, eid, workflow, action = running_setup(runtime, tmp_path, monkeypatch)
    expected = unsaved(store, action)
    prompt_hash = intent(store, eid, expected)
    agent = scripted(
        workflow,
        ReadbackAgent(
            thread=[{"id": "turn-9", "status": "completed", "prompt_sha256": prompt_hash}],
            turns={("thread-1", "turn-9"): turn_result("continue")},
        ),
    )

    await workflow.resolver.resolve_action(expected)

    saved = saved_action(store, expected["id"])
    assert (saved["state"], saved["turn_id"], saved["attempts"]) == ("completed", "turn-9", 1)
    assert agent.calls == []
    assert kinds(store, eid, "workspace_commit_intent", "workspace_commit_settled") == [
        "workspace_commit_intent",
        "workspace_commit_settled",
    ]


@pytest.mark.asyncio
async def test_a_proven_missing_research_turn_fails_for_inspection_without_a_continuation(
    runtime, tmp_path, monkeypatch
):
    store, eid, workflow, action = running_setup(runtime, tmp_path, monkeypatch)
    expected = unsaved(store, action)
    intent(store, eid, expected)
    scripted(workflow, ReadbackAgent(thread=[]))
    monkeypatch.setattr(workflow.resolver, "dispatched_at", lambda _action: clock.now() - 301)

    await workflow.resolver.resolve_action(expected)

    saved = saved_action(store, expected["id"])
    assert (saved["state"], saved["error"]) == ("failed", "turn_not_started")
    assert [row["id"] for row in store.rows(
        "SELECT id FROM agent_actions WHERE experiment_id=?", (eid,)
    )] == [expected["id"]]
    exp = store.experiment(eid)
    assert (exp["hold_reason"], exp["error"]) == ("agent_dispatch_uncertain", "turn_not_started")
    evidence = [
        json.loads(row["payload"])
        for row in store.rows(
            "SELECT payload FROM events WHERE experiment_id=? AND kind='agent_action_changed'", (eid,)
        )
    ]
    assert len(evidence) == 1
    assert evidence[0]["action_id"] == expected["id"]
    assert evidence[0]["thread_id"] == "thread-1"
    assert evidence[0]["thread_turns"] == 0
    assert evidence[0]["state"] == "failed"
    assert evidence[0]["error"] == "turn_not_started"
    assert kinds(store, eid, "agent_continuation_queued") == []


@pytest.mark.asyncio
async def test_readback_never_changes_a_cancelled_experiment_phase(runtime, tmp_path, monkeypatch):
    store, eid, workflow, action = running_setup(runtime, tmp_path, monkeypatch)
    store.update(eid, phase="cancelled")
    scripted(workflow, ReadbackAgent(turns={("thread-1", "turn-1"): turn_result("continue")}))

    receipt = await workflow.reconcile_action(action["id"])

    assert receipt["state"] == "completed"
    assert store.experiment(eid)["phase"] == "cancelled"
