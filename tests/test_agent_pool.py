"""Derived agent reservations at every admission site (T1.6a-1).

Each admission site claims its saved source row before creating the dispatch
task. The registry is shadow bookkeeping in this card, so the tests assert the
recorded reservation, unchanged external call counts, and the release or retain
rule that a finished task leaves behind.
"""

import asyncio
import contextlib
import inspect
import json
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from helpers import IterationAgent, role_step, seed_saved_contract3, wait_until
from test_iteration_autopilot import activated

from experimenttracker.agent_pool import experiment_owner, project_owner
from experimenttracker.codex_client import DemoAgent
from experimenttracker.workflow import Workflow


class GatedIterationAgent(IterationAgent):
    """Hold one signed-in phase open so its reservation is observable."""

    models = [{"id": "gpt-6-luna", "name": "GPT-6 Luna", "reasoning_efforts": ["low"]}]

    def __init__(self, gated_phase):
        super().__init__()
        self.gated_phase = gated_phase
        self.release = asyncio.Event()
        self.phases = {}

    async def begin(self, thread, prompt, schema, *, workspace=None, **kwargs):
        if "Plan title:" in prompt:
            self.calls += 1
            turn = f"title-{self.calls}"
            self.results[(thread, turn)] = {"title": f"Saved title {self.calls}"}
            phase = "title"
        else:
            turn = await super().begin(thread, prompt, schema, workspace=workspace, **kwargs)
            phase = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])["phase"]
        self.phases[(thread, turn)] = phase
        return turn

    async def finish(self, thread, turn):
        if self.phases.get((thread, turn)) == self.gated_phase:
            await self.release.wait()
        return await super().finish(thread, turn)


def create(store):
    return seed_saved_contract3(store, "Exercise synthetic iteration mechanisms.")


def saved(store, table, action_id):
    return store.one(f"SELECT * FROM {table} WHERE id=?", (action_id,))


def insert_action(store, eid, state, *, phase="plan", thread=None, turn=None):
    action_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,input,created_at,updated_at) "
            "VALUES(?,?,?,1,?,?,?,'prompt',0,0)",
            (action_id, eid, phase, state, thread, turn),
        )
    return action_id


def claimed(wf, source_table, action_id, role, owner):
    reservation = wf.agent_pool.lookup(source_table, action_id)
    assert (reservation.slot, reservation.role, reservation.owner) == (0, role, owner)
    return reservation


async def until_released(wf, source_table, action_id):
    await wait_until(
        lambda: wf.agent_pool.lookup(source_table, action_id) is None, ticks=lambda: asyncio.sleep(0.01)
    )


@pytest.mark.parametrize(
    "state,retained",
    [
        *((state, False) for state in ("pending", "completed", "failed", "superseded", "waiting")),
        *((state, True) for state in ("dispatching", "running", "uncertain", "received")),
    ],
)
def test_finished_task_settles_from_its_source_row(runtime, state, retained):
    store, _, _, wf = runtime
    eid = create(store)
    action_id = insert_action(store, eid, state, thread="saved-thread", turn="saved-turn")
    reservation = wf.agent_pool.claim(
        role="stage",
        source_table="agent_actions",
        row=saved(store, "agent_actions", action_id),
        owner=experiment_owner(eid),
        slot=wf.agent_pool.primary,
    )
    assert reservation.slot == 0
    wf.agent_pool.settle(reservation)
    if not retained:
        assert wf.agent_pool.lookup("agent_actions", action_id) is None
        return
    # Ownership survives the ended task without a live slot, with the exact saved turn.
    kept = wf.agent_pool.lookup("agent_actions", action_id)
    assert (kept.slot, kept.thread_id, kept.turn_id) == (None, "saved-thread", "saved-turn")


@pytest.mark.usefixtures("contract_three_fixture")
async def test_monitor_and_conversation_admissions_claim_then_release_unsent_turns(runtime, monkeypatch):
    store, _, agent, wf = runtime
    monitored, discussed = create(store), create(store)
    wf.agent_ready = True
    wf.gate.update(await agent.quota())
    store.update(monitored, phase="training")
    store.update(discussed, phase="complete")
    wake_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_wakeups(id,experiment_id,reason,due_at,state,created_at) "
            "VALUES(?,?,?,0,'pending',0)",
            (wake_id, monitored, json.dumps({"reason": "synthetic telemetry gap"})),
        )
    wf.controller.server = object()
    monkeypatch.setattr(wf.controller, "diagnostic_jobs", lambda _: [{"intent_id": "synthetic-job"}])
    monkeypatch.setattr(wf, "dispatch", AsyncMock())
    monkeypatch.setattr(wf.conversation, "prompt", lambda exp, message: "Discuss\nPAYLOAD:\n{}")
    monkeypatch.setattr(wf.conversation, "dispatch", AsyncMock())
    try:
        await role_step(wf.controller)
        watched = store.one("SELECT * FROM agent_actions WHERE experiment_id=?", (monitored,))
        claimed(wf, "agent_actions", watched["id"], "monitor", experiment_owner(monitored))
        await wf.agent_task
        # No turn was sent: the pending source row releases the claim.
        await until_released(wf, "agent_actions", watched["id"])
        wf.agent_task = wf.active_action = None
        store.command(uuid4(), "discuss", {"text": "Explain the result"}, eid=discussed)
        await role_step(wf.conversation)
        spoken = store.one(
            "SELECT * FROM agent_actions WHERE experiment_id=? AND phase LIKE 'discuss:%'", (discussed,)
        )
        claimed(wf, "agent_actions", spoken["id"], "conversation", experiment_owner(discussed))
        await wf.agent_task
        await until_released(wf, "agent_actions", spoken["id"])
    finally:
        wf.controller.server = None


async def test_research_admission_claims_the_project_run_and_retains_received(runtime, monkeypatch):
    store, _, agent, wf = runtime
    service, run = activated(runtime, monkeypatch)
    wf.agent_ready = True
    wf.gate.update(await agent.quota())
    agent.resolved_model = "demo"
    agent.models = [{"id": "demo", "reasoning_efforts": ["medium"], "default_effort": "medium"}]

    async def begin(thread, prompt, schema, **kwargs):
        agent.calls += 1
        agent.results[(thread, f"research-{agent.calls}")] = {
            "summary": "Synthetic checkpoint",
            "central_argument": "No real science in fixture",
            "uncertainty": "Real retrieval remains a live prerequisite",
            "outcome": "wait",
        }
        return f"research-{agent.calls}"

    monkeypatch.setattr(agent, "begin", begin)
    with store.tx() as db:
        service.wake(db, run["id"], "synthetic-input", "experiment_change", {"synthetic": True})
    await role_step(wf.research)
    (row,) = store.rows("SELECT * FROM autopilot_actions WHERE run_id=?", (run["id"],))
    claimed(wf, "autopilot_actions", row["id"], "research", project_owner("test-project"))
    await wf.research.task
    await wait_until(
        lambda: saved(store, "autopilot_actions", row["id"])["state"] == "received",
        ticks=lambda: asyncio.sleep(0.01),
    )
    # A received result keeps owner exclusion without a live connection.
    resume = saved(store, "autopilot_actions", row["id"])
    reservation = wf.agent_pool.lookup("autopilot_actions", row["id"])
    assert reservation.slot is None and reservation.role == "research"
    assert (reservation.thread_id, reservation.turn_id) == (resume["thread_id"], "research-1")
    assert agent.calls == 1
    await wf.research.close()


async def test_supervisor_admission_claims_the_project_turn(runtime):
    store, _, agent, wf = runtime
    wf.agent_ready = True
    wf.gate.update(await agent.quota())
    identity = str(uuid4())
    wf.supervisor.memory.enqueue(
        "test-project", {"request_id": identity, "kind": "chat", "text": "Summarize the measurement."}
    )
    await role_step(wf.supervisor)
    claimed(wf, "project_turns", identity, "supervisor", project_owner("test-project"))
    await wf.supervisor.task
    await wait_until(
        lambda: saved(store, "project_turns", identity)["state"] == "received",
        ticks=lambda: asyncio.sleep(0.01),
    )
    turn = saved(store, "project_turns", identity)
    reservation = wf.agent_pool.lookup("project_turns", identity)
    assert reservation.slot is None and reservation.role == "supervisor"
    assert (reservation.thread_id, reservation.turn_id) == (turn["thread_id"], turn["turn_id"])
    assert agent.calls == 1


@pytest.mark.parametrize("sessions", [1, 2])
async def test_start_reconstructs_saved_turns_before_connecting_the_agent(runtime, monkeypatch, sessions):
    store, scheduler, _, old = runtime
    old.config.codex.max_sessions = sessions
    eid = create(store)
    action_id = insert_action(
        store, eid, "uncertain", phase="discuss:seed", thread="action-thread", turn="action-turn"
    )
    turn_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO project_turns(id,project_id,payload,origin,state,thread_id,turn_id,created_at,updated_at) "
            "VALUES(?,?,'{}','local','running','turn-thread','turn-id',0,0)",
            (turn_id, "test-project"),
        )
    service, run = activated(runtime, monkeypatch)
    research_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO autopilot_actions(id,run_id,cursor,control_revision,state,thread_id,turn_id,created_at,updated_at) "
            "VALUES(?,?,0,0,'dispatching','run-thread','run-turn',0,0)",
            (research_id, run["id"]),
        )
    resume = Workflow(old.config, store, scheduler, DemoAgent())
    connecting = {}
    original = Workflow.connect_agent

    async def connect(self, slot=None):
        connecting.setdefault("keys", sorted(r.key for r in self.agent_pool.occupied()))
        await original(self, slot)

    monkeypatch.setattr(Workflow, "connect_agent", connect)
    await resume.start()
    assert connecting["keys"] == sorted(
        [("agent_actions", action_id), ("project_turns", turn_id), ("autopilot_actions", research_id)]
    )
    assert len(resume.agent_pool.slots) == sessions
    # Reconstructed ownership has no live connection and keeps the exact saved turn.
    assert {r.key: (r.role, r.slot, r.thread_id, r.turn_id) for r in resume.agent_pool.occupied()} == {
        ("agent_actions", action_id): ("conversation", None, "action-thread", "action-turn"),
        ("project_turns", turn_id): ("supervisor", None, "turn-thread", "turn-id"),
        ("autopilot_actions", research_id): ("research", None, "run-thread", "run-turn"),
    }
    assert {r.owner for r in resume.agent_pool.occupied()} == {
        experiment_owner(eid),
        project_owner("test-project"),
    }
    assert resume.agent.calls == 0
    await resume.close()


async def test_configured_slots_share_one_owner_and_keep_primary_state_canonical(runtime):
    """A configured connection is slot state, not a second workflow with its own owner."""
    store, scheduler, _, old = runtime
    old.config.codex.max_sessions = 2
    wf = Workflow(old.config, store, scheduler, DemoAgent())
    await wf.connect_agent()
    await wf.start_agent_pool()

    # Every configured connection is a slot of the one pool; none is another workflow.
    assert [slot.index for slot in wf.agent_pool.slots] == [0, 1]
    assert not any(isinstance(slot, Workflow) for slot in wf.agent_pool.slots)
    assert "Workflow(" not in inspect.getsource(Workflow.start_agent_pool)
    # One owner, gate, registry and controller set: every slot names the same workflow.
    assert {id(slot.workflow) for slot in wf.agent_pool.slots} == {id(wf)}
    assert wf.agent_pool.workflow is wf and wf.agent_pool.primary is wf.agent_pool.slots[0]

    # The primary connection's state is one state, readable and writable both ways.
    primary = wf.agent_pool.primary
    wf.agent_ready = True
    assert primary.agent_ready is True
    primary.agent_error = "synthetic connection error"
    assert wf.agent_error == "synthetic connection error"
    primary.agent_task = marker = asyncio.create_task(asyncio.Event().wait())
    assert wf.agent_task is marker
    marker.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await marker
    wf.agent_task = None
    # Replacing the primary transport cannot leave a stale copy in slot zero.
    replacement = DemoAgent()
    wf.agent = replacement
    assert wf.agent_pool.slots[0].agent is replacement and wf.agent is replacement
    assert wf.agent_pool.slots[1].agent is not replacement
    await wf.close()
