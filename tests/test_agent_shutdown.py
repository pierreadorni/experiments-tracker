"""Quota interruption, readback settlement and shutdown through reservations (T1.6a-3).

Every interrupt and readback leaves through the reservation's own process, a saved
turn settled outside its dispatch task releases exactly what its row proves, and
shutdown stops admissions before it closes each transport once.
"""

import asyncio
import json
from uuid import uuid4

import pytest
from helpers import seed_saved_contract3

from experimenttracker.agent_pool import experiment_owner, project_owner
from experimenttracker.codex_client import DemoAgent, NeedsAttention
from experimenttracker.workflow import Workflow


class LaneAgent(DemoAgent):
    """Managed-agent double with its own connection identity, interrupt log and close count."""

    def __init__(self):
        super().__init__()
        self.interrupts = []
        self.closes = 0

    async def interrupt(self, thread, turn):
        self.interrupts.append((thread, turn))

    async def close(self):
        self.closes += 1


class ReadbackAgent(LaneAgent):
    """Managed-agent double that can read one saved thread and leaves another unreadable."""

    def __init__(self):
        super().__init__()
        self.threads = {}

    async def read_thread(self, thread_id):
        if thread_id not in self.threads:
            raise NeedsAttention("synthetic thread is unreadable")
        return self.threads[thread_id]


def create(store):
    return seed_saved_contract3(store, "Exercise synthetic agent ownership.")


def insert_action(store, eid, state, *, phase="prepare", thread=None, turn=None):
    action_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,input,created_at,updated_at) "
            "VALUES(?,?,?,1,?,?,?,'prompt',0,0)",
            (action_id, eid, phase, state, thread, turn),
        )
    return store.one("SELECT * FROM agent_actions WHERE id=?", (action_id,))


def insert_project_turn(store, *, thread, turn):
    turn_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO project_turns(id,project_id,payload,origin,state,thread_id,turn_id,created_at,updated_at) "
            "VALUES(?,?,'{}','local','running',?,?,0,0)",
            (turn_id, "test-project", thread, turn),
        )
    return store.one("SELECT * FROM project_turns WHERE id=?", (turn_id,))


def claim(wf, slot, row, role="stage", source_table="agent_actions", owner=None):
    return wf.agent_pool.claim(
        role=role,
        source_table=source_table,
        row=row,
        owner=owner or experiment_owner(row["experiment_id"]),
        slot=slot,
    )


def save_dispatch_intent(store, action, prompt_sha256="synthetic-prompt"):
    """Save the exact dispatch intent a readback needs to prove the saved turn."""
    with store.tx() as db:
        store.emit(
            db,
            action["experiment_id"],
            "agent_dispatch_intent",
            {
                "action_id": action["id"],
                "thread_id": action["thread_id"],
                "prompt_sha256": prompt_sha256,
            },
        )


async def pooled(runtime):
    """One pool over two slots; each slot has its own started agent process."""
    store, scheduler, _, old = runtime
    old.config.codex.max_sessions = 2
    wf = Workflow(old.config, store, scheduler, LaneAgent())
    await wf.connect_agent()
    await wf.start_agent_pool()
    return wf


async def test_quota_interrupt_reaches_each_live_reservation_once_through_its_own_process(
    runtime, monkeypatch
):
    wf = await pooled(runtime)
    store, first, second = wf.store, wf.agent_pool.slots[0], wf.agent_pool.slots[1]
    left_eid, right_eid, saved_eid = create(store), create(store), create(store)
    left = insert_action(store, left_eid, "running", thread="left", turn="left-turn")
    right = insert_action(store, right_eid, "running", thread="right", turn="right-turn")
    saved = insert_action(store, saved_eid, "running", thread="saved", turn="saved-turn")
    project = insert_project_turn(store, thread="project-thread", turn="project-turn")
    # Reconstructed ownership first: the two live claims below own a process.
    wf.agent_pool.reconstruct()
    assert wf.agent_pool.lookup("agent_actions", saved["id"]).connection is None
    claim(wf, first, left)
    claim(wf, second, right)
    claim(wf, second, project, "supervisor", "project_turns", project_owner("test-project"))

    quota = wf.agent.quota

    async def denied():
        return {**await quota(), "ordinaryUsageAllowed": False}

    monkeypatch.setattr(wf.agent, "quota", denied)
    await wf.refresh_quota()

    # Each turn is interrupted exactly once, on the process that holds it.
    assert first.agent.interrupts == [("left", "left-turn")]
    assert sorted(second.agent.interrupts) == [("project-thread", "project-turn"), ("right", "right-turn")]
    assert sum(len(slot.agent.interrupts) for slot in wf.agent_pool.slots) == 3
    for row in (left, right):
        action = store.one("SELECT * FROM agent_actions WHERE id=?", (row["id"],))
        assert (action["state"], action["interrupt_reason"]) == ("running", "quota_hold")
        assert store.experiment(row["experiment_id"])["hold_reason"] == "quota_hold"
    # A reservation without a live connection is never interrupted elsewhere.
    kept = store.one("SELECT * FROM agent_actions WHERE id=?", (saved["id"],))
    assert (kept["state"], kept["interrupt_reason"]) == ("running", None)
    assert store.experiment(saved_eid)["hold_reason"] is None
    assert wf.agent_pool.lookup("agent_actions", saved["id"]).slot is None
    await wf.close()


async def test_shutdown_stops_admissions_before_roles_and_closes_each_transport_once(runtime, monkeypatch):
    wf = await pooled(runtime)
    store, slot = wf.store, wf.agent_pool.slots[1]
    held = insert_action(store, create(store), "uncertain", thread="held", turn="held-turn")
    wf.agent_pool.reconstruct()
    waiting = asyncio.Event()

    async def unfinished():
        await waiting.wait()

    live = insert_action(store, create(store), "running", thread="live", turn="live-turn")
    claim(wf, slot, live)
    slot.agent_task = asyncio.create_task(unfinished())
    await asyncio.sleep(0)
    observed = []

    async def research_close():
        observed.append(("research", wf.closing, wf.agent_pool.closing))

    async def supervisor_close():
        observed.append(("supervisor", wf.closing, wf.agent_pool.closing))

    async def controller_close():
        observed.append(("controller", wf.closing, wf.agent_pool.closing))

    monkeypatch.setattr(wf.research, "close", research_close)
    monkeypatch.setattr(wf.supervisor, "close", supervisor_close)
    monkeypatch.setattr(wf.controller, "close", controller_close)
    await wf.close()

    # Admissions stopped before the first role and before any transport closed.
    assert observed == [("research", True, True), ("supervisor", True, True), ("controller", True, True)]
    assert [entry.agent.closes for entry in wf.agent_pool.slots] == [1, 1]
    assert slot.agent_task.cancelled()
    # A withdrawn turn keeps its saved uncertainty and its saved identities.
    kept = store.one("SELECT * FROM agent_actions WHERE id=?", (held["id"],))
    assert (kept["state"], kept["thread_id"], kept["turn_id"]) == ("uncertain", "held", "held-turn")
    reservation = wf.agent_pool.lookup("agent_actions", held["id"])
    assert (reservation.slot, reservation.thread_id, reservation.turn_id) == (None, "held", "held-turn")
    assert store.one("SELECT state FROM agent_actions WHERE id=?", (live["id"],))["state"] == "running"


@pytest.mark.usefixtures("contract_three_fixture")
async def test_restart_and_session_shrink_keep_logical_ownership_without_duplicating_a_turn(runtime):
    store, scheduler, _, old = runtime
    accepted = insert_action(
        store, create(store), "uncertain", phase="discuss:seed", thread="accepted", turn="turn-a"
    )
    unreadable = insert_action(
        store, create(store), "uncertain", phase="discuss:seed", thread="unreadable", turn="turn-b"
    )
    agent = ReadbackAgent()
    agent.threads = {"accepted": [{"id": "turn-a", "status": "completed"}]}
    agent.results[("accepted", "turn-a")] = {"reply": "Synthetic reply"}
    save_dispatch_intent(store, accepted)
    old.config.codex.max_sessions = 1
    resumed = Workflow(old.config, store, scheduler, agent)
    await resumed.start()

    # A shrunken pool keeps every logical reservation and its exact saved IDs.
    assert len(resumed.agent_pool.slots) == 1
    assert {(r.key, r.thread_id, r.turn_id) for r in resumed.agent_pool.occupied()} == {
        (("agent_actions", accepted["id"]), "accepted", "turn-a"),
        (("agent_actions", unreadable["id"]), "unreadable", "turn-b"),
    }
    await resumed.resolver.resolve()

    adopted = store.one("SELECT * FROM agent_actions WHERE id=?", (accepted["id"],))
    assert (adopted["state"], adopted["thread_id"], adopted["turn_id"]) == (
        "completed",
        "accepted",
        "turn-a",
    )
    assert resumed.agent_pool.lookup("agent_actions", accepted["id"]) is None
    kept = store.one("SELECT * FROM agent_actions WHERE id=?", (unreadable["id"],))
    assert kept["state"] == "uncertain"
    reservation = resumed.agent_pool.lookup("agent_actions", unreadable["id"])
    assert (reservation.slot, reservation.thread_id, reservation.turn_id) == (
        None,
        "unreadable",
        "turn-b",
    )
    # Readback never starts a turn and never queues a replacement row.
    assert agent.calls == 0
    assert len(store.rows("SELECT id FROM agent_actions")) == 2
    await resumed.close()


@pytest.mark.usefixtures("contract_three_fixture")
async def test_title_readback_settles_the_reservation_retained_outside_its_task(runtime):
    store, scheduler, _, old = runtime
    wf = Workflow(old.config, store, scheduler, LaneAgent())
    wf.agent_ready = True
    action = insert_action(store, create(store), "running", phase="title", thread="title", turn="title-turn")
    wf.agent.results[("title", "title-turn")] = {"title": "Saved synthetic title"}
    wf.agent_pool.reconstruct()
    retained = wf.agent_pool.lookup("agent_actions", action["id"])
    assert (retained.slot, retained.thread_id, retained.turn_id) == (None, "title", "title-turn")

    await wf.advance_title()

    settled = store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))
    assert (settled["state"], json.loads(settled["result"])) == (
        "completed",
        {"title": "Saved synthetic title"},
    )
    # Settled outside its dispatch task: the retained reservation is released.
    assert wf.agent_pool.lookup("agent_actions", action["id"]) is None
    await wf.close()


async def test_dispatch_callback_after_store_close_does_not_raise(runtime):
    store, scheduler, _, old = runtime
    wf = Workflow(old.config, store, scheduler, LaneAgent())
    action = insert_action(store, create(store), "running", thread="thread", turn="turn")
    gate = asyncio.Event()

    async def unfinished():
        await gate.wait()

    # The pool's own claim-and-settle pair, without the slot flag close() withdraws.
    reservation = wf.agent_pool.claim(
        role="stage",
        source_table="agent_actions",
        row=action,
        owner=experiment_owner(action["experiment_id"]),
        slot=wf.agent_pool.primary,
    )
    task = asyncio.create_task(unfinished())
    task.add_done_callback(lambda _task: wf.agent_pool.settle(reservation))
    loop = asyncio.get_running_loop()
    failures = []
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: failures.append(context))
    try:
        await wf.close()
        store.close()
        gate.set()
        await task
        await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(previous)

    # Shutdown already withdrew the owner: its settled task reads nothing.
    assert failures == []
    reservation = wf.agent_pool.lookup("agent_actions", action["id"])
    assert (reservation.slot, reservation.thread_id) == (0, "thread")
