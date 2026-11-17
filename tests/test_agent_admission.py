"""One pool selector admits agent work (T1.6b-1).

Every admission site keeps its deterministic preparation and offers a proposal; the
pool sorts proposals by priority class and each role's own scan order, re-checks
eligibility and claims the owner and connection in one synchronous section before
the task exists. These tests drive that selection with fake dispatch factories over
temporary SQLite, so no managed turn is sent.
"""

import asyncio
from uuid import uuid4

import pytest

from experimenttracker.agent_pool import (
    CONVERSATION_PRIORITY,
    EXPERIMENT_PRIORITY,
    RESEARCH_PRIORITY,
    SUPERVISOR_PRIORITY,
    TITLE_PRIORITY,
    experiment_owner,
)
from experimenttracker.codex_client import DemoAgent
from experimenttracker.workflow import Workflow


def create(store):
    return store.command(
        uuid4(), "create", {"brief": "Offer synthetic turns.", "recipe_id": "agent_experiment_v1"}
    )["experiment_id"]


def saved_action(store, eid, *, state="pending", created_at=0):
    action_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,created_at,updated_at) "
            "VALUES(?,?,'plan',1,?,'prompt',?,0)",
            (action_id, eid, state, created_at),
        )
    return store.one("SELECT * FROM agent_actions WHERE id=?", (action_id,))


def offer(wf, *, role, priority, row, started, connections=None):
    """Offer one proposal whose dispatch reports the connection it was given."""

    async def dispatch(connection):
        started.append((role, wf.agent_pool.slot_index(connection)))

    wf.agent_pool.propose(
        role=role,
        source_table="agent_actions",
        row=row,
        owner=experiment_owner(row["experiment_id"]),
        priority=priority,
        connections=(wf,) if connections is None else connections,
        dispatch=dispatch,
    )


async def pooled(runtime):
    """One pool over two started connections, so only a busy owner can hold work back."""
    store, scheduler, _, old = runtime
    old.config.codex.max_sessions = 2
    wf = Workflow(old.config, store, scheduler, DemoAgent())
    await wf.connect_agent()
    await wf.start_agent_pool()
    return wf


def release(wf):
    """Clear the busy flag of a task the test already awaited, as draining does."""
    wf.agent_task = wf.active_action = None


async def test_priority_class_beats_call_order_and_each_role_keeps_its_scan(runtime):
    store, _, _, wf = runtime
    wf.agent_ready = True
    started, claimed_ids = [], []
    # Priority class decides first, then a role's own rank inside its class: the first
    # scanned stage turn wins even though the second was created earlier, and the
    # older monitor turn wins the shared rank through its saved identity.
    title = saved_action(store, create(store), created_at=0)
    supervisor = saved_action(store, create(store), created_at=0)
    research = saved_action(store, create(store), created_at=0)
    stage = saved_action(store, create(store), created_at=3)
    conversation = saved_action(store, create(store), created_at=0)
    stage_later_in_scan = saved_action(store, create(store), created_at=2)
    monitor = saved_action(store, create(store), created_at=1)
    offers = [
        ("title", TITLE_PRIORITY, title),
        ("supervisor", SUPERVISOR_PRIORITY, supervisor),
        ("research", RESEARCH_PRIORITY, research),
        ("stage", EXPERIMENT_PRIORITY, stage),
        ("conversation", CONVERSATION_PRIORITY, conversation),
        ("stage", EXPERIMENT_PRIORITY, stage_later_in_scan),
        ("monitor", EXPERIMENT_PRIORITY, monitor),
    ]
    expected = [conversation, monitor, stage, stage_later_in_scan, research, supervisor, title]
    roles = {saved["id"]: offered for offered, _, saved in offers}
    for row in expected:
        for offered, priority, saved in offers:
            offer(wf, role=offered, priority=priority, row=saved, started=started)
        wf.agent_pool.admit()
        claimed_ids.append(wf.active_action)
        await wf.agent_task
        assert started == [(roles[row["id"]], 0)]
        release(wf)
        started.clear()
        offers = [entry for entry in offers if entry[2]["id"] != claimed_ids[-1]]
        assert wf.agent_pool.proposals == []
    assert claimed_ids == [row["id"] for row in expected]
    assert offers == []


async def test_a_busy_owner_never_holds_up_another_eligible_proposal(runtime):
    wf = await pooled(runtime)
    store = wf.store
    shared, other = create(store), create(store)
    started = []
    connections = tuple(wf.slots)
    offer(
        wf,
        role="conversation",
        priority=CONVERSATION_PRIORITY,
        row=saved_action(store, shared),
        started=started,
        connections=connections,
    )
    offer(
        wf,
        role="stage",
        priority=EXPERIMENT_PRIORITY,
        row=saved_action(store, shared),
        started=started,
        connections=connections,
    )
    offer(
        wf,
        role="title",
        priority=TITLE_PRIORITY,
        row=saved_action(store, other),
        started=started,
        connections=connections,
    )
    wf.agent_pool.admit()
    await asyncio.gather(*[wf.agent_task] + [lane.agent_task for lane in wf.slots[1:]])
    # The shared owner claims one connection; the unrelated owner still gets the other.
    assert sorted(started) == [("conversation", 0), ("title", 1)]
    await wf.close()


@pytest.mark.parametrize("state", ["dispatching", "running", "uncertain", "received"])
async def test_a_saved_unsettled_turn_keeps_its_owner_from_claiming_twice(runtime, state):
    store, _, _, wf = runtime
    wf.agent_ready = True
    eid = create(store)
    saved_action(store, eid, state=state)
    offered = saved_action(store, eid)
    started = []
    offer(wf, role="stage", priority=EXPERIMENT_PRIORITY, row=offered, started=started)
    wf.agent_pool.admit()
    assert started == [] and wf.agent_task is None
    assert wf.agent_pool.lookup("agent_actions", offered["id"]) is None


async def test_a_live_claim_keeps_its_owner_off_a_second_connection(runtime):
    wf = await pooled(runtime)
    eid = create(wf.store)
    started, release_turn = [], asyncio.Event()

    async def waiting(connection):
        started.append(wf.agent_pool.slot_index(connection))
        await release_turn.wait()

    for row in (saved_action(wf.store, eid), saved_action(wf.store, eid)):
        wf.agent_pool.propose(
            role="stage",
            source_table="agent_actions",
            row=row,
            owner=experiment_owner(eid),
            priority=EXPERIMENT_PRIORITY,
            connections=tuple(wf.slots),
            dispatch=waiting,
        )
    wf.agent_pool.admit()
    await asyncio.sleep(0)
    # A second connection is free, but the owner's first turn is still live.
    assert started == [0]
    release_turn.set()
    await wf.agent_task
    await wf.close()


async def test_the_claim_precedes_the_task_and_an_unsent_start_releases_it(runtime):
    store, _, _, wf = runtime
    wf.agent_ready = True
    row = saved_action(store, create(store))
    claimed = []

    async def never_sent(connection):
        reservation = wf.agent_pool.lookup("agent_actions", row["id"])
        claimed.append((reservation.slot, reservation.connection is not None))

    wf.agent_pool.propose(
        role="stage",
        source_table="agent_actions",
        row=row,
        owner=experiment_owner(row["experiment_id"]),
        priority=EXPERIMENT_PRIORITY,
        connections=(wf,),
        dispatch=never_sent,
    )
    assert len(wf.agent_pool.proposals) == 1  # nothing is claimed before the selection runs
    wf.agent_pool.admit()
    assert claimed == [] and wf.active_action == row["id"]
    await wf.agent_task
    assert claimed == [(0, True)]
    # The turn never started: the claim is released and the source row stays pending.
    assert wf.agent_pool.lookup("agent_actions", row["id"]) is None
    assert store.one("SELECT state FROM agent_actions WHERE id=?", (row["id"],))["state"] == "pending"


async def test_one_configured_turn_holds_a_second_role_back(runtime, monkeypatch):
    store, _, agent, wf = runtime
    wf.agent_ready = True
    wf.gate.update(await agent.quota())
    monkeypatch.setattr(wf.conversation, "prompt", lambda exp, message: "Discuss\nPAYLOAD:\n{}")
    busy, waiting = create(store), create(store)
    saved_action(store, busy, state="running")
    store.update(waiting, phase="complete")
    store.command(uuid4(), "discuss", {"text": "Explain the synthetic result"}, eid=waiting)
    await wf.conversation.advance()
    # The conversation site offers its prepared turn; the pool holds it because another
    # experiment's saved running turn already occupies the one configured turn.
    assert [proposal.role for proposal in wf.agent_pool.proposals] == ["conversation"]
    wf.agent_pool.admit()
    assert wf.agent_task is None
    discussion = store.one("SELECT * FROM discussions WHERE experiment_id=?", (waiting,))
    assert (discussion["state"], discussion["hold_reason"]) == ("queued", None)
