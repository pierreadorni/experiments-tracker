"""Mixed roles share configured turns under owner and capacity limits (T1.6b-3).

A new turn needs a free configured connection and one free configured turn; a saved
turn whose outcome is unknown keeps its unit until it is read back. These tests drive
real engine passes over temporary SQLite with a deterministic demo double whose turn
waits after begin, so two roles are provably live at once on distinct connections and
no managed turn is sent.
"""

import asyncio
import json
from uuid import uuid4

import pytest
from helpers import wait_until
from test_iteration_autopilot import activated
from test_knowledge import completed

from experimenttracker.agent_pool import EXPERIMENT_PRIORITY, experiment_owner, project_owner
from experimenttracker.codex_client import DemoAgent
from experimenttracker.config import ProjectConfig
from experimenttracker.workflow import Workflow


class GatedAgent(DemoAgent):
    """Demo turn that holds after begin until the test releases the shared gate.

    Every configured connection gets its own instance, as ``start_agent_pool`` clones
    the primary agent; the gate and the recorded turns are shared so a test can prove
    two saved turns are live at the same time on two processes.
    """

    gate = None
    begun = []

    def __init__(self):
        super().__init__()
        self.closes = 0

    async def begin(self, thread, prompt, schema, *, workspace=None, effort="low", project_workspace=None):
        # A stage prompt carries PAYLOAD, a project decision carries PROJECT.
        marker = "\nPAYLOAD:\n" if "\nPAYLOAD:\n" in prompt else "\nPROJECT:\n"
        phase = json.loads(prompt.split(marker, 1)[1]).get("phase")
        if phase in ("plan", "prepare", "discuss", "project_chat", "project_synthesize"):
            turn = await super().begin(thread, prompt, schema, workspace=workspace, effort=effort)
        else:
            # A saved project decision has no deterministic demo shape of its own.
            self.calls += 1
            turn = f"decision-{self.calls}"
            self.results[(thread, turn)] = {
                "summary": "Synthetic checkpoint",
                "central_argument": "No real science in this fixture",
                "uncertainty": "Real retrieval remains a live prerequisite",
                "outcome": "wait",
            }
        type(self).begun.append((id(self), phase, thread, turn))
        return turn

    async def finish(self, thread, turn):
        await type(self).gate.wait()
        return await super().finish(thread, turn)

    async def close(self):
        self.closes += 1


def create(store):
    return store.command(
        uuid4(), "create", {"brief": "Share synthetic configured turns.", "recipe_id": "agent_experiment_v1"}
    )["experiment_id"]


def insert_action(store, eid, state, *, phase="title", thread=None, turn=None, created_at=0):
    action_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,input,created_at,updated_at) "
            "VALUES(?,?,?,1,?,?,?,'prompt',?,0)",
            (action_id, eid, phase, state, thread, turn, created_at),
        )
    return store.one("SELECT * FROM agent_actions WHERE id=?", (action_id,))


def insert_project_turn(store, *, thread, turn, project="test-project"):
    identity = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO project_turns(id,project_id,payload,origin,state,thread_id,turn_id,created_at,updated_at) "
            "VALUES(?,?,'{}','local','dispatching',?,?,0,0)",
            (identity, project, thread, turn),
        )
    return store.one("SELECT * FROM project_turns WHERE id=?", (identity,))


async def pooled(runtime, *, sessions=2):
    """A started pool with the configured number of separate demo connections."""
    store, scheduler, _, old = runtime
    old.config.codex.max_sessions = sessions
    GatedAgent.gate = asyncio.Event()
    GatedAgent.begun = []
    wf = Workflow(old.config, store, scheduler, GatedAgent())
    await wf.start()
    return wf


def reserved(wf, source_table, action_id):
    reservation = wf.agent_pool.lookup(source_table, action_id)
    return reservation if reservation is not None and reservation.connection is not None else None


def running(store, eid):
    return store.one(
        "SELECT * FROM agent_actions WHERE experiment_id=? AND phase='plan' AND state='running'", (eid,)
    )


async def settle(wf, tasks):
    """Release the shared gate, let the admitted turns finish and apply their results."""
    GatedAgent.gate.set()
    await asyncio.gather(*tasks)
    await wf.tick(force_observe=True)


async def test_two_connections_overlap_an_experiment_and_a_project_decision(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    wf = await pooled(runtime)
    store, eid = wf.store, create(wf.store)
    # One central quota decision and one registry serve both configured connections.
    assert wf.slots[1].gate is wf.gate and wf.slots[1].agent_pool is wf.agent_pool
    with store.tx() as db:
        service.wake(db, run["id"], "synthetic-input", "experiment_change", {"synthetic": True})
    # The research step prepares its turn; the next control pass selects both roles.
    await wf.tick(force_observe=True)
    plan = await wait_until(lambda: running(store, eid), ticks=lambda: wf.tick(force_observe=True))
    decision = await wait_until(
        lambda: store.one("SELECT * FROM autopilot_actions WHERE state='running'", ()),
        ticks=lambda: wf.tick(force_observe=True),
    )
    # Two saved rows run at the same time on different configured connections.
    left = wf.agent_pool.lookup("agent_actions", plan["id"])
    right = wf.agent_pool.lookup("autopilot_actions", decision["id"])
    assert (left.connection.slot, right.connection.slot) == (0, 1)
    assert (plan["thread_id"], plan["turn_id"]) == ("demo-thread-" + eid, "demo-turn-1")
    assert (decision["thread_id"], decision["turn_id"]) == ("demo-thread-work", "decision-1")
    # Each connection sent exactly its own turn: the primary never spoke for lane two.
    assert (wf.agent.calls, wf.slots[1].agent.calls) == (1, 1)
    tasks = (wf.agent_task, wf.research.task)
    await settle(wf, tasks)
    assert store.one("SELECT state FROM agent_actions WHERE id=?", (plan["id"],))["state"] == "completed"
    assert (store.experiment(eid)["plan_review"] or {}).get("state") == "awaiting_review"
    assert store.one("SELECT state FROM autopilot_actions WHERE id=?", (decision["id"],))["state"] in (
        "received",
        "completed",
    )
    await wf.close()


async def test_two_connections_overlap_an_experiment_and_an_owner_project_turn(runtime):
    wf = await pooled(runtime)
    store, eid = wf.store, create(wf.store)
    plan = await wait_until(lambda: running(store, eid), ticks=lambda: wf.tick(force_observe=True))
    identity = wf.supervisor.memory.enqueue(
        "test-project", {"request_id": str(uuid4()), "kind": "chat", "text": "Summarize the measurement."}
    )["request_id"]
    # An experiment turn stays live on the primary connection; the owner's project
    # turn is admitted on the free one instead of waiting behind a global lane guard.
    reservation = await wait_until(
        lambda: reserved(wf, "project_turns", identity), ticks=lambda: wf.tick(force_observe=True)
    )
    assert reserved(wf, "agent_actions", plan["id"]).connection.slot == 0
    assert reservation.connection.slot == 1
    turn = store.one("SELECT * FROM project_turns WHERE id=?", (identity,))
    second = id(wf.slots[1].agent)
    sent = [entry for entry in GatedAgent.begun if entry[0] == second]
    assert turn["state"] == "running" and len(sent) == 1
    assert (turn["thread_id"], turn["turn_id"]) == sent[0][2:]
    assert (wf.agent.calls, wf.slots[1].agent.calls) == (1, 1)
    tasks = (wf.agent_task, wf.supervisor.task)
    await settle(wf, tasks)
    assert store.one("SELECT state FROM project_turns WHERE id=?", (identity,))["state"] in (
        "received",
        "completed",
    )
    await wf.close()


async def test_one_configured_turn_serves_the_higher_priority_role_first(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    wf = await pooled(runtime, sessions=1)
    store, eid = wf.store, create(wf.store)
    with store.tx() as db:
        service.wake(db, run["id"], "synthetic-input", "experiment_change", {"synthetic": True})
    await wf.tick(force_observe=True)
    plan = await wait_until(lambda: running(store, eid), ticks=lambda: wf.tick(force_observe=True))
    (decision,) = store.rows("SELECT * FROM autopilot_actions WHERE run_id=?", (run["id"],))
    # The experiment class outranks research, and one configured turn is occupied.
    assert (decision["state"], decision["thread_id"], decision["turn_id"]) == ("pending", None, None)
    assert wf.research.task is None and wf.agent.calls == 1
    tasks = (wf.agent_task,)
    await settle(wf, tasks)
    await wait_until(
        lambda: (
            store.one("SELECT state FROM autopilot_actions WHERE id=?", (decision["id"],))["state"]
            in ("received", "completed")
        ),
        ticks=lambda: wf.tick(force_observe=True),
    )
    # The one configured turn passed to research after the experiment's saved turn
    # settled, and the decision's saved identity is the turn research really sent.
    sent = store.one("SELECT * FROM autopilot_actions WHERE id=?", (decision["id"],))
    decisions = [entry for entry in GatedAgent.begun if entry[1] not in ("plan", "prepare", "discuss")]
    assert len(decisions) == 1
    assert (sent["thread_id"], sent["turn_id"]) == decisions[0][2:]
    assert store.one("SELECT state FROM agent_actions WHERE id=?", (plan["id"],))["state"] == "completed"
    assert wf.agent.calls == 2
    await wf.close()


async def test_unknown_saved_turns_count_once_and_readback_needs_no_capacity(runtime):
    store, scheduler, _, old = runtime
    old.config.codex.max_sessions = 2
    wf = Workflow(old.config, store, scheduler, GatedAgent())
    wf.agent_ready = True
    wf.agent.results[("title-thread", "title-turn")] = {"title": "Saved synthetic title"}
    title = insert_action(store, create(store), "running", thread="title-thread", turn="title-turn")
    project = insert_project_turn(store, thread="project-thread", turn="project-turn")
    wf.agent_pool.reconstruct()
    # Each saved unsettled turn takes one configured turn, and each is counted once.
    assert wf.agent_pool.turn_debt() == {("agent_actions", title["id"]), ("project_turns", project["id"])}
    assert wf.agent_pool.turn_capacity() == 0
    wf.agent_pool.claim(
        role="title",
        source_table="agent_actions",
        row=title,
        owner=experiment_owner(title["experiment_id"]),
        workflow=wf,
    )
    # The same turn claimed by a live task is still one unit, not two.
    assert wf.agent_pool.turn_capacity() == 0
    pending = insert_action(store, create(store), "pending", phase="plan")
    started = []
    offer(wf, pending, started)
    wf.agent_pool.admit()
    assert started == [] and wf.agent_pool.proposals == []
    # Readback is not an admission: it settles the saved turn without a new turn.
    await wf.advance_title()
    assert store.one("SELECT state FROM agent_actions WHERE id=?", (title["id"],))["state"] == "completed"
    assert wf.agent_pool.lookup("agent_actions", title["id"]) is None
    assert wf.agent.calls == 0
    assert wf.agent_pool.turn_capacity() == 1
    # A proposal kept from an earlier pass never dispatches a row that already settled.
    stale = insert_action(store, create(store), "pending", phase="plan")
    offer(wf, stale, started)
    store.db.execute("UPDATE agent_actions SET state='completed' WHERE id=?", (stale["id"],))
    wf.agent_pool.admit()
    assert started == [] and wf.agent_pool.lookup("agent_actions", stale["id"]) is None
    await wf.close()


async def test_priority_orders_research_before_a_weekly_project_turn(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    wf = await pooled(runtime, sessions=1)
    store = wf.store
    # Weekly synthesis belongs to a project the activated research run does not own.
    checkout = wf.config.state_dir / "weekly-project"
    checkout.mkdir(parents=True, exist_ok=True)
    project = ProjectConfig(id="weekly-project", name="weekly-project", checkout=checkout, writable=False)
    wf.config.projects.append(project)
    store.projects[project.id] = project
    completed(store, project=project.id)
    memory = wf.supervisor.memory
    memory.refresh()
    weekly = memory.enqueue(project.id, {"request_id": str(uuid4()), "kind": "synthesize"}, origin="weekly")[
        "request_id"
    ]
    with store.tx() as db:
        service.wake(db, run["id"], "synthetic-input", "experiment_change", {"synthetic": True})
    # Both roles offer a prepared turn; the single admission orders them by class,
    # not by the step that happened to prepare one of them first.
    await wf.research.advance()
    await wf.supervisor.advance()
    assert sorted(proposal.role for proposal in wf.agent_pool.proposals) == ["research", "supervisor"]
    wf.agent_pool.admit()
    (decision,) = store.rows("SELECT * FROM autopilot_actions WHERE run_id=?", (run["id"],))
    assert reserved(wf, "autopilot_actions", decision["id"]).connection.slot == 0
    assert wf.agent_pool.turn_capacity() == 0
    # The weekly turn keeps its saved place; only the higher class used the connection.
    assert wf.agent_pool.lookup("project_turns", weekly) is None and wf.supervisor.task is None
    GatedAgent.gate.set()
    await wf.research.task
    assert store.one("SELECT state FROM project_turns WHERE id=?", (weekly,))["state"] == "queued"
    await wf.close()


def offer(wf, row, started, *, role="stage", connections=None):
    """Offer one saved row as a proposal whose dispatch reports its start."""

    async def dispatch(_lane):
        started.append(row["id"])

    wf.agent_pool.propose(
        role=role,
        source_table="agent_actions",
        row=row,
        owner=experiment_owner(row["experiment_id"]),
        priority=EXPERIMENT_PRIORITY,
        connections=(wf,) if connections is None else connections,
        dispatch=dispatch,
    )


async def test_saved_turns_beyond_a_shrunken_pool_admit_nothing_and_keep_readback(runtime):
    store, scheduler, _, old = runtime
    old.config.codex.max_sessions = 2
    wf = Workflow(old.config, store, scheduler, GatedAgent())
    wf.agent_ready = True
    wf.agent.results[("title-thread", "title-turn")] = {"title": "Saved synthetic title"}
    title = insert_action(store, create(store), "running", thread="title-thread", turn="title-turn")
    project = insert_project_turn(store, thread="project-thread", turn="project-turn")
    wf.agent_pool.reconstruct()
    assert wf.agent_pool.turn_capacity() == 0
    # Shrinking the configured sessions keeps both saved identities and leaves the pool
    # owing more possibly-live turns than it can host.
    wf.config.codex.max_sessions = 1
    assert wf.agent_pool.turn_capacity() == -1
    pending = insert_action(store, create(store), "pending", phase="plan")
    started = []
    offer(wf, pending, started)
    wf.agent_pool.admit()
    # Debt beyond the configured sessions never turns into a negative free pass.
    assert started == [] and wf.agent_task is None
    assert wf.agent_pool.lookup("agent_actions", pending["id"]) is None
    assert store.one("SELECT state FROM agent_actions WHERE id=?", (pending["id"],))["state"] == "pending"
    # Readback needs no capacity: it settles one saved turn and frees exactly one unit.
    await wf.advance_title()
    assert store.one("SELECT state FROM agent_actions WHERE id=?", (title["id"],))["state"] == "completed"
    assert wf.agent.calls == 0
    assert wf.agent_pool.turn_capacity() == 0
    store.db.execute("UPDATE project_turns SET state='failed' WHERE id=?", (project["id"],))
    wf.agent_pool.settle_row("project_turns", project["id"])
    assert wf.agent_pool.turn_capacity() == 1
    offer(wf, store.one("SELECT * FROM agent_actions WHERE id=?", (pending["id"],)), started)
    wf.agent_pool.admit()
    assert wf.active_action == pending["id"]
    await wf.agent_task
    assert started == [pending["id"]]
    await wf.close()


async def test_a_live_claim_counts_one_turn_until_its_row_settles(runtime):
    store, scheduler, _, old = runtime
    old.config.codex.max_sessions = 2
    wf = Workflow(old.config, store, scheduler, GatedAgent())
    wf.agent_ready = True
    action = insert_action(store, create(store), "running", thread="thread", turn="turn")
    wf.agent_pool.reconstruct()
    wf.agent_pool.claim(
        role="stage",
        source_table="agent_actions",
        row=action,
        owner=experiment_owner(action["experiment_id"]),
        workflow=wf,
    )
    # The claim and its saved row are the same turn: one unit, not two.
    assert wf.agent_pool.turn_capacity() == 1
    # A controller settles the row before the dispatch task's callback runs: that turn
    # no longer holds capacity, while its owner stays excluded and its process claimed.
    store.db.execute("UPDATE agent_actions SET state='received' WHERE id=?", (action["id"],))
    assert wf.agent_pool.turn_capacity() == 2
    assert not wf.agent_pool.owner_free(experiment_owner(action["experiment_id"]))
    assert wf.agent_pool.lookup("agent_actions", action["id"]).connection.slot == 0
    store.db.execute("UPDATE agent_actions SET state='failed' WHERE id=?", (action["id"],))
    assert wf.agent_pool.turn_capacity() == 2
    wf.agent_pool.settle(wf.agent_pool.lookup("agent_actions", action["id"]))
    assert wf.agent_pool.owner_free(experiment_owner(action["experiment_id"]))
    await wf.close()


@pytest.mark.parametrize(
    "offers",
    [("head", "later", "watch"), ("head", "watch", "later"), ("watch", "head", "later")],
)
async def test_role_heads_merge_by_priority_and_saved_identity(runtime, offers):
    wf = await pooled(runtime, sessions=3)
    store, lanes = wf.store, tuple(wf.slots)
    # The stage scan ranks a newer turn first while the monitor turn's saved identity
    # sits between the two: scan rank and creation time conflict on purpose.
    rows = {
        "head": insert_action(store, create(store), "pending", phase="plan", created_at=5),
        "later": insert_action(store, create(store), "pending", phase="plan", created_at=0),
        "watch": insert_action(store, create(store), "pending", phase="monitor", created_at=3),
    }
    roles = {"head": "stage", "later": "stage", "watch": "monitor"}
    started = []
    for name in offers:
        offer(wf, rows[name], started, role=roles[name], connections=lanes)
    wf.agent_pool.admit()
    await asyncio.gather(*[lane.agent_task for lane in wf.slots if lane.agent_task is not None])
    # Only role heads compete: the monitor head wins on saved identity, then the stage
    # queue keeps its own scan order, independent of the order the roles offered.
    assert started == [rows["watch"]["id"], rows["head"]["id"], rows["later"]["id"]]
    await wf.close()


async def test_an_ineligible_role_head_does_not_hide_its_next_offer(runtime):
    wf = await pooled(runtime)
    store, lanes = wf.store, tuple(wf.slots)
    head = insert_action(store, create(store), "pending", phase="plan", created_at=5)
    later = insert_action(store, create(store), "pending", phase="plan", created_at=0)
    watch = insert_action(store, create(store), "pending", phase="monitor", created_at=3)
    started = []
    for role, row in (("stage", head), ("stage", later), ("monitor", watch)):
        offer(wf, row, started, role=role, connections=lanes)
    # The stage head settled outside this selection: its role's next offer still runs
    # on the second free connection instead of being hidden behind the stale head.
    store.db.execute("UPDATE agent_actions SET state='completed' WHERE id=?", (head["id"],))
    wf.agent_pool.admit()
    await asyncio.gather(*[lane.agent_task for lane in wf.slots if lane.agent_task is not None])
    assert started == [watch["id"], later["id"]]
    assert wf.agent_pool.lookup("agent_actions", head["id"]) is None
    await wf.close()


async def test_a_live_project_role_keeps_its_process_from_reconnecting(runtime):
    wf = await pooled(runtime)
    store = wf.store
    # A project turn still runs on each connection; a closed socket must not cost
    # either one its process before that turn settles.
    first = insert_project_turn(store, thread="first-thread", turn="first-turn")
    second = insert_project_turn(store, thread="second-thread", turn="second-turn", project="other-project")
    for lane, row in ((wf, first), (wf.slots[1], second)):
        wf.agent_pool.claim(
            role="supervisor",
            source_table="project_turns",
            row=row,
            owner=project_owner(row["project_id"]),
            workflow=lane,
        )
    assert wf.agent_pool.process_busy(wf) and wf.agent_pool.process_busy(wf.slots[1])
    wf.agent_ready = False
    wf.slots[1].agent_ready = False
    closed_at = wf.last_agent_start + 120
    await wf.step_runtime(closed_at)
    await wf.step_control_after_observe(closed_at)
    assert [lane.agent.closes for lane in wf.slots] == [0, 0]
    assert [lane.agent.start_count for lane in wf.slots] == [1, 1]
    # Once both turns settle, the closed processes reconnect once each.
    store.db.execute("UPDATE project_turns SET state='failed'")
    wf.agent_pool.settle_row("project_turns", first["id"])
    wf.agent_pool.settle_row("project_turns", second["id"])
    assert not wf.agent_pool.process_busy(wf) and not wf.agent_pool.process_busy(wf.slots[1])
    await wf.step_runtime(closed_at)
    await wf.step_control_after_observe(closed_at)
    assert [lane.agent.closes for lane in wf.slots] == [1, 1]
    assert [lane.agent.start_count for lane in wf.slots] == [2, 2]
    await wf.close()
