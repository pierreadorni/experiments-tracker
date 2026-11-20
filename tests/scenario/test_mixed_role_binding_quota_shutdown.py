"""Fresh-binding denial, quota interruption and full-slot shutdown (T1.6c-3).

The shared multi-slot fixture keeps one explicit fake backend, so these cases are
decided by the production selector, dispatch paths, quota gate and shutdown sequence:
a cached selection admits and then a fresh denial refuses the saved turn, a denied
research decision keeps its pending action and the run's existing hold, denied quota
evidence interrupts every occupied role through the process that holds it, and closing
with every configured slot occupied stops admissions, keeps the exact saved identities
and never declares a remote turn stopped. Synthetic mechanism acceptance only: no live
scheduler, managed account, socket runtime or research.
"""

import asyncio
import contextlib
import json
import sqlite3
from dataclasses import dataclass
from uuid import uuid4

import pytest
from test_knowledge import completed

from experimenttracker.codex_client import TransportError
from scenario import FakeGate
from scenario.test_mixed_role_overlap import (
    DECISION,
    EXPERIMENT_PHASE,
    activated,
    begin_turns,
    create,
    experiment_turn,
    live_both,
    research_turn,
    saved,
    synthesize_turn,
    turn_task,
)
from scenario.test_mixed_role_recovery import quota_payload, steps

DENIED = 95  # both synthetic windows below the ordinary reserve


@dataclass(frozen=True)
class Occupied:
    """Two live role turns: a saved experiment research turn and one project-role row."""

    experiment: dict
    experiment_gate: FakeGate
    project: dict
    project_table: str
    project_gate: FakeGate

    @property
    def rows(self):
        return [("agent_actions", self.experiment), (self.project_table, self.project)]


def hold_session(monkeypatch, scenario, gate):
    """Park the managed session setup so fresh authority evidence arrives after admission."""
    original = scenario.slots.agent.session

    async def session(self, *args, **kwargs):
        await gate.wait()
        return await original(self, *args, **kwargs)

    monkeypatch.setattr(scenario.slots.agent, "session", session)


def denied(scenario):
    """Denied synthetic evidence for both required windows, as a fresh refresh observes it."""
    scenario.agent_backend.used_percent = scenario.agent_backend.weekly_used_percent = DENIED


async def both_occupied(scenario, wf, second="weekly"):
    """Occupy both configured slots with command-created experiment and project turns."""
    store = scenario.store
    if second == "weekly":
        completed(store, summary="Synthetic completed source.")
        identity = wf.supervisor.memory.enqueue(
            "test-project", {"request_id": str(uuid4()), "kind": "synthesize"}, origin="weekly"
        )["request_id"]
        table, phase, turn, match = "project_turns", "project_synthesize", synthesize_turn, {"id": identity}
    else:
        service, run = activated(scenario)
        with store.tx() as db:
            service.wake(db, run["id"], "synthetic-input", "experiment_change", {"synthetic": True})
        table, phase, turn, match = "autopilot_actions", "research", research_turn, {"run_id": run["id"]}
    plan_gate, project_gate = scenario.gate(EXPERIMENT_PHASE), scenario.gate(phase)
    scenario.agent.script(EXPERIMENT_PHASE, turn=experiment_turn, turns=1)
    scenario.agent.script(phase, turn=turn, turns=1)
    eid = create(store)

    def both_running():
        plan = saved(store, "agent_actions", experiment_id=eid, phase="research", state="running")
        project = saved(store, table, state="running", **match)
        return (plan, project) if plan and project else None

    plan, project = await scenario.run(both_running, gates=[plan_gate, project_gate])
    live_both(scenario, wf, ("agent_actions", plan), (table, project))
    return Occupied(plan, plan_gate, project, table, project_gate)


def saved_actions(store, eid):
    """Every saved agent action of one experiment, oldest first."""
    return store.rows("SELECT * FROM agent_actions WHERE experiment_id=? ORDER BY created_at", (eid,))


def record_close(monkeypatch, wf, owner, name, observed, slots):
    """Record a role's shutdown with the flags and transports it observes, then close it."""
    original = owner.close

    async def close():
        observed.append((name, wf.closing, wf.agent_pool.closing, len(slots.named("close"))))
        await original()

    monkeypatch.setattr(owner, "close", close)


@pytest.mark.parametrize("source", ["agent_actions", "project_turns"])
async def test_a_fresh_denial_after_the_cached_admit_never_reaches_begin(multi_scenario, monkeypatch, source):
    scenario = multi_scenario
    wf = await scenario.started()
    store, backend, slots = scenario.store, scenario.agent_backend, scenario.slots
    session_gate = FakeGate()
    hold_session(monkeypatch, scenario, session_gate)
    if source == "agent_actions":
        scenario.agent.script(EXPERIMENT_PHASE, turn=experiment_turn, turns=1)
        experiment = create(store, "Synthetic refused binding.")

        def claimed_row():
            return saved(store, "agent_actions", experiment_id=experiment, phase="research")

    else:
        completed(store, summary="Synthetic completed source.")
        identity = wf.supervisor.memory.enqueue(
            "test-project", {"request_id": str(uuid4()), "kind": "synthesize"}, origin="weekly"
        )["request_id"]
        scenario.agent.script("project_synthesize", turn=synthesize_turn, turns=1)

        def claimed_row():
            return saved(store, "project_turns", id=identity)

    # The production pass offers the saved row and the pool claims a connection before
    # the dispatch waits inside its session setup.
    await scenario.run(session_gate.entered.is_set, gates=[session_gate])
    claimed = claimed_row()
    reservation = wf.agent_pool.lookup(source, claimed["id"])
    assert (claimed["turn_id"], reservation.connection is not None) == (None, True)
    task = (
        wf.supervisor.task if source == "project_turns" else wf.agent_pool.slots[reservation.slot].agent_task
    )
    assert (backend.turns_started, slots.named("begin")) == (0, [])

    # Fresh evidence arrives while the session setup waits: the binding check before the
    # saved intent refuses, so this exact turn is never sent.
    wf.gate.update(quota_payload(DENIED))
    session_gate.release.set()
    await task
    await asyncio.sleep(0)  # drain the done callback that releases the never-sent claim

    final = claimed_row()
    expected = ("pending", None) if source == "agent_actions" else ("waiting", None)
    assert final["id"] == claimed["id"] and (final["state"], final["turn_id"]) == expected
    if source == "agent_actions":
        assert not wf.agent_pool.quota(eid=experiment).admitted
        slot = wf.agent_pool.slots[reservation.slot]
        assert (slot.agent_task, slot.active_action) == (None, None)
    else:
        assert final["hold_reason"] == "quota_hold"
        assert (wf.supervisor.task, wf.supervisor.active_id) == (None, None)
    assert (backend.turns_started, slots.named("begin")) == (0, [])
    assert slots.named("read_thread") == [] and slots.named("read_turn") == []
    assert not store.rows(
        "SELECT seq FROM events WHERE kind IN ('agent_dispatch_intent','agent_turn_started')"
    )
    # The never-sent claim and the configured turn it held are both released.
    assert wf.agent_pool.lookup(source, final["id"]) is None
    assert wf.agent_pool.turn_capacity() == 2
    await wf.close()


async def test_a_denied_research_turn_keeps_its_pending_decision_and_hold(multi_scenario, monkeypatch):
    scenario = multi_scenario
    wf = await scenario.started()
    store, backend, slots = scenario.store, scenario.agent_backend, scenario.slots
    service, run = activated(scenario)
    with store.tx() as db:
        service.wake(db, run["id"], "synthetic-input", "experiment_change", {"synthetic": True})
    session_gate = FakeGate()
    hold_session(monkeypatch, scenario, session_gate)
    scenario.agent.script("research", turn=research_turn, turns=1)

    # The cached selection filter admits and the pool claims a connection before the
    # dispatch waits inside its session setup.
    await scenario.run(session_gate.entered.is_set, gates=[session_gate])
    claimed = saved(store, "autopilot_actions", run_id=run["id"])
    reservation = wf.agent_pool.lookup("autopilot_actions", claimed["id"])
    assert (claimed["state"], claimed["turn_id"], reservation.connection is not None) == (
        "pending",
        None,
        True,
    )
    assert (backend.turns_started, slots.named("begin")) == (0, [])

    # Fresh denied evidence arrives while the session setup waits: the binding admission
    # refuses, records the run hold it already used and leaves this decision pending.
    denied(scenario)
    wf.gate.update(quota_payload(DENIED))
    session_gate.release.set()
    await wf.research.task
    await asyncio.sleep(0)  # drain the done callback that releases the never-sent claim

    pending = saved(store, "autopilot_actions", run_id=run["id"])
    assert pending["id"] == claimed["id"]
    assert (pending["state"], pending["thread_id"], pending["turn_id"]) == ("pending", None, None)
    held = saved(store, "autopilot_runs", id=run["id"])
    quota = wf.agent_pool.quota(run=held)
    assert not quota.admitted and held["hold_reason"] == quota.reason
    assert [
        json.loads(event["payload"])["reason"]
        for event in store.rows(
            "SELECT payload FROM autopilot_events WHERE run_id=? AND kind='inference_hold'", (run["id"],)
        )
    ] == [quota.reason]
    assert (backend.turns_started, slots.named("begin")) == (0, [])
    assert not store.rows(
        "SELECT seq FROM events WHERE kind IN ('agent_dispatch_intent','agent_turn_started')"
    )
    # The never-sent claim, the busy flag it published and its configured turn are released.
    assert wf.agent_pool.lookup("autopilot_actions", pending["id"]) is None
    assert (wf.research.task, wf.research.active_id) == (None, None)
    assert wf.agent_pool.turn_capacity() == 2

    # A denial neither retries nor replaces the decision: fresh admitted quota sends that
    # exact saved action once, and the hold clears with the ordinary admission.
    backend.used_percent = backend.weekly_used_percent = 0
    research_gate = scenario.gate("research")
    running = await scenario.run(
        lambda: saved(store, "autopilot_actions", run_id=run["id"], state="running"),
        gates=[research_gate],
    )
    assert running["id"] == claimed["id"]
    assert begin_turns(scenario, "research") == [running["turn_id"]]
    assert (backend.turns_started, len(slots.named("begin"))) == (1, 1)
    assert saved(store, "autopilot_runs", id=run["id"])["hold_reason"] is None

    research_gate.release.set()
    await turn_task(wf, "autopilot_actions", running)
    applied = await scenario.run(
        lambda: saved(store, "autopilot_actions", run_id=run["id"], state="completed")
    )
    assert applied["id"] == claimed["id"]
    assert saved(store, "autopilot_runs", id=run["id"])["central_argument"] == DECISION["central_argument"]
    await wf.close()


async def test_denied_quota_interrupts_each_occupied_role_through_its_own_process(multi_scenario):
    scenario = multi_scenario
    wf = await scenario.started()
    store, backend, slots = scenario.store, scenario.agent_backend, scenario.slots
    occupied = await both_occupied(scenario, wf)
    assert (backend.turns_started, len(slots.named("begin"))) == (2, 2)

    # Denied evidence on both windows: the production refresh decides, then interrupts
    # every occupied known turn through the process that holds it.
    denied(scenario)
    assert not (await wf.refresh_quota()).admitted
    expected = set()
    for table, row in occupied.rows:
        reservation = wf.agent_pool.lookup(table, row["id"])
        slot = wf.agent_pool.slots[reservation.connection.slot]
        expected.add(
            (
                reservation.connection.slot,
                slot.agent.instance,
                reservation.connection.start,
                row["thread_id"],
                row["turn_id"],
            )
        )
    interrupted = {
        (call["slot"], call["instance"], call["generation"], call["thread"], call["turn"])
        for call in slots.named("interrupt")
    }
    assert len(slots.named("interrupt")) == 2 and interrupted == expected

    # No admission follows the denial, and each interrupted turn keeps its exact identity.
    plan = saved(store, "agent_actions", id=occupied.experiment["id"])
    project = saved(store, occupied.project_table, id=occupied.project["id"])
    assert (plan["state"], plan["turn_id"], plan["interrupt_reason"]) == (
        "running",
        occupied.experiment["turn_id"],
        "quota_hold",
    )
    assert store.experiment(plan["experiment_id"])["hold_reason"] == "quota_hold"
    assert (project["state"], project["thread_id"], project["turn_id"]) == (
        "running",
        occupied.project["thread_id"],
        occupied.project["turn_id"],
    )
    # Each turn is still owned by the claim that holds it (a live claim caches the
    # thread it was dispatched with; the saved row is the exact identity it interrupts).
    assert {r.key for r in wf.agent_pool.occupied()} == {
        ("agent_actions", plan["id"]),
        (occupied.project_table, project["id"]),
    }
    assert wf.agent_pool.turn_capacity() == 0

    # Another source waits on the denied gate instead of beginning a turn.
    waiting = create(store, "Synthetic waiting for quota.")
    await steps(scenario, 2, gates=[occupied.experiment_gate, occupied.project_gate])
    assert not any(action["turn_id"] for action in saved_actions(store, waiting))
    assert (backend.turns_started, len(slots.named("begin"))) == (2, 2)

    # Fresh admitted quota alone resumes nothing: the interrupted turn settles first.
    scenario.agent_backend.used_percent = scenario.agent_backend.weekly_used_percent = 0
    assert (await wf.refresh_quota()).admitted
    assert (backend.turns_started, len(slots.named("interrupt"))) == (2, 2)
    occupied.experiment_gate.release.set()
    await turn_task(wf, "agent_actions", plan)
    await scenario.run(
        lambda: saved(store, "agent_actions", id=plan["id"], state="completed"),
        gates=[occupied.project_gate],
    )
    settled = saved(store, "agent_actions", id=plan["id"])
    assert (settled["state"], settled["thread_id"], settled["turn_id"]) == (
        "completed",
        plan["thread_id"],
        plan["turn_id"],
    )
    assert (backend.turns_started, len(slots.named("begin"))) == (2, 2)
    # The freed configured turn serves the waiting source exactly once.
    scenario.agent.script(EXPERIMENT_PHASE, turn=experiment_turn, turns=1)
    await scenario.run(
        lambda: saved(store, "agent_actions", experiment_id=waiting, phase="research", state="completed"),
        gates=[occupied.project_gate],
    )
    assert (backend.turns_started, len(slots.named("begin"))) == (3, 3)
    occupied.project_gate.release.set()
    await turn_task(wf, occupied.project_table, occupied.project)
    await wf.close()


async def test_unreadable_turns_are_not_declared_stopped_by_shutdown_or_fresh_quota(multi_scenario):
    scenario = multi_scenario
    wf = await scenario.started()
    store, backend, slots = scenario.store, scenario.agent_backend, scenario.slots
    occupied = await both_occupied(scenario, wf)
    experiment, project = occupied.experiment, occupied.project
    # Neither saved thread can be read, so no process can prove its turn over.
    for row in (experiment, project):
        backend.unreadable[row["thread_id"]] = TransportError("synthetic thread/read closed")
    denied(scenario)
    assert not (await wf.refresh_quota()).admitted
    assert len(slots.named("interrupt")) == 2
    plan_task = turn_task(wf, "agent_actions", experiment)

    # Fresh permission and quota alone neither retries nor replaces an unknown outcome.
    backend.used_percent = backend.weekly_used_percent = 0
    assert (await wf.refresh_quota()).admitted
    await steps(scenario, 2, gates=[occupied.experiment_gate, occupied.project_gate])
    assert (backend.turns_started, len(slots.named("begin"))) == (2, 2)

    # Closing the client ends the local process, never the remote turn: the rows keep
    # running with their exact identities and the new generation begins nothing.
    await scenario.restart()
    wf = await scenario.started()
    assert plan_task.cancelled()
    for table, row in occupied.rows:
        kept = saved(store, table, id=row["id"])
        assert (kept["state"], kept["thread_id"], kept["turn_id"]) == (
            "running",
            row["thread_id"],
            row["turn_id"],
        )
        reservation = wf.agent_pool.lookup(table, row["id"])
        assert (reservation.connection, reservation.thread_id, reservation.turn_id) == (
            None,
            row["thread_id"],
            row["turn_id"],
        )
    assert (backend.turns_started, len(slots.named("begin"))) == (2, 2)
    assert wf.agent_pool.turn_debt() == {(table, row["id"]) for table, row in occupied.rows}

    # Readback on the restarted connections keeps both turns uncertain, without a retry.
    def both_uncertain():
        rows = {table: saved(store, table, id=row["id"]) for table, row in occupied.rows}
        return rows if all(row["state"] == "uncertain" for row in rows.values()) else None

    rows = await scenario.run(both_uncertain)
    assert {row["thread_id"] for row in rows.values()} == {experiment["thread_id"], project["thread_id"]}
    # The restarted connections tried an exact readback and recorded its failure, so the
    # uncertainty is known evidence rather than an assumption made when the client closed.
    assert all("synthetic thread/read closed" in (row["error"] or "") for row in rows.values())
    assert wf.agent_pool.turn_capacity() == 0
    assert (backend.turns_started, len(slots.named("begin"))) == (2, 2)

    # One known outcome settles exactly its own turn; the unreadable project turn keeps
    # its uncertainty and its saved debt.
    del backend.unreadable[experiment["thread_id"]]
    backend.turns[(experiment["thread_id"], experiment["turn_id"])].update(
        result=experiment_turn(None, None), state="completed"
    )
    await scenario.run(lambda: saved(store, "agent_actions", id=experiment["id"], state="completed"))
    assert saved(store, "agent_actions", id=experiment["id"])["state"] == "completed"
    assert saved(store, occupied.project_table, id=project["id"])["state"] == "uncertain"
    assert (backend.turns_started, len(slots.named("begin"))) == (2, 2)
    assert wf.agent_pool.turn_debt() == {(occupied.project_table, project["id"])}
    assert wf.agent_pool.turn_capacity() == 1
    await wf.close()


@pytest.mark.parametrize("second", ["weekly", "research"])
async def test_shutdown_with_every_slot_occupied_closes_each_transport_once(
    multi_scenario, monkeypatch, second
):
    scenario = multi_scenario
    wf = await scenario.started()
    store, backend, slots = scenario.store, scenario.agent_backend, scenario.slots
    occupied = await both_occupied(scenario, wf, second)
    plan_task = turn_task(wf, "agent_actions", occupied.experiment)

    # Shutdown stops admissions before any role and before any transport closes.
    observed = []
    for owner, name in (
        (wf.research, "research"),
        (wf.supervisor, "supervisor"),
        (wf.controller, "controller"),
    ):
        record_close(monkeypatch, wf, owner, name, observed, slots)
    await wf.close()

    assert observed == [(name, True, True, 0) for name in ("research", "supervisor", "controller")]
    assert plan_task.cancelled()
    assert [call["slot"] for call in slots.named("close")] == [0, 1]
    assert [call["instance"] for call in slots.named("close")] == [
        entry.agent.instance for entry in wf.agent_pool.slots
    ]
    # A withdrawn local task is not a finished remote turn: exact identities survive.
    for table, row in occupied.rows:
        kept = saved(store, table, id=row["id"])
        assert (kept["state"], kept["thread_id"], kept["turn_id"]) == (
            "running",
            row["thread_id"],
            row["turn_id"],
        )
        reservation = wf.agent_pool.lookup(table, row["id"])
        assert reservation is not None and reservation.connection is not None
    assert wf.agent_pool.turn_capacity() == 0
    pending = [wf.research.task, wf.supervisor.task, *(slot.agent_task for slot in wf.agent_pool.slots)]
    assert [task for task in pending if task is not None and not task.done()] == []
    assert (backend.turns_started, len(slots.named("begin"))) == (2, 2)

    # Every task ended with the close, so no done callback remains to write: a later
    # read-only connection sees the rows exactly as shutdown left them.
    assert wf.closing and wf.agent_pool.closing
    store.close()
    loop = asyncio.get_running_loop()
    failures = []
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: failures.append(context))
    try:
        occupied.experiment_gate.release.set()
        occupied.project_gate.release.set()
        for _ in range(3):
            await asyncio.sleep(0)  # drain already-scheduled callbacks only
    finally:
        loop.set_exception_handler(previous)
    assert failures == []
    with contextlib.closing(sqlite3.connect(f"{store.path.as_uri()}?mode=ro", uri=True)) as reader:
        kept = {
            row[0]: row[1:] for row in reader.execute("SELECT id,state,thread_id,turn_id FROM agent_actions")
        }
    assert kept[occupied.experiment["id"]] == (
        "running",
        occupied.experiment["thread_id"],
        occupied.experiment["turn_id"],
    )
