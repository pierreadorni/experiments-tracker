"""Submission races: observation never decides an intent whose submission is in flight.

Synthetic mechanism acceptance only: no live scheduler, model runtime or research.
"""

import asyncio
import json
import sys
from uuid import uuid4

import pytest
from helpers import seed_saved_contract3
from scenario import (
    FakeClock,
    FakeCodexBackend,
    FakeSlurmBackend,
    ScenarioTimeout,
    diagnostics,
    settle,
)
from scenario.conftest import Scenario

from experimenttracker import clock, resolver
from experimenttracker.config import JobResources
from experimenttracker.core import job_obligations
from experimenttracker.store import Store

GATE_SECONDS = 20.0  # a real-time failure bound, never a progress condition


PLAN = {
    "schema_version": 3,
    "summary": "Synthetic lost-submission scenario.",
    "protocol": "Synthetic mechanism check, not scientific research.",
    "synthetic_demo": True,
    "parameters_json": "{}",
    "files": ["recipe.py"],
    "figures": [],
    "stages": [
        {
            "id": "train",
            "title": "Synthetic training",
            "needs": [],
            "outputs": ["metrics.json"],
            "result_file": "metrics.json",
            "resources": JobResources().model_dump(),
        }
    ],
}
SOURCE = "print('synthetic scenario recipe')\n"


def plan_turn(payload, workspace):
    return {
        "schema_version": 3,
        "title": "Synthetic scenario",
        "summary": PLAN["summary"],
        "protocol": PLAN["protocol"],
        "stages": [
            {
                "id": "train",
                "title": "Synthetic training",
                "summary": "Synthetic task.",
                "needs": [],
                "resources": JobResources().model_dump(),
            }
        ],
    }


def prepare_turn(payload, workspace):
    runtime = workspace / "runtime/bin/python"
    runtime.parent.mkdir(parents=True, exist_ok=True)
    if not runtime.exists():
        runtime.symlink_to(sys.executable)
    (workspace / "recipe.py").write_text(SOURCE)
    return {**PLAN, "python_executable": str(runtime)}


def interpret_turn(payload, workspace):
    """The report result shape; adoption ends this scenario before it is dispatched."""
    return {
        "title": "Synthetic result",
        "summary": "Synthetic scenario result.",
        "observations": [{"text": "Synthetic.", "evidence_ids": [payload["evidence_ids"][0]]}],
        "limitations": ["Synthetic infrastructure only."],
        "next_ideas": [],
        "scientific_outcome": "inconclusive",
    }


@pytest.fixture
async def scenario(config, monkeypatch):
    """One demo session on the T1.2b fakes, as the scenario package's own fixture builds it."""
    fake_clock = FakeClock()
    fake_clock.install(monkeypatch)
    store = Store(config.state_dir / "workflow.sqlite")
    state = Scenario(config, store, fake_clock, FakeSlurmBackend(), FakeCodexBackend())
    try:
        yield state
    finally:
        try:
            await state.close()
        finally:
            store.close()


def events(store, eid, kind):
    return [
        json.loads(row["payload"])
        for row in store.rows(
            "SELECT payload FROM events WHERE experiment_id=? AND kind=? ORDER BY seq", (eid, kind)
        )
    ]


def saved_intent(store, *, created_at=None):
    """One durable submission intent without a scheduler identity, as observation read it."""
    eid = seed_saved_contract3(store, "Synthetic submission race")
    intent_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,job_id,script_hash,resources,"
            "snapshot_hash,created_at) VALUES(?,?,'prepare',1,'submitting',NULL,'synthetic','{}','control',?)",
            (intent_id, eid, clock.now() if created_at is None else created_at),
        )
        job_obligations.sync_job(db, intent_id)
    return eid, store.one("SELECT * FROM jobs WHERE intent_id=?", (intent_id,))


class ReadScheduler:
    """A scheduler read that finds nothing and records that it was asked."""

    def __init__(self):
        self.reconciled = []

    async def reconcile(self, intent_id):
        self.reconciled.append(intent_id)
        return None


class GatedScheduler(ReadScheduler):
    """A scheduler read that parks at a gate, so another owner can act during the read."""

    def __init__(self):
        super().__init__()
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def reconcile(self, intent_id):
        self.reconciled.append(intent_id)
        self.entered.set()
        await self.release.wait()
        return None


async def tick_until_gated(scenario, gate, *, attempts=10, timeout=GATE_SECONDS):
    """Run ticks until one parks inside the gated submission.

    ``attempts`` bounds the tick count and ``timeout`` one tick: both only detect a
    scenario that made no progress, and neither is a progress condition.
    """
    for _ in range(attempts):
        tick = asyncio.ensure_future(scenario.workflow.tick(force_observe=True))
        entered = asyncio.ensure_future(gate.entered.wait())
        limit = asyncio.timeout(timeout)
        try:
            async with limit:
                await asyncio.wait([tick, entered], return_when=asyncio.FIRST_COMPLETED)
                if gate.entered.is_set():
                    return tick
                await tick
        except TimeoutError:
            tick.cancel()
            if not limit.expired():
                raise
            raise ScenarioTimeout(
                f"no submission reached the gate within {timeout}s: "
                + diagnostics(scenario.workflow, gates=(gate,))
            ) from None
        finally:
            entered.cancel()
        await settle(scenario.workflow, gates=(gate,))
    raise ScenarioTimeout(
        f"no submission reached the gate in {attempts} ticks: "
        + diagnostics(scenario.workflow, gates=(gate,))
    )


async def planned(scenario, brief):
    """Create one synthetic experiment and approve its plan; nothing is submitted yet."""
    store = scenario.store
    eid = seed_saved_contract3(store, brief)
    scenario.agent.script("plan", turn=plan_turn, turns=1)
    scenario.agent.script("prepare", turn=prepare_turn, turns=1)
    scenario.agent.script("interpret", turn=interpret_turn, turns=1)
    await scenario.approve_plan(eid)
    scenario.one_session()
    return eid


async def test_a_snapshot_decided_before_resolve_is_not_decided_again(runtime):
    store, _, _, workflow = runtime
    eid, snapshot = saved_intent(store)
    scheduler = ReadScheduler()
    workflow.scheduler_for = lambda exp, saved: scheduler

    # Another owner saved the exact identity and its one receipt while observation
    # still held the row it read from the job list.
    assert resolver.adopt(store, eid, snapshot, "4201", observer="observe_jobs")["job_id"] == "4201"

    assert await resolver.resolve(workflow, store.experiment(eid), snapshot) is None

    assert scheduler.reconciled == []  # a decided row is not read from the scheduler
    assert store.one("SELECT state,job_id FROM jobs WHERE intent_id=?", (snapshot["intent_id"],)) == {
        "state": "submitted",
        "job_id": "4201",
    }
    assert len(events(store, eid, "job_reconciled")) == 1
    assert store.experiment(eid)["hold_reason"] is None


@pytest.mark.parametrize("waited", [0, 601])
async def test_a_row_decided_during_the_scheduler_read_writes_no_hold(runtime, waited):
    store, _, _, workflow = runtime
    eid, job = saved_intent(store, created_at=clock.now() - waited)
    scheduler = GatedScheduler()
    workflow.scheduler_for = lambda exp, saved: scheduler
    resolving = asyncio.ensure_future(resolver.resolve(workflow, store.experiment(eid), job))
    async with asyncio.timeout(GATE_SECONDS):
        await scheduler.entered.wait()

    # The owner of the in-flight attempt saves its accepted identity during the read.
    assert resolver.adopt(store, eid, job, "4201", observer="observe_jobs")["state"] == "submitted"
    scheduler.release.set()
    assert await resolving is None

    assert scheduler.reconciled == [job["intent_id"]]  # the one read, no extra call
    assert store.one("SELECT state,job_id,error FROM jobs WHERE intent_id=?", (job["intent_id"],)) == {
        "state": "submitted",
        "job_id": "4201",
        "error": None,
    }
    assert len(events(store, eid, "job_reconciled")) == 1
    assert store.experiment(eid)["hold_reason"] is None
