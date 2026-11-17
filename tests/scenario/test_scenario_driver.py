"""Driver unit tests: bounded settling, gated waits and failure diagnostics."""

import asyncio
import time
from types import SimpleNamespace
from uuid import uuid4

import pytest

from scenario import FakeGate, ScenarioTimeout, run_until, settle


class StubTick:
    """A workflow-shaped object whose tick never returns."""

    def __init__(self, workflow):
        self.store = workflow.store
        self.scheduler = workflow.scheduler
        self.agent = workflow.agent
        self.slots = [self]
        self.agent_task = self.quota_task = self.backup_task = None
        # Each role publishes its dispatch task on its own controller.
        self.research = SimpleNamespace(task=None)
        self.supervisor = SimpleNamespace(task=None)
        self.output_checks, self.evidence_checks = {}, {}
        self.ticks = 0

    async def tick(self, *, force_observe=False):
        self.ticks += 1
        await asyncio.Event().wait()


class IdleTick(StubTick):
    async def tick(self, *, force_observe=False):
        self.ticks += 1


class GatedTick(StubTick):
    """A tick that creates its own task, parked on a registered fake gate."""

    def __init__(self, workflow, gate):
        super().__init__(workflow)
        self.gate = gate

    async def tick(self, *, force_observe=False):
        self.ticks += 1
        self.agent_task = asyncio.create_task(self.gate.wait())


async def test_hung_tick_fails_within_the_real_time_bound_with_diagnostics(scenario):
    eid = scenario.store.command(
        uuid4(),
        "create",
        {"brief": "Exercise a synthetic scenario driver.", "recipe_id": "agent_experiment_v1"},
    )["experiment_id"]
    stuck = StubTick(scenario.workflow)
    started = time.monotonic()
    with pytest.raises(ScenarioTimeout) as failure:
        await run_until(stuck, lambda: False, clock=scenario.clock, timeout=0.5)
    assert time.monotonic() - started < 10  # the bound, not a progress condition, ended the run
    assert stuck.ticks == 1
    experiment = scenario.store.experiment(eid)
    text = str(failure.value)
    assert "no progress within 0.5s" in text
    assert eid in text and experiment["phase"] in text  # experiment state
    assert "actions=" in text and "jobs=" in text  # saved actions and jobs
    assert "submitted=0" in text and "begin=0" in text  # fake backend counts
    assert "pending=" in text and "gated=" in text  # engine task handles


async def test_predicate_true_before_the_first_tick_advances_nothing(scenario):
    stuck = StubTick(scenario.workflow)
    wall = scenario.clock.wall
    assert await run_until(stuck, lambda: "saved", clock=scenario.clock, timeout=0.5) == "saved"
    assert stuck.ticks == 0 and scenario.clock.wall == wall


async def test_step_advances_fake_time_and_ticks_until_the_predicate_holds(scenario):
    idle = IdleTick(scenario.workflow)
    wall = scenario.clock.wall
    assert await run_until(idle, lambda: idle.ticks >= 3, clock=scenario.clock, step=30, timeout=2)
    assert idle.ticks == 3 and scenario.clock.wall == wall + 3 * 30


async def test_task_created_by_a_tick_and_parked_on_a_gate_is_reported_not_awaited(scenario):
    gate = FakeGate()
    gated = GatedTick(scenario.workflow, gate)
    await run_until(gated, lambda: gated.ticks == 1, clock=scenario.clock, gates=[gate], timeout=2)
    parked = gated.agent_task
    assert parked is gate.task and gate.entered.is_set() and not parked.done()
    gate.release.set()
    assert (await settle(gated)).waiting == ()
    assert parked.done() and not parked.cancelled() and parked.exception() is None

    second = FakeGate()
    other = GatedTick(scenario.workflow, second)
    await other.tick()
    await second.entered.wait()
    assert (await settle(other, gates=[second])).waiting == (second.task,)
    assert second.task is other.agent_task and not other.agent_task.done()
    second.release.set()
    assert (await settle(other)).waiting == ()
    assert other.agent_task.done() and not other.agent_task.cancelled()
