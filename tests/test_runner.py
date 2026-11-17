"""Engine tasks run independently: isolation, gates, primary-agent exclusion, shutdown."""

import asyncio
import sqlite3
import threading

import pytest
from fastapi.testclient import TestClient
from scenario import FakeClock, FakeGate

from experimenttracker import retention
from experimenttracker.api import create_app
from experimenttracker.engine import integration_runtime
from experimenttracker.engine.runner import INTEGRATION_SECONDS, Runner
from experimenttracker.workflow import CONTROL_SERVICE, HEALTH_COUNTED_STEPS, OBSERVE_SERVICE

# The sequential positions around the omitted observation, then the gated region.
CONTROL_FRAGMENTS = [
    "step_control_before_observe",
    "step_runtime",
    "step_control_after_observe",
    "step_resolver",
]
GATED_CONTROL_FRAGMENTS = ["step_control_advance", "step_control_finish"]
AUTH = {"Authorization": "Bearer " + "t" * 40}


def recorder(calls, name):
    async def record(*args, **kwargs):
        calls.append(name)

    return record


def record_steps(monkeypatch, workflow, names, calls):
    for name in names:
        monkeypatch.setattr(workflow, name, recorder(calls, name))


def block_maintenance(store):
    release_id = "b" * 40
    with store.tx() as db:
        db.execute(
            "INSERT INTO releases(commit_id,summary,state,updated_at) VALUES(?,?,?,?)",
            (release_id, "synthetic blocked maintenance", "ready", 0),
        )
        db.execute(
            "INSERT INTO maintenance(request_id,release_id,state,created_at,updated_at) VALUES(?,?,?,?,?)",
            ("synthetic-blocked-request", release_id, "waiting", 0, 0),
        )


def slow_periods(runner, monkeypatch, seconds=30):
    """Keep the loops from taking further passes while a test drives single passes."""
    monkeypatch.setattr(runner, "periods", lambda: dict.fromkeys(runner.steps, seconds))


async def test_control_pass_runs_the_fragments_without_observation_and_launches(runtime, monkeypatch):
    _, _, _, workflow = runtime
    calls, launches = [], []
    record_steps(monkeypatch, workflow, CONTROL_FRAGMENTS + GATED_CONTROL_FRAGMENTS, calls)
    monkeypatch.setattr(workflow, "observe_jobs", recorder(calls, "observe_jobs"))
    monkeypatch.setattr(workflow.maintenance, "launch", lambda: launches.append("launch"))
    runner = Runner(workflow)

    assert await runner.run_once("control") is True

    assert calls == CONTROL_FRAGMENTS + GATED_CONTROL_FRAGMENTS
    assert launches == ["launch"]
    # The resolver runs inside the control pass, so that pass reports its health too.
    assert {"control", "resolver"} <= set(runner.last_success)


async def test_maintenance_gate_skips_gated_work_and_completes_the_control_pass(runtime, monkeypatch):
    store, _, _, workflow = runtime
    block_maintenance(store)
    calls, launches, advanced = [], [], []
    record_steps(monkeypatch, workflow, CONTROL_FRAGMENTS + GATED_CONTROL_FRAGMENTS, calls)
    monkeypatch.setattr(workflow.maintenance, "launch", lambda: launches.append("launch"))
    monkeypatch.setattr(workflow.research, "advance", recorder(advanced, "advance"))
    runner = Runner(workflow)

    assert await runner.run_once("control") is True
    assert calls == CONTROL_FRAGMENTS
    assert launches == ["launch"]  # launch stays after a gated-but-successful control pass

    # A gated task is skipped, and the skip counts as success: the gate is intended.
    assert await runner.run_once("research") is True
    assert advanced == []
    assert "research" in runner.last_success


async def test_successful_passes_record_the_families_they_really_served(runtime, monkeypatch):
    _, _, _, workflow = runtime
    calls = []
    record_steps(monkeypatch, workflow, CONTROL_FRAGMENTS + GATED_CONTROL_FRAGMENTS, calls)
    monkeypatch.setattr(workflow, "step_observe", recorder(calls, "step_observe"))
    monkeypatch.setattr(workflow.maintenance, "launch", lambda: None)
    runner = Runner(workflow)

    assert await runner.run_once("control") is True
    assert set(CONTROL_SERVICE) <= set(workflow.serviced_at)
    workflow.serviced_at.clear()

    assert await runner.run_once("observe") is True
    assert set(OBSERVE_SERVICE) <= set(workflow.serviced_at)


async def test_a_failed_pass_records_no_service(runtime, monkeypatch):
    _, _, _, workflow = runtime

    async def failing_control():
        raise RuntimeError("synthetic control failure")

    monkeypatch.setattr(workflow, "step_control", failing_control)
    runner = Runner(workflow)

    assert await runner.run_once("control") is False
    assert workflow.serviced_at == {}


async def test_stuck_observation_does_not_stop_control_or_watchdog(runtime, monkeypatch):
    _, _, _, workflow = runtime
    clock = FakeClock()
    clock.install(monkeypatch)
    gate = FakeGate()

    async def parked_observation(*args, **kwargs):
        await gate.wait()

    monkeypatch.setattr(workflow, "observe_jobs", parked_observation)
    runner = Runner(workflow)
    observation = asyncio.create_task(runner.run_once("observe"))
    try:
        async with asyncio.timeout(5):
            await gate.entered.wait()
        # The former sequential blocker is parked, yet unrelated tasks complete.
        assert await runner.run_once("control") is True
        assert await runner.run_once("watchdog") is True
        assert {"control", "resolver", "watchdog"} <= set(runner.last_success)
        assert "observe" not in runner.last_success
        # Observation never succeeded, so the heartbeat stays unsatisfied.
        assert runner.health() == (None, None)
    finally:
        gate.release.set()
    assert await observation is True
    assert runner.health()[0] is None


def test_healthz_stays_unhealthy_while_observation_keeps_failing(config, runtime, monkeypatch):
    store, _, _, workflow = runtime
    attempted = threading.Event()

    async def failing_observation(*args, **kwargs):
        attempted.set()
        raise RuntimeError("observation is stuck")

    monkeypatch.setattr(workflow, "step_observe", failing_observation)
    app = create_app(config, components=(None, store, workflow))
    with TestClient(app) as client:
        assert attempted.wait(timeout=10)  # failure detector, never a progress condition
        response = client.get("/healthz")
        assert response.status_code == 503
        assert response.json() == {"ok": False}
        status = client.get("/api/status", headers=AUTH).json()
        assert status["last_tick"] is None
        assert status["executor_error"]
        # The failing task stays the only one that does not complete its pass.
        assert "watchdog" in app.state.runner.last_success


async def test_failing_task_is_isolated_and_recovers_on_its_next_pass(runtime, monkeypatch):
    _, _, _, workflow = runtime
    broken = [True]

    async def step_watchdog(now):
        if broken[0]:
            raise RuntimeError("synthetic watchdog failure")

    monkeypatch.setattr(workflow, "step_watchdog", step_watchdog)
    runner = Runner(workflow)

    assert await runner.run_once("watchdog") is False
    assert runner.health()[1]
    assert await runner.run_once("control") is True

    broken[0] = False
    assert await runner.run_once("watchdog") is True
    assert runner.health()[1] is None


async def test_primary_agent_lock_excludes_control_research_and_supervisor(runtime, monkeypatch):
    _, _, _, workflow = runtime
    gate = FakeGate()
    started = {"research": asyncio.Event(), "supervisor": asyncio.Event()}
    order = []

    async def parked_draining(now):
        order.append("control")
        await gate.wait()

    async def step_research():
        started["research"].set()
        order.append("research")

    async def step_supervisor():
        started["supervisor"].set()
        order.append("supervisor")

    async def step_observe(now):
        order.append("observe")

    monkeypatch.setattr(workflow, "step_control_after_observe", parked_draining)
    monkeypatch.setattr(workflow, "step_research", step_research)
    monkeypatch.setattr(workflow, "step_supervisor", step_supervisor)
    monkeypatch.setattr(workflow, "step_observe", step_observe)
    runner = Runner(workflow)

    control = asyncio.create_task(runner.run_once("control"))
    async with asyncio.timeout(5):
        await gate.entered.wait()
    research = asyncio.create_task(runner.run_once("research"))
    supervisor = asyncio.create_task(runner.run_once("supervisor"))
    try:
        # Both agent-owned steps wait for the primary-agent slot control holds.
        for name in ("research", "supervisor"):
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0.2):
                    await started[name].wait()
        # Observation never takes that lock, so it completes while control holds it.
        assert await runner.run_once("observe") is True
        assert order == ["control", "observe"]
    finally:
        gate.release.set()
    await asyncio.gather(control, research, supervisor)
    assert order == ["control", "observe", "research", "supervisor"]


async def test_stop_waits_for_a_threaded_pass_before_the_store_closes(runtime, monkeypatch):
    store, _, _, workflow = runtime
    started, finished, release = threading.Event(), threading.Event(), threading.Event()

    def parked_prune(store, cursor=0, **kwargs):
        started.set()
        assert release.wait(10)  # failure detector, never a progress condition
        finished.set()
        return {"cursor": 0, "pruned": 0}

    monkeypatch.setattr(retention, "prune_batch", parked_prune)
    runner = Runner(workflow)
    slow_periods(runner, monkeypatch)
    await runner.start()

    stop = asyncio.create_task(runner.stop())
    assert await asyncio.to_thread(started.wait, 10)
    # A cancelled task would leave its thread writing; shutdown must await the pass.
    assert not stop.done()
    assert "retention" in runner.tasks
    release.set()
    async with asyncio.timeout(10):
        await stop

    assert finished.is_set()
    assert runner.tasks == {}
    await workflow.close()
    store.close()
    with pytest.raises(sqlite3.ProgrammingError):
        store.rows("SELECT 1")


async def test_only_one_runner_starts_per_workflow(runtime, monkeypatch):
    _, _, _, workflow = runtime
    runner = Runner(workflow)
    slow_periods(runner, monkeypatch)
    await runner.start()
    with pytest.raises(RuntimeError):
        await runner.start()
    await runner.stop()
    assert runner.tasks == {}


async def test_heartbeat_is_the_oldest_counted_success_and_excludes_backup(runtime, monkeypatch):
    _, _, _, workflow = runtime
    clock = FakeClock()
    clock.install(monkeypatch)
    runner = Runner(workflow)

    assert runner.health() == (None, None)
    for name in ("control", "observe", "watchdog", "research", "supervisor", "retention"):
        assert await runner.run_once(name) is True
        clock.advance(1)
    tick, error = runner.health()
    assert error is None
    assert tick == clock.now() - 6  # the first counted success, from the control pass

    assert await runner.run_once("backup") is True
    clock.advance(120)
    assert runner.health() == (tick, None)  # backup was already a background task
    await workflow.backup_task


async def test_integration_pass_is_gated_threaded_and_health_neutral(runtime, monkeypatch):
    store, _, _, workflow = runtime
    calls, threads = [], []

    def record(workspaces):
        calls.append(workspaces)
        threads.append(threading.current_thread().name)

    monkeypatch.setattr(integration_runtime, "run", record)
    runner = Runner(workflow)
    assert runner.periods()["integration"] == INTEGRATION_SECONDS
    block_maintenance(store)

    assert await runner.run_once("integration") is True
    assert calls == []

    with store.tx() as db:
        db.execute("UPDATE maintenance SET state='complete'")
    assert await runner.run_once("integration") is True
    # The driver gets the same Workspaces instance the rest of the workflow uses.
    assert calls == [workflow.workspaces]
    # Git and the project check block, so the pass runs off the event loop.
    assert threads and threads[0] != threading.current_thread().name
    # A queued integration is drained work, not part of the counted heartbeat.
    assert "integration" not in HEALTH_COUNTED_STEPS
    assert runner.health() == (None, None)


async def test_stop_waits_for_a_threaded_integration_pass(runtime, monkeypatch):
    store, _, _, workflow = runtime
    started, finished, release = threading.Event(), threading.Event(), threading.Event()

    def parked(workspaces):
        started.set()
        assert release.wait(10)  # failure detector, never a progress condition
        finished.set()

    monkeypatch.setattr(integration_runtime, "run", parked)
    runner = Runner(workflow)
    slow_periods(runner, monkeypatch)
    await runner.start()

    stop = asyncio.create_task(runner.stop())
    assert await asyncio.to_thread(started.wait, 10)
    # A cancelled task would leave its thread fast-forwarding a checkout; stop waits.
    assert not stop.done()
    release.set()
    async with asyncio.timeout(10):
        await stop

    assert finished.is_set()
    assert runner.tasks == {}
