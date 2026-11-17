"""The manual tick adapter runs the shared steps in the original sequential order."""

from uuid import uuid4

from experimenttracker import clock
from experimenttracker.workflow import CONTROL_SERVICE, HEALTH_COUNTED_STEPS, OBSERVE_SERVICE, Workflow

STEPS = (
    "step_control_before_observe",
    "step_runtime",
    "step_observe",
    "step_control_after_observe",
    "step_resolver",
    "step_control_advance",
    "step_watchdog",
    "step_control_finish",
    "step_supervisor",
    "step_research",
    "step_retention",
    "step_backup",
)
UNBLOCKED_STEPS = STEPS[:5]


def record_steps(monkeypatch, calls):
    for name in STEPS:
        assert hasattr(Workflow, name)

        async def recorder(*args, _name=name, **kwargs):
            calls.append(_name)

        monkeypatch.setattr(Workflow, name, recorder)


def block_maintenance(store):
    release_id = "b" * 40
    with store.tx() as db:
        db.execute(
            "INSERT INTO releases(commit_id,summary,state,updated_at) VALUES(?,?,?,?)",
            (release_id, "synthetic blocked maintenance", "ready", clock.now()),
        )
        db.execute(
            "INSERT INTO maintenance(request_id,release_id,state,created_at,updated_at) VALUES(?,?,?,?,?)",
            (str(uuid4()), release_id, "waiting", clock.now(), clock.now()),
        )


async def test_tick_calls_every_step_once_in_the_recorded_order(runtime, monkeypatch):
    _, _, _, workflow = runtime
    calls = []
    record_steps(monkeypatch, calls)

    await workflow.tick(force_observe=True)

    assert calls == list(STEPS)
    await workflow.close()


async def test_maintenance_gate_still_stops_after_the_resolver(runtime, monkeypatch):
    store, _, _, workflow = runtime
    calls = []
    record_steps(monkeypatch, calls)
    block_maintenance(store)

    await workflow.tick(force_observe=True)

    # Observation, input replies and agent draining keep running; the gated work stops.
    assert calls == list(UNBLOCKED_STEPS)
    assert set(workflow.serviced_at) == set(OBSERVE_SERVICE)
    await workflow.close()


async def test_tick_records_the_service_of_the_passes_it_ran(runtime, monkeypatch):
    _, _, _, workflow = runtime
    calls = []
    record_steps(monkeypatch, calls)

    await workflow.tick(force_observe=True)

    assert set(CONTROL_SERVICE + OBSERVE_SERVICE) <= set(workflow.serviced_at)
    assert workflow.started_at is not None
    await workflow.close()


def test_health_counted_steps_name_the_step_methods():
    assert set(HEALTH_COUNTED_STEPS) == {
        "control",
        "observe",
        "research",
        "supervisor",
        "watchdog",
        "retention",
        "resolver",
    }
    for name in HEALTH_COUNTED_STEPS:
        assert any(step.startswith(f"step_{name}") for step in STEPS), name
