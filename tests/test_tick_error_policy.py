"""Bugs hold one experiment; only an explicit ExperimentFailure ends it."""

import time
from uuid import uuid4

import pytest

from experimenttracker.errors import ExperimentFailure
from experimenttracker.projects import ProjectUnavailable
from experimenttracker.store import encode
from experimenttracker.workflow import Workflow


def submit(store):
    return store.command(
        uuid4(),
        "create",
        {
            "brief": "Compare linear and quadratic models on shared synthetic data.",
            "recipe_id": "toy_compare_v1",
        },
    )["experiment_id"]


def raising(exc):
    calls = []

    async def advance(workflow, eid):
        calls.append(eid)
        raise exc

    advance.calls = calls
    return advance


@pytest.mark.parametrize("error", [KeyError("saved snapshot identity"), ValueError("broken invariant")])
async def test_unexpected_error_holds_the_experiment_and_skips_its_next_tick(runtime, monkeypatch, error):
    store, _, _, workflow = runtime
    eid = submit(store)
    phase = store.experiment(eid)["phase"]
    advance = raising(error)
    monkeypatch.setattr(Workflow, "advance", advance)

    await workflow.tick(force_observe=True)

    exp = store.experiment(eid)
    assert advance.calls == [eid]
    assert exp["phase"] == phase
    assert exp["hold_reason"] == "executor_error"
    assert exp["error"] == str(error)
    count, next_at = workflow.error_backoff[eid]
    assert count == 0 and 0 < next_at - time.time() <= 30

    await workflow.tick(force_observe=True)

    # Inside the backoff window the experiment is skipped, not advanced again.
    assert advance.calls == [eid]
    assert store.experiment(eid)["hold_reason"] == "executor_error"

    # Once the window elapses the same experiment is retried with a longer delay.
    workflow.error_backoff[eid] = (0, time.time() - 1)
    await workflow.tick(force_observe=True)
    assert advance.calls == [eid, eid]
    count, next_at = workflow.error_backoff[eid]
    assert count == 1 and 30 < next_at - time.time() <= 60
    await workflow.close()


async def test_oserror_notifies_once_per_distinct_error_text(runtime, monkeypatch):
    store, _, _, workflow = runtime
    eid = submit(store)
    for error in (OSError("lost workspace"), OSError("lost workspace"), OSError("out of file handles")):
        advance = raising(error)
        monkeypatch.setattr(Workflow, "advance", advance)
        workflow.error_backoff.clear()
        await workflow.tick(force_observe=True)
        assert advance.calls == [eid]

    texts = [
        row["text"]
        for row in store.rows(
            "SELECT text FROM inbox WHERE experiment_id=? AND kind='executor_error'", (eid,)
        )
    ]
    assert len(texts) == 2
    assert sum("lost workspace" in text for text in texts) == 1
    assert sum("out of file handles" in text for text in texts) == 1
    assert store.experiment(eid)["hold_reason"] == "executor_error"
    await workflow.close()


async def test_successful_advance_clears_backoff(runtime, monkeypatch):
    store, _, _, workflow = runtime
    eid = submit(store)
    monkeypatch.setattr(Workflow, "advance", raising(OSError("transient directory failure")))
    await workflow.tick(force_observe=True)
    assert workflow.error_backoff[eid]

    async def repaired(workflow, eid):
        store.update(eid, hold_reason=None, error=None)

    monkeypatch.setattr(Workflow, "advance", repaired)
    workflow.error_backoff[eid] = (0, time.time() - 1)
    await workflow.tick(force_observe=True)

    assert eid not in workflow.error_backoff
    assert store.experiment(eid)["hold_reason"] is None

    # A recurrence still advances on the first tick after `next_at` and not before.
    advance = raising(OSError("transient directory failure"))
    monkeypatch.setattr(Workflow, "advance", advance)
    await workflow.tick(force_observe=True)
    assert advance.calls == [eid] and workflow.error_backoff[eid][0] == 0
    await workflow.close()


async def test_backoff_is_per_experiment_and_does_not_block_other_work(runtime, monkeypatch):
    store, _, _, workflow = runtime
    broken, healthy = submit(store), submit(store)
    calls = []

    async def advance(workflow, eid):
        calls.append(eid)
        if eid == broken:
            raise OSError("transient workspace failure")

    monkeypatch.setattr(Workflow, "advance", advance)
    await workflow.tick(force_observe=True)
    await workflow.tick(force_observe=True)
    assert calls.count(broken) == 1
    assert calls.count(healthy) == 2
    assert healthy not in workflow.error_backoff
    await workflow.close()


def test_backoff_caps_at_thirty_minutes(runtime, monkeypatch):
    store, _, _, workflow = runtime
    eid = submit(store)
    monkeypatch.setattr("experimenttracker.clock.now", lambda: 1000.0)
    workflow.error_backoff[eid] = (5, 1000.0)
    workflow.hold_advance_error(eid, OSError("still unavailable"))
    assert workflow.error_backoff[eid] == (6, 2800.0)
    workflow.hold_advance_error(eid, OSError("still unavailable"))
    assert workflow.error_backoff[eid] == (7, 2800.0)


async def test_experiment_failure_still_fails_the_experiment(runtime, monkeypatch):
    store, _, _, workflow = runtime
    eid = submit(store)
    monkeypatch.setattr(
        Workflow,
        "advance",
        raising(ExperimentFailure("reference_protocol_mismatch", "the reference uses a different protocol")),
    )

    await workflow.tick(force_observe=True)

    exp = store.experiment(eid)
    assert (exp["phase"], exp["hold_reason"]) == ("failed", "reference_protocol_mismatch")
    assert exp["error"] == "the reference uses a different protocol"
    assert eid not in workflow.error_backoff
    assert store.rows("SELECT 1 FROM inbox WHERE experiment_id=? AND kind='executor_error'", (eid,)) == []
    await workflow.close()


async def test_project_unavailable_keeps_its_own_hold(runtime, monkeypatch):
    store, _, _, workflow = runtime
    eid = submit(store)
    phase = store.experiment(eid)["phase"]
    monkeypatch.setattr(
        Workflow, "advance", raising(ProjectUnavailable("project folder is missing or its identity changed"))
    )

    await workflow.tick(force_observe=True)

    exp = store.experiment(eid)
    assert exp["phase"] == phase and exp["hold_reason"] == "project_unavailable"
    assert eid not in workflow.error_backoff
    await workflow.close()


@pytest.mark.parametrize(
    "metrics",
    [
        [],
        [
            {"name": "measurement", "variant": "method", "value": 1},
            {"name": "measurement", "variant": "method", "value": 2},
        ],
    ],
)
def test_iteration_result_metric_conflicts_are_terminal(runtime, metrics):
    store, _, _, workflow = runtime
    eid = submit(store)
    snapshot = {
        "plan": {
            "protocol": "Requested protocol",
            "stages": [{"id": "evaluate", "result_file": "results.json"}],
        }
    }
    with store.tx() as db:
        db.execute(
            "UPDATE experiments SET snapshot=?,partial_results=? WHERE id=?",
            (
                encode(snapshot),
                encode({"tasks": {"evaluate": {"result": {"metrics": metrics}}}}),
                eid,
            ),
        )

    with pytest.raises(ExperimentFailure) as failure:
        workflow.prepare_iteration_results(store.experiment(eid))

    assert failure.value.reason == "validation_failed"
    assert "metric" in failure.value.message
