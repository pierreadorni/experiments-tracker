"""C5 resolver scenarios: adoption, proven absence, quarantine and an unreadable scheduler.

Synthetic mechanism acceptance only: no live scheduler, model runtime or research.
"""

import json
from uuid import uuid4

import pytest

from experimenttracker.core import job_obligations
from experimenttracker.research.job_dispatch import dispatch
from experimenttracker.research.session import follow_up_turn
from experimenttracker.research.settlement import complete
from experimenttracker.slurm import Observation
from scenario.test_iteration_scenario_lost_submission import lost_submission, observe_steps


def receipts(store, eid):
    return [
        json.loads(row["payload"])
        for row in store.rows(
            "SELECT payload FROM events WHERE experiment_id=? AND kind='job_reconciled'", (eid,)
        )
    ]


async def test_lost_response_adopts_one_match_exactly_once(scenario, tmp_path):
    store, backend = scenario.store, scenario.slurm_backend
    eid, intent, _, _ = await lost_submission(scenario, tmp_path)
    assert (intent["state"], intent["job_id"]) == ("submitting", None)

    # Before the accounting lag a missing match is uncertainty, not absence.
    await observe_steps(scenario, 2)
    waiting = store.one("SELECT state,job_id FROM jobs WHERE intent_id=?", (intent["intent_id"],))
    assert waiting == {"state": "submitting", "job_id": None}
    assert store.experiment(eid)["error"] == "No matching job found; operator reconciliation required"
    assert receipts(store, eid) == []

    saved_job_id = backend.matches[intent["intent_id"]][0]
    backend.observations[saved_job_id] = Observation("PENDING", pending_reason="Synthetic queue")
    backend.reveal()
    await observe_steps(scenario, 2)

    adopted = store.one("SELECT state,job_id FROM jobs WHERE intent_id=?", (intent["intent_id"],))
    assert adopted == {"state": "submitted", "job_id": saved_job_id}
    [receipt] = receipts(store, eid)
    assert receipt == {
        "intent_id": intent["intent_id"],
        "job_id": saved_job_id,
        "evidence": {"query": "squeue+sacct", "observer": "observe_jobs", "matched": 1},
    }
    assert store.experiment(eid)["hold_reason"] is None
    assert backend.submit_calls == [intent["intent_id"]]


async def test_absent_after_the_accounting_lag_records_a_receipt_and_the_next_attempt_is_new(
    scenario, tmp_path
):
    store, backend = scenario.store, scenario.slurm_backend
    eid, intent, action, request = await lost_submission(scenario, tmp_path)
    assert backend.matches[intent["intent_id"]]  # accepted externally, invisible to accounting
    fresh_request = {**request, "request_id": str(uuid4())}
    with pytest.raises(ValueError, match="uncertain outcome"):
        await dispatch(scenario.workflow, eid, action, fresh_request)
    assert not store.one("SELECT 1 FROM jobs WHERE intent_id=?", (fresh_request["request_id"],))
    assert backend.submit_calls == [intent["intent_id"]]

    scenario.clock.advance(601)
    await observe_steps(scenario, 1)

    saved = store.one("SELECT * FROM jobs WHERE intent_id=?", (intent["intent_id"],))
    assert (saved["state"], saved["job_id"]) == ("failed", None)
    assert job_obligations.not_submitted(saved)
    [receipt] = receipts(store, eid)
    assert receipt["outcome"] == "not_submitted" and receipt["intent_id"] == intent["intent_id"]
    assert receipt["evidence"]["accounting_lag_seconds"] == 600
    assert (
        store.one(
            "SELECT state FROM obligations WHERE target=? ORDER BY id DESC LIMIT 1", (intent["intent_id"],)
        )["state"]
        == "abandoned"
    )

    # A scheduler record that appears afterwards never revives the abandoned intent.
    backend.reveal()
    await observe_steps(scenario, 2)
    assert store.one("SELECT job_id FROM jobs WHERE intent_id=?", (intent["intent_id"],))["job_id"] is None
    assert len(receipts(store, eid)) == 1
    assert backend.submit_calls == [intent["intent_id"]]

    # C4 requires an explicit request from an authorized turn for new compute.
    replay = await dispatch(scenario.workflow, eid, action, request)
    assert replay == saved
    assert backend.submit_calls == [intent["intent_id"]]
    backend.lose_response = backend.lag = False
    retried = await dispatch(scenario.workflow, eid, action, fresh_request)
    assert retried["intent_id"] == fresh_request["request_id"]
    assert retried["job_id"] != backend.matches[intent["intent_id"]][0]
    assert retried["state"] == "submitted"
    assert set(
        job["intent_id"] for job in store.rows("SELECT intent_id FROM jobs WHERE experiment_id=?", (eid,))
    ) == {intent["intent_id"], retried["intent_id"]}
    assert backend.submit_calls == [intent["intent_id"], retried["intent_id"]]
    settled = await complete(
        scenario.workflow, action, {"status": "wait", "summary": "Wait for compute", "next": "Observe job"}
    )
    assert settled["state"] == "completed"
    scenario.clock.advance(60)
    follow_up = follow_up_turn(store, eid)
    assert f"Job {intent['intent_id']}: confirmed not submitted after accounting lag" in follow_up["input"]
    assert follow_up["thread_id"] == action["thread_id"]
    assert follow_up_turn(store, eid) is None


async def test_several_matches_quarantine_the_intent_without_adopting_it(scenario, tmp_path):
    store, backend = scenario.store, scenario.slurm_backend
    eid, intent, _, _ = await lost_submission(scenario, tmp_path)

    backend.accept(intent["intent_id"], True)  # a second scheduler job for one intent
    backend.reveal()
    await observe_steps(scenario, 2)

    saved = store.one("SELECT state,job_id,error FROM jobs WHERE intent_id=?", (intent["intent_id"],))
    assert saved == {"state": "submitting", "job_id": None, "error": None}
    experiment = store.experiment(eid)
    assert experiment["hold_reason"] == "submission_uncertain"
    assert "multiple scheduler jobs" in experiment["error"]
    [receipt] = receipts(store, eid)
    assert receipt["outcome"] == "quarantined" and receipt["intent_id"] == intent["intent_id"]
    assert receipt["evidence"]["matched"] == "several"
    assert store.one("SELECT kind FROM inbox WHERE experiment_id=?", (eid,)) == {"kind": "executor_error"}
    assert (
        store.one(
            "SELECT state FROM obligations WHERE target=? ORDER BY id DESC LIMIT 1", (intent["intent_id"],)
        )["state"]
        == "open"
    )
    assert backend.submit_calls == [intent["intent_id"]]


async def test_unreadable_scheduler_concludes_nothing(scenario, tmp_path):
    store, backend = scenario.store, scenario.slurm_backend
    eid, intent, _, _ = await lost_submission(scenario, tmp_path)

    backend.unreachable_intents[intent["intent_id"]] = TimeoutError("synthetic sacct timed out")
    scenario.clock.advance(601)
    await observe_steps(scenario, 2)

    saved = store.one("SELECT state,job_id,error FROM jobs WHERE intent_id=?", (intent["intent_id"],))
    assert saved == {"state": "submitting", "job_id": None, "error": None}
    experiment = store.experiment(eid)
    assert (experiment["hold_reason"], experiment["error"]) == (
        "submission_uncertain",
        "synthetic sacct timed out",
    )
    assert receipts(store, eid) == []
    assert (
        store.one(
            "SELECT state FROM obligations WHERE target=? ORDER BY id DESC LIMIT 1", (intent["intent_id"],)
        )["state"]
        == "open"
    )
    assert backend.submit_calls == [intent["intent_id"]]
