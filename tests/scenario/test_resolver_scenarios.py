"""C5 resolver scenarios: adoption, proven absence, quarantine and an unreadable scheduler.

Synthetic mechanism acceptance only: no live scheduler, model runtime or research.
"""

import json

from helpers import seed_saved_contract3

from experimenttracker import early_repair
from experimenttracker.core import job_obligations
from experimenttracker.models import Implementation
from experimenttracker.slurm import Observation
from experimenttracker.store import encode
from scenario import settle
from scenario.test_iteration_scenario_lost_submission import interpret_turn, plan_turn, prepare_turn


async def steps(scenario, count):
    """Advance through explicit ticks only; a condition is never inferred from yields."""
    for _ in range(count):
        await scenario.workflow.tick(force_observe=True)
        await settle(scenario.workflow)
        scenario.clock.advance(scenario.step)


async def lost_submission(scenario):
    """Drive one synthetic experiment to a saved intent whose sbatch response was lost."""
    store, backend = scenario.store, scenario.slurm_backend
    eid = seed_saved_contract3(store, "Synthetic resolver scenario.")
    scenario.agent.script("plan", turn=plan_turn, turns=1)
    scenario.agent.script("prepare", turn=prepare_turn, turns=1)
    scenario.agent.script("interpret", turn=interpret_turn, turns=1)
    await scenario.approve_plan(eid)
    scenario.one_session()
    backend.lose_response = backend.lag = True
    await scenario.run(lambda: store.experiment(eid)["hold_reason"] == "submission_uncertain")
    return eid, store.one("SELECT * FROM jobs WHERE experiment_id=?", (eid,))


def receipts(store, eid):
    return [
        json.loads(row["payload"])
        for row in store.rows(
            "SELECT payload FROM events WHERE experiment_id=? AND kind='job_reconciled'", (eid,)
        )
    ]


def retry_revision(workflow, eid, intent_id):
    """Save the selective-repair revision that follows a proven-absence compute failure."""
    store = workflow.store
    exp = store.experiment(eid)
    revision = exp["evidence_revision"] + 1
    contract = exp["contract"]
    contract["repair_count"] = contract.get("repair_count", 0) + 1
    contract["early_repair"] = {
        "state": "preparing",
        "revision": revision,
        "failed_intents": [intent_id],
        "error": exp["error"],
    }
    with store.tx() as db:
        db.execute(
            "UPDATE experiments SET contract=?,evidence_revision=?,phase='preparing',hold_reason='automatic_repair' WHERE id=?",
            (encode(contract), revision, eid),
        )
    exp = store.experiment(eid)
    plan = Implementation.model_validate(
        {
            **exp["snapshot"]["plan"],
            "reuse_rationale": "A proven absence reuses no compute.",
            "repair_decisions": [
                {
                    "stage_id": "train",
                    "decision": "retry",
                    "rationale": "The scheduler never received the first intent.",
                }
            ],
        }
    )
    early_repair.apply(workflow, exp, plan, {**exp["snapshot"], "plan": plan.model_dump()})
    return revision


async def test_lost_response_adopts_one_match_exactly_once(scenario):
    store, backend = scenario.store, scenario.slurm_backend
    eid, intent = await lost_submission(scenario)
    assert (intent["state"], intent["job_id"]) == ("submitting", None)

    # Before the accounting lag a missing match is uncertainty, not absence.
    await steps(scenario, 2)
    waiting = store.one("SELECT state,job_id FROM jobs WHERE intent_id=?", (intent["intent_id"],))
    assert waiting == {"state": "submitting", "job_id": None}
    assert store.experiment(eid)["error"] == "No matching job found; operator reconciliation required"
    assert receipts(store, eid) == []

    saved_job_id = backend.matches[intent["intent_id"]][0]
    backend.observations[saved_job_id] = Observation("PENDING", pending_reason="Synthetic queue")
    backend.reveal()
    await scenario.run(
        lambda: (
            store.one("SELECT job_id FROM jobs WHERE intent_id=?", (intent["intent_id"],))["job_id"]
            == saved_job_id
        )
    )

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


async def test_absent_after_the_accounting_lag_records_a_receipt_and_the_next_attempt_is_new(scenario):
    store, backend = scenario.store, scenario.slurm_backend
    eid, intent = await lost_submission(scenario)
    assert backend.matches[intent["intent_id"]]  # accepted externally, invisible to accounting

    scenario.clock.advance(601)
    await steps(scenario, 1)

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
    await steps(scenario, 2)
    assert store.one("SELECT job_id FROM jobs WHERE intent_id=?", (intent["intent_id"],))["job_id"] is None
    assert len(receipts(store, eid)) == 1
    assert backend.submit_calls == [intent["intent_id"]]

    # The next admission is a new attempt: one new intent, never the abandoned one.
    assert store.experiment(eid)["hold_reason"] == "compute_failed"
    revision = retry_revision(scenario.workflow, eid, intent["intent_id"])
    await steps(scenario, 3)
    submitted = store.rows("SELECT * FROM jobs WHERE experiment_id=? ORDER BY attempt", (eid,))
    assert len(submitted) == 2
    retried = [job for job in submitted if job["intent_id"] != intent["intent_id"]]
    assert [job["attempt"] for job in retried] == [revision]
    assert backend.submit_calls == [intent["intent_id"], retried[0]["intent_id"]]


async def test_several_matches_quarantine_the_intent_without_adopting_it(scenario):
    store, backend = scenario.store, scenario.slurm_backend
    eid, intent = await lost_submission(scenario)

    backend.accept(intent["intent_id"], True)  # a second scheduler job for one intent
    backend.reveal()
    await steps(scenario, 2)

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


async def test_unreadable_scheduler_concludes_nothing(scenario):
    store, backend = scenario.store, scenario.slurm_backend
    eid, intent = await lost_submission(scenario)

    backend.unreachable_intents[intent["intent_id"]] = TimeoutError("synthetic sacct timed out")
    scenario.clock.advance(601)
    await steps(scenario, 2)

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
