"""Contract 4 durable submission recovery with a synthetic scheduler."""

import json

import pytest
from test_research_job_dispatch import accept
from test_research_job_source import setup_request

from experimenttracker.research.job_dispatch import dispatch
from experimenttracker.research.settlement import complete
from experimenttracker.slurm import Observation, SubmissionUncertain


async def observe_steps(scenario, count):
    for _ in range(count):
        await scenario.workflow.observe_jobs()
        scenario.clock.advance(scenario.step)


async def lost_submission(scenario, tmp_path):
    """Submit from one exact approved research turn and lose its scheduler reply."""
    workflow, backend = scenario.workflow, scenario.slurm_backend
    store = workflow.store
    _, eid, action, request = setup_request(workflow, tmp_path)
    assert store.experiment(eid)["contract"]["execution_contract"] == 4
    assert not store.rows("SELECT * FROM stages WHERE experiment_id=?", (eid,))
    accept(store, eid, request)
    scenario.one_session()

    async def synthetic_preflight(intent, script, root, resources):
        assert script.is_file() and script.parent == root
        assert not resources.gpus_per_job
        assert not store.one("SELECT 1 FROM jobs WHERE intent_id=?", (intent,))

    scenario.slurm.preflight = synthetic_preflight
    backend.lose_response = backend.lag = True
    with pytest.raises(SubmissionUncertain, match="synthetic sbatch response lost"):
        await dispatch(workflow, eid, action, request)
    intent = store.one("SELECT * FROM jobs WHERE intent_id=?", (request["request_id"],))
    assert (await dispatch(workflow, eid, action, request)) == intent
    await workflow.observe_jobs()
    assert store.experiment(eid)["hold_reason"] == "submission_uncertain"
    return eid, intent, action, request


async def test_lost_submission_intent_survives_lag_and_restart_then_reconciles_exactly(scenario, tmp_path):
    store, backend = scenario.store, scenario.slurm_backend
    eid, intent, action, _ = await lost_submission(scenario, tmp_path)

    [accepted] = backend.accepted
    assert accepted["intent_saved"] is True  # Visible on a separate read-only connection before send.
    assert accepted["intent_id"] == intent["intent_id"]
    assert (intent["state"], intent["job_id"]) == ("submitting", None)
    assert backend.submit_calls == [intent["intent_id"]]

    await observe_steps(scenario, 3)
    still_uncertain = store.one("SELECT state,job_id FROM jobs WHERE intent_id=?", (intent["intent_id"],))
    assert still_uncertain == {"state": "submitting", "job_id": None}
    assert "No matching job found" in store.experiment(eid)["error"]
    assert backend.submit_calls == [intent["intent_id"]]

    settled = await complete(
        scenario.workflow, action, {"status": "wait", "summary": "Job pending", "next": "Wait for accounting"}
    )
    assert settled["state"] == "completed"
    stopped = scenario.workflow
    replacement = await scenario.restart()
    assert stopped is not replacement and stopped.closing and replacement.store is store
    assert scenario.slurm.backend is backend and scenario.agent.backend is scenario.agent_backend
    assert store.one("SELECT state FROM jobs WHERE intent_id=?", (intent["intent_id"],)) == {
        "state": "submitting"
    }
    await replacement.tick(force_observe=True)
    await observe_steps(scenario, 2)
    assert backend.submit_calls == [intent["intent_id"]]

    saved_job_id = accepted["job_id"]
    backend.observations[saved_job_id] = Observation("PENDING", pending_reason="Synthetic queue")
    backend.reveal()
    await observe_steps(scenario, 2)

    adopted = store.one("SELECT * FROM jobs WHERE intent_id=?", (intent["intent_id"],))
    assert (adopted["job_id"], adopted["state"]) == (saved_job_id, "submitted")
    assert store.experiment(eid)["hold_reason"] is None
    [reconciled] = store.rows(
        "SELECT payload FROM events WHERE experiment_id=? AND kind='job_reconciled'", (eid,)
    )
    assert json.loads(reconciled["payload"])["job_id"] == saved_job_id
    assert backend.submit_calls == [intent["intent_id"]]
    assert store.rows("SELECT id,state FROM agent_actions WHERE experiment_id=?", (eid,)) == [
        {"id": action["id"], "state": "completed"}
    ]
    assert scenario.agent_backend.turns_started == 0
