"""Lost-submission scenario: durable intent, lost response, lag, restart and exact adoption.

Synthetic mechanism acceptance only: no live scheduler, model runtime or research.
"""

import json
import sys
from uuid import uuid4

from experimenttracker.config import JobResources
from experimenttracker.slurm import Observation
from scenario import settle

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


async def steps(scenario, count):
    """Advance through explicit ticks only; a condition is never inferred from yields."""
    for _ in range(count):
        await scenario.workflow.tick(force_observe=True)
        await settle(scenario.workflow)
        scenario.clock.advance(scenario.step)


async def test_lost_submission_intent_survives_lag_and_restart_then_reconciles_exactly(scenario):
    store, backend = scenario.store, scenario.slurm_backend
    eid = store.command(
        uuid4(),
        "create",
        {"brief": "Synthetic lost-submission response.", "recipe_id": "agent_experiment_v1"},
    )["experiment_id"]
    assert store.experiment(eid)["contract"]["execution_contract"] == 3
    scenario.agent.script("plan", turn=plan_turn, turns=1)
    scenario.agent.script("prepare", turn=prepare_turn, turns=1)
    scenario.agent.script("interpret", turn=interpret_turn, turns=1)
    await scenario.approve_plan(eid)
    scenario.one_session()

    # The scheduler accepts the job externally, then the response is lost and the
    # accepted identity stays invisible to accounting queries.
    backend.lose_response = backend.lag = True
    await scenario.run(lambda: store.experiment(eid)["hold_reason"] == "submission_uncertain")

    [accepted] = backend.accepted
    intent = store.one("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
    assert accepted["intent_saved"] is True  # committed before acceptance, on a separate connection
    assert accepted["intent_id"] == intent["intent_id"]
    assert (intent["state"], intent["job_id"]) == ("submitting", None)
    assert store.experiment(eid)["hold_reason"] == "submission_uncertain"
    assert backend.submit_calls == [intent["intent_id"]]

    await steps(scenario, 3)
    still_uncertain = store.one("SELECT state,job_id FROM jobs WHERE intent_id=?", (intent["intent_id"],))
    assert (still_uncertain["state"], still_uncertain["job_id"]) == ("submitting", None)
    # A missing scheduler match is accounting lag: the engine records uncertainty, not absence.
    assert "No matching job found" in store.experiment(eid)["error"]
    assert backend.submit_calls == [intent["intent_id"]]

    # A replacement workflow reuses the same SQLite state and the same fake backend.
    stopped = scenario.workflow
    replacement = await scenario.restart()
    assert stopped is not replacement and stopped.closing and replacement.store is store
    assert scenario.slurm.backend is backend and scenario.agent.backend is scenario.agent_backend
    assert (
        store.one("SELECT state FROM jobs WHERE intent_id=?", (intent["intent_id"],))["state"] == "submitting"
    )
    await steps(scenario, 2)
    assert backend.submit_calls == [intent["intent_id"]]

    saved_job_id = backend.matches[intent["intent_id"]][0]
    backend.observations[saved_job_id] = Observation("PENDING", pending_reason="Synthetic queue")
    backend.reveal()
    await scenario.run(
        lambda: (
            store.one("SELECT job_id FROM jobs WHERE intent_id=?", (intent["intent_id"],))["job_id"]
            == saved_job_id
        )
    )

    adopted = store.one("SELECT * FROM jobs WHERE intent_id=?", (intent["intent_id"],))
    assert adopted["job_id"] == saved_job_id and adopted["state"] == "submitted"
    assert store.experiment(eid)["hold_reason"] is None
    reconciled = store.rows(
        "SELECT payload FROM events WHERE experiment_id=? AND kind='job_reconciled'", (eid,)
    )
    assert len(reconciled) == 1
    assert json.loads(reconciled[0]["payload"])["job_id"] == saved_job_id
    assert backend.submit_calls == [intent["intent_id"]]  # one submission for the exact intent
    assert scenario.agent_backend.turns_started == 2  # plan and prepare only; adoption ends the run
    assert scenario.agent_backend.scripts["interpret"]["seen"] == 0
