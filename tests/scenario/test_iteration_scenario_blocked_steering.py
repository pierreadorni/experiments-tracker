"""Contract 3 blocked-stage steering preserves the saved attempt.

Synthetic mechanism acceptance only: no live scheduler, model runtime or research.
"""

import json
import sys
from uuid import uuid4

from helpers import seed_saved_contract3

PLAN = {
    "schema_version": 3,
    "summary": "Synthetic blocked agent stage.",
    "protocol": "Synthetic mechanism check, not scientific research.",
    "synthetic_demo": True,
    "parameters_json": "{}",
    "files": ["recipe.py"],
    "figures": [],
    "stages": [
        {
            "id": "evaluate",
            "title": "Synthetic agent evaluation",
            "executor": "agent",
            "instruction": "Read inputs.json and write the synthetic result.",
            "outputs": ["results.json"],
            "result_file": "results.json",
            "needs": [],
        }
    ],
}
SOURCE = "print('synthetic scenario recipe')\n"
BLOCKED = "Synthetic input unavailable"
STEERING = "Use the synthetic stand-in input instead."


def plan_turn(payload, workspace):
    return {
        "schema_version": 3,
        "title": "Synthetic blocked stage",
        "summary": PLAN["summary"],
        "protocol": PLAN["protocol"],
        "stages": [
            {
                "id": "evaluate",
                "title": "Synthetic agent evaluation",
                "summary": "Synthetic agent task.",
                "executor": "agent",
                "needs": [],
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


def blocked_turn(payload, workspace):
    return {"outcome": "blocked", "summary": BLOCKED}


async def test_blocked_agent_stage_steering_queues_one_continuation_on_the_saved_attempt(
    scenario, contract_three_engine
):
    store, agent_backend = scenario.store, scenario.agent_backend
    eid = seed_saved_contract3(store, "Synthetic blocked agent stage.")
    assert store.experiment(eid)["contract"]["execution_contract"] == 3
    scenario.agent.script("plan", turn=plan_turn, turns=1)
    scenario.agent.script("prepare", turn=prepare_turn, turns=1)
    scenario.agent.script("agent:evaluate", turn=blocked_turn, turns=2)
    await scenario.approve_plan(eid)

    # The engine reaches the agent stage through its ordinary turns: no saved rows
    # are inserted by this scenario.
    await scenario.run(lambda: store.experiment(eid)["hold_reason"] == "agent_stage_blocked")
    blocked = store.one(
        "SELECT * FROM agent_actions WHERE experiment_id=? AND phase='agent:evaluate'", (eid,)
    )
    job = store.one("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
    exp = store.experiment(eid)
    assert (exp["phase"], exp["error"]) == ("training", BLOCKED)
    assert (blocked["state"], blocked["previous_action_id"]) == ("completed", None)
    assert blocked["thread_id"] and blocked["turn_id"]
    assert (job["stage_id"], job["state"]) == ("evaluate", "agent")
    assert (
        json.loads(job["execution_manifest"])["intent_id"] == job["intent_id"]
    )  # attempt bound to its intent
    assert agent_backend.turns_started == 3  # plan, prepare and the blocked stage turn
    assert agent_backend.starts[2]["thread_id"] == blocked["thread_id"]
    assert agent_backend.threads == 3  # one new managed thread per ordinary phase

    # Later steering and its exact duplicate: the continuation is queued once, in the
    # command transaction, on the same thread and attempt, carrying the guidance.
    request = uuid4()
    applied = store.command(request, "steer", {"text": STEERING, "mode": "later"}, eid=eid)
    assert store.command(request, "steer", {"text": STEERING, "mode": "later"}, eid=eid) == applied
    [continuation] = store.rows("SELECT * FROM agent_actions WHERE previous_action_id=?", (blocked["id"],))
    assert continuation["id"] == str(request) and continuation["state"] == "pending"
    assert continuation["error"] == STEERING and continuation["input"] == blocked["input"]
    assert continuation["thread_id"] == blocked["thread_id"]
    assert store.experiment(eid)["hold_reason"] is None
    assert store.rows("SELECT * FROM steering WHERE experiment_id=?", (eid,))[0]["state"] == "applied"
    assert len(store.rows("SELECT * FROM agent_actions WHERE experiment_id=?", (eid,))) == 4

    # The continuation is dispatched on the saved thread with the saved prompt plus
    # the guidance; its own blocked outcome leaves the single attempt untouched.
    await scenario.run(lambda: agent_backend.turns_started == 4)
    resumed = agent_backend.starts[3]
    assert resumed["thread_id"] == blocked["thread_id"]
    assert STEERING in resumed["prompt"] and blocked["input"] in resumed["prompt"]
    await scenario.run(lambda: store.experiment(eid)["hold_reason"] == "agent_stage_blocked")
    assert store.one("SELECT * FROM agent_actions WHERE id=?", (blocked["id"],)) == blocked
    assert store.rows("SELECT * FROM agent_actions WHERE previous_action_id=?", (blocked["id"],)) == [
        store.one("SELECT * FROM agent_actions WHERE id=?", (continuation["id"],))
    ]
    unchanged = store.one("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
    assert unchanged == job  # same intent, attempt root and manifest
    assert store.experiment(eid)["thread_id"] == blocked["thread_id"]
    assert agent_backend.turns_started == 4
