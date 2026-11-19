"""Uncertain-turn resolver scenarios: adopt, absence and unreadable-thread backoff.

Synthetic mechanism acceptance only; no live scheduler, model runtime or research.
"""

import json
import sys

from helpers import seed_saved_contract3

from experimenttracker.codex_client import TransportError
from experimenttracker.config import JobResources
from experimenttracker.core import advancement_decisions
from experimenttracker.resolver import ABSENT_AFTER_SECONDS, UNREADABLE_LIMIT
from experimenttracker.slurm import Observation
from scenario import settle

PLAN = {
    "schema_version": 3,
    "summary": "Synthetic uncertain-turn scenario.",
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


def plan_turn(payload, workspace):
    return {
        "schema_version": 3,
        "title": "Synthetic resolver scenario",
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
    (workspace / "recipe.py").write_text("print('synthetic resolver scenario')\n")
    return {**PLAN, "python_executable": str(runtime)}


def create(store, brief):
    eid = seed_saved_contract3(store, brief)
    with store.tx() as db:
        advancement_decisions.open_lifetime(db, eid)
    return eid


def actions(store, eid):
    return store.rows("SELECT * FROM agent_actions WHERE experiment_id=? ORDER BY created_at", (eid,))


async def steps(scenario, count):
    """Advance through explicit ticks only; a condition is never inferred from yields."""
    for _ in range(count):
        await scenario.workflow.tick(force_observe=True)
        await settle(scenario.workflow)
        scenario.clock.advance(scenario.step)


async def test_lost_turn_start_is_adopted_after_restart_without_a_second_dispatch(scenario):
    store, backend = scenario.store, scenario.agent_backend
    eid = create(store, "Synthetic lost turn start.")
    scenario.agent.script(
        "plan", turn=plan_turn, begin_error=TransportError("turn/start response lost"), turns=1
    )
    await scenario.run(lambda: store.experiment(eid)["hold_reason"] == "agent_dispatch_uncertain")
    [action] = actions(store, eid)
    assert (action["state"], action["turn_id"], action["attempts"]) == ("uncertain", None, 1)
    [start] = backend.starts
    # Only the response was lost: the runtime created the exact turn.
    backend.turns[(start["thread_id"], start["turn_id"])].update(
        result=plan_turn(None, None), state="completed"
    )

    await scenario.restart()
    await scenario.run(lambda: (store.experiment(eid)["plan_review"] or {}).get("state") == "awaiting_review")

    [adopted] = actions(store, eid)  # the saved action is completed, never duplicated
    assert (adopted["id"], adopted["state"]) == (action["id"], "completed")
    assert (adopted["thread_id"], adopted["turn_id"]) == (start["thread_id"], start["turn_id"])
    assert json.loads(adopted["result"])["title"] == "Synthetic resolver scenario"
    assert backend.turns_started == 1 and len(backend.starts) == 1
    [reconciled] = store.rows(
        "SELECT payload FROM events WHERE experiment_id=? AND kind='agent_turn_identity_reconciled'", (eid,)
    )
    evidence = json.loads(reconciled["payload"])
    assert (evidence["action_id"], evidence["turn_id"]) == (action["id"], start["turn_id"])


async def test_absent_turn_waits_before_the_bound_then_continues_exactly_once(scenario):
    store, backend = scenario.store, scenario.agent_backend
    eid = create(store, "Synthetic turn that never started.")
    scenario.agent.script(
        "plan",
        turn=plan_turn,
        begin_error=TransportError("turn/start response lost before create"),
        turns=1,
        started=False,  # the runtime refused the turn, so the thread cannot contain it
    )
    await scenario.run(lambda: store.experiment(eid)["hold_reason"] == "agent_dispatch_uncertain")
    [action] = actions(store, eid)
    assert (action["state"], action["turn_id"], action["attempts"]) == ("uncertain", None, 1)
    attempts = backend.turns_started

    # A readable thread with no matching turn is not absence until the bound passes.
    await steps(scenario, 2)
    assert [row["state"] for row in actions(store, eid)] == ["uncertain"]
    assert backend.turns_started == attempts
    assert store.experiment(eid)["hold_reason"] == "agent_dispatch_uncertain"

    await scenario.restart()
    # The decision is made while dispatch is paused, so the queued continuation can
    # start with a turn that really exists.
    store.update(eid, pause_agent=1)
    scenario.clock.advance(ABSENT_AFTER_SECONDS)
    await scenario.run(lambda: len(actions(store, eid)) == 2)

    failed, continuation = actions(store, eid)
    assert (failed["id"], failed["state"], failed["error"]) == (action["id"], "failed", "turn_not_started")
    assert failed["thread_id"] == action["thread_id"] and failed["turn_id"] is None
    assert continuation["previous_action_id"] == action["id"] and continuation["state"] == "pending"
    assert backend.turns_started == attempts  # the continuation is queued, not yet dispatched

    scenario.agent.script("plan", turn=plan_turn, turns=1)
    store.update(eid, pause_agent=0)
    await scenario.run(lambda: (store.experiment(eid)["plan_review"] or {}).get("state") == "awaiting_review")
    [failed, dispatched] = actions(store, eid)
    assert dispatched["id"] == continuation["id"] and dispatched["state"] == "completed"
    assert dispatched["turn_id"] and dispatched["thread_id"] == failed["thread_id"]
    assert backend.turns_started == attempts + 1 and len(backend.starts) == attempts + 1
    [decision] = store.rows(
        "SELECT payload FROM events WHERE experiment_id=? AND kind='agent_action_changed' "
        "AND json_extract(payload,'$.error')='turn_not_started'",
        (eid,),
    )
    assert json.loads(decision["payload"])["action_id"] == action["id"]


async def test_unreadable_thread_retries_then_holds_only_that_experiment(scenario):
    store, backend = scenario.store, scenario.agent_backend
    slurm_backend = scenario.slurm_backend
    peer = create(store, "Synthetic unrelated experiment.")
    scenario.agent.script("plan", turn=plan_turn, turns=1)
    scenario.agent.script("prepare", turn=prepare_turn, turns=1)
    await scenario.approve_plan(peer)
    await scenario.run(
        lambda: any(
            row["experiment_id"] == peer and row["job_id"] for row in store.rows("SELECT * FROM jobs")
        )
    )
    peer_job = store.one("SELECT * FROM jobs WHERE experiment_id=?", (peer,))
    slurm_backend.observations[peer_job["job_id"]] = Observation("PENDING", pending_reason="Synthetic queue")

    eid = create(store, "Synthetic unreadable uncertain turn.")
    scenario.agent.script(
        "plan", turn=plan_turn, begin_error=TransportError("turn/start response lost"), turns=1
    )
    await scenario.run(lambda: store.experiment(eid)["hold_reason"] == "agent_dispatch_uncertain")
    [action] = actions(store, eid)
    backend.unreadable[action["thread_id"]] = TransportError("thread/read transport closed")

    for _ in range(UNREADABLE_LIMIT - 1):
        scenario.clock.advance(310)  # longer than every retry backoff
        await steps(scenario, 1)
    assert not store.rows("SELECT * FROM inbox WHERE experiment_id=?", (eid,))
    assert store.experiment(eid)["hold_reason"] == "agent_dispatch_uncertain"

    scenario.clock.advance(310)
    await steps(scenario, 1)
    # No adoption, no replacement and no identity change for the unreadable action.
    assert [(row["id"], row["state"]) for row in actions(store, eid)] == [(action["id"], "uncertain")]
    [notice] = store.rows("SELECT * FROM inbox WHERE experiment_id=?", (eid,))
    assert "no readable thread" in notice["text"] and action["id"] in notice["text"]
    assert not store.rows(
        "SELECT * FROM inbox WHERE experiment_id=? AND text LIKE '%no readable thread%'", (peer,)
    )

    # The unrelated experiment keeps running: its own scheduler observation advances.
    slurm_backend.observations[peer_job["job_id"]] = Observation("RUNNING", started_at=scenario.clock.now())
    await steps(scenario, 1)
    observed = store.one("SELECT * FROM jobs WHERE intent_id=?", (peer_job["intent_id"],))
    assert (observed["state"], observed["scheduler_state"]) == ("submitted", "RUNNING")
