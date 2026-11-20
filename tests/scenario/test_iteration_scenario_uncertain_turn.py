"""Contract 4 uncertain-turn scenarios through the shared resolver and readback."""

import json

import pytest
from test_research_create import create, git_project
from test_research_job_observe import saved_job

from experimenttracker.codex_client import TransportError
from experimenttracker.resolver import ABSENT_AFTER_SECONDS, NOT_STARTED, UNREADABLE_LIMIT
from experimenttracker.slurm import Observation
from scenario import settle

TURN_END = {"status": "wait", "summary": "Synthetic resolver scenario", "next": "timer:3600"}


@pytest.fixture
def research_scenario(scenario, tmp_path):
    git_project(scenario.config, scenario.store, tmp_path, research_loop=True)
    return scenario


def actions(store, eid):
    return store.rows("SELECT * FROM agent_actions WHERE experiment_id=? ORDER BY created_at", (eid,))


def start_lost_turn(scenario, brief, *, started=True):
    _, _, receipt = create(scenario.store, brief=brief, execution_contract=4)
    scenario.agent.script(
        "experiment_research",
        begin_error=TransportError("synthetic turn/start response lost"),
        turns=1,
        started=started,
    )
    return receipt["experiment_id"]


async def steps(scenario, count):
    for _ in range(count):
        await scenario.workflow.tick(force_observe=True)
        await settle(scenario.workflow)
        scenario.clock.advance(scenario.step)


async def test_lost_turn_start_is_adopted_after_restart_without_a_second_dispatch(research_scenario):
    scenario = research_scenario
    store, backend = scenario.store, scenario.agent_backend
    eid = start_lost_turn(scenario, "Synthetic lost research turn start.")
    await scenario.run(lambda: any(row["state"] == "uncertain" for row in actions(store, eid)))
    [action] = actions(store, eid)
    assert (action["phase"], action["state"], action["turn_id"], action["attempts"]) == (
        "research",
        "uncertain",
        None,
        1,
    )
    [start] = backend.starts
    [dispatch] = store.rows(
        "SELECT payload FROM events WHERE experiment_id=? AND kind='agent_dispatch_intent'", (eid,)
    )
    intent = json.loads(dispatch["payload"])
    assert (intent["action_id"], intent["thread_id"], intent["prompt_sha256"]) == (
        action["id"],
        start["thread_id"],
        start["prompt_sha256"],
    )
    scenario.agent.complete(start["thread_id"], start["turn_id"], TURN_END)

    await scenario.restart()
    await scenario.run(lambda: actions(store, eid)[0]["state"] == "completed")

    [adopted] = actions(store, eid)
    assert (adopted["id"], adopted["thread_id"], adopted["turn_id"]) == (
        action["id"],
        start["thread_id"],
        start["turn_id"],
    )
    assert json.loads(adopted["result"])["summary"] == TURN_END["summary"]
    await steps(scenario, 2)
    assert actions(store, eid) == [adopted]
    assert backend.turns_started == 1 and backend.starts == [start]


async def test_absent_research_turn_waits_for_the_bound_then_holds_without_redispatch(research_scenario):
    scenario = research_scenario
    store, backend = scenario.store, scenario.agent_backend
    eid = start_lost_turn(scenario, "Synthetic research turn that never started.", started=False)
    await scenario.run(lambda: any(row["state"] == "uncertain" for row in actions(store, eid)))
    [action] = actions(store, eid)
    assert (action["state"], action["turn_id"], action["attempts"]) == ("uncertain", None, 1)
    assert backend.turns == {}

    await steps(scenario, 2)
    assert [row["state"] for row in actions(store, eid)] == ["uncertain"]
    assert backend.turns_started == 1

    await scenario.restart()
    scenario.clock.advance(ABSENT_AFTER_SECONDS)
    await scenario.run(lambda: actions(store, eid)[0]["state"] == "failed")
    await steps(scenario, 2)

    [failed] = actions(store, eid)
    assert (failed["id"], failed["error"], failed["thread_id"], failed["turn_id"], failed["attempts"]) == (
        action["id"],
        NOT_STARTED,
        action["thread_id"],
        None,
        1,
    )
    assert backend.turns_started == 1 and len(backend.starts) == 1
    assert store.experiment(eid)["hold_reason"] == "agent_dispatch_uncertain"
    [decision] = store.rows(
        "SELECT payload FROM events WHERE experiment_id=? AND kind='agent_action_changed' "
        "AND json_extract(payload,'$.error')=?",
        (eid, NOT_STARTED),
    )
    assert json.loads(decision["payload"])["action_id"] == action["id"]
    [notice] = store.rows("SELECT text FROM inbox WHERE experiment_id=?", (eid,))
    assert action["id"] in notice["text"] and "inspection required" in notice["text"]


async def test_unreadable_thread_retries_then_holds_only_that_experiment(research_scenario):
    scenario = research_scenario
    store, backend, slurm = scenario.store, scenario.agent_backend, scenario.slurm_backend
    peer, peer_job = saved_job(store)
    slurm.observations[peer_job["job_id"]] = Observation("PENDING", pending_reason="Synthetic queue")

    eid = start_lost_turn(scenario, "Synthetic unreadable uncertain research turn.")
    await scenario.run(lambda: any(row["state"] == "uncertain" for row in actions(store, eid)))
    [action] = actions(store, eid)
    backend.unreadable[action["thread_id"]] = TransportError("thread/read transport closed")

    for _ in range(UNREADABLE_LIMIT - 1):
        scenario.clock.advance(310)
        await steps(scenario, 1)
    assert not store.rows("SELECT * FROM inbox WHERE experiment_id=?", (eid,))
    assert actions(store, eid) == [action]

    scenario.clock.advance(310)
    await steps(scenario, 1)
    assert [(row["id"], row["state"]) for row in actions(store, eid)] == [(action["id"], "uncertain")]
    [notice] = store.rows("SELECT * FROM inbox WHERE experiment_id=?", (eid,))
    assert "no readable thread" in notice["text"] and action["id"] in notice["text"]
    assert not store.rows(
        "SELECT * FROM inbox WHERE experiment_id=? AND text LIKE '%no readable thread%'", (peer["id"],)
    )

    # The separate saved C4 intent still receives authoritative terminal accounting.
    slurm.observations[peer_job["job_id"]] = Observation("COMPLETED", "0:0")
    await steps(scenario, 1)
    observed = store.one(
        "SELECT intent_id,job_id,state,scheduler_state FROM jobs WHERE experiment_id=?", (peer["id"],)
    )
    assert observed == {
        "intent_id": peer_job["intent_id"],
        "job_id": peer_job["job_id"],
        "state": "complete",
        "scheduler_state": "COMPLETED",
    }
    assert slurm.submit_calls == []
    assert backend.turns_started == 1
