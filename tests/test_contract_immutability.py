"""A saved execution contract never changes after creation.

Every frozen-contract guard re-reads the saved contract after an await and expects
the same answer. That reasoning only holds while no Store path rewrites the key,
so this test drives each contract rewrite in ``store.py`` from a saved state and
compares the whole contract with the one it started from.
"""

import json
from uuid import uuid4

import pytest
from helpers import save_snapshot, seed_saved_contract3

MANIFEST = {
    "snapshot_hash": "synthetic-contract-immutability",
    "parameters": {},
    "plan": {"schema_version": 3, "synthetic_demo": True, "stages": [{"id": "train"}]},
}


def create(store):
    return seed_saved_contract3(store, "Synthetic contract immutability")


def action(store, eid, phase, **extra):
    identity = str(uuid4())
    row = {"state": "failed", "thread_id": "saved-thread", "input": "saved prompt", **extra}
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,"
            "input,error,result,created_at,updated_at) VALUES(?,?,?,1,?,?,NULL,?,NULL,?,0,0)",
            (identity, eid, phase, row["state"], row["thread_id"], row["input"], row.get("result")),
        )
    return identity


def job(store, eid, stage, state):
    with store.tx() as db:
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,script_hash,resources,"
            "snapshot_hash,created_at) VALUES(?,?,?,1,?,'synthetic','{}','synthetic',0)",
            (str(uuid4()), eid, stage, state),
        )


def rewrite(store, eid, before, changed):
    """Assert the store kept the whole saved contract apart from ``changed``."""
    after = store.experiment(eid)["contract"]
    assert after["execution_contract"] == before["execution_contract"] == 3
    assert {key: value for key, value in after.items() if key not in changed} == before


@pytest.mark.usefixtures("contract_three_fixture")
def test_every_store_contract_rewrite_preserves_the_execution_contract(runtime):
    store = runtime[0]

    resolved = create(store)
    before = store.experiment(resolved)["contract"]
    save_snapshot(store, resolved, MANIFEST)  # store.py:885
    rewrite(store, resolved, before, {"parameters", "synthetic_demo", "plan"})

    repaired = create(store)
    save_snapshot(store, repaired, MANIFEST)
    store.update(repaired, phase="training", hold_reason="agent_stage_blocked")
    action(store, repaired, "agent:train", state="completed", result=json.dumps({"outcome": "blocked"}))
    job(store, repaired, "train", "agent")
    before = store.experiment(repaired)["contract"]
    store.command(uuid4(), "action", {"action": "repair_implementation"}, eid=repaired)  # store.py:1272
    assert store.experiment(repaired)["contract"]["manual_repair_requests"] == 1
    rewrite(store, repaired, before, {"manual_repair_requests"})

    budgeted = create(store)
    store.update(budgeted, phase="failed", hold_reason="agent_budget_exhausted")
    action(store, budgeted, "plan")
    before = store.experiment(budgeted)["contract"]
    store.command(uuid4(), "action", {"action": "retry_agent"}, eid=budgeted)  # store.py:1372
    assert store.experiment(budgeted)["contract"]["manual_agent_turn_requests"] == 1
    rewrite(store, budgeted, before, {"manual_agent_turn_requests"})

    failed = create(store)
    save_snapshot(store, failed, MANIFEST)
    store.update(failed, phase="failed", hold_reason="compute_failed")
    before = store.experiment(failed)["contract"]
    store.command(uuid4(), "action", {"action": "retry_agent"}, eid=failed)  # store.py:1412
    assert store.experiment(failed)["contract"]["manual_repair_requests"] == 1
    rewrite(store, failed, before, {"manual_repair_requests"})
