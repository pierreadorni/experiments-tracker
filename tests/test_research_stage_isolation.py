"""Contract 4 must never enter the saved contract-3 stage engine.

Each test stands in for the later archive activation by widening only the legacy
archive predicate to accept execution contract 4, exactly as that card will. The
production predicate, the create path and the configuration stay untouched, so
every refusal below comes from the stage-engine gate under test.

Shared backend paths keep working: job and turn settlement, publication,
integration and completed-work discussions are deliberately not gated.
"""

import json
from uuid import uuid4

import pytest
from helpers import seed_saved_contract3

from experimenttracker import controller, legacy_archive
from experimenttracker.store import encode


def saved(store, *, contract=4, phase="training", hold_reason=None, snapshot=None):
    """Create a real saved experiment, then rewrite only its saved contract."""
    eid = seed_saved_contract3(store, "Saved run")
    with store.tx() as db:
        row = db.execute("SELECT contract FROM experiments WHERE id=?", (eid,)).fetchone()
        value = json.loads(row["contract"])
        value["execution_contract"] = contract
        db.execute(
            "UPDATE experiments SET contract=?,phase=?,hold_reason=?,snapshot=? WHERE id=?",
            (encode(value), phase, hold_reason, encode(snapshot) if snapshot else None, eid),
        )
    return eid


@pytest.fixture
def widened_archive(monkeypatch):
    """Accept contract 4 as live while older contracts stay frozen, as T4.1c will."""

    def execution_frozen(exp):
        return exp["contract"].get("execution_contract") not in (3, 4)

    monkeypatch.setattr(legacy_archive, "execution_frozen", execution_frozen)
    monkeypatch.setattr(controller, "execution_frozen", execution_frozen)


def add_job(store, eid, intent, *, state="failed", job_id="123", scheduler_state=None):
    with store.tx() as db:
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,state,job_id,script_hash,resources,"
            "snapshot_hash,created_at,scheduler_state) VALUES(?,?,'prepare',?,?,?,?,?,0,?)",
            (intent, eid, state, job_id, "s", "{}", "h", scheduler_state),
        )


def add_steering(store, eid, identity, mode):
    with store.tx() as db:
        db.execute(
            "INSERT INTO steering(request_id,experiment_id,text,created_at,mode,state) "
            "VALUES(?,?,?,0,?,'pending')",
            (identity, eid, "Saved guidance", mode),
        )


async def test_stage_tools_refuse_a_saved_contract_four_session(runtime, widened_archive):
    store, _, _, wf = runtime
    eid = saved(store, contract=4)
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,"
            "created_at,updated_at) VALUES(?,?,'plan',1,'running','prompt',0,0)",
            (str(uuid4()), eid),
        )
    requests = {
        "check_agent_stage": {},
        "check_manifest": {"kind": "plan", "manifest": {}},
        "register_analysis_file": {"filename": "plot.svg"},
        "register_result": {"stage_id": "train"},
        "set_project_execution": {
            "request_id": str(uuid4()),
            "remote_project": "/remote",
            "rsync_up": "up",
            "rsync_down": "down",
        },
        "submit_stage": {"stage_id": "train", "request_id": str(uuid4())},
    }
    for name, arguments in requests.items():
        with pytest.raises(ValueError, match="execution contract 3"):
            await wf.controller.call(eid, name, arguments)
    # Contract-4 job reads are scoped to the live research turn, not every saved phase.
    with pytest.raises(ValueError, match="active research turn"):
        await wf.controller.call(eid, "job_status", {"request_id": str(uuid4())})
