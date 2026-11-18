"""C10 archive classification keeps execution contracts 3 and 4 live (T4.1c)."""

from uuid import uuid4

import pytest

from experimenttracker import legacy_archive
from experimenttracker.core import action_obligations
from experimenttracker.store import encode


def saved_experiment(store, contract, *, phase="training"):
    """Create one saved fixture, then hand-edit the contract the way history did."""
    eid = store.command(
        uuid4(), "create", {"brief": "Synthetic archive boundary.", "recipe_id": "agent_experiment_v1"}
    )["experiment_id"]
    value = {
        key: item for key, item in store.experiment(eid)["contract"].items() if key != "execution_contract"
    }
    if contract is not None:
        value["execution_contract"] = contract
    with store.tx() as db:
        db.execute(
            "UPDATE experiments SET contract=?,phase=?,pending_request=? WHERE id=?",
            (encode(value), phase, encode({"identity": "saved-rpc"}), eid),
        )
    return eid


def save_contract(store, eid, contract):
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))


def pending_action(store, eid, phase="research"):
    identity = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,created_at,updated_at) "
            "VALUES(?,?,?,1,'pending','saved prompt',1,1)",
            (identity, eid, phase),
        )
        store.emit(db, eid, "agent_action_changed", {"action_id": identity, "state": "pending"})
    return identity


def sync_action(store, action_id):
    with store.tx() as db:
        return action_obligations.sync_action(db, action_id)


@pytest.mark.parametrize(
    ("contract", "frozen", "active"),
    [
        ({}, True, True),
        ({"execution_contract": None}, True, True),
        ({"execution_contract": 1}, True, True),
        ({"execution_contract": 2}, True, True),
        ({"execution_contract": 3}, False, False),
        ({"execution_contract": 4}, False, False),
        ({"execution_contract": 5}, True, True),
    ],
)
def test_execution_frozen_keeps_contract_four_live(contract, frozen, active):
    exp = {"contract": contract, "phase": "training", "deleted_at": None}
    assert legacy_archive.execution_frozen(exp) is frozen
    assert legacy_archive.archive_active(exp) is active


def test_startup_sweep_classifies_supported_and_unsupported_saved_contracts(runtime):
    store, *_ = runtime
    live = [saved_experiment(store, contract) for contract in (3, 4)]
    explicit_null = saved_experiment(store, 5)
    save_contract(
        store, explicit_null, {**store.experiment(explicit_null)["contract"], "execution_contract": None}
    )
    archived = [
        saved_experiment(store, None),
        explicit_null,
        saved_experiment(store, 1),
        saved_experiment(store, 2),
        saved_experiment(store, 5),
    ]

    store.archive_legacy_experiments()

    for eid in live:
        exp = store.experiment(eid)
        assert (exp["phase"], exp["hold_reason"], exp["pending_request"]) == (
            "training",
            None,
            {"identity": "saved-rpc"},
        )
    for eid in archived:
        exp = store.experiment(eid)
        assert exp["hold_reason"] == "legacy_contract_archived"
        assert exp["pending_request"] == {"identity": "saved-rpc"}
    assert len(store.rows("SELECT * FROM inbox WHERE kind='required_decision'")) == len(archived)

    revisions = {eid: store.experiment(eid)["revision"] for eid in live + archived}
    store.archive_legacy_experiments()
    assert {eid: store.experiment(eid)["revision"] for eid in live + archived} == revisions


def test_pending_research_action_on_contract_four_keeps_admission(runtime):
    store, *_ = runtime
    eid = saved_experiment(store, 4)
    action = pending_action(store, eid)
    stages = store.rows("SELECT id,state FROM stages WHERE experiment_id=? ORDER BY id", (eid,))

    obligation = sync_action(store, action)

    assert (
        obligation["id"],
        obligation["state"],
        obligation["owner"],
        obligation["kind"],
        obligation["wait"],
        obligation["target"],
    ) == (f"agent-action:{action}:0", "open", "backend", "agent_turn", "admission", action)
    assert store.one("SELECT state FROM agent_actions WHERE id=?", (action,))["state"] == "pending"
    assert store.rows("SELECT id,state FROM stages WHERE experiment_id=? ORDER BY id", (eid,)) == stages


def test_archived_pending_action_abandons_its_admission_obligation(runtime):
    store, *_ = runtime
    eid = saved_experiment(store, 4)
    action = pending_action(store, eid)
    assert sync_action(store, action)["state"] == "open"

    save_contract(store, eid, {**store.experiment(eid)["contract"], "execution_contract": 2})
    settled = sync_action(store, action)

    assert (settled["state"], settled["owner"], settled["wait"]) == ("abandoned", "backend", "admission")
    assert store.one("SELECT state FROM agent_actions WHERE id=?", (action,))["state"] == "pending"


def test_archived_discussion_action_keeps_its_admission_obligation(runtime):
    store, *_ = runtime
    eid = saved_experiment(store, None)
    action = pending_action(store, eid, phase="discuss:1")

    obligation = sync_action(store, action)

    assert (obligation["state"], obligation["owner"], obligation["wait"]) == (
        "open",
        "backend",
        "admission",
    )


@pytest.mark.parametrize("contract", [3, 4])
@pytest.mark.parametrize("phase", ["discuss:question", "integration:request"])
def test_shared_pending_actions_keep_admission_on_supported_contracts(runtime, contract, phase):
    store, *_ = runtime
    eid = saved_experiment(store, contract)
    action = pending_action(store, eid, phase=phase)

    obligation = sync_action(store, action)

    assert (obligation["state"], obligation["owner"], obligation["kind"], obligation["wait"]) == (
        "open",
        "backend",
        "agent_turn",
        "admission",
    )


@pytest.mark.parametrize(
    "phase", ["plan", "prepare", "interpret", "publish", "title", "monitor", "agent:train"]
)
def test_contract_three_only_pending_stage_action_is_not_admitted_on_contract_four(runtime, phase):
    store, *_ = runtime
    eid = saved_experiment(store, 4)
    action = pending_action(store, eid, phase=phase)
    prefix = f"agent-action:{action}:"

    assert sync_action(store, action) is None
    assert (
        store.one("SELECT COUNT(*) AS n FROM obligations WHERE substr(id,1,?)=?", (len(prefix), prefix))["n"]
        == 0
    )
    assert store.one("SELECT state FROM agent_actions WHERE id=?", (action,))["state"] == "pending"


def add_artifact(store, eid, name, evidence_revision):
    with store.tx() as db:
        db.execute(
            "INSERT INTO artifacts(id,experiment_id,stage_id,path,name,sha256,size,media_type,"
            "evidence_revision,local_valid) VALUES(?,?,?,?,?,?,?,?,?,1)",
            (
                str(uuid4()),
                eid,
                "train",
                f"/saved/{name}",
                name,
                "a" * 64,
                1,
                "application/json",
                evidence_revision,
            ),
        )


def add_job(store, eid, attempt, commit):
    with store.tx() as db:
        db.execute(
            "INSERT OR IGNORE INTO stages(experiment_id,id,state,attempt,updated_at) "
            "VALUES(?,'train','complete',?,1.0)",
            (eid, attempt),
        )
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,script_hash,resources,"
            "snapshot_hash,created_at,execution_manifest) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                str(uuid4()),
                eid,
                "train",
                attempt,
                "complete",
                "hash",
                "{}",
                "snapshot",
                1.0,
                encode({"commit": commit}),
            ),
        )


def test_projection_keeps_contract_four_out_of_archived_evidence_layout(runtime):
    store, *_ = runtime
    four = saved_experiment(store, 4)
    archived = saved_experiment(store, 2)
    for eid in (four, archived):
        save_contract(store, eid, {**store.experiment(eid)["contract"], "auto_repair": {"revision": 2}})
        add_artifact(store, eid, "current.json", 2)
        add_artifact(store, eid, "retired.json", 0)
        add_job(store, eid, 1, "1" * 40)
        add_job(store, eid, 2, "d" * 40)
        with store.tx() as db:
            db.execute("UPDATE experiments SET evidence_revision=2 WHERE id=?", (eid,))

    four_view = store.projection(four)
    archived_view = store.projection(archived)

    assert {artifact["name"] for artifact in four_view["artifacts"]} == {"current.json", "retired.json"}
    assert {artifact["name"] for artifact in archived_view["artifacts"]} == {"current.json"}
    assert sorted(job["attempt"] for job in four_view["jobs"]) == [1, 2]
    assert sorted(job["attempt"] for job in archived_view["jobs"]) == [2]
    assert next(job for job in four_view["jobs"] if job["attempt"] == 2)["commit"] == "d" * 40
    assert "commit" not in archived_view["jobs"][0]
