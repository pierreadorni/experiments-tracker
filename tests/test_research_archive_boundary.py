"""C10 archive classification keeps execution contracts 3 and 4 live (T4.1c)."""

from uuid import uuid4

import pytest
from helpers import seed_saved_contract3

from experimenttracker import iteration, legacy_archive
from experimenttracker.core import action_obligations
from experimenttracker.store import Conflict, encode


def saved_experiment(store, contract, *, phase="training"):
    """Create one saved fixture, then hand-edit the contract the way history did."""
    eid = seed_saved_contract3(store, "Synthetic archive boundary.")
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
        ({"execution_contract": 4}, False, False),
        ({"execution_contract": 5}, True, True),
    ],
)
def test_execution_frozen_keeps_contract_four_live(contract, frozen, active):
    exp = {"contract": contract, "phase": "training", "deleted_at": None}
    assert legacy_archive.execution_frozen(exp) is frozen
    assert legacy_archive.archive_active(exp) is active


@pytest.mark.parametrize("archive_three", [False, True])
def test_startup_sweep_classifies_supported_and_unsupported_saved_contracts(
    runtime, monkeypatch, archive_three
):
    store, *_ = runtime
    supported = (4,) if archive_three else (3, 4)
    monkeypatch.setattr(legacy_archive, "SUPPORTED_EXECUTION_CONTRACTS", supported)
    live = [saved_experiment(store, contract) for contract in supported]
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
    if archive_three:
        archived.append(saved_experiment(store, 3))

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


@pytest.mark.parametrize(
    ("contract", "phase", "admitted"),
    [
        (3, "discuss:question", True),
        (3, "integration:request", False),
        (4, "discuss:question", True),
        (4, "integration:request", True),
    ],
)
def test_archive_preserves_pending_discussions_and_contract_four_integration(
    runtime, monkeypatch, contract, phase, admitted
):
    store, *_ = runtime
    eid = saved_experiment(store, contract, phase="complete")
    action = pending_action(store, eid, phase=phase)
    monkeypatch.setattr(legacy_archive, "SUPPORTED_EXECUTION_CONTRACTS", (4,))

    obligation = sync_action(store, action)

    if not admitted:
        assert obligation is None
        return
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


async def test_frozen_contract_three_keeps_stage_writers_inert(runtime, monkeypatch):
    store, _, _, workflow = runtime
    eid = saved_experiment(store, 3, phase="preparing")
    with store.tx() as db:
        db.execute("UPDATE experiments SET pending_request=NULL WHERE id=?", (eid,))
    identity = pending_action(store, eid, "prepare")
    action = store.one("SELECT * FROM agent_actions WHERE id=?", (identity,))
    before = store.experiment(eid)
    stages = store.rows("SELECT * FROM stages WHERE experiment_id=?", (eid,))
    monkeypatch.setattr(legacy_archive, "SUPPORTED_EXECUTION_CONTRACTS", (4,))

    assert iteration.enabled(before)
    assert not iteration.runnable(before)
    await workflow.step_control_advance()
    await workflow.dispatch(action, workflow.agent_pool.primary)

    assert store.experiment(eid) == before
    assert store.rows("SELECT * FROM stages WHERE experiment_id=?", (eid,)) == stages
    assert store.one("SELECT * FROM agent_actions WHERE id=?", (identity,)) == action


def test_frozen_terminal_contract_three_refuses_stage_repair(runtime, monkeypatch):
    store, *_ = runtime
    eid = saved_experiment(store, 3, phase="failed")
    monkeypatch.setattr(legacy_archive, "SUPPORTED_EXECUTION_CONTRACTS", (4,))
    with pytest.raises(Conflict, match="legacy execution is archived"):
        store.command(str(uuid4()), "action", {"action": "repair_implementation"}, eid=eid)


def test_frozen_contract_three_projects_only_the_exact_pending_discussion_action(runtime, monkeypatch):
    store, *_ = runtime
    eid = saved_experiment(store, 3, phase="complete")
    active = pending_action(store, eid, "discuss:active")
    pending_action(store, eid, "discuss:other")
    with store.tx() as db:
        db.execute("UPDATE agent_actions SET state='running' WHERE id=?", (active,))
        db.execute(
            "UPDATE experiments SET pending_request=? WHERE id=?",
            (encode({"id": "current", "action_id": active}), eid),
        )
    monkeypatch.setattr(legacy_archive, "SUPPORTED_EXECUTION_CONTRACTS", (4,))

    assert store.projection(eid)["agent_actions"] == [
        {"id": active, "phase": "discuss:active", "state": "running", "created_at": 1, "updated_at": 1}
    ]


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


@pytest.mark.parametrize("contract", [3, 4])
def test_projection_preserves_revisions_for_contract_three_and_four(runtime, monkeypatch, contract):
    store, *_ = runtime
    retained = saved_experiment(store, contract)
    archived = saved_experiment(store, 2)
    for eid in (retained, archived):
        save_contract(store, eid, {**store.experiment(eid)["contract"], "auto_repair": {"revision": 2}})
        add_artifact(store, eid, "current.json", 2)
        add_artifact(store, eid, "retired.json", 0)
        add_job(store, eid, 1, "1" * 40)
        add_job(store, eid, 2, "d" * 40)
        with store.tx() as db:
            db.execute("UPDATE experiments SET evidence_revision=2 WHERE id=?", (eid,))

    monkeypatch.setattr(legacy_archive, "SUPPORTED_EXECUTION_CONTRACTS", (4,))
    assert legacy_archive.execution_frozen(store.experiment(retained)) is (contract == 3)
    retained_view = store.projection(retained)
    archived_view = store.projection(archived)

    assert {artifact["name"] for artifact in retained_view["artifacts"]} == {"current.json", "retired.json"}
    assert {artifact["name"] for artifact in archived_view["artifacts"]} == {"current.json"}
    assert sorted(job["attempt"] for job in retained_view["jobs"]) == [1, 2]
    assert sorted(job["attempt"] for job in archived_view["jobs"]) == [2]
    assert next(job for job in retained_view["jobs"] if job["attempt"] == 2)["commit"] == "d" * 40
    assert "commit" not in archived_view["jobs"][0]
