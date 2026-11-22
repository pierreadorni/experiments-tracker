"""Saved steering changes planning or safely replaces an in-flight run."""

import json
from uuid import uuid4

import pytest
from helpers import seed_saved_contract3, wait_for_experiment

from experimenttracker import creation_policy
from experimenttracker.config import ProjectConfig
from experimenttracker.slurm import Observation
from experimenttracker.steering import process_restarts
from experimenttracker.store import Conflict
from experimenttracker.workflow import Workflow
from experimenttracker.workspaces import initialize_project


async def wait_for(workflow, store, eid, predicate):
    await wait_for_experiment(workflow, store, eid, predicate)


class WaitingScheduler:
    def __init__(self):
        self.cancelled = []

    async def submit(self, intent_id, script, root, *, resources=None):
        return "12345"

    async def observe(self, job_id):
        return Observation("CANCELLED" if self.cancelled else "RUNNING", "1:0" if self.cancelled else None)

    async def cancel(self, job_id, intent_id):
        self.cancelled.append((job_id, intent_id))
        return True

    async def reconcile(self, intent_id):
        return "12345"


def test_subscription_reserve_survives_workflow_restart(runtime):
    store, scheduler, agent, old = runtime
    identity = str(uuid4())
    payload = {"five_hour_percent": 65, "weekly_percent": 30}
    assert store.command(identity, "reserve", payload) == store.command(identity, "reserve", payload)
    restored = Workflow(old.config, store, scheduler, agent)
    assert restored.status(None)["quota_reserve"] == payload
    assert restored.gate.config.recovery_remaining == (70, 35)
    assert old.config.quota.five_hour_min_remaining == 25


@pytest.fixture
def blocked_store(runtime):
    store, _, _, _ = runtime
    eid = seed_saved_contract3(store, "Synthetic settled implementation")
    identity = str(uuid4())
    now = store.experiment(eid)["updated_at"]
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,"
            "input,result,attempts,created_at,updated_at) "
            "VALUES(?,?,'prepare',1,'completed','saved-thread','saved-turn','saved input',?,1,?,?)",
            (identity, eid, json.dumps({"outcome": "blocked", "summary": "Missing input"}), now, now),
        )
    store.update(eid, phase="preparing", hold_reason="implementation_blocked", error="Missing input")
    return store, eid, identity


def pending_question(store, eid, *, expired=True):
    pending = {
        "id": "saved-question",
        "rpc_id": 42,
        "expired": expired,
        "method": "item/tool/requestUserInput",
        "params": {"questions": [{"id": "dataset", "question": "Which dataset?"}]},
    }
    store.update(eid, pending_request=pending)
    return pending


@pytest.mark.parametrize("state", ["pending", "dispatching", "running", "uncertain"])
@pytest.mark.parametrize("kind", ["steer", "answer"])
@pytest.mark.usefixtures("contract_three_engine")
def test_feedback_never_resubmits_an_unsettled_turn(blocked_store, state, kind):
    store, eid, identity = blocked_store
    pending_question(store, eid)
    store.db.execute("UPDATE agent_actions SET state=? WHERE id=?", (state, identity))
    payload = (
        {"text": "Use dataset A", "mode": "later"}
        if kind == "steer"
        else {
            "pending_request_id": "saved-question",
            "answer": "Use dataset A",
        }
    )
    before = store.experiment(eid)
    with pytest.raises(Conflict, match="pending or uncertain|settled"):
        store.command(uuid4(), kind, payload, eid=eid)
    assert store.experiment(eid) == before
    assert not store.rows("SELECT * FROM steering WHERE experiment_id=?", (eid,))
    assert len(store.rows("SELECT * FROM agent_actions WHERE experiment_id=?", (eid,))) == 1


@pytest.mark.parametrize("implementation", [True, False])
@pytest.mark.usefixtures("contract_three_engine")
def test_feedback_can_queue_behind_an_unrelated_pending_action(blocked_store, implementation):
    store, eid, identity = blocked_store
    other = str(uuid4())
    if not implementation:
        for stage in ("model_check", "other_check"):
            store.db.execute("INSERT INTO stages(experiment_id,id,updated_at) VALUES(?,?,0)", (eid, stage))
        store.db.execute("UPDATE agent_actions SET phase='agent:model_check' WHERE id=?", (identity,))
        store.update(eid, phase="training", hold_reason="agent_stage_blocked")
    store.db.execute(
        "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,created_at,updated_at) "
        "VALUES(?,?,?,1,'pending','queued work',0,0)",
        (other, eid, "interpret" if implementation else "agent:other_check"),
    )
    store.command(uuid4(), "steer", {"text": "Use dataset A", "mode": "later"}, eid=eid)
    resumed = store.one("SELECT * FROM agent_actions WHERE previous_action_id=?", (identity,))
    assert resumed["state"] == "pending" and "Use dataset A" in resumed["error"]
    assert store.one("SELECT state FROM agent_actions WHERE id=?", (other,))["state"] == "pending"


@pytest.mark.parametrize("kind", ["steer", "answer"])
@pytest.mark.usefixtures("contract_three_engine")
def test_feedback_and_resume_roll_back_together(blocked_store, monkeypatch, kind):
    store, eid, _ = blocked_store
    if kind == "answer":
        pending_question(store, eid)
    tables = ("experiments", "stages", "agent_actions", "steering", "commands", "events", "outbox")
    before = {table: store.rows(f"SELECT * FROM {table} ORDER BY rowid") for table in tables}
    original = store.emit

    def fail_after_resuming(db, eid, event, payload=None):
        if event == "steering_resumed_blocked_work":
            raise RuntimeError("synthetic transaction failure")
        return original(db, eid, event, payload)

    monkeypatch.setattr(store, "emit", fail_after_resuming)
    payload = (
        {"text": "Use dataset A", "mode": "later"}
        if kind == "steer"
        else {
            "pending_request_id": "saved-question",
            "answer": "Use dataset A",
        }
    )
    with pytest.raises(RuntimeError, match="synthetic transaction failure"):
        store.command(uuid4(), kind, payload, eid=eid)
    assert {table: store.rows(f"SELECT * FROM {table} ORDER BY rowid") for table in tables} == before


@pytest.mark.usefixtures("contract_three_engine")
def test_autopilot_steering_resumes_blocked_work_in_its_transaction(blocked_store, runtime, monkeypatch):
    from test_iteration_autopilot import activated

    store, eid, identity = blocked_store
    service, run = activated(runtime, monkeypatch)
    store.db.execute(
        "INSERT INTO autopilot_members VALUES(?,?,?,'question','exploration','test',0,1,0,NULL,'[]',?)",
        (eid, run["id"], run["project_id"], 1000000),
    )
    request = {
        "request_id": str(uuid4()),
        "expected_revision": run["revision"],
        "kind": "control",
        "experiment_id": eid,
        "control": "steer",
        "text": "Use dataset A",
    }
    result = service.command("test-project", request)
    assert service.command("test-project", request) == result
    continuations = store.rows("SELECT * FROM agent_actions WHERE previous_action_id=?", (identity,))
    assert len(continuations) == 1 and "Use dataset A" in continuations[0]["error"]
    assert store.experiment(eid)["hold_reason"] is None
    assert store.one("SELECT state FROM steering WHERE experiment_id=?", (eid,))["state"] == "applied"


@pytest.mark.usefixtures("contract_three_engine")
def test_blocked_answer_uses_the_existing_command_transaction(blocked_store):
    store, eid, identity = blocked_store
    pending_question(store, eid)
    request = str(uuid4())
    with store.tx() as db:
        store.command(
            request,
            "answer",
            {
                "pending_request_id": "saved-question",
                "answer": "Use dataset A",
            },
            eid=eid,
            _db=db,
        )
        continuation = db.execute(
            "SELECT * FROM agent_actions WHERE previous_action_id=?", (identity,)
        ).fetchone()
        assert continuation["state"] == "pending" and "Use dataset A" in continuation["error"]
    assert store.experiment(eid)["pending_request"] is None
    assert store.experiment(eid)["hold_reason"] is None


@pytest.mark.parametrize("invalid", ["stale", "approval", "expired_active"])
def test_answers_keep_identity_permission_and_live_rpc_checks(blocked_store, invalid):
    store, eid, identity = blocked_store
    pending = pending_question(store, eid)
    payload = {"pending_request_id": pending["id"], "answer": "Use dataset A"}
    if invalid == "stale":
        payload["pending_request_id"] = "another-question"
    elif invalid == "approval":
        pending.update(method="item/commandExecution/requestApproval", params={})
        store.update(eid, pending_request=pending)
    else:
        store.update(eid, hold_reason="needs_attention")
        store.db.execute("UPDATE agent_actions SET state='running' WHERE id=?", (identity,))
    with pytest.raises(Conflict):
        store.command(uuid4(), "answer", payload, eid=eid)
    assert store.experiment(eid)["pending_request"] == pending
    assert not store.rows("SELECT * FROM steering WHERE experiment_id=?", (eid,))


@pytest.mark.parametrize("state", ["pending", "cancelling"])
@pytest.mark.usefixtures("contract_three_engine")
def test_manual_retry_never_consumes_restart_steering(blocked_store, state):
    store, eid, _ = blocked_store
    request = str(uuid4())
    store.db.execute(
        "INSERT INTO steering(request_id,experiment_id,text,created_at,mode,state) VALUES(?,?,?,0,'restart',?)",
        (request, eid, "Create a corrected replacement", state),
    )
    with pytest.raises(Conflict, match="pending or uncertain"):
        store.command(uuid4(), "action", {"action": "retry_agent"}, eid=eid)
    assert store.one("SELECT state FROM steering WHERE request_id=?", (request,))["state"] == state
    assert not store.rows("SELECT 1 FROM agent_actions WHERE previous_action_id IS NOT NULL")


@pytest.mark.parametrize("session", [False, True])
@pytest.mark.usefixtures("contract_three_engine")
def test_manual_retry_consumes_later_feedback_for_other_known_failures(blocked_store, session):
    store, eid, identity = blocked_store
    # Guidance was accepted before the terminal failure, as on an older owner.
    store.update(eid, hold_reason=None)
    steering = str(uuid4())
    store.command(steering, "steer", {"text": "Use dataset A", "mode": "later"}, eid=eid)
    error = "session resume rejected: archived" if session else "agent output validation: missing input"
    store.db.execute(
        "UPDATE agent_actions SET state='failed',error=?,turn_id=?,attempts=? WHERE id=?",
        (error, None if session else "saved-turn", 0 if session else 1, identity),
    )
    store.update(
        eid,
        phase="preparing" if session else "failed",
        hold_reason="agent_session_unavailable" if session else "agent_correction_stalled",
    )
    request = str(uuid4())
    result = store.command(request, "action", {"action": "retry_agent"}, eid=eid)
    assert store.command(request, "action", {"action": "retry_agent"}, eid=eid) == result
    resumed = store.one("SELECT * FROM agent_actions WHERE id=?", (identity if session else request,))
    assert resumed["state"] == "pending" and "Use dataset A" in resumed["error"]
    if not session:
        assert resumed["previous_action_id"] == identity and error in resumed["error"]
    assert store.one("SELECT state FROM steering WHERE request_id=?", (steering,))["state"] == "applied"


@pytest.mark.usefixtures("contract_three_engine")
def test_restart_admission_preserves_legacy_but_refuses_incompatible_candidate(runtime, monkeypatch):
    store, _, _, _ = runtime
    eid = seed_saved_contract3(store)
    request, payload = str(uuid4()), {"text": "Replace this run", "mode": "restart"}

    monkeypatch.setattr(creation_policy, "MODE", "legacy")
    result = store.command(request, "steer", payload, eid=eid)
    assert result["state"] == "applied"
    assert store.experiment(eid)["pause_compute"] is True

    # An independent experiment proves the same fresh request is rejected before
    # its command receipt, steering row, or compute pause can be written.
    other = seed_saved_contract3(store)
    monkeypatch.setattr(creation_policy, "MODE", "candidate")
    with pytest.raises(Conflict, match="writable Git project checkout"):
        store.command(uuid4(), "steer", payload, eid=other)
    assert store.experiment(other)["pause_compute"] is False
    assert not store.rows("SELECT * FROM steering WHERE experiment_id=?", (other,))


@pytest.mark.parametrize(
    ("workspace_mode", "reference"),
    [("shared", False), ("auto", True)],
)
@pytest.mark.usefixtures("contract_three_engine")
def test_candidate_restart_refuses_shared_or_reference_replacement(
    runtime, config, tmp_path, monkeypatch, workspace_mode, reference
):
    store, _, _, _ = runtime
    checkout = tmp_path / "compatible-project"
    checkout.mkdir()
    initialize_project(checkout)
    project = ProjectConfig(id="test-project", name="Test project", checkout=checkout, research_loop=True)
    config.projects = [project]
    store.projects[project.id] = project
    eid = seed_saved_contract3(store)
    exp = store.experiment(eid)
    contract = exp["contract"]
    contract["workspace_mode"] = workspace_mode
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (json.dumps(contract), eid))
    if reference:
        reference_id = seed_saved_contract3(store)
        store.update(reference_id, phase="complete", snapshot={"plan": {"schema_version": 3}})
        store.db.execute("UPDATE experiments SET reference_experiment_id=? WHERE id=?", (reference_id, eid))
    monkeypatch.setattr(creation_policy, "MODE", "candidate")

    message = "does not copy a reference" if reference else "own named branch"
    with pytest.raises(Conflict, match=message):
        store.command(uuid4(), "steer", {"text": "Replace this run", "mode": "restart"}, eid=eid)
    assert not store.rows("SELECT * FROM steering WHERE experiment_id=?", (eid,))
    assert store.experiment(eid)["pause_compute"] is False


@pytest.mark.asyncio
@pytest.mark.usefixtures("contract_three_engine")
async def test_delayed_restart_does_not_cancel_when_replacement_became_incompatible(runtime, monkeypatch):
    store, scheduler, _, workflow = runtime
    eid = seed_saved_contract3(store)
    store.update(eid, snapshot={"plan": {"schema_version": 3, "stages": []}})
    with store.tx() as db:
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,state,job_id,script_hash,resources,snapshot_hash,created_at) "
            "VALUES('saved-intent',?,'prepare','submitted','saved-job','hash','{}','snapshot',0)",
            (eid,),
        )
    cancelled = []

    async def cancel(job_id, intent_id):
        cancelled.append((job_id, intent_id))

    scheduler.cancel = cancel
    request = str(uuid4())
    monkeypatch.setattr(creation_policy, "MODE", "legacy")
    store.command(request, "steer", {"text": "Replace this run", "mode": "restart"}, eid=eid)
    monkeypatch.setattr(creation_policy, "MODE", "candidate")

    await process_restarts(workflow)

    assert cancelled == []
    assert store.experiment(eid)["phase"] != "cancelled"
    assert store.one("SELECT state FROM steering WHERE request_id=?", (request,))["state"] == "pending"
    assert not store.rows("SELECT * FROM experiments WHERE replaces_experiment_id=?", (eid,))
    assert not store.rows(
        "SELECT 1 FROM events WHERE experiment_id=? AND kind='job_cancellation_intent'", (eid,)
    )


@pytest.mark.asyncio
@pytest.mark.usefixtures("contract_three_engine")
async def test_candidate_restart_creates_eligible_contract_four_replacement(
    runtime, config, tmp_path, monkeypatch
):
    store, _, _, workflow = runtime
    checkout = tmp_path / "restart-project"
    checkout.mkdir()
    initialize_project(checkout)
    project = ProjectConfig(id="test-project", name="Test project", checkout=checkout, research_loop=True)
    config.projects = [project]
    store.projects[project.id] = project
    eid = seed_saved_contract3(store)
    store.update(eid, snapshot={"plan": {"schema_version": 3, "stages": []}})
    request = str(uuid4())
    monkeypatch.setattr(creation_policy, "MODE", "candidate")
    store.command(request, "steer", {"text": "Use a separate branch", "mode": "restart"}, eid=eid)

    await workflow.tick()

    steering = store.one("SELECT * FROM steering WHERE request_id=?", (request,))
    replacement = store.experiment(steering["replacement_experiment_id"])
    assert replacement["contract"]["execution_contract"] == 4
    assert replacement["replaces_experiment_id"] == eid
    assert steering["state"] == "applied"
