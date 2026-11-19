"""Saved steering changes planning or safely replaces an in-flight run."""

import json
from uuid import uuid4

import pytest
from helpers import IterationAgent, approve_plan, seed_saved_contract3, wait_for_experiment

from experimenttracker.codex_client import DemoAgent
from experimenttracker.models import SteerCommand
from experimenttracker.slurm import Observation
from experimenttracker.store import Conflict
from experimenttracker.workflow import Workflow


async def wait_for(workflow, store, eid, predicate):
    await wait_for_experiment(workflow, store, eid, predicate)


async def test_plan_is_reviewed_before_source_is_written(runtime):
    store, scheduler, _, old = runtime
    workflow = Workflow(old.config, store, scheduler, IterationAgent())
    eid = seed_saved_contract3(store, "Test an entirely new method and show its frontier.")
    await wait_for(
        workflow, store, eid, lambda exp: (exp["plan_review"] or {}).get("state") == "awaiting_review"
    )
    assert store.experiment(eid)["snapshot"] is None
    assert not (workflow.root(eid) / "work/recipe.py").exists()
    assert scheduler.submissions == 0
    store.command(
        uuid4(),
        "plan_review",
        {"decision": "revise", "plan_revision": 1, "feedback": "Use scale 1 as the primary baseline."},
        eid=eid,
    )
    await wait_for(
        workflow,
        store,
        eid,
        lambda exp: (
            (exp["plan_review"] or {}).get("state") == "awaiting_review"
            and exp["plan_review"]["revision"] == 2
        ),
    )
    assert store.experiment(eid)["snapshot"] is None
    assert scheduler.submissions == 0
    await workflow.close()


async def test_long_steering_survives_planning_revision(runtime):
    store, scheduler, _, old = runtime
    workflow = Workflow(old.config, store, scheduler, IterationAgent())
    eid = seed_saved_contract3(store, "Test an entirely new method and show its frontier.")
    await wait_for(
        workflow, store, eid, lambda exp: (exp["plan_review"] or {}).get("state") == "awaiting_review"
    )
    text = "Beginning of context\n" + "x" * 49958 + "\nEnd of context here."
    assert len(text) == 50000
    command = SteerCommand(request_id=uuid4(), text=text)
    with pytest.raises(ValueError):
        SteerCommand(request_id=uuid4(), text=text + "x")
    store.command(command.request_id, "steer", command.model_dump(mode="json"), eid=eid)
    await wait_for(
        workflow,
        store,
        eid,
        lambda exp: (exp["plan_review"] or {}).get("revision") == 2,
    )
    revised = store.one(
        "SELECT input FROM agent_actions WHERE experiment_id=? AND phase='plan' AND evidence_revision=2",
        (eid,),
    )
    assert json.loads(revised["input"].split("\nPAYLOAD:\n", 1)[1])["plan_revision_feedback"] == text
    assert (
        store.one("SELECT text FROM steering WHERE request_id=?", (str(command.request_id),))["text"] == text
    )
    await workflow.close()


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


async def test_restart_cancels_only_saved_job_and_creates_one_replacement(runtime):
    store, _, _, old = runtime
    scheduler = WaitingScheduler()
    workflow = Workflow(old.config, store, scheduler, IterationAgent())
    eid = seed_saved_contract3(store, "Test an entirely new method and show its frontier.")
    await approve_plan(workflow, store, eid)
    await wait_for(
        workflow,
        store,
        eid,
        lambda exp: bool(store.one("SELECT 1 FROM jobs WHERE experiment_id=? AND state='submitted'", (eid,))),
    )
    job = store.one("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
    store.command(uuid4(), "steer", {"text": "Also add a per-class plot.", "mode": "later"}, eid=eid)
    request_id = str(uuid4())
    payload = {"text": "Use scale 1 rather than scale 2 for the main run.", "mode": "restart"}
    assert store.command(request_id, "steer", payload, eid=eid) == store.command(
        request_id, "steer", payload, eid=eid
    )
    with pytest.raises(Conflict, match="restart is already pending"):
        store.command(uuid4(), "steer", payload, eid=eid)
    await wait_for(workflow, store, eid, lambda exp: exp["phase"] == "cancelled")
    steering = store.one("SELECT * FROM steering WHERE request_id=?", (request_id,))
    child = store.experiment(steering["replacement_experiment_id"])
    assert scheduler.cancelled == [(job["job_id"], job["intent_id"])]
    assert child["replaces_experiment_id"] == eid
    assert "scale 1" in child["brief"]
    assert "Also add a per-class plot." in child["contract"]["restart_context"]["guidance"]
    assert len(store.rows("SELECT id FROM experiments WHERE replaces_experiment_id=?", (eid,))) == 1
    await workflow.tick(force_observe=True)
    assert len(store.rows("SELECT id FROM experiments WHERE replaces_experiment_id=?", (eid,))) == 1
    await workflow.close()


async def test_implementation_drift_requires_a_second_approval(runtime):
    store, scheduler, _, old = runtime

    class AmendedAgent(IterationAgent):
        async def begin(self, thread, prompt, schema, *, workspace=None, **kwargs):
            turn = await super().begin(thread, prompt, schema, workspace=workspace, **kwargs)
            if json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])["phase"] == "prepare":
                result = {
                    **self.results[(thread, turn)],
                    "protocol": "An amended scientific protocol.",
                    "objective_change": "Change the scientific objective rather than only its implementation.",
                }
                self.results[(thread, turn)] = result
            return turn

    workflow = Workflow(old.config, store, scheduler, AmendedAgent())
    eid = seed_saved_contract3(store, "Test an entirely new method and show its frontier.")
    await approve_plan(workflow, store, eid)
    await wait_for(
        workflow, store, eid, lambda exp: (exp["plan_review"] or {}).get("scope") == "implementation"
    )
    review = store.experiment(eid)["plan_review"]
    assert review["state"] == "awaiting_review" and review["revision"] == 2
    assert scheduler.submissions == 0 and store.experiment(eid)["snapshot"] is None
    store.command(uuid4(), "plan_review", {"decision": "approve", "plan_revision": 2}, eid=eid)
    await wait_for(workflow, store, eid, lambda exp: bool(exp["snapshot"]))
    assert store.experiment(eid)["snapshot"]["plan"]["protocol"] == review["protocol"]
    assert scheduler.submissions == 0
    await workflow.close()


async def test_failed_run_restart_reconciles_a_job_that_was_last_seen_running(runtime):
    store, _, _, old = runtime
    scheduler = WaitingScheduler()
    workflow = Workflow(old.config, store, scheduler, IterationAgent())
    eid = seed_saved_contract3(store, "Test an entirely new method and show its frontier.")
    await approve_plan(workflow, store, eid)
    await wait_for(
        workflow,
        store,
        eid,
        lambda e: bool(store.one("SELECT 1 FROM jobs WHERE experiment_id=? AND state='submitted'", (eid,))),
    )
    with store.tx() as db:
        db.execute("UPDATE jobs SET state='failed',scheduler_state='RUNNING' WHERE experiment_id=?", (eid,))
    store.update(eid, phase="failed", error="Artifact validation failed")
    with pytest.raises(Conflict, match="restart of a failed run"):
        store.command(uuid4(), "steer", {"text": "Add a plot", "mode": "later"}, eid=eid)
    identity = str(uuid4())
    store.command(identity, "steer", {"text": "Use scale 1", "mode": "restart"}, eid=eid)
    await wait_for(workflow, store, eid, lambda e: e["phase"] == "cancelled")
    child_id = store.one("SELECT replacement_experiment_id FROM steering WHERE request_id=?", (identity,))[
        "replacement_experiment_id"
    ]
    assert scheduler.cancelled and len(scheduler.cancelled) == 1
    assert store.experiment(child_id)["replaces_experiment_id"] == eid
    assert (
        store.one("SELECT scheduler_state FROM jobs WHERE experiment_id=?", (eid,))["scheduler_state"]
        == "CANCELLED"
    )
    saved_job = store.one("SELECT intent_id,job_id FROM jobs WHERE experiment_id=?", (eid,))
    obligation = store.one(
        "SELECT state,owner,wait FROM obligations WHERE id LIKE ? ORDER BY id DESC LIMIT 1",
        (f"job:{saved_job['intent_id']}:%",),
    )
    assert obligation == {
        "state": "abandoned",
        "owner": "external",
        "wait": f"job:{saved_job['job_id']}",
    }
    await workflow.close()


async def test_message_arriving_during_interpretation_is_queued_for_discussion(runtime):
    store, scheduler, _, old = runtime
    steering_id = str(uuid4())

    class LateAgent(IterationAgent):
        async def begin(self, thread, prompt, schema, *, workspace=None, **kwargs):
            payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
            if payload["phase"] == "discuss":
                return await DemoAgent.begin(self, thread, prompt, schema, workspace=workspace, **kwargs)
            turn = await super().begin(thread, prompt, schema, workspace=workspace, **kwargs)
            if payload["phase"] == "interpret":
                store.command(
                    steering_id,
                    "steer",
                    {"text": "Also update the report with a metric plot.", "mode": "later"},
                    eid=payload["experiment_id"],
                )
            return turn

    workflow = Workflow(old.config, store, scheduler, LateAgent())
    eid = seed_saved_contract3(store, "Test an entirely new method and show its frontier.")
    await approve_plan(workflow, store, eid)
    await wait_for(workflow, store, eid, lambda exp: exp["phase"] == "complete")
    saved = store.one("SELECT * FROM steering WHERE request_id=?", (steering_id,))
    assert saved["state"] == "discussion_queued"
    assert store.one("SELECT text FROM discussions WHERE experiment_id=?", (eid,))["text"] == saved["text"]
    assert scheduler.submissions == 3
    await workflow.close()


def test_subscription_reserve_survives_workflow_restart(runtime):
    store, scheduler, agent, old = runtime
    identity = str(uuid4())
    payload = {"five_hour_percent": 65, "weekly_percent": 30}
    assert store.command(identity, "reserve", payload) == store.command(identity, "reserve", payload)
    restored = Workflow(old.config, store, scheduler, agent)
    assert restored.status(None)["quota_reserve"] == payload
    assert restored.gate.config.recovery_remaining == (70, 35)
    assert old.config.quota.five_hour_min_remaining == 25


async def test_planning_clarification_waits_for_durable_answer(runtime):
    store, scheduler, _, old = runtime

    class QuestionAgent(IterationAgent):
        async def begin(self, thread_id, prompt, schema, *, workspace=None, **kwargs):
            payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
            turn = await super().begin(thread_id, prompt, schema, workspace=workspace, **kwargs)
            if payload["phase"] == "plan" and not payload.get("plan_revision_feedback"):
                self.results[(thread_id, turn)] = {
                    "schema_version": 3,
                    "title": "Clarify the dataset",
                    "summary": "The dataset is missing.",
                    "protocol": "Dataset selection is needed before choosing compute.",
                    "questions": ["Which dataset and split should be used?"],
                    "stages": [],
                }
            elif payload["phase"] == "plan":
                assert payload["plan_revision_feedback"] == "Use dataset A, official split."
                assert payload["approved_plan"]["questions"] == ["Which dataset and split should be used?"]
            return turn

    workflow = Workflow(old.config, store, scheduler, QuestionAgent())
    eid = seed_saved_contract3(store, "Test an entirely new method and show its frontier.")
    await wait_for(workflow, store, eid, lambda exp: bool((exp["plan_review"] or {}).get("questions")))
    review = store.experiment(eid)["plan_review"]
    assert review["state"] == "awaiting_review"
    assert scheduler.submissions == 0
    with pytest.raises(Conflict, match="answer the planning questions"):
        store.command(uuid4(), "plan_review", {"decision": "approve", "plan_revision": 1}, eid=eid)
    for _ in range(3):
        await workflow.tick()
    assert store.experiment(eid)["snapshot"] is None
    await workflow.close()
    workflow = Workflow(old.config, store, scheduler, QuestionAgent())
    await workflow.tick()
    assert store.experiment(eid)["plan_review"]["questions"] == review["questions"]
    assert len(store.rows("SELECT id FROM agent_actions WHERE experiment_id=? AND phase='plan'", (eid,))) == 1
    request = str(uuid4())
    answer = {"decision": "revise", "plan_revision": 1, "feedback": "Use dataset A, official split."}
    first = store.command(request, "plan_review", answer, eid=eid)
    assert store.command(request, "plan_review", answer, eid=eid) == first
    await wait_for(
        workflow,
        store,
        eid,
        lambda exp: (
            (exp["plan_review"] or {}).get("revision") == 2
            and exp["plan_review"]["state"] == "awaiting_review"
        ),
    )
    assert not store.experiment(eid)["plan_review"].get("questions")
    assert scheduler.submissions == 0
    await workflow.close()


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
