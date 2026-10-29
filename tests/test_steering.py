"""Saved steering changes planning or safely replaces an in-flight run."""

import asyncio
import json
from uuid import uuid4

import pytest
from test_generic import ImplementationAgent, approve_plan

from experimenttracker.codex_client import DemoAgent
from experimenttracker.models import SteerCommand
from experimenttracker.slurm import Observation
from experimenttracker.store import Conflict
from experimenttracker.workflow import Workflow


async def wait_for(workflow, store, eid, predicate):
    deadline = asyncio.get_running_loop().time() + 10
    while asyncio.get_running_loop().time() < deadline:
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.01)
        if predicate(store.experiment(eid)):
            return
    raise AssertionError(store.experiment(eid))


async def test_plan_is_reviewed_before_source_is_written(runtime):
    store, scheduler, _, old = runtime
    agent = ImplementationAgent()
    workflow = Workflow(old.config, store, scheduler, agent)
    eid = store.command(
        uuid4(),
        "create",
        {"brief": "Test an entirely new method and show its frontier.", "recipe_id": "agent_experiment_v1"},
    )["experiment_id"]
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
    workflow = Workflow(old.config, store, scheduler, ImplementationAgent())
    eid = store.command(
        uuid4(),
        "create",
        {"brief": "Test an entirely new method and show its frontier.", "recipe_id": "agent_experiment_v1"},
    )["experiment_id"]
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
    assert store.experiment(eid)["plan_review"]["feedback"] == text
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
    workflow = Workflow(old.config, store, scheduler, ImplementationAgent())
    eid = store.command(
        uuid4(),
        "create",
        {"brief": "Test an entirely new method and show its frontier.", "recipe_id": "agent_experiment_v1"},
    )["experiment_id"]
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

    class AmendedAgent(ImplementationAgent):
        async def begin(self, thread, prompt, schema, *, workspace=None):
            turn = await super().begin(thread, prompt, schema, workspace=workspace)
            if json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])["phase"] == "prepare":
                result = {
                    **self.results[(thread, turn)],
                    "protocol": "An amended scientific protocol.",
                    "objective_change": "Change the scientific objective rather than only its implementation.",
                }
                self.results[(thread, turn)] = result
            return turn

    workflow = Workflow(old.config, store, scheduler, AmendedAgent())
    eid = store.command(
        uuid4(),
        "create",
        {"brief": "Test an entirely new method and show its frontier.", "recipe_id": "agent_experiment_v1"},
    )["experiment_id"]
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
    workflow = Workflow(old.config, store, scheduler, ImplementationAgent())
    eid = store.command(
        uuid4(),
        "create",
        {"brief": "Test an entirely new method and show its frontier.", "recipe_id": "agent_experiment_v1"},
    )["experiment_id"]
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
    await workflow.close()


async def test_message_arriving_during_interpretation_is_queued_for_discussion(runtime):
    store, scheduler, _, old = runtime
    steering_id = str(uuid4())

    class LateAgent(ImplementationAgent):
        async def begin(self, thread, prompt, schema, *, workspace=None):
            payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
            if payload["phase"] == "discuss":
                return await DemoAgent.begin(self, thread, prompt, schema, workspace=workspace)
            turn = await super().begin(thread, prompt, schema, workspace=workspace)
            if payload["phase"] == "interpret":
                store.command(
                    steering_id,
                    "steer",
                    {"text": "Also update the report with a metric plot.", "mode": "later"},
                    eid=payload["experiment_id"],
                )
            return turn

    workflow = Workflow(old.config, store, scheduler, LateAgent())
    eid = store.command(
        uuid4(),
        "create",
        {"brief": "Test an entirely new method and show its frontier.", "recipe_id": "agent_experiment_v1"},
    )["experiment_id"]
    await approve_plan(workflow, store, eid)
    await wait_for(workflow, store, eid, lambda exp: exp["phase"] == "complete")
    saved = store.one("SELECT * FROM steering WHERE request_id=?", (steering_id,))
    assert saved["state"] == "discussion_queued"
    assert store.one("SELECT text FROM discussions WHERE experiment_id=?", (eid,))["text"] == saved["text"]
    assert scheduler.submissions == 2
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

    class QuestionAgent(ImplementationAgent):
        async def begin(self, thread_id, prompt, schema, *, workspace=None):
            payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
            turn = await super().begin(thread_id, prompt, schema, workspace=workspace)
            if payload["phase"] == "plan" and not payload.get("plan_revision_feedback"):
                self.results[(thread_id, turn)] = {
                    "title": "Clarify the dataset",
                    "summary": "The dataset is missing.",
                    "protocol": "Dataset selection is needed before choosing compute.",
                    "questions": ["Which dataset and split should be used?"],
                    "stages": [],
                }
            elif payload["phase"] == "plan":
                assert payload["plan_revision_feedback"] == "Use dataset A, official split."
                assert payload["previous_plan"]["questions"] == ["Which dataset and split should be used?"]
            return turn

    workflow = Workflow(old.config, store, scheduler, QuestionAgent())
    eid = store.command(
        uuid4(),
        "create",
        {
            "brief": "Test an entirely new method and show its frontier.",
            "recipe_id": "agent_experiment_v1",
        },
    )["experiment_id"]
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
