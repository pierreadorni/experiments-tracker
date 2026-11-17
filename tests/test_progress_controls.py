import json
import time
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from helpers import IterationAgent, approve_plan, wait_for_experiment

from experimenttracker.agent_pool import experiment_owner
from experimenttracker.api import create_app
from experimenttracker.progress import describe_item
from experimenttracker.slurm import SlurmScheduler, scheduler_time
from experimenttracker.store import Conflict, Store
from experimenttracker.workflow import Workflow


def create(store, **extra):
    eid = store.command(
        uuid4(),
        "create",
        {
            "brief": "Compare the fixed synthetic models.",
            "recipe_id": "agent_experiment_v1",
            **extra,
        },
    )["experiment_id"]
    assert store.experiment(eid)["contract"]["execution_contract"] == 3
    return eid


def test_delete_is_transactional_idempotent_and_preserves_history(runtime):
    store, _, _, _ = runtime
    eid = create(store)
    request = uuid4()
    with pytest.raises(Conflict, match="finish"):
        store.command(request, "delete", {}, eid=eid)
    assert not store.one("SELECT * FROM commands WHERE request_id=?", (str(request),))
    store.update(eid, phase="failed", hold_reason="agent_recovery_exhausted")
    store.notify(eid, "required_decision", "Old failed run")
    receipt = store.command(request, "delete", {}, eid=eid)
    deleted = store.experiment(eid)
    assert deleted["deleted_at"] and deleted["hold_reason"] == "agent_recovery_exhausted"
    revision = deleted["revision"]
    assert store.command(request, "delete", {}, eid=eid) == receipt
    assert store.experiment(eid)["revision"] == revision
    assert store.rows("SELECT * FROM events WHERE experiment_id=?", (eid,))
    with pytest.raises(Conflict, match="deleted"):
        store.command(uuid4(), "note", {"text": "late note"}, eid=eid)


def test_delete_cannot_hide_an_uncertain_turn(runtime):
    store, _, _, _ = runtime
    eid = create(store)
    store.update(eid, phase="failed")
    now = time.time()
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,created_at,updated_at) VALUES(?,?,?,1,'uncertain','input',?,?)",
            (str(uuid4()), eid, "prepare", now, now),
        )
    with pytest.raises(Conflict, match="uncertain"):
        store.command(uuid4(), "delete", {}, eid=eid)
    assert store.experiment(eid)["deleted_at"] is None


def test_deleted_run_disappears_from_api_and_inbox(config, runtime):
    store, _, _, workflow = runtime
    eid = create(store)
    store.update(eid, phase="failed")
    store.notify(eid, "required_decision", "Old failed run")
    store.command(uuid4(), "delete", {}, eid=eid)
    with TestClient(create_app(config, components=(None, store, workflow))) as client:
        headers = {"Authorization": "Bearer " + "t" * 40}
        assert client.get("/api/experiments", headers=headers).json()["experiments"] == []
        assert client.get("/api/inbox", headers=headers).json()["entries"] == []
        assert client.get(f"/api/experiments/{eid}", headers=headers).status_code == 410


async def test_selected_model_and_effort_survive_restart_and_both_turns(runtime):
    store, scheduler, _, old = runtime
    agent = IterationAgent()
    workflow = Workflow(old.config, store, scheduler, agent)
    eid = create(
        store, recipe_id="agent_experiment_v1", model="deterministic-demo", reasoning_effort="medium"
    )
    assert store.experiment(eid)["contract"]["execution_contract"] == 3
    sessions, efforts = [], []
    original_session, original_begin = agent.session, agent.begin

    async def session(*args, **kwargs):
        sessions.append(kwargs["model"])
        return await original_session(*args, **kwargs)

    async def begin(*args, **kwargs):
        efforts.append(kwargs["effort"])
        return await original_begin(*args, **kwargs)

    agent.session, agent.begin = session, begin
    # Reopen the saved database before any dispatch: settings belong to the experiment.
    reopened = Store(store.path)
    assert reopened.experiment(eid)["contract"]["agent"] == {
        "model": "deterministic-demo",
        "reasoning_effort": "medium",
    }
    reopened.close()
    await approve_plan(workflow, store, eid)
    await wait_for_experiment(workflow, store, eid, lambda e: e["phase"] == "complete")
    assert sessions == ["deterministic-demo"] * 3 and efforts == ["medium"] * 3
    assert scheduler.submissions == 3
    await workflow.close()


async def test_unavailable_model_stops_before_thread_or_job_dispatch(runtime):
    store, scheduler, agent, workflow = runtime
    eid = create(store, recipe_id="agent_experiment_v1", model="unadvertised", reasoning_effort="medium")
    assert store.experiment(eid)["contract"]["execution_contract"] == 3
    await wait_for_experiment(workflow, store, eid, lambda e: e["hold_reason"] == "model_unavailable")
    assert agent.calls == scheduler.submissions == 0
    assert not store.experiment(eid)["thread_id"]
    await workflow.close()


async def test_activity_correlates_exact_turn_and_is_bounded_and_idempotent(runtime):
    store, _, _, workflow = runtime
    eid = create(store)
    aid, now = str(uuid4()), time.time()
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,input,created_at,updated_at) VALUES(?,?,?,1,'running','thread','turn','input',?,?)",
            (aid, eid, "prepare", now, now),
        )
    workflow.agent_pool.claim(
        role="stage",
        source_table="agent_actions",
        row=store.one("SELECT * FROM agent_actions WHERE id=?", (aid,)),
        owner=experiment_owner(eid),
        slot=workflow.agent_pool.primary,
    )

    def message(i, method="item/started", thread="thread"):
        return {
            "method": method,
            "params": {
                "threadId": thread,
                "turnId": "turn",
                "item": {"id": str(i), "type": "reasoning", "text": "private reasoning"},
            },
        }

    start_count = workflow.agent.start_count
    await workflow._activity(workflow.agent_pool.primary, message(0, thread="other"), start_count)
    assert not store.projection(eid)["stage_activity"]
    await workflow._activity(workflow.agent_pool.primary, message(0), start_count)
    revision = store.experiment(eid)["revision"]
    await workflow._activity(workflow.agent_pool.primary, message(0), start_count)
    assert store.experiment(eid)["revision"] == revision
    await workflow._activity(workflow.agent_pool.primary, message(0, "item/completed"), start_count)
    await workflow._activity(workflow.agent_pool.primary, message(0), start_count)
    assert store.projection(eid)["stage_activity"][0]["state"] == "complete"
    for i in range(1, 75):
        await workflow._activity(workflow.agent_pool.primary, message(i), start_count)
    rows = store.projection(eid)["stage_activity"]
    assert len(rows) == 60 and "private reasoning" not in json.dumps(rows)


def test_activity_never_publishes_commands_output_diffs_or_secret_paths():
    message = {
        "method": "item/completed",
        "params": {
            "item": {
                "id": "one",
                "type": "commandExecution",
                "command": "cat /private/token",
                "aggregatedOutput": "secret",
                "commandActions": [{"type": "read", "path": "/private/api-token"}],
                "exitCode": 0,
            }
        },
    }
    assert describe_item(message)[2] == "Read context"
    message["params"]["item"] = {
        "id": "two",
        "type": "fileChange",
        "changes": [{"path": "/private/recipe.py", "diff": "+++ file\n+secret payload\n-old\n"}],
    }
    assert describe_item(message)[2] == "1 files written · +1 −1 · recipe.py"


async def test_scheduler_queue_running_and_exact_accounting_timestamps(config, monkeypatch):
    scheduler = SlurmScheduler(config.slurm)
    responses = iter(
        [
            "42|PENDING|2099-01-01T12:00:00|Priority",
            "42|RUNNING|2026-10-06T12:00:00|node",
            "",
            "42.batch|FAILED|1:0|Unknown|Unknown\n42|COMPLETED|0:0|2026-10-06T12:00:00|2026-10-06T12:01:00",
            "42|PENDING|2020-01-01T12:00:00|Resources",
            "",
            "",
        ]
    )

    async def command(*args, **kwargs):
        assert args[:2] == ("env", "TZ=UTC")
        return next(responses)

    monkeypatch.setattr("experimenttracker.slurm.command", command)
    queued = await scheduler.observe("42")
    assert queued.estimated_start_at and queued.started_at is None and queued.pending_reason == "Priority"
    running = await scheduler.observe("42")
    assert running.started_at and running.estimated_start_at is None
    done = await scheduler.observe("42")
    assert done.state == "COMPLETED" and done.exit_code == "0:0"
    assert done.ended_at - done.started_at == 60
    assert (await scheduler.observe("42")).estimated_start_at is None
    assert (await scheduler.observe("42")).state == "ACCOUNTING_PENDING"
    assert scheduler_time("Unknown") is None


async def test_deleted_reference_remains_usable_for_existing_child(runtime):
    store, scheduler, _, old = runtime
    agent = IterationAgent()
    workflow = Workflow(old.config, store, scheduler, agent)
    parent = create(store, recipe_id="agent_experiment_v1")
    assert store.experiment(parent)["contract"]["execution_contract"] == 3
    await approve_plan(workflow, store, parent)
    await wait_for_experiment(workflow, store, parent, lambda e: e["phase"] == "complete")
    child = create(store, recipe_id="agent_experiment_v1", reference_experiment_id=parent)
    assert store.experiment(child)["contract"]["execution_contract"] == 3
    before = len(store.rows("SELECT id FROM experiments"))
    with pytest.raises(Conflict, match="completed"):
        create(store, recipe_id="agent_experiment_v1", reference_experiment_id=child)
    assert len(store.rows("SELECT id FROM experiments")) == before
    store.command(uuid4(), "delete", {}, eid=parent)
    with pytest.raises(Conflict, match="completed"):
        create(store, recipe_id="agent_experiment_v1", reference_experiment_id=parent)
    await approve_plan(workflow, store, child)
    await wait_for_experiment(workflow, store, child, lambda e: e["phase"] == "complete")
    assert store.experiment(child)["contract"]["reference"]["experiment_id"] == parent
    assert store.experiment(parent)["snapshot"]
    await workflow.close()


def test_public_discussion_summaries_stream_with_bounds_and_throttling():
    from experimenttracker.progress import DiscussionActivity

    activity = DiscussionActivity()

    def delta(text, **extra):
        return {
            "method": "item/reasoning/summaryTextDelta",
            "params": {
                "itemId": "reason",
                "summaryIndex": 0,
                "delta": text,
                **extra,
            },
        }

    assert activity.describe("action", delta("Checking saved evidence"), 0)[2] == "Checking saved evidence"
    assert activity.describe("action", delta(" and figures"), 0.1) is None
    assert activity.describe("action", delta(" before revising."), 2)[2].endswith(
        "and figures before revising."
    )
    completed = {
        "method": "item/completed",
        "params": {
            "item": {
                "id": "reason",
                "type": "reasoning",
                "summary": ["Public summary"],
                "content": ["private raw reasoning"],
                "text": "private text",
            }
        },
    }
    assert activity.describe("action", completed, 2.1) == (
        "reason",
        "reasoning",
        "Public summary",
        "complete",
    )
    assert len(activity.describe("next-action", delta("x" * 50000), 3)[2]) == 1200
    assert activity.describe("next-action", delta("ignored", summaryIndex=100), 4) is None
    commentary = {
        "method": "item/completed",
        "params": {
            "item": {
                "id": "comment",
                "type": "agentMessage",
                "phase": "commentary",
                "text": "I found the saved measurements.",
            }
        },
    }
    assert activity.describe("action", commentary, 5)[2] == "I found the saved measurements."
    commentary["params"]["item"]["phase"] = "final_answer"
    assert activity.describe("action", commentary, 6) is None
