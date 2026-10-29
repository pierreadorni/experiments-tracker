import asyncio
import json
import time
from uuid import uuid4

import pytest
from test_workflow import submit, until

from experimenttracker.config import ProjectConfig
from experimenttracker.knowledge import WEEK, Knowledge, ProjectSupervisor
from experimenttracker.store import Conflict


def completed(store, *, project="test-project", summary="A measured result with limited scope."):
    eid = store.command(
        uuid4(),
        "create",
        {
            "brief": "Compare linear and quadratic models.",
            "recipe_id": "toy_compare_v1",
            "project_id": project,
        },
    )["experiment_id"]
    store.update(
        eid,
        phase="complete",
        analysis={
            "title": "Comparison",
            "summary": summary,
            "limitations": ["Synthetic data only."],
            "next_ideas": [],
        },
    )
    return eid


def enqueue(memory, kind="synthesize", pid="test-project", **kwargs):
    return memory.enqueue(pid, {"request_id": str(uuid4()), "kind": kind, **kwargs})["request_id"]


async def finish(supervisor, identity):
    for _ in range(100):
        await supervisor.advance()
        await asyncio.sleep(0.005)
        row = supervisor.memory.store.one("SELECT * FROM project_turns WHERE id=?", (identity,))
        if row["state"] in ("completed", "failed", "uncertain"):
            return row
    pytest.fail("Project turn did not reach a terminal or uncertain outcome")


async def test_synthesis_versions_tldrs_chat_and_replay(runtime):
    store, scheduler, agent, wf = runtime
    eid = submit(store)
    await until(wf, store, eid, lambda e: e["phase"] == "complete")
    memory = wf.supervisor.memory
    memory.refresh()
    assert memory.view("test-project")["pending_experiment_ids"] == [eid]
    assert agent.calls == 2  # indexing does not invoke another model
    identity = enqueue(memory)
    result = await finish(wf.supervisor, identity)
    assert result["state"] == "completed", result
    report = memory.report("test-project")
    assert report["version"] == 1
    assert set(report["sources"]) == {eid}
    assert report["document"]["source_scope"] == "synthetic"
    blocks = report["document"]["sections"][0]["blocks"]
    table = next(block for block in blocks if block["kind"] == "table")
    assert table["rows"][0]["cells"][2]["display"]
    assert any(block["kind"] == "figure" for block in blocks)
    assert memory.view("test-project")["pending_experiment_ids"] == []
    assert scheduler.submissions == 3
    calls = agent.calls
    memory.complete(result)
    memory.enqueue("test-project", json.loads(result["payload"]))
    await wf.supervisor.advance()
    assert agent.calls == calls
    chat = enqueue(memory, "chat", text="What should we test next?")
    result = await finish(wf.supervisor, chat)
    assert result["state"] == "completed", result
    saved = json.loads(result["input"].split("\nPAYLOAD:\n")[1])
    assert saved["working_report"] == report["document"]
    assert saved["experiments"][0]["tldr"]
    assert saved["details"][0]["details"]["sha256"]
    assert memory.report("test-project") == report
    summary = dict(store.experiment(eid)["analysis"], summary="An updated interpretation.")
    store.update(eid, analysis=summary)
    memory.refresh()
    assert memory.view("test-project")["pending_experiment_ids"] == [eid]
    second = enqueue(memory)
    assert (await finish(wf.supervisor, second))["state"] == "completed"
    assert memory.report("test-project")["version"] == 2
    assert memory.report("test-project", 1) == report
    assert len(store.rows("SELECT * FROM project_memory_events")) > 3
    await wf.close()


async def test_empty_project_chat_and_cross_project_sources(runtime):
    store, _, _, wf = runtime
    await wf.start()
    store.projects["other"] = ProjectConfig(id="other", name="Other", checkout=wf.config.job_output_root)
    completed(store, project="other")
    memory = wf.supervisor.memory
    memory.refresh()
    assert memory.context("test-project")["experiments"] == []
    chat = enqueue(memory, "chat", text="Help me formulate a question.")
    assert (await finish(wf.supervisor, chat))["state"] == "completed"
    synthesis = enqueue(memory)
    result = await finish(wf.supervisor, synthesis)
    assert result["state"] == "failed"
    assert "No completed" in result["error"]
    await wf.close()


async def test_weekly_requires_fresh_plentiful_quota_and_pending_sources(runtime):
    store, _, agent, wf = runtime
    await wf.start()
    completed(store)
    memory = wf.supervisor.memory
    memory.refresh()
    with store.tx() as db:
        db.execute("UPDATE project_knowledge SET next_due=?", (time.time() - 1,))
    quota = await agent.quota()
    quota["rateLimitsByLimitId"]["codex"]["secondary"]["usedPercent"] = 60
    wf.gate.update(quota)
    await wf.supervisor.advance()
    assert not store.rows("SELECT * FROM project_turns")
    wf.gate.update(await agent.quota())
    await wf.supervisor.advance()
    row = store.one("SELECT * FROM project_turns")
    assert row["origin"] == "weekly"
    assert (await finish(wf.supervisor, row["id"]))["state"] == "completed"
    with store.tx() as db:
        db.execute("UPDATE project_knowledge SET next_due=?", (time.time() - WEEK,))
    await wf.supervisor.advance()
    assert len(store.rows("SELECT * FROM project_turns")) == 1
    await wf.close()


async def test_lost_begin_never_redispatched_and_saved_result_recovers(runtime):
    store, _, agent, wf = runtime
    await wf.start()
    completed(store)
    memory = wf.supervisor.memory
    memory.refresh()
    begin = agent.begin

    async def lost(*args, **kwargs):
        await begin(*args, **kwargs)
        raise OSError("lost turn/start response")

    agent.begin = lost
    identity = enqueue(memory)
    result = await finish(wf.supervisor, identity)
    assert result["state"] == "uncertain"
    assert result["thread_id"] and result["turn_id"] is None
    for _ in range(3):
        await wf.supervisor.advance()
    assert agent.calls == 1
    assert wf.supervisor.blocks_dispatch
    # A second independent project command is held behind uncertain inference.
    enqueue(memory, "chat", text="What next?")
    await wf.supervisor.advance()
    assert agent.calls == 1
    await wf.close()


async def test_saved_exact_turn_reconciles_without_new_inference(runtime):
    store, _, agent, wf = runtime
    await wf.start()
    completed(store)
    memory = wf.supervisor.memory
    memory.refresh()
    identity = enqueue(memory)
    await wf.supervisor.advance()
    await wf.supervisor.task
    row = store.one("SELECT * FROM project_turns WHERE id=?", (identity,))
    assert row["state"] == "received"
    memory.state(identity, "running", result=None)
    # Simulate owner restart after turn completion but before saving the response.
    supervisor = ProjectSupervisor(wf)
    result = await finish(supervisor, identity)
    assert result["state"] == "completed"
    assert agent.calls == 1
    assert memory.report("test-project")["version"] == 1
    await wf.close()


async def test_changed_source_and_foreign_citations_rejected_atomically(runtime):
    store, _, _, wf = runtime
    await wf.start()
    eid = completed(store)
    memory = wf.supervisor.memory
    memory.refresh()
    identity = enqueue(memory)
    await wf.supervisor.advance()
    await wf.supervisor.task
    row = store.one("SELECT * FROM project_turns WHERE id=?", (identity,))
    result = json.loads(row["result"])
    result["report"]["sections"][0]["experiment_ids"].append(str(uuid4()))
    memory.state(identity, "received", result=json.dumps(result))
    assert (await finish(wf.supervisor, identity))["state"] == "failed"
    assert memory.report("test-project") is None
    identity = enqueue(memory)
    await wf.supervisor.advance()
    await wf.supervisor.task
    store.update(
        eid, analysis={"title": "Changed", "summary": "Changed", "limitations": [], "next_ideas": []}
    )
    memory.refresh()
    assert (await finish(wf.supervisor, identity))["state"] == "failed"
    assert memory.report("test-project") is None
    await wf.close()


async def test_unregistered_figure_and_unknown_table_metric_rejected(runtime):
    store, _, _, wf = runtime
    await wf.start()
    eid = completed(store)
    memory = wf.supervisor.memory
    memory.refresh()
    identity = enqueue(memory)
    await wf.supervisor.advance()
    await wf.supervisor.task
    row = store.one("SELECT * FROM project_turns WHERE id=?", (identity,))
    result = json.loads(row["result"])
    result["report"]["sections"][0]["blocks"] = [
        {
            "kind": "figure",
            "caption": "Unregistered image",
            "alt": "Image",
            "experiment_id": eid,
            "artifact_id": str(uuid4()),
        }
    ]
    memory.state(identity, "received", result=json.dumps(result))
    assert (await finish(wf.supervisor, identity))["state"] == "failed"
    assert memory.report("test-project") is None

    identity = enqueue(memory)
    await wf.supervisor.advance()
    await wf.supervisor.task
    row = store.one("SELECT * FROM project_turns WHERE id=?", (identity,))
    result = json.loads(row["result"])
    result["report"]["sections"][0]["blocks"] = [
        {
            "kind": "table",
            "caption": "Unregistered measurement",
            "columns": ["Model", "Value"],
            "rows": [
                {
                    "cells": [
                        {"text": "model"},
                        {"metric": {"experiment_id": eid, "name": "missing", "variant": "model"}},
                    ],
                    "experiment_ids": [eid],
                }
            ],
        }
    ]
    memory.state(identity, "received", result=json.dumps(result))
    assert (await finish(wf.supervisor, identity))["state"] == "failed"
    assert memory.report("test-project") is None
    await wf.close()


def test_queue_identity_and_weekly_setting(runtime):
    store, _, _, wf = runtime
    memory = Knowledge(store)
    request = {"request_id": str(uuid4()), "kind": "chat", "text": "Question"}
    memory.enqueue("test-project", request)
    assert memory.enqueue("test-project", request)["state"] == "queued"
    with pytest.raises(Conflict):
        memory.enqueue("test-project", {**request, "text": "Different question"})
    with pytest.raises(Conflict):
        memory.enqueue("missing-project", request)
    enqueue(memory, "schedule", weekly=False)
    assert memory.view("test-project")["weekly"] is False


async def test_terminal_failure_releases_slot_and_quota_hold_does_not_dispatch(runtime):
    from experimenttracker.codex_client import TerminalTurnError

    store, _, agent, wf = runtime
    await wf.start()
    completed(store)
    memory = wf.supervisor.memory
    memory.refresh()
    identity = enqueue(memory)
    wf.gate.ordinary_allowed = False
    await wf.supervisor.advance()
    assert agent.calls == 0
    assert (
        store.one("SELECT hold_reason FROM project_turns WHERE id=?", (identity,))["hold_reason"]
        == "quota_hold"
    )
    wf.gate.update(await agent.quota())

    async def failed(*args):
        raise TerminalTurnError("confirmed failed turn")

    agent.finish = failed
    assert (await finish(wf.supervisor, identity))["state"] == "failed"
    await wf.supervisor.advance()
    assert not wf.supervisor.blocks_dispatch
    assert agent.calls == 1
    await wf.close()


def test_project_knowledge_api_auth_and_validation(config, runtime):
    from fastapi.testclient import TestClient

    from experimenttracker.api import create_app

    store, _, _, wf = runtime
    with TestClient(create_app(config, components=(None, store, wf))) as client:
        path = "/api/projects/test-project/knowledge"
        headers = {"Authorization": "Bearer " + "t" * 40}
        assert client.get(path).status_code == 401
        assert client.get(path, headers=headers).json()["report"] is None
        body = {"request_id": str(uuid4()), "kind": "schedule", "weekly": False}
        assert client.post(path, json=body, headers=headers).status_code == 202
        assert client.get(path, headers=headers).json()["weekly"] is False
        assert client.post(path, json={**body, "weekly": True}, headers=headers).status_code == 409
        assert (
            client.post(
                path, json={"request_id": str(uuid4()), "kind": "chat", "text": "  "}, headers=headers
            ).status_code
            == 422
        )
        assert client.get(path + "/reports/1", headers=headers).status_code == 404
        assert client.get("/api/projects/missing/knowledge", headers=headers).status_code == 404


async def test_remove_saved_idea_is_exact_durable_and_does_not_dispatch(runtime):
    store, scheduler, agent, wf = runtime
    eid = completed(store)
    idea = "Evaluate unseen scenes with the same protocol."
    analysis = {**store.experiment(eid)["analysis"], "next_ideas": [idea, "Try more seeds."]}
    store.update(eid, analysis=analysis)
    memory = wf.supervisor.memory
    memory.refresh()
    payload = {"request_id": str(uuid4()), "kind": "dismiss_idea", "experiment_id": eid, "text": idea}
    before = memory.view("test-project")["revision"]
    assert memory.enqueue("test-project", payload)["state"] == "completed"
    assert memory.enqueue("test-project", payload)["state"] == "completed"
    assert memory.view("test-project")["revision"] == before + 1
    for _ in range(55):
        enqueue(memory, "schedule", weekly=False)
    # Removal survives the recent-message limit and a new Knowledge instance.
    assert Knowledge(store).view("test-project")["dismissed_ideas"] == [{"experiment_id": eid, "text": idea}]
    assert store.experiment(eid)["analysis"] == analysis
    assert memory.summaries("test-project")[0]["next_ideas"] == analysis["next_ideas"]
    assert memory.view("test-project")["messages"][-1]["kind"] == "schedule"
    event = json.loads(
        store.rows("SELECT payload FROM project_memory_events ORDER BY revision DESC")[0]["payload"]
    )
    assert event["dismissed_ideas"] == [{"experiment_id": eid, "text": idea}]
    for extra in ({"text": "Different idea"}, {"experiment_id": str(uuid4())}):
        with pytest.raises(Conflict):
            memory.enqueue("test-project", {**payload, "request_id": str(uuid4()), **extra})
    store.projects["other"] = ProjectConfig(id="other", name="Other", checkout=wf.config.job_output_root)
    with pytest.raises(Conflict):
        memory.enqueue("other", {**payload, "request_id": str(uuid4())})
    with pytest.raises(Conflict):
        memory.enqueue("test-project", {**payload, "text": "Changed identity"})
    await wf.supervisor.advance()
    assert agent.calls == 0
    assert scheduler.submissions == 0
    await wf.close()
