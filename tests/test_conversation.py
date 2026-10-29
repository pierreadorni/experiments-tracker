import asyncio
import hashlib
import json
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from test_workflow import submit, until

from experimenttracker.api import create_app
from experimenttracker.codex_client import NeedsAttention, TransportError
from experimenttracker.config import ProjectConfig
from experimenttracker.store import Conflict
from experimenttracker.workflow import Workflow


def file_bytes(path):
    return Path(path).read_bytes()


async def script_reply(agent, workflow, *, followup=False):
    """A tool-capable fixture executes a plotting script on genuinely nested saved data."""
    begin = agent.begin

    async def execute(thread, prompt, schema, *, workspace=None, effort="low", project_workspace=None):
        assert workspace is not None
        payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
        assert str(workspace) == payload["analysis_workspace"]
        turn = await begin(
            thread,
            prompt,
            schema,
            workspace=workspace,
            effort=effort,
            **({"project_workspace": project_workspace} if project_workspace else {}),
        )
        source = workflow.store.one(
            "SELECT * FROM artifacts WHERE name='histories.json' AND experiment_id=?",
            (
                workflow.store.one(
                    "SELECT experiment_id FROM discussion_threads WHERE thread_id=?", (thread,)
                )["experiment_id"],
            ),
        )
        script = """import json, sys
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
data = json.load(open(sys.argv[1]))
fig, ax = plt.subplots(figsize=(6, 3))
for run in data['runs']:
    ax.plot(range(len(run['history']['mse'])), run['history']['mse'], label=f"depth {run['depth']}")
ax.set(xlabel='Epoch', ylabel='Standardized training MSE')
ax.legend()
fig.tight_layout()
fig.savefig('loss.svg', metadata={'Date': None})
"""
        (workspace / "plot.py").write_text(script)
        await asyncio.to_thread(
            subprocess.run, [payload["python"], "plot.py", source["path"]], cwd=workspace, check=True
        )
        result = agent.results[thread, turn]
        result["generated_artifacts"] = [
            {
                "name": name,
                "sha256": hashlib.sha256((workspace / name).read_bytes()).hexdigest(),
                "evidence_ids": [source["id"]],
            }
            for name in ("plot.py", "loss.svg")
        ]
        result["presentation"]["blocks"].append(
            {
                "kind": "figure",
                "artifact_id": "generated:loss.svg",
                "alt": "Training loss curves",
                "caption": "Saved nested training histories by depth.",
                "evidence_ids": [source["id"]],
            }
        )
        result["evidence_ids"] += ["generated:loss.svg"]
        if followup:
            result["new_experiment"] = {
                "brief": "Repeat the fixed linear and quadratic protocol with new seeds."
            }
        return turn

    agent.begin = execute


def nested_histories(store, workflow, eid):
    from experimenttracker import artifacts

    root = workflow.root(eid)
    (root / "histories.json").write_text(
        json.dumps(
            {
                "runs": [
                    {"depth": d, "history": {"mse": [d / (epoch + 1) for epoch in range(401)]}}
                    for d in (1, 2, 3)
                ]
            }
        )
    )
    artifacts.register(store, eid, "fit", root, ["histories.json"])


async def test_arbitrary_script_plot_recovery_provenance_and_exactly_once_followup(runtime, tmp_path):
    store, scheduler, agent, workflow = runtime
    checkout = tmp_path / "research"
    checkout.mkdir()
    project = ProjectConfig(id="anyup-rs", name="anyup-rs", checkout=checkout)
    workflow.config.projects.append(project)
    store.projects[project.id] = project
    eid = store.command(
        uuid4(),
        "create",
        {
            "brief": "Compare linear and quadratic models on shared synthetic data.",
            "recipe_id": "toy_compare_v1",
            "project_id": project.id,
        },
    )["experiment_id"]
    await until(workflow, store, eid, lambda e: e["phase"] == "complete")
    nested_histories(store, workflow, eid)
    originals = {a["path"]: file_bytes(a["path"]) for a in store.rows("SELECT * FROM artifacts")}
    await script_reply(agent, workflow, followup=True)
    identity = send(store, eid, "Add the training-history plot and start the follow-up")
    await workflow.tick(force_observe=True)
    await workflow.agent_task
    action = store.one("SELECT * FROM agent_actions WHERE phase=?", ("discuss:" + identity,))
    assert action["state"] == "completed"
    # Simulate a crash after sealing only the first output, before registering or saving a reply.
    _, _, files = workflow.conversation.validate(action, json.loads(action["result"]))
    from experimenttracker import discussion_outputs

    name, data = next(iter(files.items()))
    discussion_outputs.write_once(workflow.root(eid) / name, data, workflow.root(eid))
    await workflow.close()
    restarted = Workflow(workflow.config, store, scheduler, agent)
    # Complete directly so this test cannot dispatch the new scientific experiment.
    message = store.one("SELECT * FROM discussions WHERE id=?", (identity,))
    restarted.conversation.complete(message, action)
    restarted.conversation.complete(message, action)
    row = store.conversation(eid)[0]
    assert len(row["artifact_ids"]) == 3
    assert row["followup_experiment_id"] != eid
    child = store.experiment(row["followup_experiment_id"])
    assert child["reference_experiment_id"] == eid
    assert child["contract"]["project"]["id"] == "anyup-rs"
    assert child["contract"]["agent"]["model"] == "deterministic-demo"
    assert len(store.rows("SELECT * FROM experiments")) == 2
    assert len(store.rows("SELECT * FROM report_versions")) == 1
    assert agent.calls == 3 and scheduler.submissions == 3
    # Rebuild the report from the sealed artifacts, with script and original data in provenance.
    from experimenttracker import reporting

    exp = store.experiment(eid)
    records = store.rows("SELECT * FROM artifacts WHERE experiment_id=?", (eid,))
    document = json.loads(store.one("SELECT document FROM report_versions")["document"])
    reporting.build_report(exp, restarted.root(eid), records, document, 2)
    metadata = json.loads((restarted.root(eid) / "report-v2.json").read_text())
    source_names = {r["name"] for r in metadata["source_artifacts"]}
    assert "histories.json" in source_names
    assert f"discussion-{identity}-plot.py" in source_names
    assert f"discussion-{identity}-provenance.json" in source_names
    assert "generated:" not in (restarted.root(eid) / "report-v2.html").read_text()
    assert all(file_bytes(path) == data for path, data in originals.items())
    await restarted.close()


@pytest.mark.parametrize("tamper", ["hash", "symlink", "report_conflict"])
async def test_script_outputs_fail_atomically_without_changing_evidence(runtime, tamper):
    store, _, agent, workflow = runtime
    eid = await ready(runtime)
    nested_histories(store, workflow, eid)
    await script_reply(agent, workflow, followup=True)
    identity = send(store, eid, "Add a plot and start a follow-up")
    await workflow.tick(force_observe=True)
    await workflow.agent_task
    workspace = workflow.root(eid) / "discussion-work" / identity
    if tamper == "hash":
        (workspace / "loss.svg").write_text("changed after the saved turn")
    elif tamper == "symlink":
        (workspace / "loss.svg").unlink()
        (workspace / "loss.svg").symlink_to(workflow.root(eid) / "figure.svg")
    else:
        from test_reporting import document

        store.command(uuid4(), "present", {"document": document()}, eid=eid)
    failed = await response(workflow, store, identity, "failed")
    assert failed["followup_experiment_id"] is None and failed["artifact_ids"] == "[]"
    assert len(store.rows("SELECT * FROM experiments")) == 1
    assert not store.rows("SELECT * FROM artifacts WHERE stage_id LIKE 'discuss:%'")
    await workflow.close()


async def ready(runtime):
    store, _, _, workflow = runtime
    eid = submit(store)
    await until(workflow, store, eid, lambda e: e["phase"] == "complete")
    return eid


def send(store, eid, text, request=None, **selection):
    identity = request or uuid4()
    return store.command(identity, "discuss", {"text": text, **selection}, eid=eid)["request_id"]


async def response(workflow, store, identity, state="completed"):
    for _ in range(100):
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.001)
        row = store.one("SELECT * FROM discussions WHERE id=?", (identity,))
        if row["state"] == state:
            return row
    pytest.fail(f"discussion did not reach {state}: {row}")


async def test_ordered_evidence_discussion_and_report_edit_do_not_repeat_science(runtime):
    store, scheduler, agent, workflow = runtime
    eid = await ready(runtime)
    root = workflow.root(eid)
    originals = {
        name: (root / name).read_bytes()
        for name in ("results.json", "report.html", "analysis.json", "snapshot/recipe.py")
    }
    sent = []
    begin = agent.begin

    async def capture(thread, prompt, *args, **kwargs):
        sent.append(prompt)
        return await begin(thread, prompt, *args, **kwargs)

    agent.begin = capture
    identity = send(
        store, eid, "What remains uncertain?", model="deterministic-demo", reasoning_effort="medium"
    )
    assert (
        send(
            store,
            eid,
            "What remains uncertain?",
            request=identity,
            model="deterministic-demo",
            reasoning_effort="medium",
        )
        == identity
    )
    followup = send(store, eid, "What should we test next?")
    first = await response(workflow, store, identity)
    second = await response(workflow, store, followup)
    assert first["seq"] == 1 and second["seq"] == 2
    assert first["report_version"] is None and second["report_version"] is None
    action = store.one("SELECT * FROM agent_actions WHERE id=?", (second["action_id"],))
    context = json.loads(action["input"].split("\nPAYLOAD:\n", 1)[1])
    assert context["history"][0]["reply"] == first["reply"]
    assert context["results"] == json.loads(originals["results.json"])
    assert context["artifacts"] and len(context["history"]) == 1
    wire = json.loads(context["_sent_prompt"].split("\nPAYLOAD:\n", 1)[1])
    assert wire["context_mode"] == "update"
    assert wire["message"] == "What should we test next?"
    assert wire["context"] == {}
    assert "history" not in wire and "history_detail" not in wire
    assert "results" not in wire and "brief" not in wire
    assert len(context["_sent_prompt"]) < 2200
    assert sent[1] == context["_sent_prompt"]
    initial = json.loads(sent[0].split("\nPAYLOAD:\n", 1)[1])
    assert initial["context_mode"] == "initial"
    assert initial["context"]["metric_preview"]["total"] > 0
    assert "results" not in initial["context"]
    thread = store.one("SELECT thread_id FROM discussion_threads WHERE experiment_id=?", (eid,))["thread_id"]
    assert action["thread_id"] == thread
    change = send(store, eid, "Add a plot of held-out error to the report")
    edited = await response(workflow, store, change)
    assert edited["report_version"] == 2
    await workflow.tick(force_observe=True)
    view = store.projection(eid)
    assert view["report_versions"][0]["state"] == "ready"
    assert view["phase"] == "complete" and not view["hold_reason"]
    assert all((root / name).read_bytes() == content for name, content in originals.items())
    assert scheduler.submissions == 3 and agent.calls == 5
    assert len(view["agent_actions"]) == 2 and len(view["conversation"]) == 3
    assert len(store.rows("SELECT * FROM report_versions")) == 1
    await workflow.close()


async def test_fresh_quota_hold_and_pause_keep_messages_without_changing_result(runtime):
    store, scheduler, agent, workflow = runtime
    eid = await ready(runtime)
    identity = send(store, eid, "Discuss the uncertainty")
    workflow.gate.update({"ordinaryUsageAllowed": False})
    await workflow.tick(force_observe=True)
    held = store.one("SELECT * FROM discussions WHERE id=?", (identity,))
    assert held["state"] == "waiting" and held["hold_reason"] == "quota_hold" and held["action_id"] is None
    assert store.experiment(eid)["phase"] == "complete" and not store.experiment(eid)["hold_reason"]
    assert agent.calls == 2 and scheduler.submissions == 3
    await workflow.refresh_quota()
    store.command(uuid4(), "action", {"action": "pause_agent"}, eid=eid)
    await workflow.tick(force_observe=True)
    assert (
        store.one("SELECT hold_reason FROM discussions WHERE id=?", (identity,))["hold_reason"]
        == "agent_paused"
    )
    store.command(uuid4(), "action", {"action": "resume_agent"}, eid=eid)
    await response(workflow, store, identity)
    assert agent.calls == 3
    await workflow.close()


async def test_completed_turn_replays_after_restart_with_one_reply_and_one_report(runtime):
    store, _, agent, workflow = runtime
    eid = await ready(runtime)
    identity = send(store, eid, "Add a plot")
    await workflow.tick(force_observe=True)
    await workflow.agent_task
    action = store.one("SELECT * FROM agent_actions WHERE phase=?", ("discuss:" + identity,))
    assert action["state"] == "completed"
    await workflow.close()
    restarted = Workflow(workflow.config, store, workflow.scheduler, agent)
    await response(restarted, store, identity)
    for _ in range(3):
        await restarted.tick(force_observe=True)
    assert agent.calls == 3 and len(store.rows("SELECT * FROM report_versions")) == 1
    assert len(store.projection(eid)["conversation"]) == 1
    await restarted.close()


async def test_lost_dispatch_is_not_retried_and_exact_turn_can_be_reconciled(runtime):
    store, _, agent, workflow = runtime
    eid = await ready(runtime)
    begin = agent.begin

    async def lose(*args, **kwargs):
        await begin(*args, **kwargs)
        raise TransportError("lost dispatch response")

    agent.begin = lose
    identity = send(store, eid, "Discuss the result")
    row = await response(workflow, store, identity, "uncertain")
    for _ in range(3):
        await workflow.tick(force_observe=True)
    assert agent.calls == 3
    action = store.one("SELECT * FROM agent_actions WHERE id=?", (row["action_id"],))
    assert action["turn_id"] is None
    with pytest.raises(NeedsAttention):
        await workflow.reconcile_action(action["id"])
    # An operator establishes the exact saved turn identity from the runtime.
    turn = next(turn for thread, turn in agent.results if thread == action["thread_id"])
    with store.tx() as db:
        db.execute("UPDATE agent_actions SET turn_id=? WHERE id=?", (turn, action["id"]))
    await workflow.reconcile_action(action["id"])
    await response(workflow, store, identity)
    assert agent.calls == 3 and store.experiment(eid)["phase"] == "complete"
    await workflow.close()


async def test_invalid_reply_and_concurrent_report_change_preserve_original(runtime):
    store, _, agent, workflow = runtime
    eid = await ready(runtime)
    original = (workflow.root(eid) / "report.html").read_bytes()
    finish = agent.finish

    async def bad(thread, turn):
        result = await finish(thread, turn)
        result["evidence_ids"] = [str(uuid4())]
        return result

    agent.finish = bad
    identity = send(store, eid, "Explain the result")
    failed = await response(workflow, store, identity, "failed")
    assert "outside" in failed["error"] and not store.rows("SELECT * FROM report_versions")
    assert (workflow.root(eid) / "report.html").read_bytes() == original
    agent.finish = finish
    identity = send(store, eid, "Add a plot")
    await workflow.tick(force_observe=True)
    await workflow.agent_task
    from test_reporting import document

    store.command(uuid4(), "present", {"document": document()}, eid=eid)
    await workflow.tick(force_observe=True)
    failed = store.one("SELECT * FROM discussions WHERE id=?", (identity,))
    assert failed["state"] == "failed" and "report changed" in failed["error"]
    assert len(store.rows("SELECT * FROM report_versions")) == 1
    assert (workflow.root(eid) / "report.html").read_bytes() == original
    await workflow.close()


async def test_pagination_and_authenticated_commands(runtime, config):
    store, _, _, workflow = runtime
    eid = await ready(runtime)
    app = create_app(config, components=(None, store, workflow))
    app.state.store = store
    with TestClient(app) as client:
        payload = {"request_id": str(uuid4()), "text": "What next?"}
        url = f"/api/experiments/{eid}/conversation"
        assert client.post(url, json=payload).status_code == 401
        auth = {"Authorization": "Bearer " + config.api_token_file.read_text()}
        assert client.post(url, json=payload, headers=auth).status_code == 202
        assert client.post(url, json=payload, headers=auth).status_code == 202
        assert client.post(url, json={**payload, "text": "Changed"}, headers=auth).status_code == 409
        assert (
            client.post(url, json={"request_id": str(uuid4()), "text": "   "}, headers=auth).status_code
            == 422
        )
        for i in range(24):
            identity = send(store, eid, f"Saved historical message {i}")
            store.discussion_state(identity, "failed", error="Fixture: no inference")
        latest = client.get(url, headers=auth).json()
        earlier = client.get(url + f"?before={latest['messages'][0]['seq']}", headers=auth).json()
        assert latest["has_more"] and len(latest["messages"]) == 20
        assert not earlier["has_more"] and len(earlier["messages"]) == 5
        assert latest["messages"][0]["seq"] == 6 and earlier["messages"][0]["seq"] == 1
        assert len(store.projection(eid)["conversation"]) == 20
        assert store.projection(eid)["conversation_total"] == 25
        assert client.get(url + "?before=0", headers=auth).status_code == 422


async def test_incomplete_and_deleted_runs_do_not_accept_discussion(runtime):
    store, _, _, workflow = runtime
    eid = submit(store)
    with pytest.raises(Conflict):
        send(store, eid, "Discuss")
    await until(workflow, store, eid, lambda e: e["phase"] == "complete")
    store.command(uuid4(), "delete", {}, eid=eid)
    with pytest.raises(Conflict):
        send(store, eid, "Discuss")
    await workflow.close()


@pytest.mark.parametrize("always_missing", [False, True])
async def test_missing_output_manifest_continues_saved_turn_with_feedback(runtime, always_missing):
    store, scheduler, agent, workflow = runtime
    eid = await ready(runtime)
    nested_histories(store, workflow, eid)
    await script_reply(agent, workflow)
    finish = agent.finish
    saved_results = []

    async def omit_manifest(thread, turn):
        result = await finish(thread, turn)
        if always_missing or not saved_results:
            result["generated_artifacts"] = []
        saved_results.append(json.dumps(result, sort_keys=True))
        return result

    agent.finish = omit_manifest
    identity = send(store, eid, "Add the saved history plot")
    target = "failed" if always_missing else "completed"
    for _ in range(20):
        await workflow.tick(force_observe=True)
        if workflow.agent_task:
            await workflow.agent_task
        row = store.one("SELECT * FROM discussions WHERE id=?", (identity,))
        if row["state"] == target:
            break
    assert row["state"] == target
    actions = store.rows(
        "SELECT * FROM agent_actions WHERE phase=? ORDER BY created_at", ("discuss:" + identity,)
    )
    assert len(actions) == (workflow.config.codex.identical_error_limit if always_missing else 2)
    assert actions[0]["state"] == "failed"
    assert json.loads(actions[0]["result"])["generated_artifacts"] == []
    assert actions[1]["previous_action_id"] == actions[0]["id"]
    assert actions[0]["turn_id"] != actions[1]["turn_id"]
    assert "generated_artifacts entries" in actions[0]["error"]
    assert "Publication validation feedback:" in actions[1]["input"]
    assert scheduler.submissions == 3
    if always_missing:
        assert not store.rows("SELECT * FROM artifacts WHERE stage_id LIKE 'discuss:%'")
        assert row["report_version"] is None
    else:
        assert len(json.loads(row["artifact_ids"])) == 3
        action_count = len(actions)
        await workflow.tick(force_observe=True)
        assert (
            len(store.rows("SELECT * FROM agent_actions WHERE phase=?", ("discuss:" + identity,)))
            == action_count
        )
    await workflow.close()


async def test_discussion_context_is_bounded_selective_and_bootstraps_new_threads(runtime):
    store, _, _, workflow = runtime
    eid = await ready(runtime)
    identity = send(store, eid, "Explain the measurements")
    first = await response(workflow, store, identity)
    action = store.one("SELECT * FROM agent_actions WHERE id=?", (first["action_id"],))
    instruction, raw = action["input"].split("\nPAYLOAD:\n", 1)
    payload = json.loads(raw)
    payload["results"]["metrics"] = [
        {"variant": f"run-{i}", "name": "loss", "value": i / 1000, "unit": ""} for i in range(10000)
    ]
    changed = dict(action, id=str(uuid4()), input=instruction + "\nPAYLOAD:\n" + json.dumps(payload))
    wire, saved = workflow.conversation.turn_prompt(changed, action["thread_id"])
    turn = json.loads(wire.split("\nPAYLOAD:\n", 1)[1])
    assert turn["context_mode"] == "update"
    assert set(turn["context"]) == {"results_detail", "metric_preview"}
    preview = turn["context"]["metric_preview"]
    assert preview["total"] == 10000 and len(preview["rows"]) == 24
    detail = json.loads(file_bytes(turn["context"]["results_detail"]["path"]))
    assert detail == payload["results"]
    assert len(wire) < 4000
    assert json.loads(saved.split("\nPAYLOAD:\n", 1)[1])["results"] == payload["results"]
    # A replacement thread must receive its own bootstrap, even after a successful discussion.
    wire, _ = workflow.conversation.turn_prompt(changed, "replacement-thread")
    turn = json.loads(wire.split("\nPAYLOAD:\n", 1)[1])
    assert turn["context_mode"] == "initial"
    assert "brief_summary" in turn["context"] and "history_detail" in turn
    # Report publication is communicated without replaying the old report or measurements.
    payload = json.loads(raw)
    payload["base_report_version"] += 1
    payload["current_report_document"] = {"title": "Updated report", "blocks": []}
    changed["input"] = instruction + "\nPAYLOAD:\n" + json.dumps(payload)
    wire, _ = workflow.conversation.turn_prompt(changed, action["thread_id"])
    turn = json.loads(wire.split("\nPAYLOAD:\n", 1)[1])
    assert set(turn["context"]) == {"base_report_version", "current_report_document_detail"}
    await workflow.close()


@pytest.mark.parametrize("invalid", ["schema", "metric"])
async def test_invalid_report_reply_is_corrected_on_same_saved_thread(runtime, invalid):
    store, scheduler, agent, workflow = runtime
    eid = await ready(runtime)
    finish = agent.finish
    replies = 0

    async def malformed_once(thread, turn):
        nonlocal replies
        result = await finish(thread, turn)
        replies += 1
        if replies == 1:
            if invalid == "schema":
                result["presentation"]["standfirst"] = ""
            else:
                result["presentation"]["blocks"][-1]["series"][0]["points"][0]["metric"]["name"] = "missing"
        return result

    agent.finish = malformed_once
    identity = send(store, eid, "Add a plot to the report")
    # Reconciliation must save the terminal malformed reply before validation/correction.
    await workflow.tick(force_observe=True)
    await workflow.agent_task
    action = store.one("SELECT * FROM agent_actions WHERE phase=?", ("discuss:" + identity,))
    workflow._action_state(action["id"], "uncertain")
    workflow.agent_task = None
    await workflow.reconcile_action(action["id"])
    assert store.one("SELECT state FROM agent_actions WHERE id=?", (action["id"],))["state"] == "completed"
    reply = await response(workflow, store, identity)
    actions = store.rows(
        "SELECT * FROM agent_actions WHERE phase=? ORDER BY created_at", ("discuss:" + identity,)
    )
    assert len(actions) == 2 and actions[1]["previous_action_id"] == actions[0]["id"]
    assert actions[0]["thread_id"] == actions[1]["thread_id"]
    assert actions[0]["result"] and actions[0]["turn_id"] != actions[1]["turn_id"]
    assert "validation" in actions[0]["error"] and "input_value" not in actions[0]["error"]
    wire = json.loads(actions[1]["input"].split("\nPAYLOAD:\n", 1)[1])["_sent_prompt"]
    assert json.loads(wire.split("\nPAYLOAD:\n", 1)[1])["publication_feedback"] == actions[0]["error"]
    assert reply["report_version"] == 2 and scheduler.submissions == 3
    await workflow.close()


async def test_live_discussion_activity_projects_public_summaries_in_order(runtime):
    store, _, _, workflow = runtime
    eid = await ready(runtime)
    identity = send(store, eid, "Explain these results")
    aid = str(uuid4())
    import time

    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,input,created_at,updated_at) VALUES(?,?,?,1,'running','thread','turn','input',?,?)",
            (aid, eid, "discuss:" + identity, time.time(), time.time()),
        )
        db.execute("UPDATE discussions SET action_id=?,state='running' WHERE id=?", (aid, identity))
    workflow.active_action = aid
    for i in range(25):
        await workflow._activity(
            {
                "method": "item/completed",
                "params": {
                    "threadId": "thread",
                    "turnId": "turn",
                    "item": {
                        "id": str(i),
                        "type": "reasoning",
                        "summary": [f"Public update {i}"],
                        "content": ["Never publish raw reasoning"],
                    },
                },
            }
        )
    projected = store.conversation(eid)[0]["activity"]
    assert len(projected) == 20
    assert projected[0]["summary"] == "Public update 5"
    assert projected[-1]["summary"] == "Public update 24"
    assert "raw reasoning" not in json.dumps(projected)
    await workflow._activity(
        {
            "method": "item/reasoning/summaryTextDelta",
            "params": {
                "threadId": "other",
                "turnId": "turn",
                "itemId": "extra",
                "summaryIndex": 0,
                "delta": "wrong thread",
            },
        }
    )
    assert store.conversation(eid)[0]["activity"] == projected
    await workflow.close()
