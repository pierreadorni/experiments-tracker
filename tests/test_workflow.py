import asyncio
import hashlib
import json
from uuid import uuid4

import pytest

from experimenttracker import artifacts
from experimenttracker.codex_client import NeedsAttention, TransportError
from experimenttracker.slurm import Observation, SubmissionUncertain
from experimenttracker.store import Conflict, Store
from experimenttracker.workflow import Workflow


def submit(store):
    return store.command(
        uuid4(),
        "create",
        {
            "brief": "Compare linear and quadratic models on shared synthetic data.",
            "recipe_id": "toy_compare_v1",
        },
    )["experiment_id"]


async def until(workflow, store, eid, predicate, ticks=100):
    deadline = asyncio.get_running_loop().time() + max(10, ticks * 0.01)
    while asyncio.get_running_loop().time() < deadline:
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.01)
        if predicate(store.experiment(eid)):
            return
    pytest.fail(f"condition not reached: {store.experiment(eid)}")


async def test_full_lifecycle_and_duplicate_observations(runtime):
    store, scheduler, agent, workflow = runtime
    eid = submit(store)
    await until(workflow, store, eid, lambda e: e["phase"] == "complete")
    for _ in range(5):
        await workflow.tick(force_observe=True)
    assert scheduler.submissions == 3
    assert agent.calls == 2
    assert len(store.rows("SELECT * FROM agent_actions")) == 2
    assert len(store.rows("SELECT * FROM inbox WHERE kind='result_ready'")) == 1
    assert (workflow.root(eid) / "report.html").exists()
    result = json.loads((workflow.root(eid) / "results.json").read_text())
    assert result["metrics"]["quadratic"]["mse"] < result["metrics"]["linear"]["mse"]
    assert all(row["state"] == "complete" for row in store.rows("SELECT * FROM stages"))
    await workflow.close()


async def test_unsupported_tree_request_with_reference_stops_before_compute(runtime):
    store, scheduler, agent, workflow = runtime
    parent = submit(store)
    await until(workflow, store, parent, lambda e: e["phase"] == "complete")
    brief = "test a decision tree. visualize by showing the decision frontier"
    child = store.command(
        uuid4(), "create", {"brief": brief, "recipe_id": "toy_compare_v1", "reference_experiment_id": parent}
    )["experiment_id"]
    await until(workflow, store, child, lambda e: e["phase"] == "failed")
    for _ in range(3):
        await workflow.tick(force_observe=True)
    exp = store.experiment(child)
    assert exp["brief"] == brief and exp["hold_reason"] == "unsupported_request"
    assert agent.calls == 3 and scheduler.submissions == 3  # Only the parent's three jobs.
    assert not store.rows("SELECT * FROM jobs WHERE experiment_id=?", (child,))
    assert not store.rows("SELECT * FROM inbox WHERE experiment_id=? AND kind='result_ready'", (child,))
    assert (
        len(store.rows("SELECT * FROM inbox WHERE experiment_id=? AND kind='required_decision'", (child,)))
        == 1
    )
    assert exp["analysis"] is None and not (workflow.root(child) / "report.html").exists()
    prompt = store.one("SELECT input FROM agent_actions WHERE experiment_id=?", (child,))["input"]
    assert "the requested experiment, not merely context" in prompt
    await workflow.close()


async def test_missing_new_scope_attestation_cannot_launch_jobs(runtime):
    store, scheduler, agent, workflow = runtime
    original = agent.finish

    async def without_scope(thread, turn):
        result = dict(await original(thread, turn))
        result.pop("scope_status", None)
        result.pop("scope_reason", None)
        return result

    agent.finish = without_scope
    eid = submit(store)
    await until(workflow, store, eid, lambda e: e["phase"] == "failed")
    assert scheduler.submissions == 0
    assert "explicitly confirm" in store.experiment(eid)["error"]
    await workflow.close()


async def test_reference_lineage_is_frozen_and_complete_under_the_same_protocol(runtime):
    store, scheduler, agent, workflow = runtime
    parent = submit(store)
    with pytest.raises(Conflict, match="completed"):
        store.command(
            uuid4(),
            "create",
            {
                "brief": "Compare against an unfinished reference.",
                "recipe_id": "toy_compare_v1",
                "reference_experiment_id": parent,
            },
        )
    assert len(store.rows("SELECT id FROM experiments")) == 1
    await until(workflow, store, parent, lambda e: e["phase"] == "complete")
    child = store.command(
        uuid4(),
        "create",
        {
            "brief": "Repeat the fixed protocol with an explicit reference.",
            "recipe_id": "toy_compare_v1",
            "reference_experiment_id": parent,
        },
    )["experiment_id"]
    await until(workflow, store, child, lambda e: e["phase"] == "complete")
    exp = store.experiment(child)
    assert exp["reference_experiment_id"] == parent
    assert exp["contract"]["reference"]["snapshot_hash"] == exp["snapshot"]["snapshot_hash"]
    assert scheduler.submissions == 6 and agent.calls == 4
    await workflow.close()


async def test_changed_reference_protocol_stops_before_new_inference_or_compute(runtime):
    store, scheduler, agent, workflow = runtime
    parent = submit(store)
    await until(workflow, store, parent, lambda e: e["phase"] == "complete")
    different = store.experiment(parent)["snapshot"]
    different["snapshot_hash"] = "a-different-frozen-recipe"
    store.update(parent, snapshot=different)
    child = store.command(
        uuid4(),
        "create",
        {
            "brief": "Repeat against a different frozen protocol.",
            "recipe_id": "toy_compare_v1",
            "reference_experiment_id": parent,
        },
    )["experiment_id"]
    await until(workflow, store, child, lambda e: e["phase"] == "failed")
    assert store.experiment(child)["hold_reason"] == "reference_protocol_mismatch"
    assert agent.calls == 2 and scheduler.submissions == 3
    await workflow.close()


def test_commands_are_idempotent_and_conflicts_rejected(runtime):
    store, _, _, _ = runtime
    request_id = uuid4()
    payload = {"brief": "Compare the two synthetic models.", "recipe_id": "toy_compare_v1"}
    first = store.command(request_id, "create", payload)
    assert store.command(request_id, "create", payload) == first
    assert len(store.rows("SELECT * FROM experiments")) == 1
    with pytest.raises(Conflict):
        store.command(request_id, "create", {**payload, "brief": "Different brief"})
    note_id = uuid4()
    store.command(note_id, "note", {"text": "Researcher text"}, eid=first["experiment_id"])
    store.command(note_id, "note", {"text": "Researcher text"}, eid=first["experiment_id"])
    assert len(store.rows("SELECT * FROM notes")) == 1
    with pytest.raises(Conflict):
        store.command(request_id, "create", payload, origin="gateway", experiment_id=str(uuid4()))


async def test_lost_submission_response_reconciles_without_resubmitting(runtime):
    store, scheduler, agent, workflow = runtime
    original = scheduler.submit
    first = True

    async def submit_and_lose(*args):
        nonlocal first
        result = await original(*args)
        if first:
            first = False
            raise SubmissionUncertain("crash after acceptance")
        return result

    scheduler.submit = submit_and_lose
    eid = submit(store)
    await until(workflow, store, eid, lambda e: e["hold_reason"] == "submission_uncertain")
    job = store.one("SELECT * FROM jobs")
    assert job["job_id"] is None
    # Restart with the same durable scheduler journal and database.
    restarted = Workflow(workflow.config, store, scheduler, agent)
    await until(restarted, store, eid, lambda e: e["phase"] == "complete")
    assert scheduler.submissions == 3
    assert len(store.rows("SELECT * FROM jobs")) == 3
    await restarted.close()


async def test_uncertain_submission_without_match_holds(runtime):
    store, scheduler, _, workflow = runtime

    async def lose(*args):
        raise SubmissionUncertain("no observable acceptance")

    scheduler.submit = lose
    eid = submit(store)
    await until(workflow, store, eid, lambda e: e["hold_reason"] == "submission_uncertain")
    for _ in range(5):
        await workflow.tick(force_observe=True)
    assert len(store.rows("SELECT * FROM jobs")) == 1
    assert scheduler.submissions == 0
    await workflow.close()


async def test_zero_exit_with_missing_output_does_not_continue(runtime):
    store, scheduler, agent, workflow = runtime

    async def corrupt(*args):
        return "broken"

    async def completed(*args):
        return Observation("COMPLETED", "0:0")

    scheduler.submit = corrupt
    scheduler.observe = completed
    eid = submit(store)
    await until(workflow, store, eid, lambda e: e["phase"] == "failed")
    assert agent.calls == 1
    assert len(store.rows("SELECT * FROM jobs")) == 1
    await workflow.close()


async def test_accounting_delay_is_pending_not_success(runtime):
    store, scheduler, agent, workflow = runtime

    async def unknown(*args):
        return Observation("ACCOUNTING_PENDING")

    scheduler.observe = unknown
    eid = submit(store)
    await until(workflow, store, eid, lambda e: e["hold_reason"] == "accounting_pending")
    assert agent.calls == 1
    assert store.one("SELECT state FROM jobs")["state"] == "submitted"
    await workflow.close()


async def test_unchanged_job_freshness_is_published_without_repeated_domain_events(runtime):
    store, scheduler, _, workflow = runtime

    async def pending(*args):
        return Observation("PENDING")

    scheduler.observe = pending
    eid = submit(store)
    await until(workflow, store, eid, lambda e: e["phase"] == "training")
    await workflow.tick(force_observe=True)
    job = store.one("SELECT * FROM jobs")
    revision = store.experiment(eid)["revision"]
    await asyncio.sleep(0.01)
    await workflow.tick(force_observe=True)
    freshness = workflow.status(123)["job_observations"][job["intent_id"]]
    assert freshness["job_id"] == job["job_id"]
    assert freshness["observed_at"] > job["observed_at"]
    assert store.experiment(eid)["revision"] == revision
    await workflow.close()


async def test_pause_compute_and_agent_are_independent(runtime):
    store, scheduler, agent, workflow = runtime
    eid = submit(store)
    await until(workflow, store, eid, lambda e: e["phase"] == "training")
    store.command(uuid4(), "action", {"action": "pause_agent"}, eid=eid)
    # Deterministic evaluation continues even when agent work is paused.
    await until(workflow, store, eid, lambda e: e["hold_reason"] == "agent_paused")
    assert scheduler.submissions == 3
    assert agent.calls == 1
    store.command(uuid4(), "action", {"action": "resume_agent"}, eid=eid)
    await until(workflow, store, eid, lambda e: e["phase"] == "complete")
    await workflow.close()


async def test_pause_new_compute_prevents_next_submission(runtime):
    store, scheduler, _, workflow = runtime
    eid = submit(store)
    store.command(uuid4(), "action", {"action": "pause_new_compute"}, eid=eid)
    await until(workflow, store, eid, lambda e: e["hold_reason"] == "new_compute_paused")
    assert scheduler.submissions == 0
    await workflow.close()


async def test_agent_dispatch_uncertainty_is_not_retried(runtime):
    store, _, agent, workflow = runtime

    async def lose_response(*args):
        agent.calls += 1
        raise RuntimeError("response lost after dispatch")

    agent.begin = lose_response
    eid = submit(store)
    await until(workflow, store, eid, lambda e: e["hold_reason"] == "agent_dispatch_uncertain")
    for _ in range(5):
        await workflow.tick(force_observe=True)
    assert agent.calls == 1
    await workflow.close()


def test_online_backup_is_self_contained(runtime, config):
    store, _, _, _ = runtime
    eid = submit(store)
    store.backup(config.backup_dir)
    recovered = Store(config.backup_dir / "workflow.sqlite")
    assert recovered.experiment(eid)["id"] == eid
    assert len(recovered.rows("SELECT * FROM stages")) == 7
    recovered.close()


def test_artifact_paths_and_nonfinite_json_rejected(tmp_path):
    root = tmp_path / "allowed"
    root.mkdir()
    outside = tmp_path / "outside.json"
    outside.write_text('{"metric": NaN}')
    (root / "link.json").symlink_to(outside)
    with pytest.raises(ValueError):
        artifacts.allowed_path(root / "link.json", root)
    inside = root / "bad.json"
    inside.write_text(outside.read_text())
    with pytest.raises(ValueError):
        artifacts.read_json(inside, root)


async def test_lost_running_turn_is_adopted_without_duplicate_inference(runtime):
    store, _, agent, workflow = runtime
    original = agent.finish
    first = True

    async def lose_completion(*args):
        nonlocal first
        if first:
            first = False
            raise TransportError("connection lost after turn completed")
        return await original(*args)

    agent.finish = lose_completion
    eid = submit(store)
    await until(workflow, store, eid, lambda e: e["hold_reason"] == "agent_dispatch_uncertain")
    action = store.one("SELECT * FROM agent_actions")
    assert action["state"] == "uncertain"
    assert agent.calls == 1
    await workflow.reconcile_action(action["id"])
    await until(workflow, store, eid, lambda e: e["phase"] == "complete")
    assert agent.calls == 2  # preparation reused; only interpretation dispatched
    assert len(store.rows("SELECT * FROM jobs")) == 3
    await workflow.close()


async def test_operator_reconciliation_without_turn_identity_does_not_dispatch(runtime):
    store, _, agent, workflow = runtime

    async def lose_response(*args):
        agent.calls += 1
        raise NeedsAttention("dispatch response lost")

    agent.begin = lose_response
    eid = submit(store)
    await until(workflow, store, eid, lambda e: e["hold_reason"] == "agent_dispatch_uncertain")
    action = store.one("SELECT * FROM agent_actions")
    with pytest.raises(NeedsAttention):
        await workflow.reconcile_action(action["id"])
    assert agent.calls == 1
    assert store.one("SELECT state FROM agent_actions")["state"] == "uncertain"
    await workflow.close()


@pytest.mark.parametrize(
    "outcome",
    [
        "completed",
        "historical",
        "historical_missing",
        "wrong_prompt",
        "running",
        "wrong_thread",
        "invalid_result",
        "claimed_turn",
        "rollback",
    ],
)
async def test_lost_start_response_recovers_only_verified_completed_turn(
    runtime, tmp_path, outcome, monkeypatch
):
    from experimenttracker.codex_client import CodexAgent
    from experimenttracker.config import CodexConfig

    store, scheduler, agent, workflow = runtime
    original = agent.begin
    saved = {}

    async def lose_response(thread, prompt, schema, **kwargs):
        turn = await original(thread, prompt, schema, **kwargs)
        saved.update(thread=thread, turn=turn, prompt=prompt)
        raise TimeoutError()

    agent.begin = lose_response
    eid = submit(store)
    await until(workflow, store, eid, lambda e: e["hold_reason"] == "agent_dispatch_uncertain")
    action = store.one("SELECT * FROM agent_actions WHERE experiment_id=?", (eid,))
    assert action["turn_id"] is None and action["attempts"] == 1
    assert "turn may have started" in action["error"]
    assert scheduler.submissions == 0
    calls = agent.calls
    result = agent.results[(saved["thread"], saved["turn"])]
    reader = CodexAgent(CodexConfig(), tmp_path)
    expected_prompt = saved["prompt"]

    async def rpc(method, params):
        assert method == "thread/read"
        assert params == {"threadId": saved["thread"], "includeTurns": True}
        return {
            "thread": {
                "id": "other" if outcome == "wrong_thread" else saved["thread"],
                "turns": [
                    {
                        "id": saved["turn"],
                        "status": "inProgress" if outcome == "running" else "completed",
                        "items": [
                            {
                                "type": "userMessage",
                                "content": [
                                    {
                                        "type": "text",
                                        "text": "other" if outcome == "wrong_prompt" else expected_prompt,
                                    }
                                ],
                            },
                            {
                                "type": "agentMessage",
                                "text": json.dumps({} if outcome == "invalid_result" else result),
                            },
                        ],
                    }
                ],
            }
        }

    reader.server.rpc = rpc
    agent.read_turn = reader.read_turn
    intent = json.loads(store.one("SELECT payload FROM events WHERE kind='agent_dispatch_intent'")["payload"])
    assert intent["prompt_sha256"] == hashlib.sha256(saved["prompt"].encode()).hexdigest()
    if outcome in ("historical", "historical_missing"):
        del intent["prompt_sha256"]
        with store.tx() as db:
            db.execute(
                "UPDATE events SET payload=? WHERE kind='agent_dispatch_intent'", (json.dumps(intent),)
            )
            if outcome == "historical":
                previous_id = str(uuid4())
                error = "turn interrupted: None"
                db.execute(
                    "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,error,thread_id,turn_id,created_at,updated_at) VALUES(?,?,'prepare',0,'failed','',?,?,'earlier-turn',0,0)",
                    (previous_id, eid, error, saved["thread"]),
                )
                db.execute(
                    "UPDATE agent_actions SET previous_action_id=? WHERE id=?", (previous_id, action["id"])
                )
                expected_prompt = (
                    "Previous agent action failed; correct the reported error: "
                    + error
                    + "\n"
                    + saved["prompt"]
                )
    if outcome == "claimed_turn":
        with store.tx() as db:
            db.execute(
                "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,thread_id,turn_id,created_at,updated_at) VALUES(?,?,'interpret',1,'completed','',?,?,0,0)",
                (str(uuid4()), eid, saved["thread"], saved["turn"]),
            )
    if outcome == "rollback":
        original_emit = store.emit

        def fail_emit(db, eid, kind, payload=None):
            if kind == "agent_turn_identity_reconciled":
                raise RuntimeError("publication failed")
            return original_emit(db, eid, kind, payload)

        monkeypatch.setattr(store, "emit", fail_emit)
    if outcome not in ("completed", "historical"):
        with pytest.raises((NeedsAttention, ValueError, RuntimeError)):
            await workflow.reconcile_action(action["id"], turn_id=saved["turn"])
        current = store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))
        assert current["state"] == "uncertain" and current["turn_id"] is None
        assert store.experiment(eid)["hold_reason"] == "agent_dispatch_uncertain"
    else:
        receipt = await workflow.reconcile_action(action["id"], turn_id=saved["turn"])
        assert receipt["turn_id"] == saved["turn"]
        assert store.experiment(eid)["hold_reason"] is None
        assert await workflow.reconcile_action(action["id"], turn_id=saved["turn"]) == receipt
        assert len(store.rows("SELECT * FROM events WHERE kind='agent_turn_identity_reconciled'")) == 1
        assert agent.calls == calls
        agent.begin = original
        await until(workflow, store, eid, lambda e: e["phase"] == "complete")
        assert agent.calls == calls + 1  # only interpretation is new
        assert scheduler.submissions == 3
    await workflow.close()
