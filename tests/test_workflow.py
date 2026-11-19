import asyncio
import hashlib
import json
from uuid import uuid4

import pytest
from helpers import prepared, seed_saved_contract3, until

from experimenttracker import artifacts
from experimenttracker.codex_client import NeedsAttention
from experimenttracker.slurm import Observation
from experimenttracker.store import Conflict, Store


def submit(store):
    return seed_saved_contract3(store, "Compare linear and quadratic models on shared synthetic data.")


def test_commands_are_idempotent_and_conflicts_rejected(runtime):
    store, _, _, _ = runtime
    request_id = uuid4()
    payload = {"brief": "Compare the two synthetic models.", "recipe_id": "agent_experiment_v1"}
    first = store.command(request_id, "create", payload)
    assert store.command(request_id, "create", payload) == first
    assert store.experiment(first["experiment_id"])["contract"]["execution_contract"] == 3
    assert len(store.rows("SELECT * FROM experiments")) == 1
    with pytest.raises(Conflict):
        store.command(request_id, "create", {**payload, "brief": "Different brief"})
    note_id = uuid4()
    store.command(note_id, "note", {"text": "Researcher text"}, eid=first["experiment_id"])
    store.command(note_id, "note", {"text": "Researcher text"}, eid=first["experiment_id"])
    assert len(store.rows("SELECT * FROM notes")) == 1
    with pytest.raises(Conflict):
        store.command(request_id, "create", payload, origin="gateway", experiment_id=str(uuid4()))


async def test_zero_exit_with_missing_output_does_not_continue(runtime):
    store, scheduler, agent, workflow, eid = await prepared(runtime)

    async def corrupt(*args, **kwargs):
        return "broken"

    async def completed(*args):
        return Observation("COMPLETED", "0:0")

    scheduler.submit = corrupt
    scheduler.observe = completed
    await until(workflow, store, eid, lambda e: e["phase"] == "failed")
    assert agent.calls == 2  # plan and preparation; no interpretation of invalid outputs
    assert len(store.rows("SELECT * FROM jobs")) == 1
    await workflow.close()


async def test_accounting_delay_is_pending_not_success(runtime):
    store, scheduler, agent, workflow, eid = await prepared(runtime)

    async def unknown(*args):
        return Observation("ACCOUNTING_PENDING")

    scheduler.observe = unknown
    await until(workflow, store, eid, lambda e: e["hold_reason"] == "accounting_pending")
    assert agent.calls == 2
    assert store.one("SELECT state FROM jobs")["state"] == "submitted"
    await workflow.close()


async def test_unchanged_job_freshness_is_published_without_repeated_domain_events(runtime):
    store, scheduler, _, workflow, eid = await prepared(runtime)

    async def pending(*args):
        return Observation("PENDING")

    scheduler.observe = pending
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
    store, scheduler, agent, workflow, eid = await prepared(runtime)
    await until(
        workflow, store, eid, lambda e: bool(store.rows("SELECT 1 FROM jobs WHERE experiment_id=?", (eid,)))
    )
    store.command(uuid4(), "action", {"action": "pause_agent"}, eid=eid)
    # Deterministic evaluation continues even when agent work is paused.
    await until(workflow, store, eid, lambda e: e["hold_reason"] == "agent_paused")
    assert scheduler.submissions == 3
    assert agent.calls == 2
    store.command(uuid4(), "action", {"action": "resume_agent"}, eid=eid)
    await until(workflow, store, eid, lambda e: e["phase"] == "complete")
    assert agent.calls == 3  # only the interpretation turn follows the paused stage
    await workflow.close()


async def test_pause_new_compute_prevents_next_submission(runtime):
    store, scheduler, agent, workflow, eid = await prepared(runtime)
    store.command(uuid4(), "action", {"action": "pause_new_compute"}, eid=eid)
    await until(workflow, store, eid, lambda e: e["hold_reason"] == "new_compute_paused")
    assert scheduler.submissions == 0
    assert agent.calls == 2
    assert not store.rows("SELECT * FROM jobs")
    await workflow.close()


def test_online_backup_is_self_contained(runtime, config):
    store, _, _, _ = runtime
    eid = submit(store)
    stages = store.rows("SELECT * FROM stages WHERE experiment_id=? ORDER BY ordinal,id", (eid,))
    store.backup(config.backup_dir)
    recovered = Store(config.backup_dir / "workflow.sqlite")
    assert recovered.experiment(eid)["id"] == eid
    assert recovered.rows("SELECT * FROM stages WHERE experiment_id=? ORDER BY ordinal,id", (eid,)) == stages
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

    store, scheduler, agent, workflow, eid = await prepared(runtime)
    original = agent.begin
    saved = {}

    async def lose_response(thread, prompt, schema, **kwargs):
        turn = await original(thread, prompt, schema, **kwargs)
        saved.update(thread=thread, turn=turn, prompt=prompt)
        raise TimeoutError()

    agent.begin = lose_response
    await until(workflow, store, eid, lambda e: e["hold_reason"] == "agent_dispatch_uncertain")
    task = workflow.agent_task
    if task:
        await task
    await workflow.tick(force_observe=True)
    assert workflow.agent_task is None
    assert store.experiment(eid)["hold_reason"] == "agent_dispatch_uncertain"
    # The approved plan turn is also saved; recovery targets the prepare dispatch.
    action = store.one("SELECT * FROM agent_actions WHERE experiment_id=? AND phase='prepare'", (eid,))
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
    event = next(
        row
        for row in store.rows("SELECT id,payload FROM events WHERE kind='agent_dispatch_intent' ORDER BY seq")
        if json.loads(row["payload"]).get("action_id") == action["id"]
    )
    intent = json.loads(event["payload"])
    assert intent["prompt_sha256"] == hashlib.sha256(saved["prompt"].encode()).hexdigest()
    if outcome in ("historical", "historical_missing"):
        del intent["prompt_sha256"]
        with store.tx() as db:
            db.execute("UPDATE events SET payload=? WHERE id=?", (json.dumps(intent), event["id"]))
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
