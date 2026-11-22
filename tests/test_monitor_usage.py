"""Regression coverage for the first live controller's duplicate monitor chains.

These use saved synthetic telemetry and stubbed inference, never live jobs.
"""

import json
import time
from uuid import uuid4

from test_controller import create

from experimenttracker.agent_pool import experiment_owner
from experimenttracker.store import encode


def running_job(store, wf, eid, *, progress=True, age=0):
    now, identity = time.time(), str(uuid4())
    root = wf.root(eid)
    root.mkdir(parents=True, exist_ok=True)
    store.update(eid, phase="training", thread_id="research-thread", model="gpt-6.1-sol")
    with store.tx() as db:
        db.execute(
            "INSERT INTO stages(experiment_id,id,state,updated_at) VALUES(?,'train','running',?)",
            (eid, now),
        )
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,state,job_id,script_hash,resources,snapshot_hash,created_at,observed_at,scheduler_state,progress,execution_root) VALUES(?,?,'train','submitted','123','hash','{}','hash',?,?,'RUNNING',?,?)",
            (
                identity,
                eid,
                now - age,
                now,
                encode({"reported_at": now - age, "completed": 3, "total": 10}) if progress else None,
                str(root),
            ),
        )
    return identity


def due(store, wake):
    with store.tx() as db:
        db.execute("UPDATE agent_wakeups SET due_at=0 WHERE id=?", (wake["id"],))


async def test_live_token_notifications_are_saved_only_for_exact_turn(runtime):
    store, _, _, wf = runtime
    eid, identity = create(store), str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,input,created_at,updated_at) VALUES(?,?,'prepare',1,'running','thread','turn','prompt',0,0)",
            (identity, eid),
        )
    wf.agent_pool.claim(
        role="stage",
        source_table="agent_actions",
        row=store.one("SELECT * FROM agent_actions WHERE id=?", (identity,)),
        owner=experiment_owner(eid),
        slot=wf.agent_pool.primary,
    )
    message = {
        "method": "thread/tokenUsage/updated",
        "params": {
            "threadId": "thread",
            "turnId": "other",
            "tokenUsage": {
                "total": {"inputTokens": 100, "cachedInputTokens": 80, "secret": "excluded"},
                "last": {"outputTokens": 10},
                "modelContextWindow": 256_000,
            },
        },
    }
    await wf._activity(wf.agent_pool.primary, message, wf.agent.start_count)
    assert not store.rows("SELECT * FROM agent_usage")
    message["params"]["turnId"] = "turn"
    revision = store.experiment(eid)["revision"]
    publications = store.one("SELECT COUNT(*) AS n FROM outbox")["n"]
    await wf._activity(wf.agent_pool.primary, message, wf.agent.start_count)
    record = store.one("SELECT * FROM agent_usage")
    usage = json.loads(record["payload"])
    assert (record["action_id"], record["thread_id"], record["turn_id"]) == (identity, "thread", "turn")
    assert usage["thread_total"] == {"inputTokens": 100, "cachedInputTokens": 80}
    assert usage["last_request"] == {"outputTokens": 10}
    assert usage["model_context_window"] == 256_000
    assert store.experiment(eid)["revision"] == revision
    assert store.one("SELECT COUNT(*) AS n FROM outbox")["n"] == publications
    message["params"]["tokenUsage"]["total"]["inputTokens"] = 200
    await wf._activity(wf.agent_pool.primary, message, wf.agent.start_count)
    assert store.one("SELECT COUNT(*) AS n FROM agent_usage")["n"] == 1
    assert (
        json.loads(store.one("SELECT payload FROM agent_usage")["payload"])["thread_total"]["inputTokens"]
        == 200
    )
    await wf.close()
