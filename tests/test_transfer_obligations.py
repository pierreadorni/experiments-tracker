"""Focused SQLite-only transfer responsibility tests."""

import asyncio
from uuid import uuid4

import pytest

from experimenttracker.core import obligation_sources, transfer_obligations
from experimenttracker.remote import RemoteSlurmScheduler, TransferCancelled, TransferPending


def create(store):
    eid = store.command(
        uuid4(), "create", {"brief": "Synthetic transfer", "recipe_id": "agent_experiment_v1"}
    )["experiment_id"]
    return store.experiment(eid)


def insert(store, identity, exp, *, state="pending", payload='{"layout":{},"settings":{}}'):
    with store.tx() as db:
        db.execute(
            "INSERT INTO execution_transfers VALUES(?,?,'up',?,?,NULL,1)",
            (identity, exp["id"], payload, state),
        )


def episode(store, identity, index=0):
    return store.one("SELECT * FROM obligations WHERE id=?", (f"transfer:{identity}:{index}",))


def remote_scheduler(wf, exp):
    remote = RemoteSlurmScheduler(wf, exp)

    async def resolve():
        return {"host": "synthetic"}

    remote.resolve = resolve
    return remote


def test_mapper_replay_new_episode_and_rollback(runtime):
    store, _, _, _ = runtime
    exp = create(store)
    payload = '{"layout":{},"settings":{}}'
    event_count = store.one("SELECT COUNT(*) n FROM events WHERE experiment_id=?", (exp["id"],))["n"]
    outbox_count = store.one("SELECT COUNT(*) n FROM outbox")["n"]
    revision = store.experiment(exp["id"])["revision"]
    with pytest.raises(RuntimeError, match="rollback"):
        with store.tx() as db:
            db.execute(
                "INSERT INTO execution_transfers VALUES('copy',?,'up',?,'pending',NULL,1)",
                (exp["id"], payload),
            )
            transfer_obligations.sync_transfer(db, "copy")
            store.emit(db, exp["id"], "state_changed", {"transfer_id": "copy"})
            raise RuntimeError("rollback")
    assert not store.one("SELECT 1 FROM execution_transfers WHERE id='copy'")
    assert not store.one("SELECT 1 FROM obligations WHERE id LIKE 'transfer:copy:%'")
    assert store.one("SELECT COUNT(*) n FROM events WHERE experiment_id=?", (exp["id"],))["n"] == event_count
    assert store.one("SELECT COUNT(*) n FROM outbox")["n"] == outbox_count
    assert store.experiment(exp["id"])["revision"] == revision
    insert(store, "copy", exp, payload=payload)
    with store.tx() as db:
        first = transfer_obligations.sync_transfer(db, "copy")
    with store.tx() as db:
        before = dict(db.execute("SELECT * FROM execution_transfers WHERE id='copy'").fetchone())
        replay = transfer_obligations.sync_transfer(db, "copy", before=before)
    assert replay["id"] == first["id"] and replay["created_at"] == first["created_at"]
    with store.tx() as db:
        before = dict(db.execute("SELECT * FROM execution_transfers WHERE id='copy'").fetchone())
        db.execute("UPDATE execution_transfers SET state='complete' WHERE id='copy'")
        transfer_obligations.sync_transfer(db, "copy", before=before)
    with store.tx() as db:
        before = dict(db.execute("SELECT * FROM execution_transfers WHERE id='copy'").fetchone())
        db.execute("UPDATE execution_transfers SET state='pending' WHERE id='copy'")
        second = transfer_obligations.sync_transfer(db, "copy", before=before)
    assert first["state"] == "open" and second["id"] == "transfer:copy:1"


def test_old_terminal_row_without_history_stays_without_history(runtime):
    store, _, _, _ = runtime
    exp = create(store)
    insert(store, "old", exp, state="complete")
    with store.tx() as db:
        before = dict(db.execute("SELECT * FROM execution_transfers WHERE id='old'").fetchone())
        assert transfer_obligations.sync_transfer(db, "old", before=before) is None
    assert not store.one("SELECT 1 FROM obligations WHERE id LIKE 'transfer:old:%'")


async def test_complete_replay_admits_pending_episode_before_operation(runtime):
    store, _, _, wf = runtime
    exp = create(store)
    remote = remote_scheduler(wf, exp)
    assert await remote.transfer("up", lambda: asyncio.sleep(0, result="first"), identity="copy") == "first"
    first = episode(store, "copy")
    started, release = asyncio.Event(), asyncio.Event()

    async def delayed():
        started.set()
        await release.wait()
        return "late"

    replay = asyncio.create_task(remote.transfer("up", delayed, identity="copy"))
    await started.wait()
    saved = store.one("SELECT state,result FROM execution_transfers WHERE id='copy'")
    second = episode(store, "copy", 1)
    assert saved["state"] == "pending" and saved["result"] is None
    assert first["state"] == "satisfied" and second["state"] == "open"
    assert second["created_at"] >= first["created_at"]
    # An unchanged pending retry settles that episode instead of starting another.
    assert await remote.transfer("up", lambda: asyncio.sleep(0, result="second"), identity="copy") == "second"
    assert store.one("SELECT state FROM obligations WHERE id=?", (second["id"],))["state"] == "satisfied"
    assert not store.one("SELECT 1 FROM obligations WHERE id='transfer:copy:2'")
    release.set()
    with pytest.raises(RuntimeError, match="responsibility changed"):
        await replay
    assert store.one("SELECT state FROM obligations WHERE id=?", (second["id"],))["state"] == "satisfied"


async def test_pending_retry_keeps_age_and_identity_conflict_refuses(runtime):
    store, _, _, wf = runtime
    exp = create(store)
    remote = remote_scheduler(wf, exp)

    async def broken():
        raise OSError("synthetic transport issue")

    async def changed_text():
        raise OSError("different synthetic transport issue")

    with pytest.raises(TransferPending):
        await remote.transfer("up", broken, identity="copy")
    first = episode(store, "copy")
    with pytest.raises(TransferPending):
        await remote.transfer("up", changed_text, identity="copy")
    retried = episode(store, "copy")
    assert (retried["id"], retried["created_at"]) == (first["id"], first["created_at"])
    assert not store.one("SELECT 1 FROM obligations WHERE id='transfer:copy:1'")
    saved = store.one("SELECT state,result FROM execution_transfers WHERE id='copy'")
    assert saved["state"] == "pending" and saved["result"] == "different synthetic transport issue"
    with pytest.raises(ValueError, match="identity changed"):
        await remote.transfer("down", broken, identity="copy")
    with store.tx() as db:
        latest = obligation_sources.latest_episode(db, "transfer", "copy")
    assert latest["state"] == "open"


async def test_outcome_after_row_cancellation_refuses_to_settle(runtime):
    store, _, _, wf = runtime
    exp = create(store)
    remote = remote_scheduler(wf, exp)
    started, release = asyncio.Event(), asyncio.Event()

    async def delayed():
        started.set()
        await release.wait()
        return "late"

    task = asyncio.create_task(remote.transfer("up", delayed, identity="copy"))
    await started.wait()
    with store.tx() as db:
        db.execute(
            "UPDATE execution_transfers SET state='cancelled',result='Experiment cancelled' WHERE id='copy'"
        )
    release.set()
    with pytest.raises(TransferCancelled):
        await task
    assert store.one("SELECT state FROM obligations WHERE id='transfer:copy:0'")["state"] == "open"


async def test_cancelled_caller_leaves_pending_responsibility(runtime):
    store, _, _, wf = runtime
    exp = create(store)
    remote = remote_scheduler(wf, exp)
    started = asyncio.Event()

    async def waiting():
        started.set()
        await asyncio.Future()

    task = asyncio.create_task(remote.transfer("up", waiting, identity="copy"))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert store.one("SELECT state FROM execution_transfers WHERE id='copy'")["state"] == "pending"
    assert store.one("SELECT state FROM obligations WHERE id='transfer:copy:0'")["state"] == "open"
