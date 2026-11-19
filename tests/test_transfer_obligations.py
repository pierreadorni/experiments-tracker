"""Focused SQLite-only transfer responsibility tests."""

import asyncio
import hashlib
from pathlib import Path

import pytest
from helpers import seed_saved_contract3

from experimenttracker.core import obligation_sources, transfer_obligations
from experimenttracker.remote import RemoteSlurmScheduler, TransferCancelled, TransferPending
from experimenttracker.slurm import Observation
from experimenttracker.store import encode


def create(store):
    return store.experiment(seed_saved_contract3(store, "Synthetic transfer."))


def insert(store, identity, exp, *, state="pending", direction="up", payload='{"layout":{},"settings":{}}'):
    with store.tx() as db:
        db.execute(
            "INSERT INTO execution_transfers VALUES(?,?,?,?,?,NULL,1)",
            (identity, exp["id"], direction, payload, state),
        )


def episode(store, identity, index=0):
    return store.one("SELECT * FROM obligations WHERE id=?", (f"transfer:{identity}:{index}",))


def transfer_id(intent_id, suffix):
    return hashlib.sha256((intent_id + suffix).encode()).hexdigest()


def preview_job(intent_id="intent"):
    manifest = {"plan": {"stages": [{"id": "measure", "tensorboard": {"log_dir": "tensorboard/measure"}}]}}
    return {
        "intent_id": intent_id,
        "job_id": "101",
        "stage_id": "measure",
        "execution_manifest": encode(manifest),
        "snapshot_hash": "snapshot",
    }


def remote_scheduler(wf, exp):
    remote = RemoteSlurmScheduler(wf, exp)

    async def resolve():
        return {"host": "synthetic"}

    remote.resolve = resolve
    return remote


def fetch_scheduler(wf, exp, tmp_path, *, transport):
    """Saved remote layout for one fetch, with the copy boundary replaced by ``transport``."""
    remote = RemoteSlurmScheduler(wf, exp)
    layout = {"host": "jz", "remote_run": str(tmp_path / "remote"), "local_run": str(tmp_path / "local")}
    Path(layout["local_run"]).mkdir()

    async def resolve():
        return layout

    async def rsync(*args):
        await transport(args)

    remote.resolve, remote.rsync = resolve, rsync
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


async def test_preview_refresh_replays_exact_job_identity_at_unchanged_cadence(runtime, tmp_path):
    store, _, _, wf = runtime
    exp = create(store)
    copies = []

    async def transport(args):
        copies.append(args)

    remote = fetch_scheduler(wf, exp, tmp_path, transport=transport)
    job = preview_job()
    for _ in range(2):
        await remote.fetch(job, Observation("RUNNING"))
    identity = transfer_id("intent", ":preview")
    assert store.rows("SELECT id,state FROM execution_transfers WHERE direction='down'") == [
        {"id": identity, "state": "complete"}
    ]
    assert [
        (row["id"], row["state"])
        for row in store.rows(
            "SELECT id,state FROM obligations WHERE id LIKE ? ORDER BY id", (f"transfer:{identity}:%",)
        )
    ] == [(f"transfer:{identity}:0", "satisfied"), (f"transfer:{identity}:1", "satisfied")]
    # Every refresh still copies the optional event preview; no cadence was suppressed.
    assert len([args for args in copies if "--max-size=512m" in args]) == 2


async def test_preview_failure_keeps_one_identity_and_episode_age_across_restart(runtime):
    store, _, _, wf = runtime
    exp = create(store)
    identity = transfer_id("intent", ":preview")

    async def broken():
        raise OSError("synthetic preview transport issue")

    with pytest.raises(TransferPending):
        await remote_scheduler(wf, exp).transfer("down", broken, identity=identity)
    first = episode(store, identity)
    with pytest.raises(TransferPending):
        await remote_scheduler(wf, store.experiment(exp["id"])).transfer("down", broken, identity=identity)
    retried = episode(store, identity)
    assert (retried["id"], retried["created_at"]) == (first["id"], first["created_at"])
    assert not store.one("SELECT 1 FROM obligations WHERE id=?", (f"transfer:{identity}:1",))


async def test_terminal_delivery_supersedes_only_its_own_pending_preview(runtime, tmp_path):
    store, _, _, wf = runtime
    exp = create(store)
    mine, other = transfer_id("intent", ":preview"), transfer_id("other", ":preview")
    for identity in (mine, other):
        insert(store, identity, exp, direction="down")
        with store.tx() as db:
            transfer_obligations.sync_transfer(db, identity)

    async def unavailable(args):
        raise RuntimeError("synthetic download unavailable")

    remote = fetch_scheduler(wf, exp, tmp_path, transport=unavailable)
    with pytest.raises(TransferPending):
        await remote.fetch(preview_job(), Observation("COMPLETED", "0:0"))
    assert store.one("SELECT state,result FROM execution_transfers WHERE id=?", (mine,)) == {
        "state": "cancelled",
        "result": "Superseded by terminal delivery",
    }
    assert (
        store.one("SELECT state FROM obligations WHERE id=?", (f"transfer:{mine}:0",))["state"] == "abandoned"
    )
    assert store.one("SELECT state FROM execution_transfers WHERE id=?", (other,))["state"] == "pending"
    assert store.one("SELECT state FROM obligations WHERE id=?", (f"transfer:{other}:0",))["state"] == "open"
    delivery = transfer_id("intent", ":terminal-delivery")
    assert store.one("SELECT state FROM execution_transfers WHERE id=?", (delivery,))["state"] == "pending"


async def test_preview_admission_after_terminal_delivery_is_refused(runtime, tmp_path):
    store, _, _, wf = runtime
    exp = create(store)
    delivery = transfer_id("intent", ":terminal-delivery")
    insert(store, delivery, exp, direction="down")
    copied = []

    async def transport(args):
        copied.append(args)

    remote = fetch_scheduler(wf, exp, tmp_path, transport=transport)
    with pytest.raises(TransferCancelled, match="preview refused"):
        await remote.fetch(preview_job(), Observation("RUNNING"))
    assert copied == []
    assert store.one("SELECT state FROM execution_transfers WHERE id=?", (delivery,))["state"] == "pending"
    assert not store.one("SELECT 1 FROM execution_transfers WHERE id=?", (transfer_id("intent", ":preview"),))
