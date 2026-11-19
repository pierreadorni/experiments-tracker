import sqlite3

import pytest
from helpers import seed_saved_contract3

from experimenttracker.retention import compact_snapshot, compare, prune_batch, verify
from experimenttracker.store import encode


def history(store):
    eid = seed_saved_contract3(store, "Synthetic retention fixture")
    with store.tx() as db:
        store.emit(db, eid, "experiment_created")
    assert store.experiment(eid)["contract"]["execution_contract"] == 3
    # Legacy owners persisted a complete view per event. Keep these fixtures
    # explicit now that emit writes references only.
    for i in range(6):
        store.update(eid, error=str(i))
    with store.tx() as db:
        for event in store.rows("SELECT id,revision FROM events WHERE experiment_id=?", (eid,)):
            view = store.projection(eid)
            view["revision"] = event["revision"]
            db.execute(
                "UPDATE outbox SET payload=? WHERE id=?",
                (encode({"event_id": event["id"], "experiment": view}), event["id"]),
            )
    rows = store.rows(
        "SELECT o.*,e.revision FROM outbox o JOIN events e ON e.id=o.id WHERE e.experiment_id=? ORDER BY e.seq",
        (eid,),
    )
    return eid, rows


def sweep(store, **kwargs):
    cursor = total = 0
    while True:
        result = prune_batch(store, cursor, **kwargs)
        total += result["pruned"]
        cursor = result["cursor"]
        if not cursor:
            return total


def test_receipt_and_pending_snapshot_survive_partial_sweeps_and_restart(runtime):
    store, *_ = runtime
    _, rows = history(store)
    receipt = rows[-3]
    with store.tx() as db:
        db.executemany("UPDATE outbox SET delivered_at=123 WHERE id=?", [(r["id"],) for r in rows[:-2]])
    before = {r["id"]: dict(r) for r in store.rows("SELECT * FROM outbox")}
    assert sweep(store, limit=1) == len(rows) - 3
    assert sweep(store, limit=2) == 0
    # A new process starts its sweep from zero; no uncertain payload is cleared.
    for r in rows[-3:]:
        assert store.one("SELECT payload FROM outbox WHERE id=?", (r["id"],))["payload"] == r["payload"]
    for r in store.rows("SELECT * FROM outbox"):
        assert {k: v for k, v in r.items() if k != "payload"} == {
            k: v for k, v in before[r["id"]].items() if k != "payload"
        }
    with store.tx() as db:
        db.execute("UPDATE outbox SET delivered_at=124 WHERE id=?", (rows[-1]["id"],))
    assert sweep(store) == 1
    assert store.one("SELECT payload FROM outbox WHERE id=?", (receipt["id"],))["payload"] == "{}"
    assert (
        store.one("SELECT payload FROM outbox WHERE id=?", (rows[-2]["id"],))["payload"]
        == rows[-2]["payload"]
    )


def test_no_ack_and_corrupt_receipt_abort_without_pruning(runtime):
    store, *_ = runtime
    _, rows = history(store)
    assert sweep(store) == 0
    with store.tx() as db:
        db.execute("UPDATE outbox SET delivered_at=1")
        db.execute("UPDATE outbox SET payload=? WHERE id=?", ('{"invalid":true}', rows[-1]["id"]))
    with pytest.raises(ValueError, match="protected"):
        prune_batch(store)
    assert (
        store.one("SELECT payload FROM outbox WHERE id=?", (rows[0]["id"],))["payload"] == rows[0]["payload"]
    )


def test_compaction_preserves_domain_and_receipts_and_refuses_overwrite(runtime, tmp_path):
    store, *_ = runtime
    _, rows = history(store)
    with store.tx() as db:
        db.execute("UPDATE outbox SET delivered_at=1")
        # Representative transport repetition makes file-size verification meaningful.
        for row in rows[:-1]:
            db.execute(
                "UPDATE outbox SET payload=? WHERE id=?", ('{"padding":"' + "x" * 200000 + '"}', row["id"])
            )
    backup = tmp_path / "backup.sqlite"
    with sqlite3.connect(backup) as dest:
        store.db.backup(dest)
    output = tmp_path / "compact.sqlite"
    result = compact_snapshot(backup, output)
    assert result["output_bytes"] < result["source_bytes"] / 2
    assert output.stat().st_mode & 0o777 == 0o600
    with sqlite3.connect(backup) as original, sqlite3.connect(output) as compact:
        verify(compact)
        compare(original, compact)
        assert original.execute("SELECT count(*) FROM outbox WHERE payload='{}'").fetchone()[0] == 0
        compact.execute("UPDATE experiments SET pause_agent=1-pause_agent")
        with pytest.raises(ValueError, match="experiments"):
            compare(original, compact)
    with pytest.raises(ValueError):
        compact_snapshot(backup, output)


def test_empty_store_and_retired_experiments_keep_receipts(runtime):
    store, *_ = runtime
    assert sweep(store) == 0
    for phase, deleted in (("cancelled", None), ("complete", 123)):
        eid, rows = history(store)
        with store.tx() as db:
            db.execute("UPDATE experiments SET phase=?,deleted_at=? WHERE id=?", (phase, deleted, eid))
            db.execute(
                "UPDATE outbox SET delivered_at=123 WHERE id IN (SELECT id FROM events WHERE experiment_id=?)",
                (eid,),
            )
        assert sweep(store, limit=2) == len(rows) - 1
        assert (
            store.one("SELECT payload FROM outbox WHERE id=?", (rows[-1]["id"],))["payload"]
            == rows[-1]["payload"]
        )
