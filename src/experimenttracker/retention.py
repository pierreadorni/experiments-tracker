"""Conservative transport retention. Scientific/domain records are never pruned."""

import argparse
import json
import os
import sqlite3
import time
from pathlib import Path


def protected(db, eid):
    rows = db.execute(
        "SELECT e.id,e.revision,o.payload FROM experiments x JOIN events e "
        "ON e.experiment_id=x.id LEFT JOIN outbox o ON o.id=e.id "
        "WHERE x.id=? AND (e.revision=x.revision OR e.revision=("
        "SELECT MAX(e2.revision) FROM events e2 JOIN outbox o2 ON o2.id=e2.id "
        "WHERE e2.experiment_id=x.id AND o2.delivered_at IS NOT NULL))",
        (eid,),
    ).fetchall()
    current = db.execute("SELECT revision FROM experiments WHERE id=?", (eid,)).fetchone()[0]
    if current and not any(r[1] == current for r in rows):
        raise ValueError("missing current outbox snapshot")
    for identity, revision, payload in rows:
        message = json.loads(payload or "null")
        if (
            not isinstance(message, dict)
            or message.get("event_id") != identity
            or message.get("experiment", {}).get("id") != eid
            or message.get("experiment", {}).get("revision") != revision
        ):
            raise ValueError("invalid protected outbox snapshot")
    return {r[0] for r in rows}


def prune_batch(store, after_seq=0, *, limit=100, char_budget=16 * 1024 * 1024):
    """Visit bounded indexed identities, with eligibility checked under the Store lock.

    The cursor also advances past protected/pending/previously cleared rows, so
    maintenance never rescans the entire cleared backlog on each acknowledgement.
    A later sweep revisits rows whose acknowledgement changed since this sweep.
    """
    count = chars = 0
    cursor = after_seq
    with store.tx() as db:
        rows = db.execute(
            "SELECT e.seq,e.id,e.experiment_id,o.delivered_at FROM events e "
            "JOIN outbox o ON o.id=e.id WHERE e.seq>? ORDER BY e.seq LIMIT ?",
            (after_seq, limit),
        ).fetchall()
        guards = {}
        for seq, identity, eid, delivered in rows:
            if eid not in guards:
                guards[eid] = protected(db, eid)
            if delivered is not None and identity not in guards[eid]:
                size = db.execute("SELECT length(payload) FROM outbox WHERE id=?", (identity,)).fetchone()[0]
                if size > 2:
                    if chars and chars + size > char_budget:
                        break
                    db.execute("UPDATE outbox SET payload='{}' WHERE id=?", (identity,))
                    count += 1
                    chars += size - 2
            cursor = seq
    return {"cursor": cursor if rows else 0, "pruned": count, "characters_reclaimed": chars}


def verify(db):
    if db.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
        raise ValueError("database integrity check failed")
    if db.execute("PRAGMA foreign_key_check").fetchall():
        raise ValueError("database foreign key check failed")
    if db.execute(
        "SELECT 1 FROM outbox o LEFT JOIN events e ON e.id=o.id WHERE e.id IS NULL LIMIT 1"
    ).fetchone():
        raise ValueError("orphan outbox receipt")
    if db.execute(
        "SELECT 1 FROM events e LEFT JOIN outbox o ON o.id=e.id WHERE o.id IS NULL LIMIT 1"
    ).fetchone():
        raise ValueError("missing outbox receipt")
    for (eid,) in db.execute("SELECT id FROM experiments"):
        protected(db, eid)


def compare(source, target):
    """Stream exact logical rows; compaction changes pages, not domain values."""
    if (
        source.execute("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name").fetchall()
        != target.execute("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name").fetchall()
    ):
        raise ValueError("schema changed")
    guards = set()
    for (eid,) in source.execute("SELECT id FROM experiments"):
        guards.update(protected(source, eid))
    for (table,) in source.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
        quoted = '"' + table.replace('"', '""') + '"'
        columns = [r[1] for r in source.execute(f"PRAGMA table_info({quoted})")]
        ordering = ",".join(str(i + 1) for i in range(len(columns)))
        left = source.execute(f"SELECT * FROM {quoted} ORDER BY {ordering}")
        right = target.execute(f"SELECT * FROM {quoted} ORDER BY {ordering}")
        while True:
            a, b = left.fetchone(), right.fetchone()
            if a is None and b is None:
                break
            if table == "outbox" and a and b and a != b:
                identity = a[columns.index("id")]
                payload = columns.index("payload")
                delivered = a[columns.index("delivered_at")]
                if identity not in guards and delivered is not None and b[payload] == "{}":
                    a = tuple(b[payload] if i == payload else v for i, v in enumerate(a))
            if a != b:
                raise ValueError(f"logical comparison failed: {table}")


def compact_snapshot(source_path, output):
    """Create a verified compacted derivative; never replace or modify the source.

    The input must be a completed backup or a stopped owner held by its lock.
    Private siblings remain available for investigation after interruption.
    """
    from .store import Store

    source_path, output = Path(source_path).absolute(), Path(output).absolute()
    if source_path.is_symlink() or output.exists() or output.is_symlink():
        raise ValueError("require regular input and a new output path")
    if not source_path.is_file():
        raise ValueError("missing source database")
    output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    size = source_path.stat().st_size
    import shutil

    if shutil.disk_usage(output.parent).free < size * 3:
        raise ValueError("insufficient disk for working copy, compact result and SQLite temporary files")
    work = output.with_name(output.name + ".working")
    fd = os.open(work, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(fd)
    started = time.monotonic()
    source = sqlite3.connect(f"{source_path.as_uri()}?mode=ro", uri=True)
    # Hold a stable read snapshot through backup and logical comparison.
    source.execute("BEGIN")
    try:
        verify(source)
        with sqlite3.connect(work) as dest:
            source.backup(dest)
        store = Store.read_only(work)
        store.db.close()
        store.db = sqlite3.connect(work, isolation_level=None, check_same_thread=False)
        try:
            cursor = total = reclaimed = 0
            while True:
                batch = prune_batch(store, cursor)
                total += batch["pruned"]
                reclaimed += batch["characters_reclaimed"]
                cursor = batch["cursor"]
                if not cursor:
                    break
            # SQLite creates the output itself; restrictive umask prevents exposure.
            output_fd = os.open(output, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.close(output_fd)
            mask = os.umask(0o077)
            try:
                store.db.execute("VACUUM INTO ?", (str(output),))
            finally:
                os.umask(mask)
        finally:
            store.close()
        with sqlite3.connect(f"{output.as_uri()}?mode=ro", uri=True) as result:
            verify(result)
            compare(source, result)
        with output.open("rb") as handle:
            os.fsync(handle.fileno())
        directory = os.open(output.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        work.unlink()
        return {
            "pruned": total,
            "characters_reclaimed": reclaimed,
            "source_bytes": size,
            "output_bytes": output.stat().st_size,
            "seconds": round(time.monotonic() - started, 2),
        }
    finally:
        source.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--backup", type=Path, required=True, help="completed backup, never the live database"
    )
    parser.add_argument("--output", type=Path, help="create a compacted derivative; source is never modified")
    args = parser.parse_args()
    if args.output:
        result = compact_snapshot(args.backup, args.output)
    else:
        db = sqlite3.connect(f"{args.backup.absolute().as_uri()}?mode=ro", uri=True)
        try:
            verify(db)
            guards = set()
            for (eid,) in db.execute("SELECT id FROM experiments"):
                guards.update(protected(db, eid))
            count = chars = pending = 0
            for identity, size, delivered in db.execute("SELECT id,length(payload),delivered_at FROM outbox"):
                pending += delivered is None
                if delivered is not None and identity not in guards and size > 2:
                    count += 1
                    chars += size - 2
            result = {"candidate_rows": count, "reclaimable_characters": chars, "pending_rows": pending}
        finally:
            db.close()
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
