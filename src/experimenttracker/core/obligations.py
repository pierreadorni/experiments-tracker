"""C2 obligations: who or what a saved source record is waiting on, and why.

The caller owns the transaction that changes a source record; these helpers only
read and write the `obligations` row inside it and never commit, emit or dispatch.
"""

from math import isfinite

from .. import clock

KINDS = frozenset(
    {
        "agent_turn",
        "job",
        "transfer",
        "owner_answer",
        "owner_approval",
        "publication",
        "integration",
        "reconcile",
        "timer",
    }
)
OWNERS = frozenset({"backend", "agent", "owner", "external"})
# Highest first; ties fall back to due time (null last), created time, then ID.
OWNER_PRIORITY = {"owner": 0, "agent": 1, "backend": 2, "external": 3}
_FIXED_FIELDS = ("experiment_id", "project_id", "kind", "target", "owner")


def open(
    db,
    obligation_id,
    *,
    experiment_id=None,
    project_id=None,
    kind,
    target=None,
    owner,
    wait=None,
    detail=None,
    due_at=None,
):
    """Save one open obligation, or replay a saved ID without rebinding it.

    The ID, its scope, kind, target and owner are immutable: a mismatch raises
    `ValueError`, including on a terminal row. A terminal row is returned as
    saved, never reopened. An open row takes the caller's new wait/detail/due
    only where they differ, so unchanged timestamps survive a replay. Callers
    must inspect the returned row's state.
    """
    require_transaction(db)
    _text(obligation_id, "obligation_id")
    _text(kind, "kind")
    _text(owner, "owner")
    if kind not in KINDS:
        raise ValueError(f"unknown obligation kind: {kind}")
    if owner not in OWNERS:
        raise ValueError(f"unknown obligation owner: {owner}")
    _optional_text(experiment_id, "experiment_id")
    _optional_text(project_id, "project_id")
    _optional_text(target, "target")
    _optional_text(wait, "wait")
    _optional_text(detail, "detail")
    _optional_time(due_at, "due_at")
    wanted = {
        "experiment_id": experiment_id,
        "project_id": project_id,
        "kind": kind,
        "target": target,
        "owner": owner,
    }
    row = _row(db, obligation_id)
    if row is not None:
        for field in _FIXED_FIELDS:
            if row[field] != wanted[field]:
                raise ValueError(f"obligation {obligation_id} is already bound to another {field}")
        if row["state"] != "open":
            return row
        changes = {"wait": wait, "detail": detail, "due_at": due_at}
        if all(row[field] == value for field, value in changes.items()):
            return row
        return _update(db, obligation_id, {**changes, "updated_at": clock.now()})
    now = clock.now()
    return _insert(
        db,
        {
            "id": obligation_id,
            **wanted,
            "wait": wait,
            "state": "open",
            "detail": detail,
            "created_at": now,
            "updated_at": now,
            "due_at": due_at,
        },
    )


def satisfy(db, obligation_id):
    """Record the obligation as satisfied; `None` if it was never saved."""
    return _transition(db, obligation_id, "satisfied")


def abandon(db, obligation_id):
    """Record the obligation as abandoned; `None` if it was never saved."""
    return _transition(db, obligation_id, "abandoned")


def open_for(db, *, experiment_id=None, project_id=None):
    """Read open rows for the supplied scope, oldest first; both filters AND."""
    sql = "SELECT * FROM obligations WHERE state='open'"
    args = []
    for field, value in (("experiment_id", experiment_id), ("project_id", project_id)):
        if value is not None:
            sql += f" AND {field}=?"
            args.append(value)
    return [dict(row) for row in db.execute(sql + " ORDER BY created_at,id", args)]


def derived_status(rows):
    """The highest-priority open row as a dict, or `None` when nothing is open."""
    open_rows = [row for row in rows if row["state"] == "open"]
    return dict(min(open_rows, key=_rank)) if open_rows else None


def _rank(row):
    due_at = row["due_at"]
    return (OWNER_PRIORITY[row["owner"]], due_at is None, due_at or 0.0, row["created_at"], row["id"])


def _transition(db, obligation_id, state):
    require_transaction(db)
    _text(obligation_id, "obligation_id")
    row = _row(db, obligation_id)
    if row is None or row["state"] != "open":
        return row
    return _update(db, obligation_id, {"state": state, "updated_at": clock.now()})


def require_transaction(db):
    """Refuse an obligation read or write made outside the caller's transaction."""
    if not db.in_transaction:
        raise RuntimeError("obligations require the caller's active transaction")


def _row(db, obligation_id):
    row = db.execute("SELECT * FROM obligations WHERE id=?", (obligation_id,)).fetchone()
    return None if row is None else dict(row)


def _insert(db, fields):
    columns = ",".join(fields)
    db.execute(
        f"INSERT INTO obligations({columns}) VALUES({','.join('?' * len(fields))})",
        tuple(fields.values()),
    )
    return _row(db, fields["id"])


def _update(db, obligation_id, fields):
    assignments = ",".join(f"{field}=?" for field in fields)
    db.execute(f"UPDATE obligations SET {assignments} WHERE id=?", (*fields.values(), obligation_id))
    return _row(db, obligation_id)


def _text(value, name):
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _optional_text(value, name):
    if value is not None and not isinstance(value, str):
        raise ValueError(f"{name} must be a string or None")
    return value


def _optional_time(value, name):
    if value is not None:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{name} must be a number or None")
        try:
            finite = isfinite(value)
        except OverflowError:  # an int beyond float range cannot be a stored time
            finite = False
        if not finite:
            raise ValueError(f"{name} must be finite")
    return value
