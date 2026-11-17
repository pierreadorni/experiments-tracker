"""Stable responsibility episodes for obligations attached to saved sources.

Call these helpers inside the same transaction as the source transition. A caller
may set ``source_transition`` only after proving that transition from saved state.
"""

from . import obligations

_FIXED_FIELDS = ("experiment_id", "project_id", "kind", "target", "owner")
_SOURCE_FIELDS = ("experiment_id", "project_id", "target")


def episode_id(source_type, saved_id, episode):
    """Return the stable ID for one saved-source responsibility episode."""
    _source_parts(source_type, saved_id)
    if isinstance(episode, bool) or not isinstance(episode, int) or episode < 0:
        raise ValueError("episode must be a non-negative integer")
    return f"{source_type}:{saved_id}:{episode}"


def latest_episode(db, source_type, saved_id):
    """Return the latest saved episode, without creating or changing anything.

    Restart callbacks must separately verify the exact current source identity
    and state before terminalizing the returned open row. A missing row stays
    missing; a terminal row is returned as terminal for the caller to inspect.
    """
    _require_transaction(db)
    _source_parts(source_type, saved_id)
    _, latest = _history(db, source_type, saved_id)
    return latest


def open_episode(
    db,
    source_type,
    saved_id,
    *,
    source_transition=False,
    experiment_id=None,
    project_id=None,
    kind,
    target=None,
    owner,
    wait=None,
    detail=None,
    due_at=None,
):
    """Open or replay a source episode without reopening a terminal ID.

    ``source_transition=True`` is required to advance past a terminal episode or
    to change immutable responsibility. It is a caller assertion backed by the
    source's old and new saved state in this transaction; replay/backfill must
    leave it false. Mutable wait/detail/due changes reuse the open episode.
    """
    _require_transaction(db)
    _source_parts(source_type, saved_id)
    if not isinstance(source_transition, bool):
        raise ValueError("source_transition must be a boolean")
    wanted = {
        "experiment_id": experiment_id,
        "project_id": project_id,
        "kind": kind,
        "target": target,
        "owner": owner,
    }
    episodes, latest = _history(db, source_type, saved_id)
    rows = list(episodes.values())
    if any(row[field] != wanted[field] for row in rows for field in _SOURCE_FIELDS):
        raise ValueError("obligation source scope or target cannot change across episodes")
    opened = [row for row in rows if row["state"] == "open"]
    if len(opened) > 1 or (opened and opened[0]["id"] != latest["id"]):
        raise ValueError(f"conflicting open obligation history for {source_type}:{saved_id}")

    if opened:
        current = opened[0]
        if _same_responsibility(current, wanted):
            return obligations.open(db, current["id"], **wanted, wait=wait, detail=detail, due_at=due_at)
        if not source_transition:
            raise ValueError("changing obligation responsibility requires a proven source transition")
        return _handoff(db, current, source_type, saved_id, max(episodes) + 1, wanted, wait, detail, due_at)
    elif latest is None:
        next_episode = 0
    elif not source_transition:
        if not _same_responsibility(latest, wanted):
            raise ValueError("terminal obligation history requires a proven source transition")
        return obligations.open(db, latest["id"], **wanted, wait=wait, detail=detail, due_at=due_at)
    else:
        next_episode = max(episodes) + 1

    return obligations.open(
        db,
        episode_id(source_type, saved_id, next_episode),
        **wanted,
        wait=wait,
        detail=detail,
        due_at=due_at,
    )


def sync_episode(
    db,
    source_type,
    saved_id,
    *,
    state,
    live_states,
    satisfied_states=frozenset(),
    abandoned_states=frozenset(),
    source_transition=False,
    experiment_id=None,
    project_id=None,
    kind,
    owner,
    wait=None,
    detail=None,
    due_at=None,
):
    """Synchronize a saved source's responsibility with its exact saved state.

    Any live state opens or replays the one episode covering that progress. A
    satisfied or abandoned state settles only an already-open episode, and a
    terminal state with no history stays without history.
    """
    _require_transaction(db)
    _source_parts(source_type, saved_id)
    if state in live_states:
        row = open_episode(
            db,
            source_type,
            saved_id,
            source_transition=source_transition,
            experiment_id=experiment_id,
            project_id=project_id,
            kind=kind,
            target=saved_id,
            owner=owner,
            wait=wait,
            detail=detail,
            due_at=due_at,
        )
        if row["state"] != "open":
            raise ValueError(f"live {source_type} state conflicts with terminal obligation history")
        return row
    if state in satisfied_states:
        settle = satisfy_episode
    elif state in abandoned_states:
        settle = abandon_episode
    else:
        raise ValueError(f"unsupported saved {source_type} state: {state}")
    latest = latest_episode(db, source_type, saved_id)
    if latest is None or latest["state"] != "open":
        return latest
    return settle(db, source_type, saved_id, int(latest["id"].rsplit(":", 1)[1]))


def satisfy_episode(db, source_type, saved_id, episode):
    _require_transaction(db)
    return obligations.satisfy(db, episode_id(source_type, saved_id, episode))


def abandon_episode(db, source_type, saved_id, episode):
    _require_transaction(db)
    return obligations.abandon(db, episode_id(source_type, saved_id, episode))


def _same_responsibility(row, wanted):
    return all(row[field] == wanted[field] for field in _FIXED_FIELDS)


def _history(db, source_type, saved_id):
    prefix = f"{source_type}:{saved_id}:"
    source_key = f"{source_type}:{saved_id}"
    rows = [
        dict(row)
        for row in db.execute(
            "SELECT * FROM obligations WHERE substr(id,1,?)=? ORDER BY id", (len(prefix), prefix)
        )
        if row["id"].rsplit(":", 1)[0] == source_key
    ]
    episodes = {}
    for row in rows:
        suffix = row["id"].rsplit(":", 1)[1]
        if not suffix.isdigit() or str(int(suffix)) != suffix:
            raise ValueError(f"invalid obligation episode history for {source_type}:{saved_id}")
        episode = int(suffix)
        if episode in episodes:
            raise ValueError(f"duplicate obligation episode history for {source_type}:{saved_id}")
        episodes[episode] = row
    if episodes and len(episodes) != max(episodes) + 1:
        raise ValueError(f"gapped obligation episode history for {source_type}:{saved_id}")
    if any(row[field] != rows[0][field] for row in rows for field in _SOURCE_FIELDS):
        raise ValueError(f"conflicting source scope or target history for {source_type}:{saved_id}")
    return episodes, episodes[max(episodes)] if episodes else None


def _handoff(db, current, source_type, saved_id, episode, wanted, wait, detail, due_at):
    """Keep a caller-caught validation error from half-applying a handoff."""
    db.execute("SAVEPOINT obligation_episode_handoff")
    try:
        obligations.abandon(db, current["id"])
        row = obligations.open(
            db,
            episode_id(source_type, saved_id, episode),
            **wanted,
            wait=wait,
            detail=detail,
            due_at=due_at,
        )
    except BaseException:
        db.execute("ROLLBACK TO obligation_episode_handoff")
        db.execute("RELEASE obligation_episode_handoff")
        raise
    db.execute("RELEASE obligation_episode_handoff")
    return row


def _source_parts(source_type, saved_id):
    if not isinstance(source_type, str) or not source_type or ":" in source_type:
        raise ValueError("source_type must be a non-empty string without ':'")
    if not isinstance(saved_id, str) or not saved_id:
        raise ValueError("saved_id must be a non-empty string")


def _require_transaction(db):
    if not db.in_transaction:
        raise RuntimeError("obligation source helpers require the caller's active transaction")
