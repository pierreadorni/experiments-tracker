"""Map a saved pending owner request to one responsibility episode.

Call ``sync_request`` inside the transaction that changes
``experiments.pending_request``. It records only who owes the answer; it never
replies, resumes work or changes source state.
"""

import json

from . import obligation_sources

_SOURCE = "request"


def saved_pending(db, experiment_id):
    """Return the saved pending request decoded, read in the caller's transaction."""
    if not db.in_transaction:
        raise RuntimeError("request obligation read requires the caller's active transaction")
    row = db.execute("SELECT pending_request FROM experiments WHERE id=?", (experiment_id,)).fetchone()
    if row is None:
        raise ValueError(f"saved experiment does not exist: {experiment_id}")
    return _request(json.loads(row["pending_request"]) if row["pending_request"] else None)


def sync_request(db, experiment_id, *, before):
    """Synchronize the experiment's saved pending request in its transaction.

    ``before`` is the request decoded before the caller's write. A saved
    unanswered request holds one open episode; recording the owner's answer
    satisfies it, and clearing or replacing it without that answer abandons it.
    """
    if not db.in_transaction:
        raise RuntimeError("request obligation sync requires the caller's active transaction")
    if not isinstance(experiment_id, str) or not experiment_id:
        raise ValueError("experiment_id must be a non-empty string")
    experiment = db.execute("SELECT contract FROM experiments WHERE id=?", (experiment_id,)).fetchone()
    if experiment is None:
        raise ValueError(f"saved experiment does not exist: {experiment_id}")
    previous = _request(before)
    current = saved_pending(db, experiment_id)
    if previous is not None and (current is None or current["id"] != previous["id"]):
        _terminalize(db, experiment_id, previous, "satisfy" if previous.get("answer") else "abandon")
    if current is None:
        return None
    if current.get("answer"):
        return _terminalize(db, experiment_id, current, "satisfy")
    return _open(db, experiment_id, current, json.loads(experiment["contract"]))


def _open(db, experiment_id, request, contract):
    saved_id = _saved_id(experiment_id, request)
    latest = obligation_sources.latest_episode(db, _SOURCE, saved_id)
    if latest is not None and latest["state"] != "open":
        raise ValueError("live saved owner request conflicts with terminal obligation history")
    return obligation_sources.open_episode(
        db,
        _SOURCE,
        saved_id,
        experiment_id=experiment_id,
        project_id=(contract.get("project") or {}).get("id"),
        kind="owner_answer",
        target=saved_id,
        owner="owner",
        wait=f"request:{request['id']}",
    )


def _terminalize(db, experiment_id, request, operation):
    saved_id = _saved_id(experiment_id, request)
    latest = obligation_sources.latest_episode(db, _SOURCE, saved_id)
    if latest is None or latest["state"] != "open":
        return latest
    episode = int(latest["id"].rsplit(":", 1)[1])
    settle = (
        obligation_sources.satisfy_episode if operation == "satisfy" else obligation_sources.abandon_episode
    )
    return settle(db, _SOURCE, saved_id, episode)


def _saved_id(experiment_id, request):
    return f"{experiment_id}:{request['id']}"


def _request(value):
    if value is None:
        return None
    if not isinstance(value, dict) or not isinstance(value.get("id"), str) or not value["id"]:
        raise ValueError("saved pending request must be an object with a non-empty id")
    return value
