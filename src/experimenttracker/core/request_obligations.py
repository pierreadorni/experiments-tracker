"""Map a saved pending owner request to one responsibility episode.

Call ``sync_request`` inside the transaction that changes
``experiments.pending_request``. It records only who owes the answer; it never
replies, resumes work or changes source state.
"""

from . import obligation_sources

_SOURCE = "request"
_LIVE_STATES = frozenset({"awaiting_answer"})
_SATISFIED_STATES = frozenset({"answered"})
_ABANDONED_STATES = frozenset({"abandoned"})


def saved_pending(db, experiment_id):
    """Return the saved pending request decoded, read in the caller's transaction."""
    return obligation_sources.saved_json(db, experiment_id, "pending_request", _request)


def sync_request(db, experiment_id, *, before):
    """Synchronize the experiment's saved pending request in its transaction.

    ``before`` is the request decoded before the caller's write. A saved
    unanswered request holds one open episode; recording the owner's answer
    satisfies it, and clearing or replacing it without that answer abandons it.
    """
    obligation_sources.require_transaction(db)
    if not isinstance(experiment_id, str) or not experiment_id:
        raise ValueError("experiment_id must be a non-empty string")
    project_id = obligation_sources.source_project_id(db, experiment_id, "experiment")
    previous = _request(before)
    current = saved_pending(db, experiment_id)
    if previous is not None and (current is None or current["id"] != previous["id"]):
        state = "answered" if previous.get("answer") else "abandoned"
        _sync(db, experiment_id, previous, project_id, state)
    if current is None:
        return None
    state = "answered" if current.get("answer") else "awaiting_answer"
    return _sync(db, experiment_id, current, project_id, state)


def _sync(db, experiment_id, request, project_id, state):
    return obligation_sources.sync_episode(
        db,
        _SOURCE,
        f"{experiment_id}:{request['id']}",
        state=state,
        live_states=_LIVE_STATES,
        satisfied_states=_SATISFIED_STATES,
        abandoned_states=_ABANDONED_STATES,
        experiment_id=experiment_id,
        project_id=project_id,
        kind="owner_answer",
        owner="owner",
        wait=f"request:{request['id']}",
    )


def _request(value):
    if value is None:
        return None
    if not isinstance(value, dict) or not isinstance(value.get("id"), str) or not value["id"]:
        raise ValueError("saved pending request must be an object with a non-empty id")
    return value
