"""Map a saved plan-review generation to one owner-approval responsibility episode.

Call ``sync_review`` inside the transaction that changes
``experiments.plan_review``. It records only who owes the decision; it never
approves, revises or dispatches work.
"""

from . import obligation_sources

_SOURCE = "review"
_LIVE_STATES = frozenset({"awaiting_review"})
_SATISFIED_STATES = frozenset({"approved"})
_ENDED_STATES = frozenset({"cancelled"})
_SUPERSEDED_STATES = _ENDED_STATES | _LIVE_STATES | {"drafting"}


def saved_review(db, experiment_id):
    """Return the saved plan review decoded, read in the caller's transaction."""
    return obligation_sources.saved_json(db, experiment_id, "plan_review", _review)


def sync_review(db, experiment_id, *, before):
    """Synchronize the experiment's saved plan review in its transaction.

    ``before`` is the review decoded before the caller's write. An awaiting
    generation holds one open episode; approval satisfies it, and cancellation
    or a replacement by another revision abandons that generation. A drafting
    revision owes nothing until it is saved as awaiting review again.
    """
    obligation_sources.require_transaction(db)
    if not isinstance(experiment_id, str) or not experiment_id:
        raise ValueError("experiment_id must be a non-empty string")
    project_id = obligation_sources.source_project_id(db, experiment_id, "experiment")
    previous = _review(before)
    current = saved_review(db, experiment_id)
    if previous is not None and (current is None or current["revision"] != previous["revision"]):
        _sync(db, experiment_id, previous, project_id, live=frozenset(), ended=_SUPERSEDED_STATES)
    if current is None or current["state"] == "drafting":
        return None
    return _sync(db, experiment_id, current, project_id, live=_LIVE_STATES, ended=_ENDED_STATES)


def _sync(db, experiment_id, review, project_id, *, live, ended):
    return obligation_sources.sync_episode(
        db,
        _SOURCE,
        f"{experiment_id}:{review['revision']}",
        state=review["state"],
        live_states=live,
        satisfied_states=_SATISFIED_STATES,
        abandoned_states=ended,
        experiment_id=experiment_id,
        project_id=project_id,
        kind="owner_approval",
        owner="owner",
        wait=f"review:{experiment_id}:{review['revision']}",
    )


def _review(value):
    if value is None:
        return None
    revision = value.get("revision") if isinstance(value, dict) else None
    state = value.get("state") if isinstance(value, dict) else None
    if (
        isinstance(revision, bool)
        or not isinstance(revision, int)
        or revision < 1
        or not isinstance(state, str)
        or not state
    ):
        raise ValueError("saved plan review must carry a positive revision and a state")
    return value
