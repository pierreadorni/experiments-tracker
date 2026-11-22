"""Close saved advancement lifetimes and run the shared cancellation responsibility.

Contract 3 opened one backend advancement responsibility per experiment; frozen
records may still hold one, and a terminal phase closes it. Nothing opens a
lifetime any more. Cancellation opens its own reconciliation responsibility for
every contract. Callers own the transaction; this module never dispatches work
or creates schema.
"""

import json

from . import obligation_sources

SOURCE_TYPE = "advancement"
CANCELLATION = "cancellation"
_TERMINAL = frozenset({"complete", "failed", "cancelled"})


def close_lifetime(db, experiment_id, outcome):
    """Close the open lifetime: complete satisfies it, otherwise it is abandoned.

    The last saved decision stays on the closed episode as the final record.
    """
    obligation_sources.require_transaction(db)
    if outcome not in _TERMINAL:
        raise ValueError(f"unknown advancement lifetime outcome: {outcome}")
    latest = obligation_sources.latest_episode(db, SOURCE_TYPE, experiment_id)
    operation = "satisfy" if outcome == "complete" else "abandon"
    return obligation_sources.terminalize(db, SOURCE_TYPE, experiment_id, latest, operation)


def sync_lifetime(db, experiment_id, previous_phase, phase):
    """Close the saved lifetime of an experiment that reaches a terminal phase."""
    obligation_sources.require_transaction(db)
    if phase == previous_phase:
        return None
    if phase in _TERMINAL:
        return close_lifetime(db, experiment_id, phase)
    return None


def open_cancellation(db, experiment_id, *, unconfirmed=False):
    """Open the backend responsibility that reconciles this saved cancellation."""
    obligation_sources.require_transaction(db)
    exp = _experiment(db, experiment_id)
    return _open(
        db, CANCELLATION, exp, wait="cancellation_unconfirmed" if unconfirmed else "cancellation_pending"
    )


def settle_cancellation(db, experiment_id):
    """Satisfy the open cancellation responsibility once that procedure completes."""
    obligation_sources.require_transaction(db)
    latest = obligation_sources.latest_episode(db, CANCELLATION, experiment_id)
    return obligation_sources.terminalize(db, CANCELLATION, experiment_id, latest, "satisfy")


def _open(db, source_type, exp, *, wait, detail=None, source_transition=False):
    return obligation_sources.open_episode(
        db,
        source_type,
        exp["id"],
        source_transition=source_transition,
        experiment_id=exp["id"],
        project_id=_project_id(exp),
        kind="reconcile",
        target=exp["id"],
        owner="backend",
        wait=wait,
        detail=detail,
    )


def _experiment(db, experiment_id):
    if not isinstance(experiment_id, str) or not experiment_id:
        raise ValueError("experiment_id must be a non-empty string")
    row = db.execute("SELECT * FROM experiments WHERE id=?", (experiment_id,)).fetchone()
    if row is None:
        raise ValueError("unknown experiment")
    exp = dict(row)
    try:
        exp["contract"] = json.loads(exp["contract"])
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid saved experiment contract") from exc
    if not isinstance(exp["contract"], dict):
        raise ValueError("invalid saved experiment contract")
    return exp


def _project_id(exp):
    project = exp["contract"].get("project") or {}
    return project.get("id") if isinstance(project, dict) else None
