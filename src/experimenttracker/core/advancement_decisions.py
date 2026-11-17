"""Read and save the experiment-lifetime advancement decisions.

One backend responsibility tracks advancing each contract-3 experiment: it opens
with the experiment, survives its source handoffs, and closes at the terminal
boundary. Saved decisions cover only the empty/unblocked decision and the
explicit admission waits below; source-backed reason families are later c1
cards. Cancellation closes advancement and opens its own reconciliation
responsibility. Callers own the transaction; this module never dispatches work
or creates schema.
"""

import json

from . import obligation_sources

SOURCE_TYPE = "advancement"
CANCELLATION = "cancellation"
_VERSION = 1
_TERMINAL = frozenset({"complete", "failed", "cancelled"})
_WAIT_BY_REASON = {
    "agent_busy": "admission:agent_busy",
    "agent_paused": "admission:agent_paused",
    "agent_unavailable": "backend:agent_unavailable",
    "compute_busy": "admission:compute_busy",
    "executor_unavailable": "backend:executor_unavailable",
    "new_compute_paused": "admission:new_compute_paused",
    "project_busy": "admission:project_busy",
    "project_unavailable": "backend:project_unavailable",
    "quota_hold": "admission:quota",
    "workspace_unavailable": "backend:workspace_unavailable",
}
_FIELDS = {"version", "phase", "evidence_revision", "reason", "wait"}


def open_lifetime(db, experiment_id):
    """Open the lifetime that tracks advancing one saved experiment.

    Only contract-3 creation establishes advancement; frozen legacy experiments
    and archives keep none, and no decision is saved until one is decided.
    """
    obligation_sources.require_transaction(db)
    exp = _experiment(db, experiment_id)
    _require_saved(exp)
    if not _contract3(exp):
        return None
    return _open(db, SOURCE_TYPE, exp, wait="none")


def close_lifetime(db, experiment_id, outcome):
    """Close the open lifetime: complete satisfies it, otherwise it is abandoned.

    The last saved decision stays on the closed episode as the final record, so a
    repaired retry opens the next episode instead of mutating it.
    """
    obligation_sources.require_transaction(db)
    if outcome not in _TERMINAL:
        raise ValueError(f"unknown advancement lifetime outcome: {outcome}")
    latest = obligation_sources.latest_episode(db, SOURCE_TYPE, experiment_id)
    operation = "satisfy" if outcome == "complete" else "abandon"
    return obligation_sources.terminalize(db, SOURCE_TYPE, experiment_id, latest, operation)


def sync_lifetime(db, experiment_id, previous_phase, phase):
    """Track one proven saved phase transition of the experiment lifetime.

    A terminal phase closes the episode; a genuine terminal-to-active retry or
    repair opens the next episode. A lifetime never opens here: creation owns
    that, and pre-existing rows stay without advancement until backfill.
    """
    obligation_sources.require_transaction(db)
    if phase == previous_phase:
        return None
    if phase in _TERMINAL:
        return close_lifetime(db, experiment_id, phase)
    if previous_phase not in _TERMINAL:
        return None
    exp = _experiment(db, experiment_id)
    if not _contract3(exp):
        return None
    if obligation_sources.latest_episode(db, SOURCE_TYPE, experiment_id) is None:
        return None
    return _open(db, SOURCE_TYPE, exp, wait="none", source_transition=True)


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


def read(db, experiment_id):
    """Return the latest validated decision: active, or final after a close."""
    obligation_sources.require_transaction(db)
    exp = _experiment(db, experiment_id)
    _require_saved(exp)
    if not _contract3(exp):
        return None
    row = obligation_sources.latest_episode(db, SOURCE_TYPE, experiment_id)
    if row is None:
        return None
    _check_binding(row, exp)
    record = _decode(row["detail"])
    if record is None:
        return None
    _validate_pair(record["reason"], record["wait"])
    if row["wait"] != record["wait"]:
        raise ValueError("advancement decision wait does not match its obligation")
    if exp["phase"] not in _TERMINAL:
        if row["state"] != "open":
            raise ValueError("terminal advancement history on an active experiment")
        _check_generation(exp, record["phase"], record["evidence_revision"])
    elif row["state"] == "open":
        raise ValueError("open advancement lifetime on a terminal experiment")
    return record


def set_decision(db, experiment_id, *, phase, evidence_revision, reason, wait):
    """Save or replay one supported source-less decision on the open lifetime."""
    obligation_sources.require_transaction(db)
    exp = _experiment(db, experiment_id)
    _require_saved(exp)
    if exp["phase"] in _TERMINAL:
        raise ValueError("terminal experiments cannot save advancement decisions")
    if not _contract3(exp):
        raise ValueError("advancement decisions require execution contract 3")
    _check_generation(exp, phase, evidence_revision)
    _validate_pair(reason, wait)

    latest = obligation_sources.latest_episode(db, SOURCE_TYPE, experiment_id)
    if latest is None:
        raise ValueError("advancement decisions require an opened lifetime episode")
    _check_binding(latest, exp)
    if latest["state"] != "open":
        raise ValueError("terminal advancement lifetime requires a proven phase transition")
    record = {
        "version": _VERSION,
        "phase": phase,
        "evidence_revision": evidence_revision,
        "reason": reason,
        "wait": wait,
    }
    detail = json.dumps(record, sort_keys=True, separators=(",", ":"))
    return _open(db, SOURCE_TYPE, exp, wait=wait, detail=detail)


def _validate_pair(reason, wait):
    if reason is None:
        expected = "none"
    elif isinstance(reason, str):
        expected = _WAIT_BY_REASON.get(reason)
    else:
        expected = None
    if expected is None or not isinstance(wait, str) or wait != expected:
        raise ValueError("unsupported advancement reason and wait combination in c1a")


def _decode(detail):
    if detail is None:
        return None
    try:
        record = json.loads(detail)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid advancement decision payload") from exc
    if not isinstance(record, dict) or set(record) != _FIELDS:
        raise ValueError("invalid advancement decision payload fields")
    if (
        isinstance(record["version"], bool)
        or not isinstance(record["version"], int)
        or record["version"] != _VERSION
    ):
        raise ValueError("unsupported advancement decision payload version")
    if not isinstance(record["phase"], str) or not record["phase"]:
        raise ValueError("invalid advancement decision phase")
    if (
        isinstance(record["evidence_revision"], bool)
        or not isinstance(record["evidence_revision"], int)
        or record["evidence_revision"] < 1
    ):
        raise ValueError("invalid advancement decision evidence revision")
    if record["reason"] is not None and not isinstance(record["reason"], str):
        raise ValueError("invalid advancement decision reason")
    if not isinstance(record["wait"], str):
        raise ValueError("invalid advancement decision wait")
    return record


def _check_binding(row, exp):
    expected = {
        "experiment_id": exp["id"],
        "project_id": _project_id(exp),
        "kind": "reconcile",
        "target": exp["id"],
        "owner": "backend",
    }
    if row["id"].rsplit(":", 1)[0] != f"advancement:{exp['id']}":
        raise ValueError("advancement obligation identity, scope, or ownership is invalid")
    if any(row[field] != value for field, value in expected.items()):
        raise ValueError("advancement obligation identity, scope, or ownership is invalid")


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


def _contract3(exp):
    return exp["contract"].get("execution_contract") == 3


def _require_saved(exp):
    if exp.get("deleted_at") is not None:
        raise ValueError("deleted experiments cannot read or write active advancement decisions")


def _check_generation(exp, phase, evidence_revision):
    if not isinstance(phase, str) or not phase or phase != exp["phase"]:
        raise ValueError("advancement decision must match the saved phase and evidence")
    if (
        isinstance(evidence_revision, bool)
        or not isinstance(evidence_revision, int)
        or evidence_revision < 1
        or evidence_revision != exp["evidence_revision"]
    ):
        raise ValueError("advancement decision must match the saved phase and evidence")
