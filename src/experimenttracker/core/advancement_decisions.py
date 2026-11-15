"""Read and save c1a source-less advancement decisions.

This inert helper supports only an empty/unblocked decision and the explicit
admission waits below. Source-backed reason families and terminal lifetimes are
deferred to later c1 cards. Callers own the transaction; this module never
dispatches work or creates schema.
"""

import json

from . import obligation_sources

_SOURCE_TYPE = "advancement"
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


def read(db, experiment_id):
    """Return the validated active decision, or None when no row is saved."""
    _require_transaction(db)
    exp = _experiment(db, experiment_id)
    _require_active(exp)
    if not _contract3(exp):
        return None
    row = obligation_sources.latest_episode(db, _SOURCE_TYPE, experiment_id)
    if row is None:
        return None
    _check_binding(row, exp)
    record = _decode(row["detail"])
    if record is None:
        return None
    _check_generation(exp, record["phase"], record["evidence_revision"])
    _validate_pair(record["reason"], record["wait"])
    if row["wait"] != record["wait"]:
        raise ValueError("advancement decision wait does not match its obligation")
    if row["state"] != "open":
        raise ValueError("terminal advancement history is unsupported until c1b")
    return record


def set_decision(db, experiment_id, *, phase, evidence_revision, reason, wait):
    """Save or replay one supported source-less decision on lifetime episode 0."""
    _require_transaction(db)
    exp = _experiment(db, experiment_id)
    _require_active(exp)
    if not _contract3(exp):
        raise ValueError("advancement decisions require execution contract 3")
    _check_generation(exp, phase, evidence_revision)
    _validate_pair(reason, wait)

    latest = obligation_sources.latest_episode(db, _SOURCE_TYPE, experiment_id)
    if latest is not None:
        _check_binding(latest, exp)
        if latest["id"] != f"advancement:{experiment_id}:0":
            raise ValueError("later advancement lifetime episodes require c1b")
        if latest["state"] != "open":
            raise ValueError("terminal advancement history requires c1b lifetime transition proof")
    record = {
        "version": _VERSION,
        "phase": phase,
        "evidence_revision": evidence_revision,
        "reason": reason,
        "wait": wait,
    }
    detail = json.dumps(record, sort_keys=True, separators=(",", ":"))
    return obligation_sources.open_episode(
        db,
        _SOURCE_TYPE,
        experiment_id,
        experiment_id=experiment_id,
        project_id=_project_id(exp),
        kind="reconcile",
        target=experiment_id,
        owner="backend",
        wait=wait,
        detail=detail,
    )


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
        "id": f"advancement:{exp['id']}:0",
        "experiment_id": exp["id"],
        "project_id": _project_id(exp),
        "kind": "reconcile",
        "target": exp["id"],
        "owner": "backend",
    }
    if any(row[field] != value for field, value in expected.items()):
        raise ValueError("advancement obligation identity, scope, or ownership is invalid")


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


def _require_active(exp):
    if exp.get("deleted_at") is not None:
        raise ValueError("deleted experiments cannot read or write active advancement decisions")
    if exp["phase"] in _TERMINAL:
        raise ValueError("terminal advancement decisions are unsupported until c1b")


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


def _require_transaction(db):
    if not db.in_transaction:
        raise RuntimeError("advancement decisions require the caller's active transaction")
