"""Map saved agent actions to stable responsibility obligations.

Call ``transition`` or ``sync_action`` inside the transaction that mutates the
action. They record only responsibility for the saved action; they never dispatch
or change it.
"""

import json

from .. import clock
from ..codex_client import NeedsAttention
from . import obligation_sources

_SOURCE = "agent-action"
_LIVE_STATES = {"pending", "dispatching", "uncertain", "running"}
_TERMINAL_STATES = {"completed", "failed", "cancelled", "superseded"}
_TRANSITION_FIELDS = ("state", "thread_id", "turn_id")
_IDENTITY_FIELDS = ("id", "experiment_id", "phase", "evidence_revision")
_EXPECTED_FIELDS = (*_IDENTITY_FIELDS, "thread_id", "turn_id")
_ALLOWED_RESPONSIBILITIES = {
    ("backend", "agent_turn"),
    ("backend", "reconcile"),
    ("agent", "agent_turn"),
}


def check_expected(current, expected, *, compare_result=False, compare_state=True):
    """Refuse an action whose saved snapshot changed since the caller read it."""
    fields = list(_EXPECTED_FIELDS) + (["state"] if compare_state else [])
    if compare_result:
        fields.extend(("result", "error"))
    if any(current[field] != expected[field] for field in fields):
        raise NeedsAttention("saved action changed during reconciliation; no adoption was made")


def expected_saved(db, expected, *, compare_result=False):
    """Re-read one saved action in the transaction and refuse a changed snapshot."""
    row = db.execute("SELECT * FROM agent_actions WHERE id=?", (expected["id"],)).fetchone()
    if row is None:
        raise NeedsAttention("saved action disappeared during reconciliation")
    current = dict(row)
    check_expected(current, expected, compare_result=compare_result)
    return current


def dispatch_changes(thread_id):
    """Return the saved columns recording one dispatch intent."""
    return {
        "thread_id": thread_id,
        "state": "dispatching",
        "attempts": lambda saved: saved["attempts"] + 1,
    }


def turn_changes(turn_id):
    """Return the saved columns recording one started turn."""
    return {"turn_id": turn_id, "state": "running"}


def transition(db, action_id, changes, *, expected=None, before=None, compare_result=False):
    """Apply one saved action transition inside the caller's transaction.

    ``changes`` maps columns to new values; a callable value receives the old row
    so a counter stays an increment. ``before`` is that old row when the caller
    already read it here. ``expected`` is the snapshot the caller acted on and
    must still match. The transition records its own clock time, re-syncs the
    obligation and returns the saved row.
    """
    current = (
        dict(before)
        if before is not None
        else dict(db.execute("SELECT * FROM agent_actions WHERE id=?", (action_id,)).fetchone())
    )
    if expected is not None:
        check_expected(current, expected, compare_result=compare_result)
    values = [value(current) if callable(value) else value for value in changes.values()]
    db.execute(
        f"UPDATE agent_actions SET {', '.join(f'{name}=?' for name in changes)},updated_at=? WHERE id=?",
        (*values, clock.now(), action_id),
    )
    sync_action(db, action_id, before=current)
    return dict(db.execute("SELECT * FROM agent_actions WHERE id=?", (action_id,)).fetchone())


def sync_action(db, action_id, *, before=None):
    """Synchronize one current action's obligation in its caller's transaction.

    Pass the old action row captured in the same transaction before its update.
    Only a state or exact turn-identity change proves a responsibility episode
    transition; result/error metadata updates alone remain replays.
    """
    obligation_sources.require_transaction(db)
    row = db.execute("SELECT * FROM agent_actions WHERE id=?", (action_id,)).fetchone()
    if row is None:
        raise ValueError(f"saved agent action does not exist: {action_id}")
    action = dict(row)
    if before is not None:
        previous = dict(before)
        if any(previous.get(field) != action[field] for field in _IDENTITY_FIELDS):
            raise ValueError("agent action phase or evidence identity cannot change")
    else:
        previous = None

    experiment_row = db.execute(
        "SELECT contract,phase,deleted_at FROM experiments WHERE id=?", (action["experiment_id"],)
    ).fetchone()
    if experiment_row is None:
        raise ValueError(f"agent action experiment does not exist: {action['experiment_id']}")
    experiment = dict(experiment_row)
    contract = json.loads(experiment["contract"])
    project_id = (contract.get("project") or {}).get("id")
    identity = f"phase={action['phase']};evidence_revision={action['evidence_revision']}"
    latest = obligation_sources.latest_episode(db, _SOURCE, action_id)
    _check_saved_identity(latest, action, project_id, identity)

    state = action["state"]
    if state not in _LIVE_STATES | _TERMINAL_STATES:
        raise ValueError(f"unsupported saved agent action state: {state}")
    if state == "completed":
        return obligation_sources.terminalize(db, _SOURCE, action_id, latest, "satisfy")
    if state in ("failed", "cancelled", "superseded"):
        return obligation_sources.terminalize(db, _SOURCE, action_id, latest, "abandon")

    discussion = action["phase"].startswith("discuss:")
    if state == "pending" and (
        experiment["deleted_at"] is not None
        or experiment["phase"] == "cancelled"
        or (contract.get("execution_contract") != 3 and not discussion)
    ):
        return obligation_sources.terminalize(db, _SOURCE, action_id, latest, "abandon")

    exact_turn = state == "running" and bool(action["thread_id"] and action["turn_id"])
    if exact_turn:
        owner, kind, wait = "agent", "agent_turn", f"turn:{action['turn_id']}"
    elif state == "pending":
        owner, kind, wait = "backend", "agent_turn", "admission"
    else:
        owner, kind, wait = "backend", "reconcile", f"reconcile:{action_id}"
    source_transition = bool(
        previous is not None and any(previous.get(field) != action[field] for field in _TRANSITION_FIELDS)
    )
    if latest is not None and latest["state"] != "open" and not source_transition:
        raise ValueError(
            "active saved agent action conflicts with terminal obligation history; transition proof required"
        )
    return obligation_sources.open_episode(
        db,
        _SOURCE,
        action_id,
        source_transition=source_transition,
        experiment_id=action["experiment_id"],
        project_id=project_id,
        kind=kind,
        target=action_id,
        owner=owner,
        wait=wait,
        detail=identity,
    )


def _check_saved_identity(latest, action, project_id, identity):
    if latest is None:
        return
    wanted = {
        "experiment_id": action["experiment_id"],
        "project_id": project_id,
        "target": action["id"],
        "detail": identity,
    }
    if any(latest[field] != value for field, value in wanted.items()):
        raise ValueError("saved action obligation scope, phase, or evidence identity conflicts")
    if (latest["owner"], latest["kind"]) not in _ALLOWED_RESPONSIBILITIES:
        raise ValueError("saved action obligation owner/kind pair conflicts with action responsibility")
