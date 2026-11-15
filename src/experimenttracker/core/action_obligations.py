"""Map saved agent actions to stable responsibility obligations.

Call ``sync_action`` inside the transaction that mutates the action. It records
only responsibility for the saved action; it never dispatches or changes it.
"""

import json

from . import obligation_sources

_SOURCE = "agent-action"
_LIVE_STATES = {"pending", "dispatching", "uncertain", "running"}
_TERMINAL_STATES = {"completed", "failed", "cancelled", "superseded"}
_TRANSITION_FIELDS = ("state", "thread_id", "turn_id")
_IDENTITY_FIELDS = ("id", "experiment_id", "phase", "evidence_revision")
_ALLOWED_RESPONSIBILITIES = {
    ("backend", "agent_turn"),
    ("backend", "reconcile"),
    ("agent", "agent_turn"),
}


def sync_action(db, action_id, *, before=None):
    """Synchronize one current action's obligation in its caller's transaction.

    Pass the old action row captured in the same transaction before its update.
    Only a state or exact turn-identity change proves a responsibility episode
    transition; result/error metadata updates alone remain replays.
    """
    if not db.in_transaction:
        raise RuntimeError("action obligation sync requires the caller's active transaction")
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
        return _terminalize(db, latest, action_id, "satisfy")
    if state in ("failed", "cancelled", "superseded"):
        return _terminalize(db, latest, action_id, "abandon")

    discussion = action["phase"].startswith("discuss:")
    if state == "pending" and (
        experiment["deleted_at"] is not None
        or experiment["phase"] == "cancelled"
        or (contract.get("execution_contract") != 3 and not discussion)
    ):
        return _terminalize(db, latest, action_id, "abandon")

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


def _terminalize(db, latest, action_id, operation):
    if latest is None or latest["state"] != "open":
        return latest
    episode = int(latest["id"].rsplit(":", 1)[1])
    terminalize = (
        obligation_sources.satisfy_episode if operation == "satisfy" else obligation_sources.abandon_episode
    )
    return terminalize(db, _SOURCE, action_id, episode)
