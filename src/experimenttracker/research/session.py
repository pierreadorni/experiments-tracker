"""C7 research session: queue one experiment's bounded initial turn.

The helper is inert. It records the initial action, its obligation and the queue
event in one transaction, and never starts a turn, creates a Codex thread or
dispatches work. A settled, failed or uncertain action still counts as the
experiment's one first turn, so a replay or a reopened store never queues another.
"""

import json
from uuid import uuid4

from .. import clock
from ..core import action_obligations
from .prompt import turn_prompt

_TERMINAL_PHASES = frozenset({"complete", "failed", "cancelled"})
_RESEARCH_PHASE = "research"


def _owner_constraints(db, contract):
    """Return the saved project instructions that constrain this experiment's turns."""
    project_id = (contract.get("project") or {}).get("id")
    if not project_id:
        return []
    row = db.execute(
        "SELECT instructions FROM project_preferences WHERE project_id=?", (project_id,)
    ).fetchone()
    if row is None or not row["instructions"].strip():
        return []
    return [row["instructions"]]


def first_turn(store, eid):
    """Queue one initial research action for an active contract 4 experiment.

    Returns the saved pending action, or ``None`` when the experiment is missing,
    deleted, terminal, not exactly contract 4, or already has a first turn. The
    saved row keeps its thread and turn IDs empty until a real dispatch records
    them.
    """
    with store.tx() as db:
        row = db.execute(
            "SELECT brief,contract,phase,deleted_at,evidence_revision FROM experiments WHERE id=?",
            (eid,),
        ).fetchone()
        if row is None:
            return None
        experiment = dict(row)
        contract = json.loads(experiment["contract"])
        if (
            experiment["deleted_at"] is not None
            or experiment["phase"] in _TERMINAL_PHASES
            or contract.get("execution_contract") != 4
        ):
            return None
        if db.execute(
            "SELECT 1 FROM agent_actions WHERE experiment_id=? AND phase=?", (eid, _RESEARCH_PHASE)
        ).fetchone():
            return None

        action_id = str(uuid4())
        prompt = turn_prompt(
            brief=experiment["brief"],
            owner_constraints=_owner_constraints(db, contract),
            action_id=action_id,
        )
        now = clock.now()
        db.execute(
            "INSERT INTO agent_actions"
            "(id,experiment_id,phase,evidence_revision,state,input,created_at,updated_at) "
            "VALUES(?,?,?,?,'pending',?,?,?)",
            (action_id, eid, _RESEARCH_PHASE, experiment["evidence_revision"], prompt, now, now),
        )
        action_obligations.sync_action(db, action_id)
        store.emit(db, eid, "agent_action_queued", {"phase": _RESEARCH_PHASE, "action_id": action_id})
        return dict(db.execute("SELECT * FROM agent_actions WHERE id=?", (action_id,)).fetchone())
