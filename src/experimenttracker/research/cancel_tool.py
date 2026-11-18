"""C7 ``cancel_job``: save one exact selective cancellation for the owned turn.

A live contract-4 research turn may cancel this experiment's own saved Slurm intent
with a bounded reason. The tool reuses the contract-3 selective-cancellation save
and the existing cancellation reconciler: it only records the exact request, and a
later tick reconciles the saved identity before calling the scheduler. A tool result
means saved/accepted, never externally cancelled.

The saved experiment and the caller's action snapshot are re-read inside the same
transaction that writes the request, so a moved, foreign, paused, terminal, archived
or draining turn refuses before anything is saved.
"""

import json
from uuid import UUID

from pydantic import Field, field_validator

from ..config import StrictModel
from ..core import action_obligations
from ..early_repair import save_cancellation
from ..maintenance import blocked
from .tools import require_live_research

MAX_REASON_CHARS = 2000


class CancelJob(StrictModel):
    """One idempotent cancellation; the UUIDs keep the request and intent exact."""

    request_id: UUID
    intent_id: UUID
    reason: str = Field(min_length=1, max_length=MAX_REASON_CHARS)

    @field_validator("reason")
    @classmethod
    def _bounded_text(cls, value):
        if not value.strip():
            raise ValueError("cancellation reason must be bounded text")
        return value


def _saved_experiment(db, eid):
    row = db.execute(
        "SELECT id,contract,phase,deleted_at,pause_agent FROM experiments WHERE id=?", (eid,)
    ).fetchone()
    if row is None:
        return None
    experiment = dict(row)
    experiment["contract"] = json.loads(experiment["contract"]) if experiment["contract"] else None
    experiment["pause_agent"] = bool(experiment["pause_agent"])
    return experiment


def cancel_job(store, experiment, action, request):
    """Save this experiment's exact cancellation; the reconciler still owns scancel.

    The saved experiment and the caller's exact action snapshot are re-read inside
    the transaction that saves the request, so a missing, foreign, changed, paused,
    terminal or archived research turn refuses before any write. No scheduler
    boundary runs here; the existing reconciler proceeds on a later tick.
    """
    request = CancelJob.model_validate(request)
    with store.tx() as db:
        saved = _saved_experiment(db, experiment["id"])
        if saved is None:
            raise ValueError("cancel_job requires a saved research experiment")
        saved_action = action_obligations.expected_saved(db, action)
        require_live_research(saved, saved_action)
        if blocked(store):
            raise ValueError("cancel_job requires an idle installation")
        record, _ = save_cancellation(
            store, db, saved["id"], request.intent_id, request.request_id, request.reason
        )
    return {
        "request_id": str(request.request_id),
        "intent_id": record["intent_id"],
        "state": record["state"],
        "reason": record["reason"],
    }
