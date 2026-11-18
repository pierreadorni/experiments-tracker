"""C7 ``request_integration``: queue C6 integration for this experiment's own branch.

A live contract-4 research turn may ask for its own saved named branch to enter the
existing integration queue. The tool reuses the owner ``integrate`` command for the
exact saved experiment/branch/repository identity, the bounded reason and the UUID
replay: a repeated request ID returns the saved command result instead of queueing
again. The integration worker still owns the project check, any approval, the Git
merge and the terminal stage, so a tool result means queued, never merged.

The saved experiment, the caller's action snapshot, the exact saved research thread
and the maintenance gate are re-read inside the same transaction that command uses,
so a moved, foreign or draining turn refuses before any queue entry is written.
"""

import json
from uuid import UUID

from pydantic import Field, field_validator

from ..config import StrictModel
from ..core import action_obligations
from ..maintenance import blocked
from .tools import require_live_research

MAX_REASON_CHARS = 2000


class RequestIntegration(StrictModel):
    """One idempotent request; the UUID makes a replay the same queued integration."""

    request_id: UUID
    reason: str = Field(min_length=1, max_length=MAX_REASON_CHARS)

    @field_validator("reason")
    @classmethod
    def _bounded_text(cls, value):
        if not value.strip():
            raise ValueError("integration reason must be bounded text")
        return value


def _saved_experiment(db, eid):
    row = db.execute(
        "SELECT id,contract,phase,deleted_at,pause_agent,thread_id FROM experiments WHERE id=?", (eid,)
    ).fetchone()
    if row is None:
        return None
    experiment = dict(row)
    experiment["contract"] = json.loads(experiment["contract"]) if experiment["contract"] else None
    experiment["pause_agent"] = bool(experiment["pause_agent"])
    return experiment


def request_integration(store, experiment, action, request):
    """Queue this experiment's named-branch merge; the worker still owns the merge.

    The saved experiment and the caller's exact action snapshot are re-read inside
    the same transaction that queues the existing ``integrate`` command, so a
    missing, foreign or changed research turn refuses before any write.
    """
    request = RequestIntegration.model_validate(request)
    with store.tx() as db:
        saved = _saved_experiment(db, experiment["id"])
        if saved is None:
            raise ValueError("request_integration requires a saved research experiment")
        saved_action = action_obligations.expected_saved(db, action)
        require_live_research(saved, saved_action)
        if not saved_action.get("thread_id") or saved.get("thread_id") != saved_action["thread_id"]:
            raise ValueError("request_integration requires the exact saved research thread")
        if blocked(store):
            raise ValueError("this tool requires an idle installation")
        result = store.command(
            str(request.request_id),
            "integrate",
            {"reason": request.reason},
            eid=saved["id"],
            origin="agent",
            _db=db,
        )
    return {
        "request_id": str(request.request_id),
        "experiment_id": result["experiment_id"],
        "queued": True,
        "merged": False,
        "reason": request.reason,
    }
