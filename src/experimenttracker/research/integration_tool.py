"""C7 ``request_integration``: queue C6 integration for this experiment's own branch.

A live contract-4 research turn may ask for its own saved named branch to enter the
existing integration queue. The tool reuses the owner ``integrate`` command for the
exact saved experiment/branch/repository identity, the bounded reason and the UUID
replay: a repeated request ID returns the saved command result instead of queueing
again. The integration worker still owns the project check, any approval, the Git
merge and the terminal stage, so a tool result means queued, never merged.
"""

from uuid import UUID

from pydantic import Field, field_validator

from ..config import StrictModel
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


def require_owned_action(store, experiment):
    """Refuse unless this experiment's own saved research action is the live turn."""
    action = store.one(
        "SELECT * FROM agent_actions WHERE experiment_id=? AND state IN ('dispatching','running') "
        "ORDER BY rowid DESC LIMIT 1",
        (experiment["id"],),
    )
    require_live_research(experiment, action or {})


def request_integration(workflow, experiment, request):
    """Queue this experiment's named-branch merge; the worker still owns the merge."""
    require_owned_action(workflow.store, experiment)
    if blocked(workflow.store):
        raise ValueError("this tool requires an idle installation")
    result = workflow.store.command(
        str(request.request_id),
        "integrate",
        {"reason": request.reason},
        eid=experiment["id"],
        origin="agent",
    )
    return {
        "request_id": str(request.request_id),
        "experiment_id": result["experiment_id"],
        "queued": True,
        "merged": False,
        "reason": request.reason,
    }
