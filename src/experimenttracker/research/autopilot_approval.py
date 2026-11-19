"""Read exact Autopilot orchestrator approval proof without granting or dispatching work."""

import json
from uuid import UUID

from pydantic import Field, ValidationError, field_validator

from ..config import StrictModel


class OrchestratorApproval(StrictModel):
    """One approval returned by the trusted project orchestrator for a C4 request."""

    request_id: UUID
    experiment_id: UUID
    run_id: UUID
    policy_revision: int = Field(ge=1)
    control_revision: int = Field(ge=0)
    objective: str = Field(min_length=1, max_length=2000)

    @field_validator("objective")
    @classmethod
    def objective_present(cls, value):
        if not value.strip():
            raise ValueError("objective must not be blank")
        return value

    @classmethod
    def parse(cls, value):
        """Parse the explicit ``approval`` member of a saved project-turn result."""
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except (TypeError, ValueError):
                return None
        if not isinstance(value, dict) or not isinstance(value.get("approval"), dict):
            return None
        try:
            return cls.model_validate(value["approval"])
        except (TypeError, ValueError, ValidationError):
            return None


def approved(store, experiment_id, request_id, objective, *, now=None):
    """Return proof only for one completed, current orchestrator turn and exact C4 request.

    This is deliberately separate from ``research.approval.approved``: it neither
    creates an owner answer command nor satisfies the owner's approval obligation.
    """
    try:
        eid = str(UUID(str(experiment_id)))
        rid = str(UUID(str(request_id)))
    except (TypeError, ValueError):
        return None
    if not isinstance(objective, str) or not objective.strip():
        return None

    try:
        experiment = store.experiment(eid)
    except KeyError:
        return None
    if experiment["deleted_at"] is not None or experiment["contract"].get("execution_contract") != 4:
        return None
    member = store.one("SELECT * FROM autopilot_members WHERE experiment_id=?", (eid,))
    if not member:
        return None
    requests = store.rows(
        "SELECT payload FROM events WHERE experiment_id=? AND kind='owner_approval_requested' "
        "AND json_extract(payload,'$.request_id')=? LIMIT 2",
        (eid, rid),
    )
    if len(requests) != 1:
        return None
    try:
        request_payload = json.loads(requests[0]["payload"])
    except (TypeError, ValueError):
        return None
    if (
        request_payload.get("experiment_id") != eid
        or request_payload.get("project_id") != member["project_id"]
        or request_payload.get("brief") != experiment["brief"]
        or request_payload.get("request_id") != rid
        or request_payload.get("objective") != objective
    ):
        return None
    run = store.one("SELECT * FROM autopilot_runs WHERE id=?", (member["run_id"],))
    if (
        not run
        or run["status"] != "active"
        or member["project_id"] != run["project_id"]
        or member["policy_revision"] != run["policy_revision"]
    ):
        return None

    from ..autopilot import Autopilot

    service = Autopilot(store)
    if service.authority(eid, phase="compute", now=now) is not None:
        return None

    matches = []
    for action in store.rows(
        "SELECT * FROM autopilot_actions WHERE run_id=? AND state='completed' AND result IS NOT NULL",
        (run["id"],),
    ):
        proof = OrchestratorApproval.parse(action["result"])
        if (
            proof is not None
            and str(proof.request_id) == rid
            and str(proof.experiment_id) == eid
            and str(proof.run_id) == run["id"]
            and proof.objective == objective
            and proof.policy_revision == run["policy_revision"]
            and proof.control_revision == run["control_revision"]
            and action["control_revision"] == run["control_revision"]
        ):
            matches.append(proof)
    return matches[0] if len(matches) == 1 else None
