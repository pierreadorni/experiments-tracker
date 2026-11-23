"""Read exact Autopilot orchestrator approval proof without granting or dispatching work."""

import json
from uuid import UUID

from pydantic import Field, ValidationError, field_validator

from ..autopilot_policy import lead_id
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


def approved(store, experiment_id, request_id, objective, *, now=None, db=None, attributed_action_id=None):
    """Return proof only for one current orchestrator decision and exact C4 request.

    The decision is a lead tool receipt whose action snapshot is the run's current
    control revision, or a saved completed project turn at that revision.

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
    pending = experiment["pending_request"] or None
    if pending is not None and (
        pending.get("id") != rid
        or pending.get("kind") != "approval"
        or pending.get("params", {}).get("objective") != objective
        or (pending.get("answer") and pending.get("answer_source", {}).get("kind") != "orchestrator")
    ):
        return None
    if pending is None and attributed_action_id is None:
        return None
    member = store.one("SELECT * FROM autopilot_members WHERE experiment_id=?", (eid,))
    if not member:
        return None
    query = db.execute if db is not None else store.db.execute
    requests = query(
        "SELECT payload FROM events WHERE experiment_id=? AND kind='owner_approval_requested' "
        "AND json_extract(payload,'$.request_id')=? LIMIT 2",
        (eid, rid),
    ).fetchall()
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
    if not run or run["status"] != "active" or member["project_id"] != run["project_id"]:
        return None

    from ..autopilot import Autopilot

    service = Autopilot(store)
    if service.authority(eid, phase="compute", now=now) is not None:
        return None

    # A lead tool receipt names the lead's agent action (C8); a saved receipt from the
    # retired project-turn engine names a completed ``autopilot_actions`` turn.
    receipts = store.rows(
        "SELECT r.result,r.payload,o.state AS old_state,o.control_revision AS old_control,"
        "l.experiment_id AS lead,l.autopilot_control_revision AS lead_control FROM autopilot_requests r "
        "LEFT JOIN autopilot_actions o ON o.id=substr(r.author,7) "
        "LEFT JOIN agent_actions l ON l.id=substr(r.author,7) "
        "WHERE r.project_id=? AND r.run_id=? AND r.author LIKE 'agent:%' "
        "AND json_extract(r.payload,'$.name')='approve_experiment_objective' "
        "AND json_extract(r.payload,'$.arguments.experiment_id')=? "
        "AND json_extract(r.payload,'$.arguments.approval_request_id')=? LIMIT 2",
        (run["project_id"], run["id"], eid, rid),
    )
    if len(receipts) != 1:
        return None
    receipt = receipts[0]
    if receipt["old_state"] is not None:
        if receipt["old_state"] != "completed" or receipt["old_control"] != run["control_revision"]:
            return None
    elif receipt["lead"] != lead_id(run["id"]) or receipt["lead_control"] != run["control_revision"]:
        return None
    try:
        arguments = json.loads(receipt["payload"])["arguments"]
    except (TypeError, ValueError, KeyError):
        return None
    if arguments.get("objective") != objective:
        return None
    proof = OrchestratorApproval.parse(receipt["result"])
    if (
        proof is None
        or str(proof.request_id) != rid
        or str(proof.experiment_id) != eid
        or str(proof.run_id) != run["id"]
        or proof.objective != objective
        or proof.policy_revision != run["policy_revision"]
        or proof.control_revision != run["control_revision"]
    ):
        return None
    if attributed_action_id is not None:
        wakeups = query(
            "SELECT reason,action_id,state FROM agent_wakeups WHERE experiment_id=? "
            "AND state IN ('queued','complete') AND action_id=?",
            (eid, str(attributed_action_id)),
        ).fetchall()
        if not any(
            (evidence := json.loads(row["reason"] or "null"))
            and evidence.get("event") == "orchestrator_approval"
            and evidence.get("source_id") == rid
            for row in wakeups
        ):
            return None
    return proof
