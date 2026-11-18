"""Read durable contract-4 approval proof without granting or dispatching work."""

import json
from uuid import UUID

from ..core import obligation_sources

REQUEST_EVENT = "owner_approval_requested"


def approved(store, experiment_id, request_id, objective, *, db=None):
    """Return exact owner approval for this objective, or None. Join an admission transaction if supplied."""
    try:
        request_id = str(UUID(str(request_id)))
    except (TypeError, ValueError):
        return None
    if not isinstance(objective, str) or not objective.strip():
        return None
    if db is None:
        with store.tx() as connection:
            return approved(store, experiment_id, request_id, objective, db=connection)
    try:
        exp = store.experiment(experiment_id)
    except KeyError:
        return None
    if exp["contract"].get("execution_contract") != 4 or exp["deleted_at"] is not None:
        return None
    expected = {
        "experiment_id": experiment_id,
        "project_id": exp["contract"].get("project", {}).get("id"),
        "brief": exp["brief"],
        "request_id": request_id,
        "objective": objective,
    }
    if not expected["project_id"]:
        return None
    requests = db.execute(
        "SELECT payload FROM events WHERE experiment_id=? AND kind=? "
        "AND json_extract(payload,'$.request_id')=? LIMIT 2",
        (experiment_id, REQUEST_EVENT, request_id),
    ).fetchall()
    if len(requests) != 1 or json.loads(requests[0]["payload"]) != expected:
        return None
    answers = db.execute(
        "SELECT payload,origin,state FROM commands WHERE experiment_id=? AND kind='answer' "
        "AND json_extract(payload,'$.pending_request_id')=? LIMIT 2",
        (experiment_id, request_id),
    ).fetchall()
    if len(answers) != 1:
        return None
    answer = answers[0]
    if (
        answer["origin"] not in ("local", "gateway")
        or answer["state"] != "applied"
        or json.loads(answer["payload"]) != {"pending_request_id": request_id, "answer": "accept"}
    ):
        return None
    key = f"{experiment_id}:{request_id}"
    proof = obligation_sources.latest_episode(db, "request", key)
    if proof is None or (
        proof["experiment_id"],
        proof["target"],
        proof["kind"],
        proof["owner"],
        proof["state"],
    ) != (experiment_id, key, "owner_answer", "owner", "satisfied"):
        return None
    return expected
