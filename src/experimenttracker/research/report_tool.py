"""Inert C7 ``update_report`` persistence for one exact live research turn.

The tool validates the document against the shared ``ReportDocument`` schema and
records one working-report version through the existing ``Store.working_report``
transaction. It never builds, renders or publishes a report: the existing report
runner owns those effects. The saved request UUID identifies the version, so a
replay returns the saved version and its current state, while a different document
under that UUID refuses. The caller's saved action snapshot and the experiment's
saved thread must still be the exact live research turn.
"""

import json
from uuid import UUID

from ..config import StrictModel
from ..core import action_obligations
from ..models import ReportDocument

RESEARCH_PHASE = "research"
_LIVE_STATES = frozenset({"dispatching", "running"})
_TERMINAL_PHASES = frozenset({"complete", "failed", "cancelled"})


class UpdateReport(StrictModel):
    """One new working report version for this experiment's live research turn."""

    request_id: UUID
    document: ReportDocument


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


def _require_live_research(experiment, action):
    """Refuse this tool outside the exact live research turn of its own experiment."""
    contract = experiment.get("contract") or {}
    if contract.get("execution_contract") != 4:
        raise ValueError("update_report requires execution contract 4")
    if (
        action.get("experiment_id") != experiment["id"]
        or action.get("phase") != RESEARCH_PHASE
        or action.get("state") not in _LIVE_STATES
    ):
        raise ValueError("update_report requires this experiment's active research turn")
    if not action.get("thread_id") or experiment.get("thread_id") != action["thread_id"]:
        raise ValueError("update_report requires the exact saved research thread")
    if (
        experiment.get("deleted_at") is not None
        or experiment["phase"] in _TERMINAL_PHASES
        or experiment.get("pause_agent")
    ):
        raise ValueError("update_report requires a live research experiment")


def update_report(store, experiment, action, request):
    """Persist one validated working-report version; never build or publish it.

    The saved experiment and the caller's exact action snapshot are re-read inside
    the same transaction that inserts the version, so a moved, missing or
    differently owned research turn refuses before any write.
    """
    request = UpdateReport.model_validate(request)
    with store.tx() as db:
        saved = _saved_experiment(db, experiment["id"])
        if saved is None:
            raise ValueError("update_report requires a saved research experiment")
        saved_action = action_obligations.expected_saved(db, action)
        _require_live_research(saved, saved_action)
        version = store.working_report(
            saved["id"],
            request.document.model_dump(mode="json"),
            str(request.request_id),
            _db=db,
        )
        state = db.execute(
            "SELECT state FROM report_versions WHERE experiment_id=? AND version=?", (saved["id"], version)
        ).fetchone()["state"]
    return {
        "request_id": str(request.request_id),
        "report_version": version,
        "state": state,
    }
