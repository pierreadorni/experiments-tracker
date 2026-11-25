"""One dispatch for commands from the hosted relay queue and from local ``POST /api/commands``.

Each kind calls the handler the old per-kind pollers and FastAPI routes already call, so
domain validation stays in those handlers. Accepted effects are committed by the handler.
A refusal is saved (with an inbox notice when it targets an experiment) before the caller
reports it. The caller reports a receipt only after ``dispatch`` returns, so a restart
in between redelivers the command and the same saved outcome answers it.
"""

import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from .. import clock
from ..models import (
    ActionCommand,
    AnswerCommand,
    ApproveIntegrationCommand,
    CreateExperiment,
    DeleteCommand,
    DiscussCommand,
    DontMergeCommand,
    EditTitleCommand,
    IntegrateCommand,
    NoteCommand,
    PlanReviewCommand,
    PresentCommand,
    ReserveCommand,
    RevertIntegrationCommand,
    SteerCommand,
)
from ..store import Conflict, encode

EXPERIMENT_KINDS = {
    "create": CreateExperiment,
    "delete": DeleteCommand,
    "present": PresentCommand,
    "discuss": DiscussCommand,
    "note": NoteCommand,
    "edit_title": EditTitleCommand,
    "action": ActionCommand,
    "answer": AnswerCommand,
    "plan_review": PlanReviewCommand,
    "steer": SteerCommand,
    "integrate": IntegrateCommand,
    "revert_integration": RevertIntegrationCommand,
    "approve_integration": ApproveIntegrationCommand,
    "dont_merge": DontMergeCommand,
    "reserve": ReserveCommand,
}
OTHER_KINDS = ("project_create", "project_settings", "knowledge", "autopilot", "maintenance")
KINDS = (*EXPERIMENT_KINDS, *OTHER_KINDS)
_EXPERIMENT = re.compile(r"^experiment:([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$")
_PROJECT = re.compile(r"^(?:knowledge|autopilot):([A-Za-z0-9_-]{1,100})$")
_SCHEMA_REFUSAL = "Command does not match the supported schema or experiment"


@dataclass
class Services:
    """The existing handlers; each factory may return None where a host lacks that surface."""

    store: Any
    projects: Callable[[], Any]
    memory: Callable[[], Any]
    autopilot: Callable[[], Any]
    maintenance: Callable[[], Any]


def receipt(request_id, kind, target, outcome, received_at):
    return {
        "schema_version": 1,
        "request_id": request_id,
        "kind": kind,
        "target": target,
        "state": outcome["state"],
        "result": outcome.get("result"),
        "error": outcome.get("error"),
        "received_at": received_at,
        "settled_at": None if outcome["state"] == "pending" else clock.now(),
    }


def saved_receipt(store, request_id):
    """The receipt of a non-experiment command, from the table its handler saved it in."""
    request_id = str(request_id)
    for table, kind in (
        ("project_requests", "project_create"),
        ("project_settings_requests", "project_settings"),
    ):
        row = store.one(f"SELECT * FROM {table} WHERE request_id=?", (request_id,))
        if row:
            if row["state"] == "applied":
                saved = json.loads(row["result"]) if row["result"] else None
                result = (
                    {"project_id": row["project_id"], "project": saved} if kind == "project_create" else saved
                )
                state = {"state": "applied", "result": result}
            elif row["state"] == "rejected":
                state = {"state": "rejected", "error": row["error"]}
            else:
                state = {"state": "pending"}
            target = None if kind == "project_create" else f"knowledge:{row['project_id']}"
            return receipt(request_id, kind, target, state, row["created_at"])
    row = store.one("SELECT * FROM project_turns WHERE id=?", (request_id,))
    if row:
        result = {"schema_version": 1, "request_id": request_id, "state": row["state"]}
        outcome = {"state": "applied", "result": result}
        return receipt(request_id, "knowledge", f"knowledge:{row['project_id']}", outcome, row["created_at"])
    row = store.one("SELECT * FROM autopilot_requests WHERE request_id=?", (request_id,))
    if row:
        outcome = (
            {"state": "rejected", "error": row["error"]}
            if row["state"] == "rejected"
            else {"state": "applied", "result": json.loads(row["result"]) if row["result"] else None}
        )
        return receipt(request_id, "autopilot", f"autopilot:{row['project_id']}", outcome, row["created_at"])
    row = store.one("SELECT * FROM maintenance WHERE request_id=?", (request_id,))
    if row:
        result = {"schema_version": 1, "request_id": request_id, "state": row["state"]}
        return receipt(
            request_id,
            "maintenance",
            "maintenance",
            {"state": "applied", "result": result},
            row["created_at"],
        )
    return None


def _experiment(target, *, required):
    if target is None and not required:
        return None
    match = _EXPERIMENT.match(target or "")
    if not match:
        raise Conflict("command target must be an experiment key")
    return match.group(1)


def _project(target):
    match = _PROJECT.match(target or "")
    if not match:
        raise Conflict("command target must name a project")
    return match.group(1)


def _apply(services, request_id, kind, target, payload, origin):
    if not isinstance(payload, dict) or payload.get("request_id") != request_id:
        raise Conflict("command request identity mismatch")
    store = services.store
    if kind in EXPERIMENT_KINDS:
        body = EXPERIMENT_KINDS[kind].model_validate(payload).model_dump(mode="json", exclude_none=True)
        if body["request_id"] != request_id:
            raise Conflict("command request identity mismatch")
        if kind == "reserve" and target is not None:
            raise Conflict("reserve command must not target an experiment")
        eid = None if kind == "reserve" else _experiment(target, required=kind != "create")
        result = store.command(
            request_id,
            kind,
            body,
            origin=origin,
            eid=None if kind in ("create", "reserve") else eid,
            experiment_id=eid if kind == "create" else None,
        )
        return {"state": "applied", "result": result}
    if kind == "project_create":
        projects = services.projects()
        projects.enqueue(payload, origin=origin)
        projects.tick()  # settles registration before the receipt, like the old poller
        saved = projects.receipt(request_id)
        result = {"project_id": saved["project_id"], "project": saved["project"]}
    elif kind == "project_settings":
        projects = services.projects()
        projects.enqueue_settings(_project(target), payload, origin=origin)
        projects.settings_tick()
        saved = projects.settings_receipt(request_id)
        result = saved["settings"]
    elif kind == "knowledge":
        return {"state": "applied", "result": services.memory().enqueue(_project(target), payload, origin)}
    elif kind == "autopilot":
        return {"state": "applied", "result": services.autopilot().command(_project(target), payload)}
    else:
        from ..maintenance import InstallCommand

        manager = services.maintenance()
        if manager is None:
            raise Conflict("maintenance is unavailable on this executor")
        return {"state": "applied", "result": manager.install(InstallCommand.model_validate(payload))}
    if saved["state"] == "applied":
        return {"state": "applied", "result": result}
    if saved["state"] == "rejected":
        return {"state": "rejected", "error": saved["error"]}
    return {"state": "pending"}  # still settling; the next delivery answers from the saved row


def _refuse(store, request_id, kind, target, payload, reason):
    match = _EXPERIMENT.match(target or "")
    with store.tx() as db:
        if db.execute("SELECT 1 FROM commands WHERE request_id=?", (request_id,)).fetchone():
            return  # a reused identity must not shadow the applied command's receipt
        eid = match.group(1) if match else ""
        db.execute(
            "INSERT OR IGNORE INTO rejected_gateway_commands VALUES(?,?,?,?,?,?)",
            (request_id, eid, kind, encode(payload), reason, clock.now()),
        )
        if eid and db.execute("SELECT 1 FROM experiments WHERE id=?", (eid,)).fetchone():
            text = f"Command could not be applied: {reason}"
            cursor = db.execute(
                "INSERT OR IGNORE INTO inbox VALUES(?,?,?,?,?)",
                (request_id, eid, "command_rejected", text, clock.now()),
            )
            if cursor.rowcount:
                store.emit(db, eid, "notification", {"kind": "command_rejected", "text": text})


def dispatch(services, request_id, kind, target, payload, *, origin):
    """Return {state: applied|rejected|pending, result, error}; never raises for a refusal.

    An unexpected error (a storage failure) propagates and saves nothing new: the caller
    sends no receipt, and redelivery re-enters the idempotent handler.
    """
    request_id = str(UUID(request_id))
    store = services.store
    saved = store.one("SELECT error FROM rejected_gateway_commands WHERE request_id=?", (request_id,))
    if saved:
        return {"state": "rejected", "error": saved["error"]}
    try:
        if kind not in KINDS:
            raise Conflict(f"unknown command kind {kind[:40]!r}")
        return _apply(services, request_id, kind, target, payload, origin)
    except (ValueError, KeyError) as exc:
        reason = str(exc)[:1000] if isinstance(exc, Conflict) else _SCHEMA_REFUSAL
        _refuse(store, request_id, kind, target, payload, reason)
        return {"state": "rejected", "error": reason}
