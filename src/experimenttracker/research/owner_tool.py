"""C7 ``ask_owner``: one durable owner question tied to the exact research turn.

It records a question or approval request against one live contract-4 action.
The notice, owner obligation and receipt commit together. This tool does not send
an RPC reply, accept an approval or submit compute.

The saved ``owner_question_asked`` event is the exact historical receipt, so a
replay never reopens a consumed request. A changed payload, a second unanswered
question or an answered but unconsumed request refuses. The saved shape stays
compatible with the existing answer command.
"""

import json
from typing import Literal
from uuid import UUID, uuid4

from pydantic import Field, field_validator, model_validator

from .. import clock
from ..config import StrictModel
from ..core import action_obligations, request_obligations
from ..maintenance import blocked
from ..store import Conflict, encode
from .approval import REQUEST_EVENT
from .prompt import MAX_APPROVAL_CONTEXT_BYTES, approval_context

_RESEARCH_PHASE = "research"
_LIVE_STATES = frozenset({"running"})
_TERMINAL_PHASES = frozenset({"complete", "failed", "cancelled"})
_REQUEST_METHOD = "item/tool/requestUserInput"
_QUESTION_ID = "question"
_NOTICE_KIND = "required_decision"
_EVENT_KIND = "owner_question_asked"
_MAX_QUESTION = 2000
_MAX_OPTION = 200
_MAX_OPTIONS = 10


class AskOwnerQuestion(StrictModel):
    """One durable question or objective-linked approval request."""

    request_id: UUID
    question: str = Field(min_length=1, max_length=_MAX_QUESTION)
    options: list[str] = Field(default_factory=list, max_length=_MAX_OPTIONS)
    kind: Literal["question", "approval"] = "question"
    objective: str | None = Field(default=None, max_length=_MAX_QUESTION)

    @model_validator(mode="after")
    def approval_shape(self):
        if self.kind == "approval":
            if not self.objective or not self.objective.strip() or self.options:
                raise ValueError("approval requires an objective and no custom options")
            if (
                len(approval_context(str(self.request_id), self.objective, "decline").encode())
                > MAX_APPROVAL_CONTEXT_BYTES
            ):
                raise ValueError("approval objective exceeds the exact handoff byte budget")
        elif self.objective is not None:
            raise ValueError("only approval requests take an objective")
        return self

    @field_validator("question")
    @classmethod
    def question_present(cls, value):
        if not value.strip():
            raise ValueError("question must not be blank")
        return value

    @field_validator("options")
    @classmethod
    def options_bounded(cls, values):
        if len(set(values)) != len(values):
            raise ValueError("options must be unique")
        for value in values:
            if not value.strip():
                raise ValueError("option must not be blank")
            if len(value) > _MAX_OPTION:
                raise ValueError(f"option must be at most {_MAX_OPTION} characters")
        return values


def _receipt(experiment_id, action, request_id, state):
    return {
        "schema_version": 1,
        "experiment_id": experiment_id,
        "request_id": request_id,
        "action_id": action["id"],
        "thread_id": action["thread_id"],
        "state": state,
    }


def _experiment(db, expected_action):
    row = db.execute(
        "SELECT id,brief,phase,deleted_at,contract,thread_id,pause_agent,pending_request "
        "FROM experiments WHERE id=?",
        (expected_action["experiment_id"],),
    ).fetchone()
    if row is None:
        raise Conflict("saved research experiment disappeared")
    experiment = dict(row)
    contract = json.loads(experiment["contract"])
    if experiment["deleted_at"] is not None:
        raise Conflict("a deleted experiment cannot ask the owner")
    if experiment["phase"] in _TERMINAL_PHASES:
        raise Conflict("a terminal experiment cannot ask the owner")
    if contract.get("execution_contract") != 4:
        raise Conflict("owner questions belong to a live contract 4 research turn")
    if experiment["pause_agent"]:
        raise Conflict("a paused experiment cannot ask the owner")
    return experiment


def _action(db, expected):
    current = action_obligations.expected_saved(db, expected)
    if current["phase"] != _RESEARCH_PHASE:
        raise Conflict("owner questions belong to a saved research action")
    if current["state"] not in _LIVE_STATES:
        raise Conflict("the saved research action is not running")
    if not current["thread_id"]:
        raise Conflict("the saved research action has no exact thread identity")
    return current


def _receipt_event(db, experiment_id, request_id):
    row = db.execute(
        "SELECT payload FROM events WHERE experiment_id=? AND kind=? "
        "AND json_extract(payload,'$.request_id')=? ORDER BY seq DESC LIMIT 1",
        (experiment_id, _EVENT_KIND, request_id),
    ).fetchone()
    return json.loads(row["payload"]) if row else None


def ask_owner(store, expected_action, question):
    """Save one question for the exact saved turn, refusing a changed or frozen state.

    ``expected_action`` is the snapshot the tool was called for. An exact replay
    returns the saved receipt; a changed payload, a second unanswered question or an
    answered but unconsumed request refuses.
    """
    if not isinstance(question, AskOwnerQuestion):
        question = AskOwnerQuestion.model_validate(question)
    request_id = str(question.request_id)
    options = list(question.options)
    with store.tx() as db:
        if blocked(store):
            raise Conflict("the executor is under maintenance; no owner question was saved")
        experiment = _experiment(db, expected_action)
        action = _action(db, expected_action)
        if experiment["thread_id"] != action["thread_id"]:
            raise Conflict("the saved research action and experiment threads differ")

        value = experiment["pending_request"]
        pending = json.loads(value) if value else None
        event = _receipt_event(db, experiment["id"], request_id)
        if event is not None:
            if (
                event.get("action_id") != action["id"]
                or event.get("thread_id") != action["thread_id"]
                or event.get("kind", "question") != question.kind
                or event.get("objective") != question.objective
                or event.get("question") != question.question
                or tuple(event.get("options") or ()) != tuple(options)
            ):
                raise Conflict("request_id was already used for a different owner question")
            if pending is not None and pending.get("id") == request_id:
                state = "answered" if pending.get("answer") else "open"
                return _receipt(experiment["id"], action, request_id, state)
            return _receipt(experiment["id"], action, request_id, "closed")
        if pending is not None:
            if pending.get("answer"):
                raise Conflict("the saved owner answer has not been consumed yet")
            raise Conflict("another owner question is still unanswered")

        approval = question.kind == "approval"
        saved = {
            "id": request_id,
            "action_id": action["id"],
            "thread_id": action["thread_id"],
            "method": "item/commandExecution/requestApproval" if approval else _REQUEST_METHOD,
            "params": {
                "questions": [
                    {
                        "id": _QUESTION_ID,
                        "question": question.question,
                        "options": options,
                    }
                ]
            },
            "expired": False,
        }
        if approval:
            saved["kind"] = "approval"
            saved["params"]["objective"] = question.objective
            project_id = json.loads(experiment["contract"]).get("project", {}).get("id")
            if not project_id:
                raise Conflict("approval requires an exact saved project")
            store.emit(
                db,
                experiment["id"],
                REQUEST_EVENT,
                {
                    "experiment_id": experiment["id"],
                    "project_id": project_id,
                    "brief": experiment["brief"],
                    "request_id": request_id,
                    "objective": question.objective,
                },
            )
            from ..autopilot import Autopilot

            orchestrator = Autopilot(store)
            if orchestrator.enabled(project_id):
                member = db.execute(
                    "SELECT run_id FROM autopilot_members WHERE experiment_id=? AND project_id=?",
                    (experiment["id"], project_id),
                ).fetchone()
                if member:
                    orchestrator.wake(
                        db,
                        member["run_id"],
                        request_id,
                        "experiment_approval_requested",
                        {
                            "experiment_id": experiment["id"],
                            "request_id": request_id,
                            "objective": question.objective,
                        },
                    )
        before = request_obligations.saved_pending(db, experiment["id"])
        now = clock.now()
        db.execute(
            "UPDATE experiments SET pending_request=? WHERE id=?",
            (encode(saved), experiment["id"]),
        )
        request_obligations.sync_request(db, experiment["id"], before=before)
        text = " ".join(f"Owner answer needed: {question.question}".split())
        db.execute(
            "INSERT OR IGNORE INTO inbox VALUES(?,?,?,?,?)",
            (str(uuid4()), experiment["id"], _NOTICE_KIND, text[:2000], now),
        )
        store.emit(
            db,
            experiment["id"],
            _EVENT_KIND,
            {
                "request_id": request_id,
                "action_id": action["id"],
                "thread_id": action["thread_id"],
                "question": question.question,
                "options": options,
                **({"kind": "approval", "objective": question.objective} if approval else {}),
            },
        )
        return _receipt(experiment["id"], action, request_id, "open")
