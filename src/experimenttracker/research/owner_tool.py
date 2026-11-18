"""C7 ``ask_owner``: one durable owner question tied to the exact research turn.

This is the inert question-only slice of the tool. It records a reply-free owner
question against one saved live contract 4 research action and persists the same
required-decision notice, owner-answer obligation, event and outbox row in one
transaction. It never replies to a model RPC, consumes a prompt, pauses, settles,
deletes, changes a terminal phase, approves or submits anything. Approval remains
closed until T4.4.

The saved ``owner_question_asked`` event is the exact historical receipt, so a
replay never reopens a consumed request. A changed payload, a second unanswered
question or an answered but unconsumed request refuses. The saved shape stays
compatible with the existing answer command.
"""

import json
from uuid import UUID, uuid4

from pydantic import Field, field_validator

from .. import clock
from ..config import StrictModel
from ..core import action_obligations, request_obligations
from ..maintenance import blocked
from ..store import Conflict, encode

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
    """The strict bounded input of one question; approval has no field here."""

    request_id: UUID
    question: str = Field(min_length=1, max_length=_MAX_QUESTION)
    options: list[str] = Field(default_factory=list, max_length=_MAX_OPTIONS)

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
        "SELECT id,phase,deleted_at,contract,thread_id,pause_agent,pending_request "
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

        saved = {
            "id": request_id,
            "action_id": action["id"],
            "thread_id": action["thread_id"],
            "method": _REQUEST_METHOD,
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
            },
        )
        return _receipt(experiment["id"], action, request_id, "open")
