"""C7 research session: queue one bounded turn per experiment.

The helpers are inert. Each records one research action, its obligation and the queue
event in one transaction, and never starts a turn, creates a Codex thread or
dispatches work. A settled, failed or uncertain action still counts as the
experiment's one first turn, so a replay or a reopened store never queues another.

``follow_up_turn`` is the first wake source: the exact saved owner answer of a
contract 4 question. It queues one follow-up only after that answer's coalescing
window has closed and the preceding research turn has settled, binding the due
wakeup and clearing the saved request in that same transaction. The answer text goes
only into the bounded prompt, which the projection never publishes, and no answer
text is emitted in an event. The private checkpoint Markdown is deliberately not
inlined into the saved action input, so the saved action record carries neither
notebook text nor a copy of it.
"""

import json
from uuid import uuid4

from .. import clock
from ..core import action_obligations, request_obligations
from ..maintenance import blocked
from . import wakeups
from .prompt import turn_prompt

_TERMINAL_PHASES = frozenset({"complete", "failed", "cancelled"})
_RESEARCH_PHASE = "research"
# Only a completed turn precedes a follow-up: a running or uncertain one is never
# retried, and a failed one needs the owner, not another turn.
_SETTLED_STATES = frozenset({"completed"})
_TERMINAL_ACTION_STATES = frozenset({"completed", "failed", "cancelled"})
# The notebook Markdown stays in its event and out of the saved action input.
_DEFERRED_CHECKPOINT = "[saved notebook text is not copied into a follow-up turn]"


def _owner_constraints(db, contract):
    """Return the saved project instructions that constrain this experiment's turns."""
    project_id = (contract.get("project") or {}).get("id")
    if not project_id:
        return []
    row = db.execute(
        "SELECT instructions FROM project_preferences WHERE project_id=?", (project_id,)
    ).fetchone()
    if row is None or not row["instructions"].strip():
        return []
    return [row["instructions"]]


def _saved_experiment(db, eid):
    """The saved live, unpaused contract 4 experiment, or ``None`` for anything else."""
    row = db.execute(
        "SELECT id,brief,contract,phase,deleted_at,pause_agent,pending_request,evidence_revision "
        "FROM experiments WHERE id=?",
        (eid,),
    ).fetchone()
    if row is None:
        return None
    experiment = dict(row)
    contract = json.loads(experiment["contract"])
    if (
        experiment["deleted_at"] is not None
        or experiment["phase"] in _TERMINAL_PHASES
        or experiment["pause_agent"]
        or contract.get("execution_contract") != 4
    ):
        return None
    experiment["contract"] = contract
    return experiment


def _settled_predecessor(db, eid):
    """The latest saved research action when all earlier turns are terminal."""
    rows = db.execute(
        "SELECT id,state FROM agent_actions WHERE experiment_id=? AND phase=? "
        "ORDER BY created_at DESC,rowid DESC",
        (eid, _RESEARCH_PHASE),
    ).fetchall()
    if not rows or rows[0]["state"] not in _SETTLED_STATES:
        return None
    if any(row["state"] not in _TERMINAL_ACTION_STATES for row in rows[1:]):
        return None
    return dict(rows[0])


def _saved_answer(experiment, predecessor):
    """The answered request asked by the settled predecessor, or ``None``."""
    value = experiment["pending_request"]
    pending = json.loads(value) if value else None
    if not isinstance(pending, dict) or pending.get("action_id") != predecessor["id"]:
        return None
    if pending.get("rpc_id") is not None or pending.get("expired"):
        return None
    answer = pending.get("answer")
    if not isinstance(answer, str) or not answer.strip():
        return None
    return pending, answer


def _answer_line(pending, answer):
    """One prompt line naming the saved question and the owner's exact answer."""
    questions = (pending.get("params") or {}).get("questions") or []
    question = questions[0].get("question") if questions else None
    if not isinstance(question, str) or not question.strip():
        return f"Owner answer to request {pending['id']}: {answer}"
    return f"Owner answer to {question.strip()}: {answer}"


def first_turn(store, eid):
    """Queue one initial research action for an active contract 4 experiment.

    Returns the saved pending action, or ``None`` when the experiment is missing,
    deleted, terminal, not exactly contract 4, or already has a first turn. The
    saved row keeps its thread and turn IDs empty until a real dispatch records
    them.
    """
    with store.tx() as db:
        row = db.execute(
            "SELECT id,brief,contract,phase,deleted_at,evidence_revision FROM experiments WHERE id=?",
            (eid,),
        ).fetchone()
        if row is None:
            return None
        experiment = dict(row)
        contract = json.loads(experiment["contract"])
        if (
            experiment["deleted_at"] is not None
            or experiment["phase"] in _TERMINAL_PHASES
            or contract.get("execution_contract") != 4
        ):
            return None
        if db.execute(
            "SELECT 1 FROM agent_actions WHERE experiment_id=? AND phase=?", (eid, _RESEARCH_PHASE)
        ).fetchone():
            return None
        return _queue(store, db, experiment, _owner_constraints(db, contract))


def _queue(store, db, experiment, owner_constraints, events=(), checkpoint=None):
    """Save one pending research action for this experiment and return it."""
    eid = experiment["id"]
    action_id = str(uuid4())
    prompt = turn_prompt(
        brief=experiment["brief"],
        owner_constraints=owner_constraints,
        checkpoint=checkpoint,
        events=events,
        action_id=action_id,
    )
    now = clock.now()
    db.execute(
        "INSERT INTO agent_actions"
        "(id,experiment_id,phase,evidence_revision,state,input,created_at,updated_at) "
        "VALUES(?,?,?,?,'pending',?,?,?)",
        (action_id, eid, _RESEARCH_PHASE, experiment["evidence_revision"], prompt, now, now),
    )
    action_obligations.sync_action(db, action_id)
    store.emit(db, eid, "agent_action_queued", {"phase": _RESEARCH_PHASE, "action_id": action_id})
    return dict(db.execute("SELECT * FROM agent_actions WHERE id=?", (action_id,)).fetchone())


def follow_up_turn(store, eid, *, now=None):
    """Queue one follow-up research turn for this experiment's due owner answer.

    Exactly one turn follows a settled predecessor and an answered owner request
    whose 60-second window has closed. The exact answer enters the bounded prompt
    and the due wakeup is bound to the new action in the same transaction, so a
    restart or replay finds all of it or none of it. A paused, terminal, maintained
    or non-contract-4 experiment, a running, uncertain or failed predecessor, an
    unanswered or foreign request and a job or timer wake queue nothing. Returns the
    saved pending action, or ``None`` when this pass has no owner answer to queue.
    """
    now = clock.now() if now is None else now
    due = [
        item for item in wakeups.due(store, eid, now=now) if item["event"] == wakeups.OWNER_REQUEST_ANSWERED
    ]
    if not due or blocked(store):
        return None
    with store.tx() as db:
        # The C3 blocked-phase resume flow is independent of the maintenance hold;
        # never use a C4 answer to bypass its saved blocked action.
        if db.execute(
            "SELECT 1 FROM experiments WHERE id=? AND hold_reason LIKE '%resume_blocked%'", (eid,)
        ).fetchone():
            return None
        experiment = _saved_experiment(db, eid)
        if experiment is None:
            return None
        predecessor = _settled_predecessor(db, eid)
        if predecessor is None:
            return None
        answered = _saved_answer(experiment, predecessor)
        if answered is None:
            return None
        pending, answer = answered
        bind = [item["wakeup_id"] for item in due if item["source_id"] == pending["id"]]
        if not bind:
            return None
        action = _queue(
            store,
            db,
            experiment,
            _owner_constraints(db, experiment["contract"]),
            events=[_answer_line(pending, answer)],
            checkpoint=_DEFERRED_CHECKPOINT,
        )
        wakeups.consume(store, eid, bind, action["id"], db=db, now=now)
        # The answer is consumed into the queued prompt; the saved request closes.
        db.execute("UPDATE experiments SET pending_request=NULL WHERE id=?", (eid,))
        request_obligations.sync_request(db, eid, before=pending)
        return action
