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
from .prompt import DEFERRED_CHECKPOINT, turn_prompt

_TERMINAL_PHASES = frozenset({"complete", "failed", "cancelled"})
_RESEARCH_PHASE = "research"
# Only a completed turn precedes a follow-up: a running or uncertain one is never
# retried, and a failed one needs the owner, not another turn.
_SETTLED_STATES = frozenset({"completed"})
_TERMINAL_ACTION_STATES = frozenset({"completed", "failed", "cancelled"})


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
        "SELECT id,brief,contract,phase,deleted_at,pause_agent,pending_request,evidence_revision,thread_id "
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
        "SELECT id,state,result,thread_id,turn_id FROM agent_actions WHERE experiment_id=? AND phase=? "
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


def _queue(
    store,
    db,
    experiment,
    owner_constraints,
    events=(),
    checkpoint=None,
    thread_id=None,
    owner_approval=None,
    orchestrator_approval=None,
):
    """Save one pending research action for this experiment and return it."""
    eid = experiment["id"]
    action_id = str(uuid4())
    prompt = turn_prompt(
        brief=experiment["brief"],
        owner_constraints=owner_constraints,
        checkpoint=checkpoint,
        events=events,
        action_id=action_id,
        owner_approval=owner_approval,
        orchestrator_approval=orchestrator_approval,
    )
    now = clock.now()
    db.execute(
        "INSERT INTO agent_actions"
        "(id,experiment_id,phase,evidence_revision,state,thread_id,input,created_at,updated_at) "
        "VALUES(?,?,?,?,'pending',?,?,?,?)",
        (action_id, eid, _RESEARCH_PHASE, experiment["evidence_revision"], thread_id, prompt, now, now),
    )
    action_obligations.sync_action(db, action_id)
    store.emit(db, eid, "agent_action_queued", {"phase": _RESEARCH_PHASE, "action_id": action_id})
    return dict(db.execute("SELECT * FROM agent_actions WHERE id=?", (action_id,)).fetchone())


def _continue_turn(store, db, experiment, predecessor):
    """Queue from a settled result, never from the presence of unrelated wakeups."""
    if (
        experiment["pending_request"]
        or not predecessor["turn_id"]
        or not predecessor["thread_id"]
        or predecessor["thread_id"] != experiment["thread_id"]
    ):
        return None
    try:
        result = json.loads(predecessor["result"] or "{}")
    except (TypeError, ValueError):
        return None
    if not isinstance(result, dict) or result.get("status") != "continue":
        return None
    return _queue(
        store,
        db,
        experiment,
        _owner_constraints(db, experiment["contract"]),
        checkpoint=DEFERRED_CHECKPOINT,
        thread_id=predecessor["thread_id"],
    )


def _job_events(db, experiment, predecessor, due):
    thread = predecessor["thread_id"]
    if not predecessor["turn_id"] or not thread or thread != experiment["thread_id"]:
        return [], []
    events, bind = [], []
    for item in due:
        if item["event"] == wakeups.RESEARCH_TIMER:
            if item["source_id"] != predecessor["id"]:
                continue
            events.append(f"Timer due for completed research action {predecessor['id']}")
            bind.append(item["wakeup_id"])
            continue
        if item["event"] != wakeups.JOB_TERMINAL:
            continue
        job = db.execute(
            "SELECT state,exit_code FROM jobs WHERE experiment_id=? AND intent_id=? "
            "AND job_id IS NOT NULL AND state IN ('complete','failed','cancelled')",
            (experiment["id"], item["source_id"]),
        ).fetchone()
        if job is None:
            continue
        events.append(f"Job {item['source_id']}: {job['state']} (exit {job['exit_code'] or 'unknown'})")
        bind.append(item["wakeup_id"])
    return events, bind


def follow_up_turn(store, eid, *, now=None):
    """Queue from a settled decision or due evidence; bind each consumed wake atomically."""
    now = clock.now() if now is None else now
    due = wakeups.due(store, eid, now=now)
    with store.tx() as db:
        if blocked(store):
            return None
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
        events, bind = _job_events(db, experiment, predecessor, due)
        pending = owner_approval = orchestrator_approval = None
        if experiment["pending_request"]:
            answered = _saved_answer(experiment, predecessor)
            if answered is None:
                return None
            pending, answer = answered
            source = pending.get("answer_source") or {}
            event_kind = (
                wakeups.ORCHESTRATOR_APPROVAL
                if source.get("kind") == "orchestrator"
                else wakeups.OWNER_REQUEST_ANSWERED
            )
            answer_ids = [
                item["wakeup_id"]
                for item in due
                if item["event"] == event_kind and item["source_id"] == pending["id"]
            ]
            if not answer_ids:
                return None
            bind.extend(answer_ids)
            objective = (pending.get("params") or {}).get("objective")
            if source.get("kind") == "orchestrator" and pending.get("kind") == "approval":
                from .autopilot_approval import approved as orchestrator_approved

                if orchestrator_approved(store, eid, pending["id"], objective, db=db) is not None:
                    orchestrator_approval = (pending["id"], objective)
                else:
                    events.append(
                        "The saved project-orchestrator approval is no longer current. "
                        "Do not treat it as authorization; request a new approval for this objective."
                    )
            elif pending.get("kind") == "approval" and isinstance(objective, str):
                owner_approval = (pending["id"], objective, answer)
            else:
                events.append(_answer_line(pending, answer))
        guidance = None
        # Reserve the bounded prompt's event budget for already-due wake evidence.
        if (
            not bind
            and predecessor["turn_id"]
            and predecessor["thread_id"]
            and (predecessor["thread_id"] == experiment["thread_id"])
        ):
            guidance = db.execute(
                "SELECT request_id,text FROM steering WHERE experiment_id=? AND mode='later' AND state='pending' "
                "ORDER BY created_at,rowid LIMIT 1",
                (eid,),
            ).fetchone()
        if not bind and not guidance:
            return _continue_turn(store, db, experiment, predecessor)
        if guidance:
            events.append(f"Owner steering: {guidance['text']}")
        action = _queue(
            store,
            db,
            experiment,
            _owner_constraints(db, experiment["contract"]),
            events=events,
            checkpoint=DEFERRED_CHECKPOINT,
            thread_id=experiment["thread_id"],
            owner_approval=owner_approval,
            orchestrator_approval=orchestrator_approval,
        )
        wakeups.consume(store, eid, bind, action["id"], db=db, now=now)
        wakeups.supersede_timer(db, eid, predecessor["id"])
        if guidance:
            db.execute(
                "UPDATE steering SET state='applied',applied_at=? WHERE request_id=? AND state='pending'",
                (now, guidance["request_id"]),
            )
            store.emit(db, eid, "steering_applied", {"request_ids": [guidance["request_id"]]})
        if pending is not None:
            db.execute("UPDATE experiments SET pending_request=NULL WHERE id=?", (eid,))
            request_obligations.sync_request(db, eid, before=pending)
        return action
