"""C7 event wake queue: durable exact events coalesced into one 60-second window.

Only a supported event wakes a contract 4 experiment: an answered, satisfied owner
request or a saved terminal job. An ambient tick, an unanswered, expired or
unsatisfied request and a healthy job are refused. Each event keeps one
``agent_wakeups`` row whose ID derives from the exact experiment, event and source,
so a replay adopts the saved row. Arrivals share the first arrival's due time and are
never merged, so one turn covers them without postponing the deadline or losing
distinct evidence; a closed window starts the next one.

``due`` reads the coalesced evidence and ``consume`` binds exactly those rows to one
already saved pending ``phase='research'`` action in the same transaction, so
evidence is never consumed before the action that will use it exists. Both join the
caller's transaction with the timer obligation. Nothing here creates, dispatches or
settles an action, changes a phase or publishes owner text.
"""

import json
from uuid import UUID, uuid5

from .. import clock
from ..core import obligation_sources, wakeup_obligations
from ..store import Conflict, encode

PHASE = "research"
DEBOUNCE_SECONDS = 60
MAX_DUE_WAKEUPS = 32
MAX_SOURCE_ID = 200
OWNER_REQUEST_ANSWERED = "owner_request_answered"
ORCHESTRATOR_APPROVAL = "orchestrator_approval"
JOB_TERMINAL = "job_terminal"
RESEARCH_TIMER = "research_timer"
SUPPORTED_EVENTS = frozenset({OWNER_REQUEST_ANSWERED, ORCHESTRATOR_APPROVAL, JOB_TERMINAL, RESEARCH_TIMER})
_NAMESPACE = UUID("0f4e1a2b-3c5d-4e6f-8a9b-1c2d3e4f5a6b")
_TERMINAL_PHASES = frozenset({"complete", "failed", "cancelled"})
# The terminal values saved in jobs.state; stage states are a different vocabulary.
_TERMINAL_JOB_STATES = frozenset({"complete", "failed", "cancelled"})


def wakeup_id(experiment_id, event, source_id):
    """The stable ID of one exact event; the same source always maps to one wakeup."""
    return str(uuid5(_NAMESPACE, f"{experiment_id}:{event}:{source_id}"))


def _evidence(row):
    """Return one saved wakeup's event evidence, or ``None`` for a contract 3 timer."""
    value = json.loads(row["reason"]) if row["reason"] else None
    if not isinstance(value, dict) or value.get("event") not in SUPPORTED_EVENTS:
        return None
    source_id = value.get("source_id")
    if not isinstance(source_id, str) or not source_id:
        return None
    keys = ("due_at", "state", "action_id")
    return {
        "wakeup_id": row["id"],
        "event": value["event"],
        "source_id": source_id,
        **{key: row[key] for key in keys},
    }


def _live_experiment(db, eid):
    """The saved live contract 4 experiment, or a refusal naming the missing scope."""
    row = db.execute(
        "SELECT id,contract,phase,deleted_at,pending_request FROM experiments WHERE id=?", (eid,)
    ).fetchone()
    if row is None:
        raise Conflict("saved research experiment disappeared")
    if (json.loads(row["contract"]) or {}).get("execution_contract") != 4:
        raise Conflict("event wakeups belong to a contract 4 research experiment")
    if row["deleted_at"] is not None or row["phase"] in _TERMINAL_PHASES:
        raise Conflict("a deleted or terminal experiment cannot record an event wakeup")
    return dict(row)


def _answered_request(db, experiment, source_id):
    """Refuse anything but this experiment's saved, answered, satisfied owner request."""
    value = experiment["pending_request"]
    pending = json.loads(value) if value else None
    if not isinstance(pending, dict) or pending.get("id") != source_id:
        raise Conflict("the saved owner request has another identity")
    if pending.get("rpc_id") is not None:
        raise Conflict("an app-server request is not a durable owner event")
    if pending.get("expired"):
        raise Conflict("an expired owner request is not a durable event")
    answer = pending.get("answer")
    if not isinstance(answer, str) or not answer.strip():
        raise Conflict("the saved owner request has no answer")
    action = db.execute(
        "SELECT experiment_id,phase FROM agent_actions WHERE id=?", (pending.get("action_id"),)
    ).fetchone()
    if action is None or action["experiment_id"] != experiment["id"] or action["phase"] != PHASE:
        raise Conflict("the answered request names no saved research action of this experiment")
    owner = obligation_sources.latest_episode(db, "request", f"{experiment['id']}:{source_id}")
    if owner is None or owner["state"] != "satisfied":
        raise Conflict("the saved owner request obligation is not satisfied")


def _orchestrator_approval(store, db, experiment, source_id, predecessor_id):
    """Verify exact orchestrator proof and the turn that asked for this approval."""
    pending = json.loads(experiment["pending_request"]) if experiment["pending_request"] else None
    if (
        not isinstance(pending, dict)
        or pending.get("id") != source_id
        or pending.get("kind") != "approval"
        or pending.get("action_id") != predecessor_id
        or pending.get("answer_source", {}).get("kind") != "orchestrator"
    ):
        raise Conflict("orchestrator approval does not name the requesting predecessor")
    objective = (pending.get("params") or {}).get("objective")
    from . import autopilot_approval

    proof_event = db.execute(
        "SELECT payload FROM events WHERE experiment_id=? AND kind='autopilot_objective_approved' "
        "AND json_extract(payload,'$.request_id')=? LIMIT 2",
        (experiment["id"], source_id),
    ).fetchall()
    source = pending["answer_source"]
    saved_proof = json.loads(proof_event[0]["payload"]) if len(proof_event) == 1 else {}
    if (
        len(proof_event) != 1
        or saved_proof.get("action_id") != source.get("action_id")
        or saved_proof.get("run_id") != source.get("run_id")
        or autopilot_approval.approved(store, experiment["id"], source_id, objective, db=db) is None
    ):
        raise Conflict("the exact orchestrator approval proof is not saved")


def _pending_research_action(db, eid, action_id):
    """Refuse anything but this experiment's saved pending research action."""
    row = db.execute(
        "SELECT experiment_id,phase,state FROM agent_actions WHERE id=?", (action_id,)
    ).fetchone()
    if row is None or row["experiment_id"] != eid:
        raise Conflict("the wakeup action does not belong to this experiment")
    if row["phase"] != PHASE or row["state"] != "pending":
        raise Conflict("event wakeups bind only to this experiment's pending research action")


def record(store, eid, event, source_id, *, delay=None, db=None, now=None):
    """Record one supported event wakeup, or adopt its saved row on an exact replay.

    Pass ``db`` to record inside the transaction that saves the source event. A replay
    returns the saved evidence without a second row, a changed source with the same
    stable ID is refused, and a first event sets the due time arrivals share.
    """
    if event not in SUPPORTED_EVENTS:
        raise ValueError(f"unsupported research wake event: {event!r}")
    if not isinstance(source_id, str) or not source_id.strip() or len(source_id) > MAX_SOURCE_ID:
        raise ValueError("an event wakeup needs a bounded saved source identity")
    if db is None:
        with store.tx() as transaction:
            return record(store, eid, event, source_id, delay=delay, db=transaction, now=now)
    now = clock.now() if now is None else now
    if event == RESEARCH_TIMER and (type(delay) is not int or not 60 <= delay <= 86400):
        raise ValueError("a research timer needs a delay between 60 and 86400 seconds")
    if event != RESEARCH_TIMER and delay is not None:
        raise ValueError("only a research timer accepts a delay")
    payload = encode(
        {"event": event, "source_id": source_id, **({"delay": delay} if delay is not None else {})}
    )
    wakeup = wakeup_id(eid, event, source_id)
    experiment = _live_experiment(db, eid)
    prior = db.execute("SELECT * FROM agent_wakeups WHERE id=?", (wakeup,)).fetchone()
    if prior is not None:
        if prior["experiment_id"] != eid or prior["reason"] != payload:
            raise Conflict("wakeup identity was reused for a different event")
        return _evidence(prior)
    if event == OWNER_REQUEST_ANSWERED:
        _answered_request(db, experiment, source_id)
    elif event == ORCHESTRATOR_APPROVAL:
        pending = json.loads(experiment["pending_request"]) if experiment["pending_request"] else None
        if not isinstance(pending, dict) or pending.get("id") != source_id:
            raise Conflict("the saved approval request has another identity")
        _orchestrator_approval(store, db, experiment, source_id, pending.get("action_id"))
    elif event == JOB_TERMINAL:
        job = db.execute(
            "SELECT state FROM jobs WHERE intent_id=? AND experiment_id=?", (source_id, eid)
        ).fetchone()
        if job is None or job["state"] not in _TERMINAL_JOB_STATES:
            raise Conflict("only this experiment's saved terminal job is an event")
    else:
        from .prompt import timer_delay

        predecessor = db.execute(
            "SELECT experiment_id,phase,state,result FROM agent_actions WHERE id=?", (source_id,)
        ).fetchone()
        if (
            predecessor is None
            or predecessor["experiment_id"] != eid
            or predecessor["phase"] != PHASE
            or predecessor["state"] != "completed"
        ):
            raise Conflict("a research timer needs its exact completed predecessor")
        result = json.loads(predecessor["result"] or "null")
        if (
            not isinstance(result, dict)
            or result.get("status") != "wait"
            or timer_delay(result.get("next")) != delay
        ):
            raise Conflict("a research timer must match its saved wait decision")
    due_at = now + (delay if event == RESEARCH_TIMER else DEBOUNCE_SECONDS)
    if event != RESEARCH_TIMER:
        pending = db.execute(
            "SELECT * FROM agent_wakeups WHERE experiment_id=? AND state='pending' AND due_at>? "
            "AND CASE WHEN json_valid(reason) THEN json_extract(reason,'$.event') END IN (?,?,?) "
            "ORDER BY due_at,created_at,id LIMIT 1",
            (eid, now, OWNER_REQUEST_ANSWERED, ORCHESTRATOR_APPROVAL, JOB_TERMINAL),
        ).fetchone()
        if pending is not None:
            due_at = pending["due_at"]
    db.execute(
        "INSERT INTO agent_wakeups(id,experiment_id,reason,due_at,state,action_id,created_at) "
        "VALUES(?,?,?,?,'pending',NULL,?)",
        (
            wakeup,
            eid,
            payload,
            due_at,
            now,
        ),
    )
    wakeup_obligations.sync_wakeup(db, wakeup)
    return _evidence(db.execute("SELECT * FROM agent_wakeups WHERE id=?", (wakeup,)).fetchone())


def supersede_timer(db, eid, predecessor_id):
    """A different wake made the predecessor's outstanding timer obsolete."""
    identity = wakeup_id(eid, RESEARCH_TIMER, predecessor_id)
    updated = db.execute(
        "UPDATE agent_wakeups SET state='superseded' WHERE id=? AND experiment_id=? AND state='pending'",
        (identity, eid),
    )
    if updated.rowcount:
        wakeup_obligations.sync_wakeup(db, identity)


def due(store, eid, *, now=None):
    """Return this live experiment's coalesced event evidence that is due now.

    The read is bounded, keeps one entry per distinct source event and is empty without
    a closed window, so a healthy job or an unanswered question costs nothing.
    """
    now = clock.now() if now is None else now
    try:
        experiment = store.experiment(eid)
    except KeyError:
        return []
    if (
        experiment["deleted_at"] is not None
        or experiment["phase"] in _TERMINAL_PHASES
        or (experiment["contract"] or {}).get("execution_contract") != 4
    ):
        return []
    rows = store.rows(
        "SELECT * FROM agent_wakeups WHERE experiment_id=? AND state='pending' AND due_at<=? "
        "ORDER BY due_at,created_at,id LIMIT ?",
        (eid, now, MAX_DUE_WAKEUPS),
    )
    return [item for item in map(_evidence, rows) if item is not None]


def consume(store, eid, wakeup_ids, action_id, *, db=None, now=None):
    """Bind exactly these due event wakeups to one saved pending research action.

    Each row must be this experiment's event wakeup whose window has closed. Binding
    saves ``queued`` with the exact action ID and settles its timer obligation in the
    caller's transaction; an already bound row is adopted only for the same action.
    """
    ids = list(dict.fromkeys(str(value) for value in wakeup_ids))
    if db is None:
        with store.tx() as transaction:
            return consume(store, eid, ids, action_id, db=transaction, now=now)
    if not ids:
        return []
    now = clock.now() if now is None else now
    action_id = str(action_id)
    _pending_research_action(db, eid, action_id)
    experiment = _live_experiment(db, eid)
    predecessor = db.execute(
        "SELECT id FROM agent_actions WHERE experiment_id=? AND phase=? AND state='completed' "
        "ORDER BY created_at DESC,rowid DESC LIMIT 1",
        (eid, PHASE),
    ).fetchone()
    evidence = []
    for wakeup in ids:
        row = db.execute(
            "SELECT * FROM agent_wakeups WHERE id=? AND experiment_id=?", (wakeup, eid)
        ).fetchone()
        saved = None if row is None else _evidence(row)
        if saved is None:
            raise Conflict("not this experiment's saved event wakeup")
        if saved["event"] == ORCHESTRATOR_APPROVAL:
            pending = json.loads(experiment["pending_request"]) if experiment["pending_request"] else None
            if (
                predecessor is None
                or not isinstance(pending, dict)
                or pending.get("id") != saved["source_id"]
                or pending.get("action_id") != predecessor["id"]
                or pending.get("answer_source", {}).get("kind") != "orchestrator"
            ):
                raise Conflict("orchestrator approval has no exact completed requesting predecessor")
            # record() validated current authority transactionally when it saved this
            # wakeup. A later orchestrator revision must not strand that wakeup or
            # replay the old approval as authority; the follow-up is informational.
        if saved["state"] == "pending":
            if saved["due_at"] > now:
                raise Conflict("a wakeup before its coalescing window cannot be bound")
            db.execute(
                "UPDATE agent_wakeups SET state='queued',action_id=? WHERE id=? AND state='pending'",
                (action_id, wakeup),
            )
            wakeup_obligations.sync_wakeup(db, wakeup)
            saved = {**saved, "state": "queued", "action_id": action_id}
        elif saved["state"] not in ("queued", "complete"):
            raise Conflict(f"a {saved['state']} event wakeup cannot be bound")
        elif saved["action_id"] != action_id:
            raise Conflict("this event wakeup is bound to another action")
        evidence.append(saved)
    return evidence
