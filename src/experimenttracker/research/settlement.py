"""C7 turn-end settlement: one exact research turn, one saved outcome and commit.

``complete`` accepts only a saved contract 4 research action whose thread and turn
identities still match the caller's snapshot, validates the turn end against saved
evidence, commits the named worktree once with an action-specific message, then
settles the action and its existing obligation in one transaction. It never starts or
retries a turn, reads a contract 3 stage result or changes an experiment phase.
The commit reuses ``Workspaces.commit_branch`` and its intent/settled events: a
settled receipt is reused without a second commit; an unsettled intent is left for
inspection instead of being replayed. Terminal, cancelled and deleted experiments
settle their exact raw outcome without committing.
"""

import asyncio
import json

from pydantic import ValidationError

from ..codex_client import NeedsAttention
from ..core import action_obligations
from ..store import encode
from .prompt import TurnEnd, TurnEvidence, validate_turn_end

PHASE = "research"
_COMMIT_PREFIX = "Research turn "
_ACTIVE_JOB_STATES = ("submitting", "submitted", "agent")
_TERMINAL_PHASES = frozenset({"complete", "failed", "cancelled"})
_LIVE_STATES = frozenset({"dispatching", "running", "uncertain"})
_ADOPTING_STATES = frozenset({"dispatching", "uncertain"})
_WORKSPACE_EVENTS = ("workspace_commit_intent", "workspace_commit_settled")


def commit_message(action_id):
    return f"{_COMMIT_PREFIX}{action_id}"


def _saved_action(store, expected):
    current = store.one("SELECT * FROM agent_actions WHERE id=?", (expected["id"],))
    if current is None:
        raise NeedsAttention("saved research action disappeared before settlement")
    action_obligations.check_expected(current, expected)
    return current


def _contract_four(store, expected):
    try:
        experiment = store.experiment(expected["experiment_id"])
    except KeyError:
        raise NeedsAttention("saved research experiment disappeared before settlement") from None
    if (experiment["contract"] or {}).get("execution_contract") != 4:
        raise NeedsAttention("a saved contract 3 action is not a research turn")
    if experiment["thread_id"] != expected["thread_id"]:
        raise NeedsAttention("saved experiment thread differs from the research action")
    return experiment


def _terminal(experiment):
    return experiment["deleted_at"] is not None or experiment["phase"] in _TERMINAL_PHASES


def _turn_identity(action, recovered_turn_id):
    if action["phase"] != PHASE:
        raise NeedsAttention("saved action is not a research turn")
    if action["state"] not in _LIVE_STATES:
        raise NeedsAttention("saved research action is not live")
    if not action["thread_id"]:
        raise NeedsAttention("saved research action has no exact thread identity")
    saved = action["turn_id"]
    if recovered_turn_id is None:
        if not saved:
            raise NeedsAttention("saved research action has no exact turn identity")
        return saved
    if not isinstance(recovered_turn_id, str) or not recovered_turn_id.strip():
        raise NeedsAttention("a read-back turn identity must be a non-empty string")
    if saved is not None and saved != recovered_turn_id:
        raise NeedsAttention("read-back turn differs from the saved research turn")
    if saved is None:
        if action["state"] not in _ADOPTING_STATES:
            raise NeedsAttention("a running research action cannot adopt a different turn")
        return recovered_turn_id
    return saved


def _open_question(db, experiment):
    """True when a saved owner request also has its open owner obligation."""
    pending = experiment["pending_request"]
    if not (
        isinstance(pending, dict)
        and isinstance(pending.get("id"), str)
        and pending["id"]
        and not pending.get("expired")
        and not pending.get("answer")
    ):
        return False
    row = db.execute(
        "SELECT 1 FROM obligations WHERE experiment_id=? AND state='open' AND owner='owner' "
        "AND kind IN ('owner_answer','owner_approval') AND target=? LIMIT 1",
        (experiment["id"], f"{experiment['id']}:{pending['id']}"),
    ).fetchone()
    return row is not None


def _evidence(db, experiment):
    """Observe the saved conditions C7 requires; timers stay unvalidated until T4.3."""
    eid = experiment["id"]
    active = db.execute(
        "SELECT 1 FROM jobs WHERE experiment_id=? AND state IN (?,?,?) LIMIT 1",
        (eid, *_ACTIVE_JOB_STATES),
    ).fetchone()
    question = _open_question(db, experiment)
    report = db.execute(
        "SELECT 1 FROM report_versions WHERE experiment_id=? AND state='ready' LIMIT 1", (eid,)
    ).fetchone()
    return TurnEvidence(
        active_jobs=1 if active else 0,
        pending_answers=1 if question else 0,
        open_ask_owner=question,
        published_report=report is not None,
    )


def _turn_end(raw_result):
    if isinstance(raw_result, dict):
        return TurnEnd.model_validate(raw_result)
    if isinstance(raw_result, str):
        return TurnEnd.model_validate_json(raw_result)
    raise ValueError("a research turn end must be an object or JSON object text")


def _raw_result(raw_result):
    if isinstance(raw_result, str):
        return raw_result
    try:
        return encode(raw_result)
    except (TypeError, ValueError):
        return str(raw_result)


def _commit_receipt(store, eid, message):
    """Return the one settled commit for this action, or ``None`` when none was attempted."""
    intents, settled = {}, {}
    for row in store.rows(
        "SELECT kind,payload FROM events WHERE experiment_id=? AND kind IN (?,?) ORDER BY seq",
        (eid, *_WORKSPACE_EVENTS),
    ):
        payload = json.loads(row["payload"])
        if row["kind"] == "workspace_commit_intent":
            intents[payload["id"]] = payload
        else:
            settled.setdefault(payload["id"], []).append(payload)
    matches = [payload for payload in intents.values() if payload.get("message") == message]
    if len(matches) > 1:
        raise NeedsAttention("multiple workspace commit receipts exist for the research action")
    if not matches:
        return None
    receipts = settled.get(matches[0]["id"], [])
    if len(receipts) > 1:
        raise NeedsAttention("multiple settled workspace receipts exist for the research action")
    if receipts:
        return receipts[0]["commit"]
    raise NeedsAttention("the research turn's workspace commit outcome needs inspection")


async def _commit(workflow, eid, action_id):
    message = commit_message(action_id)
    receipt = _commit_receipt(workflow.store, eid, message)
    if receipt is not None:
        return receipt
    # commit_branch records its workspace intent and runs Git; keep the wait off the loop.
    outcome = await asyncio.to_thread(workflow.workspaces.commit_branch, eid, message)
    return outcome["commit"]


def _save_turn_identity(store, expected, turn_id):
    """Persist a read-back turn even though the Git outcome still needs inspection."""
    with store.tx() as db:
        current = action_obligations.expected_saved(db, expected)
        action_obligations.transition(
            db, expected["id"], {"turn_id": turn_id}, before=current, expected=expected
        )
        store.emit(
            db,
            expected["experiment_id"],
            "agent_action_changed",
            {"action_id": expected["id"], "state": current["state"]},
        )


def _experiment_in_tx(db, eid):
    row = db.execute(
        "SELECT id,phase,deleted_at,thread_id,pending_request FROM experiments WHERE id=?", (eid,)
    ).fetchone()
    if row is None:
        raise NeedsAttention("saved research experiment disappeared during settlement")
    experiment = dict(row)
    experiment["pending_request"] = (
        json.loads(experiment["pending_request"]) if experiment["pending_request"] else None
    )
    return experiment


def _settle(store, expected, *, turn_id, state, result, error, turn=None):
    changes = {"state": state, "result": result, "error": error}
    if turn_id != expected["turn_id"]:
        changes["turn_id"] = turn_id
    with store.tx() as db:
        current = action_obligations.expected_saved(db, expected)
        if turn is not None:
            experiment = _experiment_in_tx(db, expected["experiment_id"])
            # Git awaited after validation: recheck the live experiment and its evidence.
            if not _terminal(experiment):
                if experiment["thread_id"] != expected["thread_id"]:
                    raise NeedsAttention("saved experiment thread changed during the turn commit")
                try:
                    validate_turn_end(turn, _evidence(db, experiment))
                except ValueError as exc:
                    raise NeedsAttention(
                        f"saved research evidence changed during the turn commit: {exc}"
                    ) from None
        action_obligations.transition(db, expected["id"], changes, before=current, expected=expected)
        store.emit(
            db,
            expected["experiment_id"],
            "agent_action_changed",
            {"action_id": expected["id"], "state": state},
        )
    return store.one("SELECT * FROM agent_actions WHERE id=?", (expected["id"],))


async def complete(workflow, expected_action, raw_result, *, recovered_turn_id=None):
    """Settle one exact saved research turn and return its saved action row.

    A changed row, non-contract 4 experiment, different experiment thread or different
    read-back turn refuses before any effect. Invalid evidence settles the action as
    failed without a commit and keeps its thread and turn. A terminal, cancelled or
    deleted experiment saves its exact raw outcome without committing. A valid live
    result is committed, rechecked in the final transaction and settled atomically;
    Git runs in a worker thread.
    """
    store = workflow.store
    expected = _saved_action(store, expected_action)
    experiment = _contract_four(store, expected)
    turn_id = _turn_identity(expected, recovered_turn_id)
    try:
        turn = _turn_end(raw_result)
    except (ValidationError, ValueError, TypeError) as exc:
        return _settle(
            store,
            expected,
            turn_id=turn_id,
            state="failed",
            result=_raw_result(raw_result),
            error=f"research turn end rejected: {exc}",
        )
    if _terminal(experiment):
        return _settle(
            store,
            expected,
            turn_id=turn_id,
            state="completed",
            result=_raw_result(raw_result),
            error=None,
        )
    with store.tx() as db:
        evidence = _evidence(db, experiment)
    try:
        validate_turn_end(turn, evidence)
    except ValueError as exc:
        return _settle(
            store,
            expected,
            turn_id=turn_id,
            state="failed",
            result=_raw_result(raw_result),
            error=f"research turn end rejected: {exc}",
        )
    try:
        await _commit(workflow, expected["experiment_id"], expected["id"])
    except (Exception, asyncio.CancelledError):
        if turn_id != expected["turn_id"]:
            _save_turn_identity(store, expected, turn_id)
        raise
    # Git and the receipt check awaited; refuse a row that moved while they ran.
    return _settle(
        store,
        expected,
        turn_id=turn_id,
        state="completed",
        result=encode(turn.model_dump()),
        error=None,
        turn=turn,
    )
