"""One durable integrator turn for one exact saved conflict (T3.5c).

``integration_resolution`` builds the bounded resolver input and finalizes the saved
merge without dispatching. This module is the dispatch seam between them: it prepares
one pending ``agent_actions`` row for the exact saved conflict, offers it to the
existing agent pool, and records the same dispatching/running/uncertain identity the
other roles use. The turn edits only the locked ``_integration`` worktree and then
finalizes through ``accept_resolution``, so the project check and any owner approval
still run afterwards. A conflict with a terminal resolution action is never retried
automatically; its worktree and queue row stay for owner inspection.
"""

import asyncio
import hashlib
import json
import logging
from pathlib import Path
from uuid import uuid4

from pydantic import Field, ValidationError

from .. import clock
from ..agent_pool import EXPERIMENT_PRIORITY, experiment_owner
from ..codex_client import NeedsAttention
from ..config import StrictModel
from ..core import action_obligations
from ..store import encode
from ..workspaces import WorkspaceUnavailable
from . import integration_queue as queue
from .integration_resolution import accept_resolution, resolver_input

log = logging.getLogger(__name__)

PHASE_PREFIX = "integration:"
ROLE = "integration"
MAX_ERROR = 1800
MAX_STATUS = 500
UNPROVEN_ABSENT_SECONDS = 300
_INSTRUCTIONS = (
    "Resolve one saved Git integration conflict. Work only in the supplied worktree and edit "
    "only the listed conflicted files. Remove every conflict marker, combine both sides' intended "
    "changes, and leave unrelated files unchanged. Do not stage, commit, reset, merge, rebase or "
    "abort Git; the backend verifies and commits the exact merge. Return JSON with a concise summary."
)


class ResolutionResult(StrictModel):
    """The only output an integrator turn returns; the edits are the real result."""

    summary: str = Field(min_length=1, max_length=2000)


def phase(request_id):
    return PHASE_PREFIX + request_id


def action_input(request_id, candidate, prompt):
    """The bounded saved action input: one request/candidate identity and its prompt."""
    if not isinstance(request_id, str) or not request_id or ":" in request_id:
        raise ValueError("integration resolution requires one request identity")
    if not isinstance(candidate, dict) or candidate.get("state") != "conflict":
        raise ValueError("integration resolution requires one saved conflict candidate")
    value = {"request_id": request_id, "candidate": candidate, "prompt": prompt}
    if len(encode(value)) > 64 * 1024:
        raise ValueError("the integration resolution input exceeds its bound")
    return encode(value)


def saved_input(action):
    """Decode one saved integration resolution input, refusing another phase."""
    if action.get("phase", "").rsplit(":", 1)[0] != PHASE_PREFIX.rstrip(":"):
        raise ValueError("not an integration resolution action")
    try:
        value = json.loads(action["input"])
    except (TypeError, KeyError, json.JSONDecodeError):
        raise ValueError("invalid integration resolution input") from None
    if not isinstance(value, dict) or set(value) != {"request_id", "candidate", "prompt"}:
        raise ValueError("invalid integration resolution input")
    request_id, candidate, prompt = value["request_id"], value["candidate"], value["prompt"]
    if (
        not isinstance(request_id, str)
        or not request_id
        or ":" in request_id
        or not isinstance(candidate, dict)
        or candidate.get("state") != "conflict"
        or not isinstance(prompt, str)
        or not prompt
    ):
        raise ValueError("invalid integration resolution input")
    return value


def _prompt(request_id, value):
    payload = {"phase": "integration", "request_id": request_id, "resolver": value}
    return _INSTRUCTIONS + "\nPAYLOAD:\n" + encode(payload)


def _exact(view, request_id, candidate):
    return (
        view is not None
        and view["request_id"] == request_id
        and view["state"] == "open"
        and view["stage"] == "conflict"
        and view["candidate"] == candidate
    )


def _text(value, limit=MAX_ERROR):
    return " ".join(str(value).split())[:limit]


class IntegrationResolver:
    """Prepare and dispatch one integrator turn per exact saved conflict."""

    def __init__(self, workflow):
        self.workflow = workflow
        self.store = workflow.store

    async def advance(self):
        """Queue one pending action for each saved conflict and offer it to the pool."""
        for request_id in self._conflicts():
            try:
                action = self._saved_action(request_id)
                if action is None:
                    action = await asyncio.to_thread(self._queue, request_id)
                if action is not None and action["state"] == "pending":
                    self._propose(action)
            except Exception as exc:
                # One unreadable conflict must not stop the control pass or other repos.
                log.warning("integration resolution unavailable for %s: %s", request_id, _text(exc))

    def _conflicts(self):
        rows = self.store.rows(
            "SELECT id,detail FROM obligations WHERE kind=? AND state='open' ORDER BY created_at,id",
            (queue.SOURCE,),
        )
        request_ids = []
        for row in rows:
            identity = row["id"]
            if not identity.startswith(queue.SOURCE + ":") or not identity.rsplit(":", 1)[1].isdigit():
                continue
            try:
                detail = json.loads(row["detail"])
            except (TypeError, json.JSONDecodeError):
                continue
            if not isinstance(detail, dict) or detail.get("stage") != "conflict":
                continue
            request_id = identity[len(queue.SOURCE) + 1 :].rsplit(":", 1)[0]
            if request_id and request_id not in request_ids:
                request_ids.append(request_id)
        return request_ids

    def _saved_action(self, request_id):
        return self.store.one(
            "SELECT * FROM agent_actions WHERE phase=? ORDER BY rowid DESC LIMIT 1",
            (phase(request_id),),
        )

    def _queue(self, request_id):
        """Build and save the one pending action for this exact conflict, or return None."""
        with self.store.tx() as db:
            view = queue.saved(db, request_id)
        if view is None or view["stage"] != "conflict" or view["state"] != "open":
            return None
        candidate = view["candidate"]
        eid = view["experiment_id"]
        experiment = self._experiment(eid, view)
        if experiment is None:
            return None
        try:
            value = resolver_input(view["repo"], candidate, experiment)
        except WorkspaceUnavailable as exc:
            self._status(request_id, "manual inspection: " + _text(exc))
            return None
        prompt = _prompt(request_id, value)
        action_id = str(uuid4())
        with self.store.tx() as db:
            current = queue.saved(db, request_id)
            if not _exact(current, request_id, candidate):
                return None
            row = db.execute(
                "SELECT * FROM agent_actions WHERE phase=? ORDER BY rowid DESC LIMIT 1",
                (phase(request_id),),
            ).fetchone()
            if row is not None:
                return dict(row)
            db.execute(
                "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,created_at,updated_at) "
                "VALUES(?,?,?,?,'pending',?,?,?)",
                (
                    action_id,
                    eid,
                    phase(request_id),
                    experiment["evidence_revision"],
                    action_input(request_id, candidate, prompt),
                    clock.now(),
                    clock.now(),
                ),
            )
            action_obligations.sync_action(db, action_id)
            self.store.emit(
                db,
                eid,
                "integration_resolution_queued",
                {"action_id": action_id, "request_id": request_id, "candidate_key": candidate.get("key")},
            )
            return dict(db.execute("SELECT * FROM agent_actions WHERE id=?", (action_id,)).fetchone())

    def _experiment(self, eid, view):
        """Require the saved live contract-3 experiment and its exact active branch."""
        if not eid:
            return None
        try:
            experiment = self.store.experiment(eid)
        except KeyError:
            return None
        request = view["request"]
        if (
            experiment["deleted_at"]
            or experiment["phase"] != "complete"
            or experiment["contract"].get("dont_merge")
            or request.get("experiment_id") != eid
        ):
            return None
        branch = self.store.one("SELECT * FROM experiment_branches WHERE experiment_id=?", (eid,))
        if (
            branch is None
            or branch["repo"] != view["repo"]
            or branch["branch"] != request.get("branch")
            or branch["state"] != "active"
        ):
            return None
        return experiment

    def _propose(self, action):
        data = saved_input(action)
        view = self._view(data["request_id"])
        if not _exact(view, data["request_id"], data["candidate"]):
            self.workflow._action_state(
                action["id"],
                "superseded",
                error="the saved integration conflict changed before the turn",
                expected=action,
            )
            return
        self.workflow.propose_agent(
            lambda slot, action=action: self.dispatch(slot, action),
            role=ROLE,
            source_table="agent_actions",
            row=action,
            owner=experiment_owner(action["experiment_id"]),
            priority=EXPERIMENT_PRIORITY,
            any_lane=True,
        )

    def _view(self, request_id):
        with self.store.tx() as db:
            return queue.saved(db, request_id)

    async def dispatch(self, slot, action):
        """Run the one turn after quota admission, then finalize its exact edits."""
        expected = dict(action)
        request_id = action["phase"][len(PHASE_PREFIX) :]
        try:
            data = saved_input(action)
            await self.workflow.refresh_quota(slot)
            if not self.workflow.agent_pool.quota(eid=action["experiment_id"]).admitted:
                return
            view = self._view(request_id)
            if not _exact(view, request_id, data["candidate"]):
                self._fail(action, "the saved integration conflict changed before dispatch")
                return
            experiment = self.store.experiment(action["experiment_id"])
            value = await asyncio.to_thread(resolver_input, view["repo"], data["candidate"], experiment)
            prompt = _prompt(request_id, value)
            workspace = Path(data["candidate"]["worktree"])
            thread_id, _model = await slot.agent.session(action["thread_id"], workspace, writable=True)
            if not self.workflow.agent_pool.quota(eid=action["experiment_id"]).admitted:
                return
            with self.store.tx() as db:
                current = queue.saved(db, request_id)
                if not _exact(current, request_id, data["candidate"]):
                    raise WorkspaceUnavailable("the saved integration conflict changed before dispatch")
                expected = action_obligations.expected_saved(db, expected)
                expected = action_obligations.transition(
                    db,
                    action["id"],
                    action_obligations.dispatch_changes(thread_id),
                    expected=expected,
                )
                self.store.emit(
                    db,
                    action["experiment_id"],
                    "agent_dispatch_intent",
                    {
                        "action_id": action["id"],
                        "request_id": request_id,
                        "thread_id": thread_id,
                        "prompt_sha256": hashlib.sha256(prompt.encode()).hexdigest(),
                    },
                )
            turn_id = await slot.agent.begin(
                thread_id,
                prompt,
                ResolutionResult.model_json_schema(),
                effort="low",
                workspace=workspace,
            )
            with self.store.tx() as db:
                expected = action_obligations.transition(
                    db,
                    action["id"],
                    action_obligations.turn_changes(turn_id),
                    expected=expected,
                )
                self.store.emit(
                    db,
                    action["experiment_id"],
                    "agent_turn_started",
                    {"action_id": action["id"], "request_id": request_id, "turn_id": turn_id},
                )
            result = await slot.agent.finish(thread_id, turn_id)
            self._finish(action["id"], result)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            current = self.store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))
            if current and current["state"] in ("pending", "dispatching", "running"):
                state = (
                    "uncertain"
                    if current["state"] == "dispatching" or isinstance(exc, NeedsAttention)
                    else "failed"
                )
                self.workflow._action_state(action["id"], state, error=_text(exc), expected=current)
                if state == "failed":
                    self._status(request_id, "manual inspection: " + _text(exc))
            self.workflow.agent_pool.settle_row("agent_actions", action["id"])

    def _finish(self, action_id, result):
        """Finalize the exact saved conflict, then settle the turn's durable outcome."""
        action = self.store.one("SELECT * FROM agent_actions WHERE id=?", (action_id,))
        if action is None or action["state"] not in ("running", "uncertain"):
            return
        try:
            ResolutionResult.model_validate(result)
            data = saved_input(action)
        except (ValidationError, ValueError) as exc:
            self.workflow._action_state(
                action_id,
                "failed",
                error="invalid integration resolution output: " + _text(exc),
                expected=action,
            )
            self._status(
                action["phase"][len(PHASE_PREFIX) :],
                "manual inspection: invalid integration resolution output",
            )
            return
        try:
            accept_resolution(self.store, data["request_id"], data["candidate"])
        except (ValueError, WorkspaceUnavailable) as exc:
            self.workflow._action_state(
                action_id,
                "failed",
                error="integration resolution refused: " + _text(exc),
                expected=action,
            )
            self._status(data["request_id"], "manual inspection: " + _text(exc))
            return
        self.workflow._action_state(action_id, "completed", result=result, expected=action)

    async def resolve_uncertain(self, action):
        """Read back one uncertain integrator turn; never resend it or infer absence early."""
        reservation = self.workflow.agent_pool.lookup("agent_actions", action["id"])
        slot = self.workflow.agent_pool.read_slot(reservation)
        if slot is None:
            return
        try:
            if not action["thread_id"]:
                raise NeedsAttention("saved integration dispatch has no thread identity")
            prompt_hash = self.workflow.dispatch_prompt_hash(action)
            turns = await slot.agent.read_thread(action["thread_id"])
        except Exception:
            return
        if action["turn_id"]:
            matches = [turn for turn in turns if turn["id"] == action["turn_id"]]
        else:
            matches = [turn for turn in turns if turn.get("prompt_sha256") == prompt_hash]
        if len(matches) > 1:
            self._fail(action, "several saved turns match this integration resolution")
            return
        if not matches:
            if clock.now() - self.workflow.resolver.dispatched_at(action) > UNPROVEN_ABSENT_SECONDS:
                self._fail(action, "the integration resolution turn was not found; no replay was made")
            return
        turn = matches[0]
        if turn.get("status") != "completed":
            self._fail(action, "the saved integration resolution turn is not completed")
            return
        turn_id = turn["id"]
        try:
            result = await slot.agent.read_turn(action["thread_id"], turn_id)
        except Exception:
            return
        if action["turn_id"] != turn_id:
            try:
                with self.store.tx() as db:
                    expected = action_obligations.expected_saved(db, action)
                    action_obligations.transition(
                        db,
                        action["id"],
                        {"turn_id": turn_id, "state": "running"},
                        before=expected,
                        expected=action,
                    )
            except Exception:
                return
        self._finish(action["id"], result)
        self.workflow.agent_pool.settle_row("agent_actions", action["id"])

    def _fail(self, action, error):
        self.workflow._action_state(action["id"], "failed", error=_text(error), expected=action)
        request_id = action["phase"][len(PHASE_PREFIX) :]
        self._status(request_id, "manual inspection: " + _text(error))
        self.workflow.agent_pool.settle_row("agent_actions", action["id"])

    def _status(self, request_id, text):
        """Save one bounded queue status without changing the saved conflict identity."""
        text = _text(text, MAX_STATUS)
        try:
            with self.store.tx() as db:
                view = queue.saved(db, request_id)
                if view is None or view["state"] != "open" or view["stage"] != "conflict":
                    return
                if view["status"] != text:
                    queue.checkpoint(db, request_id, "conflict", status=text)
        except (ValueError, KeyError):
            pass
