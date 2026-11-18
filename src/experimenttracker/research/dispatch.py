"""C7 research turn dispatch: offer one saved v4 action to the existing agent pool.

The inert seam between the queued contract 4 action and a real managed turn. It takes
an existing pending ``phase='research'`` row, re-verifies the exact named worktree and
saved experiment thread after every await, records the dispatch intent with the exact
prompt hash, starts the turn and awaits a required completion callback. It never
queues or settles a turn; T4.1f owns terminal settlement and exact reconciliation, so
an uncertain start is never retried here. The saved live states keep pool accounting.
"""

import asyncio
import hashlib
import logging
from pathlib import Path

from ..agent_pool import EXPERIMENT_PRIORITY, experiment_owner, role_for_phase
from ..codex_client import NeedsAttention
from ..core import action_obligations
from ..maintenance import blocked
from .checkpoints import latest_checkpoint
from .prompt import TurnEnd, with_checkpoint

log = logging.getLogger(__name__)

PHASE = "research"
MAX_ERROR = 1800
_TERMINAL_PHASES = frozenset({"complete", "failed", "cancelled"})


def saved_prompt(action):
    """The saved prompt template, excluding private notebook Markdown."""
    if action.get("phase") != PHASE:
        raise ValueError("not a research action")
    text = action.get("input")
    if not isinstance(text, str) or not text.strip():
        raise ValueError("a research action requires its saved prompt")
    return text


def _text(value, limit=MAX_ERROR):
    return " ".join(str(value).split())[:limit]


class ResearchDispatch:
    """Start one saved research turn per eligible pending row.

    ``complete(action, result)`` is the required async handoff: it receives the
    exact saved running action and the raw turn outcome, and the caller owns
    everything after that. Nothing is settled here.
    """

    def __init__(self, workflow, complete):
        if not callable(complete):
            raise TypeError("research dispatch requires a completion callback")
        self.workflow = workflow
        self.store = workflow.store
        self.complete = complete

    async def advance(self):
        """Offer every eligible pending research row; the pool owns selection."""
        for action in self.store.rows(
            "SELECT * FROM agent_actions WHERE phase=? AND state='pending' ORDER BY created_at,rowid",
            (PHASE,),
        ):
            try:
                if blocked(self.store) or self._live(action["experiment_id"]) is None:
                    continue
                self.workflow.propose_agent(
                    lambda slot, action=action: self.dispatch(slot, action),
                    role=role_for_phase(PHASE),
                    source_table="agent_actions",
                    row=action,
                    owner=experiment_owner(action["experiment_id"]),
                    priority=EXPERIMENT_PRIORITY,
                    any_lane=True,
                )
            except Exception as exc:
                # One unreadable experiment must not stop the pass or another role.
                log.warning("research dispatch unavailable for %s: %s", action["id"], _text(exc))

    def _experiment(self, eid):
        """The saved live contract 4 experiment, or None; terminal work is never offered."""
        try:
            experiment = self.store.experiment(eid)
        except KeyError:
            return None
        if (
            experiment["deleted_at"] is not None
            or experiment["phase"] in _TERMINAL_PHASES
            or (experiment["contract"] or {}).get("execution_contract") != 4
        ):
            return None
        return experiment

    def _live(self, eid):
        """The saved live experiment that is not paused, or None."""
        experiment = self._experiment(eid)
        if experiment is None or experiment["pause_agent"]:
            return None
        return experiment

    def _unchanged(self, expected):
        """Re-read one saved action and refuse a row that moved after an await."""
        current = self.store.one("SELECT * FROM agent_actions WHERE id=?", (expected["id"],))
        if current is None:
            raise NeedsAttention("saved research action disappeared")
        action_obligations.check_expected(current, expected)
        return current

    @staticmethod
    def _thread(action, experiment):
        """The saved experiment thread to resume, refusing a different saved action thread."""
        if action["thread_id"] and experiment["thread_id"] and action["thread_id"] != experiment["thread_id"]:
            raise NeedsAttention("saved research action and experiment thread identities differ")
        return action["thread_id"] or experiment["thread_id"]

    async def dispatch(self, slot, action):
        """Start one saved research turn after every gate, then hand back its raw outcome."""
        expected = dict(action)
        eid = action["experiment_id"]
        private_prompt = False
        try:
            prompt = saved_prompt(action)
            await self.workflow.refresh_quota(slot)
            if blocked(self.store) or not self.workflow.agent_pool.quota(eid=eid).admitted:
                return
            if self._live(eid) is None:
                return
            # No shared or main-checkout fallback: only the named worktree is verified.
            workspace = Path(await asyncio.to_thread(self.workflow.workspaces.branch_checkout, eid))
            # Checkout runs Git, so re-read the row, pause and thread before a session.
            expected = self._unchanged(expected)
            experiment = self._live(eid)
            if experiment is None:
                return
            thread_id = self._thread(expected, experiment)
            saved_thread = experiment["thread_id"]
            runtime_config = self.workflow.controller.session_config(eid)
            session_options = {"runtime_config": runtime_config} if runtime_config else {}
            resumed, _model = await slot.agent.session(thread_id, workspace, writable=True, **session_options)
            if thread_id is not None and resumed != thread_id:
                raise NeedsAttention("the session did not resume the saved experiment thread")
            if blocked(self.store) or not self.workflow.agent_pool.quota(eid=eid).admitted:
                return
            # The session is another await, so verify the exact worktree and thread again.
            if Path(await asyncio.to_thread(self.workflow.workspaces.branch_checkout, eid)) != workspace:
                raise NeedsAttention("the saved research worktree changed during the session")
            expected = self._unchanged(expected)
            experiment = self._live(eid)
            if experiment is None:
                return
            if experiment["thread_id"] != saved_thread:
                raise NeedsAttention("the saved experiment thread changed during dispatch")
            with self.store.tx() as db:
                expected = action_obligations.expected_saved(db, expected)
                experiment = self._live(eid)
                if experiment is None or experiment["thread_id"] != saved_thread:
                    return
                expected = action_obligations.transition(
                    db,
                    action["id"],
                    action_obligations.dispatch_changes(resumed),
                    expected=expected,
                )
                db.execute("UPDATE experiments SET thread_id=? WHERE id=?", (resumed, eid))
                if thread_id is not None:
                    checkpoint = latest_checkpoint(self.store, eid)
                    rendered = with_checkpoint(prompt, checkpoint["markdown"] if checkpoint else None)
                    private_prompt = checkpoint is not None and rendered != prompt
                    prompt = rendered
                self.store.emit(
                    db,
                    eid,
                    "agent_dispatch_intent",
                    {
                        "action_id": action["id"],
                        "phase": PHASE,
                        "thread_id": resumed,
                        "prompt_sha256": hashlib.sha256(prompt.encode()).hexdigest(),
                    },
                )
            turn_id = await slot.agent.begin(
                resumed, prompt, TurnEnd.model_json_schema(), workspace=workspace
            )
            with self.store.tx() as db:
                expected = action_obligations.transition(
                    db, action["id"], action_obligations.turn_changes(turn_id), expected=expected
                )
                self.store.emit(
                    db, eid, "agent_turn_started", {"action_id": action["id"], "turn_id": turn_id}
                )
            result = await slot.agent.finish(resumed, turn_id)
            saved = self.store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))
            if saved is None:
                raise NeedsAttention("saved research action disappeared before completion")
            # The callback sees the exact action this turn belongs to, never a row
            # that a later owner settled or superseded while the turn ran.
            action_obligations.check_expected(saved, expected, compare_result=True)
            await self.complete(saved, result)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            current = self.store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))
            if current is None or current["state"] not in ("pending", "dispatching", "running"):
                return
            # An unsent prerequisite keeps the row queued for a later pass; a turn
            # whose response was lost is durably uncertain and is never retried here.
            state = "uncertain" if current["state"] in ("dispatching", "running") else "pending"
            # A runtime exception can include the supplied input; never persist it
            # when it may quote private notebook text.
            error = (
                "Research dispatch with a private checkpoint needs inspection"
                if private_prompt
                else _text(exc)
            )
            self.workflow._action_state(action["id"], state, error=error, expected=current)
