"""C5 uncertainty resolver for saved Slurm intents without a scheduler identity and
saved uncertain agent turns.

Job observation calls :func:`resolve` at the scheduler cadence for every active
intent in ``jobs.state='submitting' AND job_id IS NULL``, before admission may
consider another submission for the experiment. Observers that only record
scheduler facts (cancellation, archived settlement) call :func:`adopt` directly.

An intent whose external submission is still in flight has an owner that saves its
own outcome; :func:`resolve` decides nothing about it. It also re-reads the saved
row, so a list snapshot taken before that outcome is not decided either.

``Workflow.tick`` runs :class:`TurnResolver` after the saved dispatch tasks have
been drained and before any experiment advances, so no live task owns a row it
decides and a continuation it queues is admitted in that same tick.

One owner decides adoption, quarantine and proven absence: the resolver never
submits, dispatches or retries, and an unreadable scheduler or thread concludes
nothing.
"""

import logging

from . import clock, legacy_archive
from .codex_client import NeedsAttention
from .core import action_obligations, job_obligations
from .execution_settings import ExecutionSettings
from .slurm import SubmissionUncertain

QUERY = "squeue+sacct"
ACCOUNTING_WINDOW_SECONDS = 6 * 86400
QUARANTINE_NOTICE = "Several scheduler jobs match one submission intent; operator reconciliation is required."
WAITING_ERROR = "No matching job found; operator reconciliation required"


def accounting_lag_seconds(exp):
    """The saved accounting lag for this experiment's execution context."""
    settings = ExecutionSettings.model_validate(exp["contract"].get("execution") or {})
    return settings.accounting_lag_seconds


def adopt(store, eid, job, job_id, *, observer):
    """Save one exact scheduler identity for the saved intent in one transaction.

    ``observer`` names the caller in the receipt evidence. Returns the saved job
    row, or None when the intent changed before the identity could be saved.
    """
    intent_id = job["intent_id"]
    with store.tx() as db:
        before = job_obligations.locked_job(db, job, "state")
        if before is None:
            return None
        cursor = db.execute(
            "UPDATE jobs SET job_id=?,state='submitted' WHERE intent_id=? AND job_id IS NULL AND state=?",
            (job_id, intent_id, job["state"]),
        )
        if not cursor.rowcount:
            return None
        job_obligations.sync_backend(db, intent_id, before=before)
        store.emit(
            db,
            eid,
            "job_reconciled",
            {
                "intent_id": intent_id,
                "job_id": job_id,
                "evidence": {"query": QUERY, "observer": observer, "matched": 1},
            },
        )
    return store.one("SELECT * FROM jobs WHERE intent_id=?", (intent_id,))


def _undecided(workflow, intent_id):
    """The saved intent while it still needs a resolver decision.

    An intent whose submission is still in flight belongs to that caller, and a row
    that already got an identity or left ``submitting`` decides nothing: deciding
    from a stale snapshot would write a false hold or an absence.
    """
    if intent_id in workflow.submissions_in_flight:
        return None
    job = workflow.store.one("SELECT * FROM jobs WHERE intent_id=?", (intent_id,))
    if job is None or job["state"] != "submitting" or job["job_id"] is not None:
        return None
    return job


async def resolve(workflow, exp, job):
    """Decide one active intent without a scheduler identity.

    Returns the saved job row when exactly one match was adopted, otherwise None.
    """
    store, eid = workflow.store, exp["id"]
    job = _undecided(workflow, job["intent_id"])
    if job is None:
        return None
    try:
        job_id = await workflow.scheduler_for(exp, job).reconcile(job["intent_id"])
    except SubmissionUncertain as exc:
        return _quarantine(workflow, eid, job, str(exc))
    except (RuntimeError, TimeoutError, OSError) as exc:
        # Unreadable scheduler: keep waiting. Remote reads are cached for the
        # configured poll interval, so repeats stay bounded without new state.
        return _waiting(workflow, eid, job, str(exc))
    if job_id:
        saved = adopt(store, eid, job, job_id, observer="observe_jobs")
        if saved:
            store.update(eid, hold_reason=None, error=None)
        return saved
    lag = accounting_lag_seconds(exp)
    waited = clock.now() - job["created_at"]
    # The accounting query covers seven days; older intents could have aged out,
    # so absence is provable only inside that window.
    if waited <= lag or waited > ACCOUNTING_WINDOW_SECONDS:
        return _waiting(workflow, eid, job, WAITING_ERROR)
    return _not_submitted(store, eid, job, lag, research=exp["contract"].get("execution_contract") == 4)


def _waiting(workflow, eid, job, error):
    """Hold the intent for another observation, unless its owner decided it meanwhile."""
    if _undecided(workflow, job["intent_id"]) is None:
        return None
    workflow.store.update(eid, hold_reason="submission_uncertain", error=error)
    return None


def _quarantine(workflow, eid, job, error):
    """Several exact matches: keep the intent and leave the owner to reconcile it."""
    store = workflow.store
    if _undecided(workflow, job["intent_id"]) is None:
        return None
    saved = store.experiment(eid)
    recorded = (saved["hold_reason"], saved["error"]) == ("submission_uncertain", error)
    store.update(eid, hold_reason="submission_uncertain", error=error)
    if recorded:
        return None
    with store.tx() as db:
        store.emit(
            db,
            eid,
            "job_reconciled",
            {
                "intent_id": job["intent_id"],
                "outcome": "quarantined",
                "evidence": {"query": QUERY, "matched": "several", "error": error},
            },
        )
    store.notify(eid, "executor_error", QUARANTINE_NOTICE)
    return None


def _not_submitted(store, eid, job, lag, *, research):
    """No record after the accounting lag: write the receipt and end the intent."""
    intent_id = job["intent_id"]
    with store.tx() as db:
        before = job_obligations.locked_job(db, job, "state")
        if before is None:
            return None
        cursor = db.execute(
            "UPDATE jobs SET state='failed',error=? WHERE intent_id=? AND job_id IS NULL AND state='submitting'",
            (job_obligations.NOT_SUBMITTED_ERROR, intent_id),
        )
        if not cursor.rowcount:
            return None
        job_obligations.sync_backend(db, intent_id, before=before)
        store.emit(
            db,
            eid,
            "job_reconciled",
            {
                "intent_id": intent_id,
                "outcome": "not_submitted",
                "evidence": {
                    "query": QUERY,
                    "matched": 0,
                    "waited_seconds": round(clock.now() - job["created_at"]),
                    "accounting_lag_seconds": lag,
                },
            },
        )
        if research:
            from .research import wakeups

            wakeups.record(store, eid, wakeups.JOB_TERMINAL, intent_id, db=db)
    store.update(eid, hold_reason=None, error=None)
    return None


log = logging.getLogger(__name__)

ABSENT_AFTER_SECONDS = 300  # A readable thread without the turn proves it never started
UNREADABLE_LIMIT = 6  # Consecutive unreadable thread reads before the owner is asked
RETRY_SECONDS = (15, 30, 60, 120, 240, 300)
NOT_STARTED = "turn_not_started"


class TurnResolver:
    """Decide saved uncertain turns once per tick; it never dispatches a turn itself."""

    def __init__(self, workflow):
        self.workflow = workflow
        self.failures = {}  # action id -> (consecutive unreadable reads, next attempt at)

    async def resolve(self):
        """Resolve every saved uncertain action; one experiment cannot stop another."""
        if not self.workflow.agent_ready:
            return
        # A saved row with a live process belongs to that task, not to the resolver.
        owned = {
            reservation.action_id
            for reservation in self.workflow.agent_pool.live()
            if reservation.source_table == "agent_actions"
        }
        for action in self.workflow.store.rows(
            "SELECT a.* FROM agent_actions a JOIN experiments e ON e.id=a.experiment_id "
            "WHERE a.state='uncertain' OR (a.phase='research' "
            "AND a.state IN ('dispatching','running') AND e.deleted_at IS NULL "
            "AND json_extract(e.contract,'$.execution_contract') IS 4) ORDER BY a.created_at"
        ):
            if action["id"] in owned:
                continue
            try:
                await self.resolve_action(action)
            except Exception:
                # A resolver failure keeps this action for the operator; the tick continues.
                log.exception("turn resolution failed for action %s", action["id"])

    async def resolve_action(self, action):
        store = self.workflow.store
        exp = store.experiment(action["experiment_id"])
        integration = action["phase"].startswith("integration:")
        if legacy_archive.execution_frozen(exp) or (
            exp["phase"] in ("complete", "failed", "cancelled") and not integration
        ):
            return
        now = clock.now()
        if now < self.failures.get(action["id"], (0, 0))[1]:
            return
        reservation = self.workflow.agent_pool.lookup("agent_actions", action["id"])
        slot = self.workflow.agent_pool.read_slot(reservation)
        if slot is None:
            # No configured connection can read the saved thread: it stays uncertain.
            return
        try:
            if not action["thread_id"]:
                raise NeedsAttention("saved dispatch has no saved thread identity")
            prompt_hash = self.workflow.dispatch_prompt_hash(action)
            turns = await slot.agent.read_thread(action["thread_id"])
        except Exception as exc:
            if action["phase"] == "research" and action["state"] != "uncertain":
                self.workflow._action_state(action["id"], "uncertain", error=str(exc), expected=action)
            self.retry_unreadable(action, exp, exc)
            return
        if action["turn_id"]:
            matches = [turn for turn in turns if turn["id"] == action["turn_id"]]
        else:
            matches = [turn for turn in turns if turn["prompt_sha256"] == prompt_hash]
        if len(matches) > 1:
            text = f"Uncertain agent turn {action['id']} matches several saved turns: " + ", ".join(
                turn["id"] for turn in matches
            )
            if integration:
                self.workflow.integration_resolver._fail(action, text)
            else:
                self.ask_owner(exp, text)
            return
        if matches:
            # Only a completed turn is adoptable; a live or terminal matching turn stays
            # with operator reconciliation, exactly as the operator readback reports it.
            if matches[0]["status"] == "completed":
                self.failures.pop(action["id"], None)
                if integration:
                    await self.workflow.integration_resolver.adopt(action, matches[0]["id"])
                else:
                    await self.workflow.adopt_turn(action["id"], turn_id=matches[0]["id"])
                self.workflow.agent_pool.settle_row("agent_actions", action["id"])
            return
        if now - self.dispatched_at(action) <= ABSENT_AFTER_SECONDS:
            return
        self.failures.pop(action["id"], None)
        self.absent(action, exp, turns)

    def dispatched_at(self, action):
        row = self.workflow.store.one(
            "SELECT created_at FROM events WHERE experiment_id=? AND kind='agent_dispatch_intent' "
            "AND json_extract(payload,'$.action_id')=? ORDER BY seq DESC LIMIT 1",
            (action["experiment_id"], action["id"]),
        )
        return row["created_at"] if row else action["updated_at"]

    def absent(self, action, exp, turns):
        """A readable thread without the turn proves it never started.

        Contract 3 roles continue once; a research turn fails for inspection instead
        and is never replayed from a copied prompt.
        """
        store = self.workflow.store
        integration = action["phase"].startswith("integration:")
        evidence = {
            "action_id": action["id"],
            "thread_id": action["thread_id"],
            "thread_turns": len(turns),
            "dispatched_at": self.dispatched_at(action),
            "state": "failed",
            "error": NOT_STARTED,
        }
        with store.tx() as db:
            failed = action_obligations.transition(
                db, action["id"], {"state": "failed", "error": NOT_STARTED}, expected=action
            )
            store.emit(db, exp["id"], "agent_action_changed", evidence)
            # A diagnostic wake owns its own failure handling; other roles admit a continuation.
            # A contract 4 research turn is never continued from the failed row: its
            # replacement would copy an action-marked prompt into a different turn.
            continuation = (
                None
                if integration or action["phase"] in ("monitor", "research")
                else store.continue_action(failed, feedback=NOT_STARTED, _db=db)
            )
            if continuation and action["phase"].startswith("discuss:"):
                db.execute(
                    "UPDATE discussions SET action_id=?,state='queued',hold_reason=NULL,error=?,updated_at=? "
                    "WHERE id=?",
                    (
                        continuation["id"],
                        NOT_STARTED,
                        clock.now(),
                        action["phase"].removeprefix("discuss:"),
                    ),
                )
        if continuation and not action["phase"].startswith("discuss:"):
            store.update(exp["id"], hold_reason="agent_continuation", error=NOT_STARTED)
        if action["phase"] == "research":
            # Proven absence keeps the existing evidence above and waits for owner
            # inspection; a bounded retry policy arrives separately.
            store.update(exp["id"], hold_reason="agent_dispatch_uncertain", error=NOT_STARTED)
            store.notify(
                exp["id"],
                "required_decision",
                f"Research turn {action['id']} was never started; inspection required",
            )
        if integration:
            self.workflow.integration_resolver.failed(action, NOT_STARTED)
        self.workflow.agent_pool.settle_row("agent_actions", action["id"])

    def ask_owner(self, exp, text):
        self.workflow.store.update(exp["id"], hold_reason="agent_dispatch_uncertain", error=text)
        self.workflow.store.notify(exp["id"], "required_decision", text)

    def retry_unreadable(self, action, exp, exc):
        """Retry an unreadable thread with backoff; after the limit ask the owner once."""
        failures = self.failures.get(action["id"], (0, 0))[0] + 1
        self.failures[action["id"]] = (
            failures,
            clock.now() + RETRY_SECONDS[min(failures, len(RETRY_SECONDS)) - 1],
        )
        if failures == UNREADABLE_LIMIT:
            text = f"Uncertain agent turn {action['id']} has no readable thread: {exc}"
            if action["phase"].startswith("integration:"):
                self.workflow.integration_resolver._fail(action, text)
            else:
                self.ask_owner(exp, text)
