import asyncio
import contextlib
import hashlib
import json
import logging
import os
import sys
import time
from functools import partial
from pathlib import Path
from uuid import uuid4

from . import (
    agent_conventions,
    artifacts,
    clock,
    creation_policy,
    early_repair,
    iteration,
    legacy_archive,
    legacy_archive_actions,
    reporting,
    resolver,
)
from .agent_pool import TITLE_PRIORITY, AgentPool, experiment_owner
from .autopilot_controller import ResearchController
from .cancellation import process_cancellations
from .codex_client import CodexAgent, NeedsAttention
from .controller import Controller, ControllerDecision
from .conversation import Conversation
from .core import (
    action_obligations,
    job_obligations,
    liveness,
    obligation_backfill,
    request_obligations,
)
from .engine.integration_resolver import IntegrationResolver
from .knowledge import ProjectSupervisor
from .maintenance import Maintenance, blocked
from .models import (
    AgentStageResult,
    Implementation,
    ResearchPlan,
)
from .progress import DiscussionActivity, describe_item
from .projects import Projects, ProjectUnavailable
from .quota import QuotaGate
from .remote import RemoteSlurmScheduler
from .research import readback as research_readback
from .research.dispatch import PHASE as RESEARCH_PHASE
from .research.dispatch import ResearchDispatch
from .research.session import first_turn, follow_up_turn
from .research.settlement import complete as settle_research_turn
from .resolver import TurnResolver
from .slurm import ResourceUnavailable
from .steering import queue_late_reports
from .store import encode
from .title import TitleSummary, luna_title_model, title_prompt
from .watchdog import engine_watchdog
from .workspaces import Workspaces

log = logging.getLogger(__name__)
TERMINAL_BAD = {
    "FAILED",
    "CANCELLED",
    "TIMEOUT",
    "OUT_OF_MEMORY",
    "NODE_FAIL",
    "PREEMPTED",
    "BOOT_FAIL",
    "DEADLINE",
}

# Tasks whose last success and latest error feed the existing /healthz heartbeat
# (design rule 8): the work the sequential tick used to run end to end, plus the
# resolver that now runs inside it. "control" covers every step_control_* fragment;
# backup stays out because it was already a background task.
HEALTH_COUNTED_STEPS = (
    "control",
    "observe",
    "research",
    "supervisor",
    "watchdog",
    "retention",
    "resolver",
)

RETENTION_SECONDS = 5  # Between bounded outbox-pruning batches
BACKUP_SECONDS = 60  # Between background state backups
# The backend source families a successful pass really handles. Its handler returns
# are the watchdog's only service evidence, so each family is recorded by the pass
# that drives it: control advances experiments, publications and cancellations,
# observation adopts saved job intents. A saved transfer row is its own evidence for
# that family, so it needs no service record even though both passes move transfers.
CONTROL_SERVICE = ("agent-action", "cancellation", "report-version")
OBSERVE_SERVICE = ("job",)


def _primary_state(name):
    """Expose one primary-slot field on the workflow; the slot keeps it canonical."""
    return property(
        lambda self: getattr(self.agent_pool.primary, name),
        lambda self, value: setattr(self.agent_pool.primary, name, value),
    )


class Workflow:
    def __init__(self, config, store, scheduler, agent):
        self.config = config
        self.maintenance = Maintenance(config, store)
        self.store = store
        self.projects = Projects(config, store)
        self.workspaces = Workspaces(self.projects)
        self.scheduler = scheduler
        self.scheduler.autopilot_intent = lambda intent: bool(
            store.one("SELECT 1 FROM autopilot_allocations WHERE intent_id=?", (intent,))
        )
        self.scheduler.autopilot_job = lambda job: bool(
            store.one("SELECT 1 FROM autopilot_allocations WHERE job_id=?", (job,))
        )
        # The one pool owns every configured connection; slot zero is the primary
        # transport, so the workflow's own state is the slot's, not a copy.
        self.agent_pool = AgentPool(self, agent)
        self.gate = QuotaGate(config.quota.model_copy(deep=True))
        saved_reserve = store.one(
            "SELECT five_hour_percent,weekly_percent FROM quota_settings WHERE singleton=1"
        )
        if saved_reserve:
            self.gate.config.five_hour_min_remaining = saved_reserve["five_hour_percent"]
            self.gate.config.weekly_min_remaining = saved_reserve["weekly_percent"]
            self.gate.config.recovery_remaining = (
                min(100, saved_reserve["five_hour_percent"] + 5),
                min(100, saved_reserve["weekly_percent"] + 5),
            )
        self.started = False
        self.started_at = None
        self.serviced_at = {}
        self.closing = False
        self.retention_cursor = 0
        self.last_retention = 0
        self.last_backup = 0
        self.backup_task = None
        self.last_scheduler = 0
        self.cluster_snapshot = None
        self.cluster_error = None
        self.last_cluster_refresh = 0
        self.last_preflight = {}
        self.last_cancellation = {}
        self.remote_schedulers = {}
        self.remote_clusters = {}
        self.remote_reads = {}
        self.remote_downloads = {}
        self.discussion_activity = DiscussionActivity()
        self._attach_handlers(self.agent_pool.primary)
        self.conversation = Conversation(self)
        self.integration_resolver = IntegrationResolver(self)
        self.supervisor = ProjectSupervisor(self)
        self.controller = Controller(self)
        self.research = ResearchController(self)
        self.research_dispatch = ResearchDispatch(self, partial(settle_research_turn, self))
        self.resolver = TurnResolver(self)
        self.submission_lock = asyncio.Lock()
        # Intent IDs whose external submission is crossing the scheduler boundary right
        # now. A submission token, not saved state: no observer may decide such an intent.
        self.submissions_in_flight = set()
        self.output_checks = {}
        self.evidence_checks = {}
        self.error_backoff = {}

    # The primary connection's state lives only in its slot; these names keep the
    # operator, CLI and single-connection paths on that one canonical state.
    agent = _primary_state("agent")
    agent_ready = _primary_state("agent_ready")
    agent_error = _primary_state("agent_error")
    agent_task = _primary_state("agent_task")
    active_action = _primary_state("active_action")
    last_agent_start = _primary_state("last_agent_start")
    last_quota_refresh = _primary_state("last_quota_refresh")

    def _attach_handlers(self, slot):
        """Route this connection's callbacks to the one owner, pinned to its slot."""
        slot.agent.request_handler = partial(self._request, slot)
        slot.agent.quota_handler = partial(self._quota_notification, slot)
        slot.agent.activity_handler = partial(self._activity, slot)

    @staticmethod
    def is_remote(exp):
        return exp["contract"].get("execution", {}).get("context") == "jean-zay"

    def scheduler_for(self, exp, job=None):
        metadata = None
        if job and job.get("execution_manifest"):
            metadata = json.loads(job["execution_manifest"]).get("execution")
        if not (metadata or self.is_remote(exp)):
            return self.scheduler
        if self.config.mode == "demo":
            raise ResourceUnavailable("Jean Zay execution requires live mode; demo stays synthetic and local")
        settings = metadata["settings"] if metadata else exp["contract"]["execution"]
        key = (
            exp["id"],
            job["intent_id"]
            if job and iteration.enabled(exp)
            else exp.get("_attempt_id", job["attempt"] if job else exp["evidence_revision"]),
            encode(settings),
        )
        cache = self.remote_schedulers
        if key not in cache:
            cache[key] = RemoteSlurmScheduler(self, exp, settings, saved=metadata)
        if not job:
            cache[key].exp = exp
        return cache[key]

    def cluster_for(self, exp):
        if not self.is_remote(exp):
            return self.cluster_snapshot
        return self.remote_clusters.get(exp["contract"]["execution"].get("ssh_host", "jz"), {}).get(
            "snapshot"
        )

    def execution_context(self, exp):
        from .execution_settings import ExecutionSettings

        settings = ExecutionSettings.model_validate(exp["contract"].get("execution", {})).model_dump()
        if legacy_archive.execution_frozen(exp):
            guide_text = (
                "Archived execution context for a historical record. Read this only to understand saved "
                "conversation context. Do not prepare, submit, retry, or mutate execution from these settings. "
                "Saved context, host and transfer templates below are historical metadata."
            )
        else:
            guide = Path(__file__).with_name("machine_contexts") / (
                "iteration-" + settings["context"] + ".md"
            )
            guide_text = guide.read_text()
        return {
            **settings,
            "agent_host": "acacia",
            "guide": guide_text,
            "templates": {
                "local_project": str(self.project_context(exp)["checkout"] or self.working_directory(exp)),
                "local_run": str(self.root(exp["id"])),
                "remote_project": "isolated checkout under the configured remote folder, supplied by executor",
                "remote_run": "run outputs under the configured remote folder, supplied by executor",
                "host": settings["ssh_host"],
            },
        }

    def root(self, eid):
        return self.projects.root(eid)

    def propose_agent(self, dispatch, *, role, source_table, row, owner, priority, any_lane=False):
        """Offer a prepared turn to the pool; only its selector claims and starts it.

        ``dispatch`` is a factory called with the claimed slot, so a stage turn can run
        on whichever connection the selector picks and every other role keeps the
        connection that prepared it.
        """
        self.agent_pool.propose(
            role=role,
            source_table=source_table,
            row=row,
            owner=owner,
            priority=priority,
            connections=self.agent_pool.slots if any_lane else (self.agent_pool.primary,),
            dispatch=dispatch,
        )

    async def start_agent_pool(self):
        """Start one transport per configured session and register its slot."""
        for index in range(1, self.config.codex.max_sessions):
            if getattr(self.agent, "is_demo", False):
                agent = type(self.agent)()
            else:
                agent = CodexAgent(self.config.codex, self.config.state_dir / f"agent-slot-{index}")
            slot = self.agent_pool.add_slot(agent)
            self._attach_handlers(slot)
            await self.connect_agent(slot)

    def working_directory(self, exp):
        saved = exp["contract"].get("working_directory")
        work = Path(saved) if saved else self.root(exp["id"]) / "work"
        base = self.projects.base_root(exp["id"])
        if work.is_symlink() or not work.resolve().is_relative_to(base.resolve()):
            raise ValueError("working directory must belong to this experiment")
        return work

    def agent_turn_ceiling(self, exp):
        cap = self.config.codex.max_agent_turns
        return None if cap is None else cap + exp["contract"].get("manual_agent_turn_requests", 0)

    @staticmethod
    def validation_feedback(exc):
        if hasattr(exc, "errors"):
            return "; ".join(
                ".".join(map(str, e["loc"])) + ": " + e["msg"]
                for e in exc.errors(include_url=False, include_input=False)
            )
        return str(exc) or type(exc).__name__

    def status(self, last_tick, executor_error=None, *, projects=None):
        contract_three_archived = 3 not in legacy_archive.SUPPORTED_EXECUTION_CONTRACTS
        return {
            "experiment_cancellation": True,
            "implementation_repair": not contract_three_archived,
            "branch_integration": True,
            "execution_contract_3_archived": contract_three_archived,
            "execution_contract_create": 4,
            **(
                {"execution_contract_create_default": 4}
                if creation_policy.defaults_to_research_loop()
                else {}
            ),
            "research_loop_projects": [
                project.id for project in self.config.projects if project.research_loop
            ],
            "autopilot": {"available": True, "schema_version": 1},
            "maintenance": self.maintenance.status(self),
            "projects": self.projects.catalog() if projects is None else projects,
            "last_tick": last_tick,
            "agent_ready": self.agent_ready,
            "agent_error": self.agent_error,
            "executor_error": executor_error,
            "models": getattr(self.agent, "models", []),
            "models_observed_at": getattr(self.agent, "models_observed_at", None),
            "default_model": getattr(self.agent, "resolved_model", None),
            "quota": self.gate.decide().__dict__,
            "quota_reserve": {
                "five_hour_percent": self.gate.config.five_hour_min_remaining,
                "weekly_percent": self.gate.config.weekly_min_remaining,
            },
            "compute_policy": {
                "agent_selected_resources": self.config.mode == "live",
                "defaults": self.config.slurm.defaults().model_dump(),
                "limits": self.config.slurm.limits.model_dump(),
                "max_jobs_per_experiment": self.config.slurm.max_jobs_per_experiment,
                "max_concurrent_jobs": self.config.slurm.max_concurrent_jobs,
            },
            "cluster": self.cluster_snapshot,
            "cluster_error": self.cluster_error,
            # Freshness travels with the replaceable heartbeat, rather than creating
            # an ever-growing domain event for each unchanged queue observation.
            "job_observations": {
                job["intent_id"]: {"job_id": job["job_id"], "observed_at": job["observed_at"]}
                for job in self.store.rows(
                    "SELECT intent_id,job_id,observed_at FROM jobs WHERE state IN ('submitting','submitted')"
                )
            },
        }

    async def _activity(self, slot, message, start_count):
        params = message.get("params", {})
        connection = self.agent_pool.connection(slot, start_count)
        reservation = self.agent_pool.live_owner(connection, params.get("threadId"), params.get("turnId"))
        if not reservation or reservation.source_table != "agent_actions":
            return
        action = self.store.one("SELECT * FROM agent_actions WHERE id=?", (reservation.action_id,))
        if not action or action["state"] not in ("dispatching", "running"):
            return
        if message["method"] == "thread/tokenUsage/updated":
            usage = params.get("tokenUsage", {})
            fields = (
                "inputTokens",
                "cachedInputTokens",
                "cacheWriteInputTokens",
                "outputTokens",
                "reasoningOutputTokens",
                "totalTokens",
            )
            saved = {
                name: {
                    key: value
                    for key, value in usage.get(name, {}).items()
                    if key in fields and type(value) is int and value >= 0
                }
                for name in ("total", "last")
            }
            payload = {"thread_total": saved["total"], "last_request": saved["last"]}
            context_window = usage.get("modelContextWindow")
            if type(context_window) is int and context_window > 0:
                payload["model_context_window"] = context_window
            with self.store.tx() as db:
                # Retain the latest exact turn counters in a local audit table;
                # token notifications never create publication snapshots.
                db.execute(
                    "INSERT INTO agent_usage(action_id,thread_id,turn_id,payload,observed_at) VALUES(?,?,?,?,?) "
                    "ON CONFLICT(action_id) DO UPDATE SET payload=excluded.payload,observed_at=excluded.observed_at",
                    (
                        action["id"],
                        action["thread_id"],
                        action["turn_id"],
                        encode(payload),
                        clock.now(),
                    ),
                )
            return
        description = (
            self.discussion_activity.describe(action["id"], message, clock.monotonic())
            if action["phase"].startswith("discuss:")
            else describe_item(message)
        )
        if description:
            self.store.activity(action, *description)

    async def start(self):
        # This process's restart grace starts here: until a real handler returns for a
        # backend family, the watchdog gives that family one interval from this time.
        self.started_at = clock.now()
        self.store.archive_legacy_experiments()
        # A database saved before the obligation source hooks keeps the same open
        # responsibilities once ticking starts; a second start re-saves nothing.
        obligation_backfill.backfill(self.store)
        self.config.job_output_root.mkdir(parents=True, exist_ok=True)
        await self.refresh_cluster()
        self.agent_pool.reconstruct()
        await self.connect_agent()
        # A request belongs to its original connection. Never replay an obsolete RPC ID after restart.
        for row in self.store.rows(
            "SELECT id,pending_request,contract FROM experiments WHERE pending_request IS NOT NULL"
        ):
            pending = json.loads(row["pending_request"])
            # A durable contract-4 owner question has no live RPC to expire; its
            # saved answer is consumed by the next research turn (T4.3).
            if self._durable_owner_question(row["id"], json.loads(row["contract"]), pending):
                continue
            pending["expired"] = True
            self.store.update(row["id"], pending_request=pending, hold_reason="expired_agent_request")
        await self.controller.start()
        await self.start_agent_pool()
        self.started = True

    def _durable_owner_question(self, eid, contract, pending):
        """True for a saved contract-4 ask_owner question, not a live app-server RPC.

        It names its own saved ``phase='research'`` action and carries no
        ``rpc_id``, so the owner's answer is saved for the next research turn
        (T4.3) instead of being replayed through an obsolete connection. Such a
        question survives restart and drain, including its saved answer.
        """
        if not isinstance(pending, dict):
            return False
        if not isinstance(pending.get("id"), str) or not pending["id"]:
            return False
        if pending.get("rpc_id") is not None:
            return False
        if (contract or {}).get("execution_contract") != 4:
            return False
        action_id = pending.get("action_id")
        if not action_id:
            return False
        action = self.store.one("SELECT experiment_id,phase FROM agent_actions WHERE id=?", (action_id,))
        return bool(action and action["experiment_id"] == eid and action["phase"] == RESEARCH_PHASE)

    async def refresh_cluster(self):
        self.last_cluster_refresh = clock.now()
        if self.config.mode == "demo":
            return
        try:
            self.cluster_snapshot = await self.scheduler.discover()
            self.cluster_error = None
        except (ResourceUnavailable, OSError, RuntimeError) as exc:
            self.cluster_snapshot = None
            self.cluster_error = str(exc)

    async def connect_agent(self, slot=None):
        """Start one connection and refresh the account quota through it."""
        slot = slot or self.agent_pool.primary
        slot.last_agent_start = clock.now()
        try:
            await slot.agent.start()
            slot.agent_ready = True
            slot.agent_error = None
            await self.refresh_quota(slot)
        except Exception as exc:
            slot.agent_ready = False
            slot.agent_error = str(exc)
            log.warning("agent unavailable: %s", exc)
            await slot.agent.close()

    async def refresh_quota(self, slot=None):
        """Read the account quota through one configured connection."""
        slot = slot or self.agent_pool.primary
        try:
            payload = await slot.agent.quota()
            self.gate.update(payload)
            decision = self.gate.decide()
            with self.store.tx() as db:
                db.execute(
                    "INSERT INTO quota_snapshots(observed_at,payload,admitted,reason) VALUES(?,?,?,?)",
                    (clock.now(), encode(payload), decision.admitted, decision.reason),
                )
            slot.last_quota_refresh = clock.now()
            await self.research.interrupt_holds()
            if not decision.admitted:
                await self.supervisor.interrupt()
                await self.agent_pool.interrupt_occupied("quota_hold")
            return decision
        except Exception as exc:
            log.warning("quota refresh failed: %s", exc)
            decision = self.gate.decide()
            if not decision.admitted:
                await self.supervisor.interrupt()
                await self.agent_pool.interrupt_occupied("quota_hold")
            return decision

    async def _quota_notification(self, slot, payload):
        self.gate.update(payload)
        if not self.closing and not self.gate.decide().admitted and self.agent_pool.occupied():
            # Do not await a correlated RPC from inside the protocol reader.
            if not slot.quota_task or slot.quota_task.done():
                slot.quota_task = asyncio.create_task(self.refresh_quota(slot))

    async def _request(self, slot, message, start_count):
        """Save the request of the one saved turn this process owns, or fail closed."""
        params = message.get("params", {})
        connection = self.agent_pool.connection(slot, start_count)
        owner = self.agent_pool.live_owner(connection, params.get("threadId"), params.get("turnId"))
        if owner and owner.source_table == "autopilot_actions":
            pending = await self.research.request(message, owner)
        elif owner and owner.source_table == "agent_actions":
            action = self.store.one("SELECT * FROM agent_actions WHERE id=?", (owner.action_id,))
            if not action:
                raise NeedsAttention("request has no owned agent turn on this connection")
            pending = {
                "id": str(uuid4()),
                "action_id": action["id"],
                "rpc_id": message["id"],
                "method": message["method"],
                "params": params,
                "expired": False,
            }
            self.store.update(action["experiment_id"], pending_request=pending, hold_reason="needs_attention")
            self.store.notify(
                action["experiment_id"], "required_decision", "Codex needs an answer or permission."
            )
        else:
            # Unmatched or supervisor request: no owner is provable and the
            # supervisor has no supported handler, so nothing is answered.
            raise NeedsAttention("request has no owned agent turn on this connection")
        self.agent_pool.remember_answer(pending["id"], connection, owner)

    async def tick(self, *, force_observe=False):
        """Run one manual pass over the shared steps in their original order.

        The maintenance gate keeps its exact position: observation, input replies
        and agent draining run before the ``blocked()`` return, and everything after
        it stays gated. All steps still run inside this one pass, so health keeps the
        sequential task set in ``HEALTH_COUNTED_STEPS`` and an empty heartbeat cannot
        hide a hung step. Only this manual adapter keeps the lazy ``start()``.
        """
        await self.step_control_before_observe()
        if not self.started:
            await self.start()
        now = clock.now()
        await self.step_runtime(now)
        await self.step_observe(now, force_observe=force_observe)
        self.record_service(OBSERVE_SERVICE)
        await self.step_control_after_observe(now)
        await self.step_resolver()
        if blocked(self.store):
            return
        await self.step_control_advance()
        self.record_service(CONTROL_SERVICE)
        await self.step_watchdog(now)
        await self.step_control_finish()
        # The one selection of a pass: every role's proposals still offered to the pool
        # compete by priority class, not by the order their step happened to run. A
        # proposal prepared after this point is selected by the next pass.
        self.agent_pool.admit()
        await self.step_supervisor()
        await self.step_research()
        await self.step_retention(now)
        await self.step_backup(now)

    async def step_control(self):
        """Run the control fragments around the omitted observation, gated as in ``tick``.

        The production runner calls this under its primary-agent lock, so the
        check-then-act regions that the sequential pass kept exclusive stay exclusive
        across their awaits. Observation runs as its own task, the resolver keeps its
        place after agent draining and before advancement, and everything from
        advancement on stays behind the maintenance gate. A gated pass is still a
        complete, successful control pass.
        """
        await self.step_control_before_observe()
        now = clock.now()
        # Runtime upkeep keeps this place and this exclusion: it reconnects the owned
        # agent and refreshes quota, which the control fragments below act on.
        await self.step_runtime(now)
        await self.step_control_after_observe(now)
        await self.step_resolver()
        if blocked(self.store):
            return
        await self.step_control_advance()
        await self.step_control_finish()
        # The manual adapter and the production runner both select once per pass, after
        # every role has offered its prepared proposals to the pool. Supervisor and
        # research proposals prepared after this point are selected by the next pass.
        self.agent_pool.admit()

    async def step_control_before_observe(self):
        """Intake: legacy archive, input replies, cancellations, projects and reserve."""
        self.store.archive_legacy_experiments()
        self.research.service.tick()
        await self.research.reply_inputs()
        await self.research.interrupt_holds()
        await process_cancellations(self)
        await early_repair.reconcile_cancellations(self)
        self.projects.tick()
        self.projects.settings_tick()
        self.supervisor.memory.refresh()
        saved_reserve = self.store.one(
            "SELECT five_hour_percent,weekly_percent FROM quota_settings WHERE singleton=1"
        )
        if saved_reserve:
            changed = (
                self.gate.config.five_hour_min_remaining != saved_reserve["five_hour_percent"]
                or self.gate.config.weekly_min_remaining != saved_reserve["weekly_percent"]
            )
            self.gate.config.five_hour_min_remaining = saved_reserve["five_hour_percent"]
            self.gate.config.weekly_min_remaining = saved_reserve["weekly_percent"]
            self.gate.config.recovery_remaining = (
                min(100, saved_reserve["five_hour_percent"] + 5),
                min(100, saved_reserve["weekly_percent"] + 5),
            )
            if changed and self.active_action and self.agent_ready:
                await self.refresh_quota()

    async def step_runtime(self, now):
        """Keep the cluster snapshot, agent connection and quota refresh current."""
        if self.config.mode == "live" and now - self.last_cluster_refresh >= 60:
            await self.refresh_cluster()
        slot = self.agent_pool.primary
        if slot.agent_ready and not slot.agent.available:
            slot.agent_ready = False
            slot.agent_error = "App Server connection closed; saved turns require reconciliation"
        if (
            not slot.agent_ready
            and not self.agent_pool.process_busy(slot)
            and now - slot.last_agent_start >= 60
        ):
            await slot.agent.close()
            await self.connect_agent(slot)
        if slot.agent_ready and now - slot.last_quota_refresh >= 45:
            if not slot.quota_task or slot.quota_task.done():
                slot.quota_task = asyncio.create_task(self.refresh_quota(slot))

    async def step_observe(self, now, *, force_observe=False):
        """Observe saved jobs and settle autopilot accounting when the cadence is due."""
        if force_observe or now - self.last_scheduler >= self.config.scheduler_seconds:
            await self.observe_jobs()
            await self.reconcile_autopilot_accounting()
            self.last_scheduler = now

    async def step_control_after_observe(self, now):
        """Submit input replies, drain finished turns and settle saved action identities."""
        for row in self.store.rows("SELECT id FROM experiments WHERE pending_request IS NOT NULL"):
            exp = self.store.experiment(row["id"])
            pending = exp["pending_request"]
            # A durable contract-4 question is answered into saved state, never
            # drained through an old RPC reply. Keep it and its saved answer.
            if self._durable_owner_question(exp["id"], exp["contract"], pending):
                continue
            action = (
                self.store.one("SELECT * FROM agent_actions WHERE id=?", (pending.get("action_id"),))
                if pending.get("action_id")
                else None
            )
            frozen_reader_reply = bool(
                legacy_archive.execution_frozen(exp)
                and exp["phase"] == "complete"
                and exp["deleted_at"] is None
                and pending.get("action_id") == self.active_action
                and action
                and action["experiment_id"] == exp["id"]
                and action["phase"].startswith("discuss:")
                and action["state"] in ("dispatching", "running")
                and self.agent_task
                and not self.agent_task.done()
                and self.agent_ready
            )
            if (
                pending.get("answer")
                and not pending.get("expired")
                and (not legacy_archive.execution_frozen(exp) or frozen_reader_reply)
            ):
                try:
                    if await self.agent_pool.reply(pending):
                        self.store.update(exp["id"], pending_request=None, hold_reason=None)
                    else:
                        # The asking process is gone; never replay an obsolete RPC ID.
                        pending["expired"] = True
                        self.store.update(
                            exp["id"], pending_request=pending, hold_reason="expired_agent_request"
                        )
                except Exception as exc:
                    self.store.update(exp["id"], error=str(exc), hold_reason="needs_attention")
        if self.agent_task and self.agent_task.done():
            # Dispatch handles its errors and persists them before returning.
            self.agent_task.result()
            self.agent_task = None
            self.active_action = None
        self.recover_terminal_actions()
        for slot in self.agent_pool.slots[1:]:
            if slot.agent_task and slot.agent_task.done():
                slot.agent_task.result()
                slot.agent_task = None
                slot.active_action = None
            if (
                not slot.agent_ready
                and not self.agent_pool.process_busy(slot)
                and now - slot.last_agent_start >= 60
            ):
                await slot.agent.close()
                await self.connect_agent(slot)
        await legacy_archive_actions.settle_actions(self)

    async def step_resolver(self):
        """Resolve saved uncertain turns before any experiment advances."""
        # Resolve saved uncertain turns before any experiment advances: no live task
        # owns these rows, and a continuation queued here is admitted in this same tick.
        await self.resolver.resolve()

    async def step_control_advance(self):
        """Steer, restart, repair and publish reports; contract 3 stages no longer advance."""
        await self.advance_reports()

    async def step_watchdog(self, now):
        """Judge engine liveness against the current clock and this process's service."""
        engine_watchdog(self.store, now, self.config.watchdog_minutes, self.service_evidence())

    def record_service(self, families):
        """Record that this pass really handled these backend source families.

        Only a handler that returned counts: an obligation's age and arbitrary events
        never do. The engine runner and the manual tick adapter record the same way.
        """
        now = clock.now()
        for family in families:
            self.serviced_at[family] = now

    def service_evidence(self):
        """This process's proof of which backend families it actually serviced."""
        return liveness.ServiceEvidence(self.started_at or clock.now(), dict(self.serviced_at))

    async def step_control_finish(self):
        """Discard failed implementations, queue late reports, advance conversation/title/controller."""
        self.discard_failed_implementation()
        queue_late_reports(self)
        await self.conversation.advance()
        await self.integration_resolver.advance()
        await self.prepare_research_turns()
        await self.research_dispatch.advance()
        await self.advance_title()

    async def prepare_research_turns(self):
        """Queue and verify one C7 turn before its dispatcher can open a session.

        A supported saved contract 4 experiment gets one first research action and
        one follow-up per due owner-answer wakeup, never a replacement for a pending,
        running or uncertain turn. Its exact named worktree is created or verified off
        the loop here, before ``ResearchDispatch`` can open a writable session, so a
        shared checkout is never a fallback. A paused, cancelled, deleted or maintained
        experiment prepares nothing, and one experiment's saved filesystem failure
        holds only that experiment.
        """
        if blocked(self.store):
            return
        for row in self.store.rows(
            "SELECT id FROM experiments WHERE deleted_at IS NULL "
            "AND phase NOT IN ('complete','failed','cancelled') "
            "AND json_extract(contract,'$.execution_contract') IS 4 "
            "AND json_extract(contract,'$.branch_workspace') IS 1 "
            "ORDER BY created_at,id"
        ):
            eid = row["id"]
            try:
                if self.store.experiment(eid)["pause_agent"]:
                    continue
                action = self.store.one(
                    "SELECT * FROM agent_actions WHERE experiment_id=? AND phase=? "
                    "ORDER BY created_at DESC,rowid DESC LIMIT 1",
                    (eid, RESEARCH_PHASE),
                )
                if action is None:
                    action = first_turn(self.store, eid)
                else:
                    # One follow-up for a due owner answer after the settled turn; a
                    # pending, running or uncertain turn is never replaced or retried.
                    action = follow_up_turn(self.store, eid) or action
                if action is None or action["state"] != "pending":
                    continue
                await asyncio.to_thread(self.workspaces.ensure_branch, eid)
            except ProjectUnavailable as exc:
                self.store.update(eid, hold_reason="workspace_unavailable", error=str(exc))
            except Exception as exc:
                log.warning("research preparation unavailable for %s: %s", eid, exc)

    async def step_supervisor(self):
        """Advance the project supervisor; the next control pass selects its proposal."""
        await self.supervisor.advance()

    async def step_research(self):
        """Advance the research controller; the next control pass selects its proposal."""
        await self.research.advance()

    async def step_retention(self, now):
        """Prune outbox payloads in bounded batches when the retention window is due."""
        if now - self.last_retention >= RETENTION_SECONDS:
            from .retention import prune_batch

            try:
                result = await asyncio.to_thread(prune_batch, self.store, self.retention_cursor)
                self.retention_cursor = result["cursor"]
                if result["pruned"]:
                    log.info("outbox retention: %s", result)
            except Exception:
                log.exception("outbox retention failed; payloads retained for investigation")
            self.last_retention = now

    async def step_backup(self, now):
        """Start a background state backup when the backup window is due."""
        if now - self.last_backup >= BACKUP_SECONDS and (not self.backup_task or self.backup_task.done()):
            self.backup_task = asyncio.create_task(self.backup_state())
            self.last_backup = now

    async def reconcile_autopilot_accounting(self):
        for row in self.store.rows(
            "SELECT j.* FROM jobs j JOIN autopilot_allocations a ON a.intent_id=j.intent_id WHERE a.state!='settled' AND j.state NOT IN ('submitting','submitted','agent') AND j.job_id IS NOT NULL"
        ):
            try:
                exp = self.store.experiment(row["experiment_id"])
                observation = await self.scheduler_for(exp, row).observe(row["job_id"])
                self.research.service.budget.observe(
                    row, observation, exp["contract"].get("execution", {}).get("context", "acacia")
                )
            except Exception:
                log.warning(
                    "Autopilot allocation accounting remains unresolved for intent %s", row["intent_id"]
                )

    async def backup_state(self):
        started = time.monotonic()
        try:
            await asyncio.to_thread(self.store.publish_backup, self.config.backup_dir)
            log.info(
                "backup complete: bytes=%d seconds=%.2f",
                (self.config.backup_dir / "workflow.sqlite").stat().st_size,
                time.monotonic() - started,
            )
        except Exception:
            log.exception("backup failed; authoritative local state remains intact")

    async def advance_title(self):
        """Use an optional small managed turn while a drafted plan waits for review."""
        # Reconcile saved turns even when the user approved or cancelled their plan
        # while the title was running. A review transition cannot discard a turn.
        for action in self.store.rows(
            "SELECT * FROM agent_actions WHERE phase IN ('title','summarize_title') ORDER BY created_at"
        ):
            exp = self.store.experiment(action["experiment_id"])
            if legacy_archive.archive_active(exp):
                continue
            if action["state"] in ("running", "dispatching"):
                if self.agent_task or not self.agent_ready:
                    return
                reservation = self.agent_pool.lookup("agent_actions", action["id"])
                slot = self.agent_pool.read_slot(reservation)
                if action["thread_id"] and action["turn_id"]:
                    try:
                        result = await slot.agent.read_turn(action["thread_id"], action["turn_id"])
                        if legacy_archive.execution_frozen(self.store.experiment(exp["id"])):
                            self._action_state(action["id"], "completed", result=result, expected=action)
                            self.agent_pool.settle_row("agent_actions", action["id"])
                            return
                        TitleSummary.model_validate(result)
                        self._action_state(action["id"], "completed", result=result, expected=action)
                    except Exception as exc:
                        self._action_state(action["id"], "uncertain", error=str(exc), expected=action)
                        self.store.update(exp["id"], hold_reason="agent_dispatch_uncertain", error=str(exc))
                else:
                    self._action_state(
                        action["id"], "uncertain", error="title dispatch outcome is unknown", expected=action
                    )
                    self.store.update(exp["id"], hold_reason="agent_dispatch_uncertain")
                self.agent_pool.settle_row("agent_actions", action["id"])
                return
            if action["state"] == "completed":
                if self.title_applicable(exp, action) and not self.store.one(
                    "SELECT 1 FROM agent_actions WHERE experiment_id=? AND phase IN ('title','summarize_title') "
                    "AND state='completed' AND created_at>?",
                    (exp["id"], action["created_at"]),
                ):
                    try:
                        summary = TitleSummary.model_validate(json.loads(action["result"]))
                        if exp["title"] != summary.title:
                            self.store.update(exp["id"], title=summary.title)
                    except (ValueError, TypeError) as exc:
                        self._action_state(
                            action["id"], "failed", error=f"title validation: {exc}", expected=action
                        )
            elif action["state"] == "pending" and not self.title_applicable(exp, action, dispatch=True):
                self._action_state(action["id"], "superseded", expected=action)
        choice = luna_title_model(getattr(self.agent, "models", []))
        if not choice:
            return
        for action in self.store.rows(
            "SELECT * FROM agent_actions WHERE phase='summarize_title' AND state='pending' ORDER BY created_at"
        ):
            exp = self.store.experiment(action["experiment_id"])
            if not iteration.runnable(exp):
                continue
            if not self.title_applicable(exp, action, dispatch=True) or exp["pause_agent"]:
                continue
            if not self.agent_ready or not self.agent_pool.quota(eid=exp["id"]).admitted:
                return
            saved = json.loads(action["input"])
            if not saved.get("model"):
                saved.update(model=choice[0], effort=choice[1])
                with self.store.tx() as db:
                    db.execute(
                        "UPDATE agent_actions SET input=?,updated_at=? WHERE id=?",
                        (encode(saved), clock.now(), action["id"]),
                    )
                action = self.store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))
            self.propose_agent(
                lambda slot, action=action: self.dispatch_title(action, slot),
                role="title",
                source_table="agent_actions",
                row=action,
                owner=experiment_owner(exp["id"]),
                priority=TITLE_PRIORITY,
                any_lane=True,
            )
            return
        for row in self.store.rows(
            "SELECT id FROM experiments WHERE recipe_id='agent_experiment_v1' AND deleted_at IS NULL "
            "AND phase='preparing' ORDER BY created_at"
        ):
            exp = self.store.experiment(row["id"])
            if not iteration.runnable(exp):
                continue
            review = exp["plan_review"] or {}
            if review.get("state") != "awaiting_review" or review.get("questions") or exp["pause_agent"]:
                continue
            revision = review["revision"]
            action = self.store.one(
                "SELECT * FROM agent_actions WHERE experiment_id=? AND phase='title' AND evidence_revision=?",
                (exp["id"], revision),
            )
            if action and action["state"] != "pending":
                continue
            if not self.agent_ready or not self.agent_pool.quota(eid=exp["id"]).admitted:
                return
            if not action:
                prompt = encode(
                    {
                        "model": choice[0],
                        "effort": choice[1],
                        "prompt": title_prompt(
                            exp["brief"],
                            exp["contract"].get("project", {}).get("name", "Project"),
                            exp["title"] or "",
                        ),
                    }
                )
                action_id = str(uuid4())
                with self.store.tx() as db:
                    db.execute(
                        "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,created_at,updated_at) "
                        "VALUES(?,?, 'title',?,'pending',?,?,?)",
                        (action_id, exp["id"], revision, prompt, clock.now(), clock.now()),
                    )
                    action_obligations.sync_action(db, action_id)
                    self.store.emit(
                        db, exp["id"], "agent_action_queued", {"phase": "title", "action_id": action_id}
                    )
                action = self.store.one("SELECT * FROM agent_actions WHERE id=?", (action_id,))
            self.propose_agent(
                lambda slot, action=action: self.dispatch_title(action, slot),
                role="title",
                source_table="agent_actions",
                row=action,
                owner=experiment_owner(exp["id"]),
                priority=TITLE_PRIORITY,
                any_lane=True,
            )
            return

    @staticmethod
    def title_applicable(exp, action, *, dispatch=False):
        # The separate title turn summarises a contract-3 plan review; contract 4 takes
        # its title from the first summary instead.
        if exp["deleted_at"] or not iteration.runnable(exp):
            return False
        if action["phase"] == "summarize_title":
            return True
        review = exp["plan_review"] or {}
        return review.get("revision") == action["evidence_revision"] and review.get("state") in (
            ("awaiting_review",) if dispatch else ("awaiting_review", "approved")
        )

    async def dispatch_title(self, action, slot):
        """Persist exact title turn identities; a lost outcome is never retried."""
        if not self.agent_pool.quota(eid=action["experiment_id"]).admitted or self.research.service.authority(
            action["experiment_id"], "title"
        ):
            return
        eid = action["experiment_id"]
        expected = action
        try:
            exp = self.store.experiment(eid)
            if not iteration.runnable(exp):
                return
            await self.refresh_quota(slot)
            if blocked(self.store):
                return
            if not self.agent_pool.quota(eid=eid).admitted:
                return
            exp = self.store.experiment(eid)
            if exp["pause_agent"]:
                return
            if not self.title_applicable(exp, action, dispatch=True):
                self._action_state(action["id"], "superseded", expected=expected)
                return
            saved = json.loads(action["input"])
            if not any(
                model["id"] == saved["model"] and saved["effort"] in model["reasoning_efforts"]
                for model in getattr(slot.agent, "models", [])
            ):
                self._action_state(
                    action["id"], "failed", error="Saved summary model is unavailable", expected=expected
                )
                return
            self.root(eid).mkdir(parents=True, exist_ok=True)
            thread_id, _ = await slot.agent.session(None, self.root(eid), model=saved["model"])
            if blocked(self.store) or self.store.experiment(eid)["phase"] == "cancelled":
                return
            if not self.agent_pool.quota(eid=eid).admitted:
                return
            with self.store.tx() as db:
                expected = action_obligations.transition(
                    db, action["id"], action_obligations.dispatch_changes(thread_id), expected=expected
                )
                self.store.emit(
                    db, eid, "agent_dispatch_intent", {"action_id": action["id"], "thread_id": thread_id}
                )
            turn_id = await slot.agent.begin(
                thread_id, saved["prompt"], TitleSummary.model_json_schema(), effort=saved["effort"]
            )
            with self.store.tx() as db:
                expected = action_obligations.transition(
                    db, action["id"], action_obligations.turn_changes(turn_id), expected=expected
                )
                self.store.emit(
                    db, eid, "agent_turn_started", {"action_id": action["id"], "turn_id": turn_id}
                )
            result = await slot.agent.finish(thread_id, turn_id)
            try:
                TitleSummary.model_validate(result)
            except ValueError as exc:
                self._action_state(
                    action["id"], "failed", error=f"title validation: {exc}", expected=expected
                )
                return
            self._action_state(action["id"], "completed", result=result, expected=expected)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            current = self.store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))
            state = (
                "uncertain"
                if current["state"] in ("dispatching", "running") or isinstance(exc, NeedsAttention)
                else "failed"
            )
            self._action_state(action["id"], state, error=str(exc), expected=expected)
            if state == "uncertain":
                self.store.update(eid, hold_reason="agent_dispatch_uncertain", error=str(exc))
        finally:
            pending = self.store.experiment(eid)["pending_request"]
            if pending:
                pending["expired"] = True
                self.store.update(eid, pending_request=pending)

    def recover_terminal_actions(self):
        """Migrate old exhausted failures only when their saved outcome is known."""
        for row in self.store.rows(
            "SELECT id FROM experiments WHERE deleted_at IS NULL AND phase='failed' AND hold_reason='agent_recovery_exhausted'"
        ):
            exp = self.store.experiment(row["id"])
            if (
                not iteration.runnable(exp)
                or exp["recipe_id"] != "agent_experiment_v1"
                or exp["pending_request"]
            ):
                continue
            if self.store.one(
                "SELECT 1 FROM agent_actions WHERE experiment_id=? AND state IN ('dispatching','running','uncertain')",
                (exp["id"],),
            ) or self.store.one(
                "SELECT 1 FROM steering WHERE experiment_id=? AND state IN ('pending','cancelling')",
                (exp["id"],),
            ):
                continue
            action = self.store.one(
                "SELECT * FROM agent_actions WHERE experiment_id=? AND phase IN ('plan','prepare','interpret') ORDER BY rowid DESC LIMIT 1",
                (exp["id"],),
            )
            if not action or action["state"] != "failed":
                continue
            known = (not action["turn_id"] and not action["attempts"]) or (
                action["turn_id"]
                and str(action["error"]).startswith(
                    (
                        "agent output validation:",
                        "turn interrupted:",
                        "turn failed:",
                        "turn timeout; interrupted",
                    )
                )
            )
            if known:
                self.store.update(
                    exp["id"],
                    phase="interpreting" if action["phase"] == "interpret" else "preparing",
                    hold_reason="agent_continuation",
                )

    def discard_failed_implementation(self):
        # Working environments, checkpoints and job-source records survive
        # repair. Retention is an owner operation, never automatic recomputation.
        return

    def repair_available(self, exp):
        """Compute repair is bounded by the owner policy, independently of output correction."""
        used = exp["contract"].get(
            "repair_count",
            len(exp["contract"].get("repair_history", [])) + bool(exp["contract"].get("auto_repair")),
        )
        ceiling = self.config.slurm.max_repairs_per_experiment
        return ceiling is None or used < ceiling + exp["contract"].get("manual_repair_requests", 0)

    def fail(self, eid, reason, error):
        exp = self.store.experiment(eid)
        if exp["phase"] == "cancelled":
            return
        if reason in ("agent_correction_stalled", "agent_recovery_exhausted", "agent_budget_exhausted"):
            action = self.store.one(
                "SELECT phase FROM agent_actions WHERE experiment_id=? ORDER BY rowid DESC LIMIT 1", (eid,)
            )
            if action and action["phase"].startswith("agent:"):
                self.store.stage(eid, action["phase"].removeprefix("agent:"), "failed", error=error)
        if (
            iteration.enabled(exp)
            and self.config.mode == "live"
            and exp["recipe_id"] == "agent_experiment_v1"
            and exp["snapshot"]
            and self.repair_available(exp)
            and reason in ("compute_failed", "validation_failed")
            and self.store.one("SELECT 1 FROM jobs WHERE experiment_id=?", (eid,))
        ):
            self.store.update(eid, phase="preparing", hold_reason="automatic_repair_pending", error=error)
            return
        self.store.update(eid, phase="failed", hold_reason=reason, error=error)
        self.store.notify(eid, "recovery_exhausted", f"Experiment stopped: {error}")

    async def observe_jobs(self):
        for job in self.store.rows(
            "SELECT j.* FROM jobs j JOIN experiments e ON e.id=j.experiment_id "
            "WHERE (j.state IN ('submitting','submitted') OR "
            "(json_extract(e.contract, '$.execution_contract')=3 AND j.state='failed' "
            "AND json_extract(j.execution_manifest, '$.executor') IS NOT 'agent' "
            "AND (j.error IS NULL OR j.error NOT LIKE ?) "
            "AND (j.scheduler_state IS NULL OR j.scheduler_state NOT IN "
            "('COMPLETED','FAILED','CANCELLED','TIMEOUT','OUT_OF_MEMORY','NODE_FAIL','PREEMPTED','BOOT_FAIL','DEADLINE','REVOKED') "
            "OR j.exit_code IS NULL))) AND e.phase!='cancelled' ORDER BY j.created_at",
            (f"{job_obligations.NOT_SUBMITTED_PREFIX}%",),
        ):
            eid = job["experiment_id"]
            try:
                exp = self.store.experiment(eid)
                if exp["contract"].get("execution_contract") == 4:
                    if exp["deleted_at"] is not None or exp["phase"] in ("complete", "failed", "cancelled"):
                        continue
                    from .research import job_observe

                    expected_route = exp["contract"].get("execution", {}).get("context", "acacia")
                    manifest = json.loads(job.get("execution_manifest") or "{}")
                    saved_route = (manifest.get("execution") or {}).get("context")
                    if saved_route is not None and saved_route != expected_route:
                        raise RuntimeError("saved execution route is incompatible with the experiment route")
                    if expected_route != "acacia":
                        if expected_route != "jean-zay" or saved_route != expected_route:
                            raise RuntimeError(
                                "saved remote execution route is missing, invalid, or incompatible"
                            )
                        manifest = json.loads(job.get("execution_manifest") or "{}")
                        job_observe.validate_remote_route(
                            exp,
                            job["intent_id"],
                            manifest.get("execution"),
                            job.get("execution_root"),
                        )
                        scheduler = self.scheduler_for(exp, job)
                        if not scheduler.due(job, clock.now()):
                            continue
                        with self.store.tx() as db:
                            db.execute(
                                "UPDATE jobs SET remote_polled_at=? WHERE intent_id=?",
                                (clock.now(), job["intent_id"]),
                            )
                        if not job["job_id"]:
                            if job["intent_id"] in self.submissions_in_flight:
                                continue
                            job = await resolver.resolve(self, exp, job)
                        if job:
                            await job_observe.observe_remote(self, exp, job)
                    else:
                        if not job["job_id"]:
                            job = await resolver.resolve(self, exp, job)
                        if job:
                            await job_observe.observe(self, exp, job)
                    continue
                if (
                    legacy_archive.execution_frozen(exp)
                    and exp["deleted_at"] is None
                    and exp["phase"] != "cancelled"
                ):
                    await legacy_archive.settle_job(self, exp, job)
                    continue
                # Live contract 3 observation was retired with its stage engine; any
                # other saved job is left untouched for operator inspection.
            except Exception as exc:
                self.store.update(eid, hold_reason="executor_unavailable", error=str(exc))

    async def commit_branch(self, exp, message):
        """Commit this experiment's exact saved named branch; None for other workspaces."""
        if not self.workspaces.branch(exp["id"]):
            return None
        return await asyncio.to_thread(self.workspaces.commit_branch, exp["id"], message)

    def prepared_python(self, exp, plan, runtime=None):
        if self.is_remote(exp):
            if any(stage.executor == "slurm" and not stage.script for stage in plan.stages):
                raise ValueError(
                    "Jean Zay stages must declare .sbatch or .sh scripts that set up their remote runtime"
                )
            return Path(sys.executable)

        candidate = (
            runtime.python
            if runtime is not None
            else Path(plan.python_executable)
            if plan.python_executable
            else self.working_directory(exp) / ".venv/bin/python"
        )
        context = self.project_context(exp)
        project = context["checkout"]
        allowed = [self.working_directory(exp)] + ([Path(project)] if project else [])
        if context.get("source_checkout"):
            allowed.append(Path(context["source_checkout"]) / ".venv")
        if runtime is not None:
            # The shared cache or a project's fixed interpreter is owner-owned, not an
            # arbitrary executable an agent chose for this experiment.
            allowed.append(runtime.root if runtime.root is not None else runtime.python.parent)
        # Reusing an exact saved runtime from this experiment is valid even
        # when the current checkout is a worktree or a newer repair directory.
        base = self.projects.base_root(exp["id"])
        repairs = [*exp["contract"].get("repair_history", []), exp["contract"].get("auto_repair")]
        for repair in repairs:
            saved = (repair or {}).get("snapshot", {}).get("python_executable")
            if saved and Path(saved).parent.resolve().is_relative_to(base.resolve()):
                if candidate == Path(saved):
                    allowed.append(Path(saved).parent)
        # A venv's interpreter is normally a symlink to the system binary; confine its directory.
        if not any(candidate.parent.resolve().is_relative_to(folder.resolve()) for folder in allowed):
            raise ValueError("Python environment must belong to the selected project or experiment workspace")
        if not candidate.is_file() or not os.access(candidate, os.X_OK):
            raise ValueError("selected Python environment is unavailable")
        return candidate

    def experiment_python(self, exp):
        return Path(
            exp["snapshot"].get("python_executable")
            or self.root(exp["id"]) / exp["snapshot"].get("python_relative_path", "work/.venv/bin/python")
        )

    def validate_resource_plan(self, exp, plan):
        if plan.schema_version != 3:
            raise ValueError("new execution contract requires schema_version 3")
        budget = exp["contract"].get("max_jobs", self.config.slurm.max_jobs_per_experiment)
        if budget is not None and sum(s.executor == "slurm" for s in plan.stages) > budget:
            raise ValueError(f"plan exceeds the {budget}-job experiment budget")
        for stage in plan.stages:
            if stage.executor == "agent":
                continue
            if (
                self.config.mode == "live"
                and exp["contract"].get("resource_policy_version") == 2
                and stage.resources is None
            ):
                raise ValueError("every live Slurm stage must declare its resources")
            resources = stage.resources or self.config.slurm.defaults()
            if isinstance(plan, Implementation) and stage.script and stage.script.endswith(".sbatch"):
                from .remote import sbatch_directives

                sbatch_directives(
                    artifacts.allowed_path(
                        self.working_directory(exp) / stage.script, self.working_directory(exp)
                    ),
                    resources,
                )
            self.config.slurm.limits.validate_request(resources)
            if self.config.mode == "demo" and resources.gpus_per_job:
                raise ValueError("demo mode cannot request GPUs")
        cap = self.config.slurm.limits.gpu_hours_per_experiment
        if (
            cap is not None
            and sum(
                (s.resources or self.config.slurm.defaults()).gpus_per_job
                * (s.resources or self.config.slurm.defaults()).walltime_minutes_per_job
                / 60
                for s in plan.stages
                if s.executor == "slurm"
            )
            > cap
        ):
            raise ValueError("plan exceeds the experiment GPU-hour budget")

    def project_context(self, exp):
        identity = exp["contract"].get("project", {"id": "test-project", "name": "Test project"})
        saved = self.projects.workspace(exp["id"])
        project = self.store.projects.get(identity["id"])
        branch = self.workspaces.branch(exp["id"])
        if branch:
            checkout = self.workspaces.branch_checkout(exp["id"])
        elif saved:
            checkout = self.workspaces.checkout(exp["id"])
        elif project and project.public()["available"]:
            checkout = str(project.checkout.resolve()) if project.checkout else None
        else:
            raise ValueError("selected project checkout is unavailable; restore its configuration")
        isolated = self.workspaces.row(exp["id"])
        candidate = exp["contract"].get("repair_project")
        return {
            **identity,
            "checkout": candidate or checkout,
            "access": "read-write" if saved else "read-only",
            "isolation": "worktree" if isolated or branch else "shared",
            "source_checkout": branch["repo"] if branch else saved["folder"] if saved else checkout,
            "base_commit": branch["head_commit"] if branch else isolated["head"] if isolated else None,
            "read_only_repositories": json.loads(isolated["baseline"]).get("read_only_repositories", [])
            if isolated
            else [],
        }

    def context_pointer(self, exp, label, value):
        """Keep full context outside the prompt at an immutable content identity."""
        content = encode(value)
        digest = hashlib.sha256(content.encode()).hexdigest()
        folder = self.root(exp["id"]) / "agent-context"
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"{label}-{digest}.json"
        if not path.exists():
            temporary = folder / f".{label}-{uuid4()}.tmp"
            try:
                temporary.write_text(content)
                os.replace(temporary, path)
            finally:
                temporary.unlink(missing_ok=True)
        elif path.read_text() != content:
            raise ValueError("saved agent context has changed")
        return {"path": str(path), "sha256": digest}

    def worktree_conventions(self, exp):
        """The saved mode is available even before the prepare worktree exists."""
        return agent_conventions.worktree_conventions(bool(exp["contract"].get("branch_workspace")))

    def _action_state(self, action_id, state, *, error=None, result=None, expected=None):
        with self.store.tx() as db:
            before = dict(db.execute("SELECT * FROM agent_actions WHERE id=?", (action_id,)).fetchone())
            if expected is not None:
                action_obligations.check_expected(before, expected, compare_state=False)
            saved_result = encode(result) if result is not None else before["result"]
            if (before["state"], before["error"], before["result"]) == (state, error, saved_result):
                return
            changes = {"state": state, "error": error, "result": saved_result}
            action_obligations.transition(db, action_id, changes, before=before, expected=expected)
            self.store.emit(
                db, before["experiment_id"], "agent_action_changed", {"action_id": action_id, "state": state}
            )

    async def reconcile_action(self, action_id, *, turn_id=None):
        """Adopt only a completed, already recorded turn; never dispatch a replacement."""
        if self.agent_task:
            raise ValueError("stop the owning daemon before operator reconciliation")
        try:
            return await self.adopt_turn(action_id, turn_id=turn_id)
        finally:
            # Settled outside any dispatch task: re-read the row so the reservation
            # releases, or keeps, exactly what the saved state proves.
            self.agent_pool.settle_row("agent_actions", action_id)

    def dispatch_prompt_hash(self, action):
        """Return the exact prompt hash recorded for this action's saved dispatch intent.

        Historical continuations prepended their predecessor's saved terminal error.
        Do not guess from the timeout error that a later readback overwrote.
        """
        intents = [
            json.loads(event["payload"])
            for event in self.store.rows(
                "SELECT payload FROM events WHERE experiment_id=? AND kind='agent_dispatch_intent' ORDER BY seq",
                (action["experiment_id"],),
            )
        ]
        intents = [event for event in intents if event.get("action_id") == action["id"]]
        if len(intents) != 1 or intents[0].get("thread_id") != action["thread_id"]:
            raise NeedsAttention("exact dispatch intent is missing or ambiguous")
        prompt_hash = intents[0].get("prompt_sha256")
        if prompt_hash:
            return prompt_hash
        previous = self.store.one(
            "SELECT * FROM agent_actions WHERE id=?", (action.get("previous_action_id"),)
        )
        if (
            not previous
            or previous["state"] != "failed"
            or not previous["error"]
            or previous["experiment_id"] != action["experiment_id"]
            or previous["phase"] != action["phase"]
            or previous["thread_id"] != action["thread_id"]
        ):
            raise NeedsAttention("historical dispatch prompt cannot be established")
        prompt = (
            "Previous agent action failed; correct the reported error: "
            + previous["error"]
            + "\n"
            + action["input"]
        )
        return hashlib.sha256(prompt.encode()).hexdigest()

    async def adopt_turn(self, action_id, *, turn_id=None):
        """Read and adopt one exact saved turn; operator reconciliation and the tick share it."""
        action = self.store.one("SELECT * FROM agent_actions WHERE id=?", (action_id,))
        if action is not None and action["phase"] == research_readback.PHASE:
            # Contract 4 settles through its own readback; the contract 3 validators
            # below must never interpret a research turn end.
            return await research_readback.reconcile(self, action, turn_id=turn_id)
        if action and action["state"] == "completed" and turn_id is not None and action["turn_id"] == turn_id:
            return {
                "action_id": action_id,
                "thread_id": action["thread_id"],
                "turn_id": turn_id,
                "state": "completed",
            }
        if not action or action["state"] not in ("uncertain", "running", "dispatching"):
            raise ValueError("action must be a saved uncertain/running dispatch")
        recovering_identity = turn_id is not None and action["turn_id"] is None
        if turn_id is not None and action["turn_id"] not in (None, turn_id):
            raise NeedsAttention("supplied turn differs from the saved turn identity")
        if recovering_identity:
            if (
                not action["thread_id"]
                or action["attempts"] != 1
                or (
                    action["phase"] not in ("plan", "prepare", "interpret")
                    and not action["phase"].startswith("agent:")
                )
            ):
                raise NeedsAttention("missing turn recovery requires one saved research dispatch")
            if self.store.one(
                "SELECT id FROM agent_actions WHERE thread_id=? AND turn_id=?", (action["thread_id"], turn_id)
            ):
                raise NeedsAttention("turn already belongs to a saved action")
            prompt_hash = self.dispatch_prompt_hash(action)
            result = await self.agent.read_turn(
                action["thread_id"], turn_id, expected_prompt_hash=prompt_hash
            )
        elif not action["thread_id"] or not action["turn_id"]:
            raise NeedsAttention("saved turn identity is missing; no inferred adoption or retry is allowed")
        else:
            result = await self.agent.read_turn(action["thread_id"], action["turn_id"])
        exp = self.store.experiment(action["experiment_id"])
        if action["phase"].startswith("discuss:"):
            # The exact terminal result must be durable before reply validation can
            # queue a correction; a malformed reply is no longer an uncertain turn.
            self._action_state(action_id, "completed", result=result, expected=action)
            return {
                "action_id": action_id,
                "thread_id": action["thread_id"],
                "turn_id": action["turn_id"],
                "state": "completed",
            }
        elif legacy_archive.execution_frozen(exp):
            if recovering_identity:
                with self.store.tx() as db:
                    before = action_obligations.expected_saved(db, action)
                    cursor = db.execute(
                        "UPDATE agent_actions SET turn_id=?,state='completed',result=?,error=NULL,updated_at=? WHERE id=? AND turn_id IS NULL",
                        (turn_id, encode(result), clock.now(), action_id),
                    )
                    if not cursor.rowcount:
                        raise NeedsAttention(
                            "saved action changed during reconciliation; no adoption was made"
                        )
                    action_obligations.sync_action(db, action_id, before=before)
                    self.store.emit(
                        db,
                        exp["id"],
                        "agent_turn_identity_reconciled",
                        {"action_id": action_id, "thread_id": action["thread_id"], "turn_id": turn_id},
                    )
            else:
                self._action_state(action_id, "completed", result=result, expected=action)
            return {
                "action_id": action_id,
                "thread_id": action["thread_id"],
                "turn_id": turn_id or action["turn_id"],
                "state": "completed",
            }
        elif action["phase"] in ("title", "summarize_title"):
            TitleSummary.model_validate(result)
        elif action["phase"].startswith("agent:"):
            AgentStageResult.model_validate(result)
        elif action["phase"] == "monitor":
            ControllerDecision.model_validate(result)
        elif action["phase"] == "plan" and exp["recipe_id"] == "agent_experiment_v1":
            ResearchPlan.model_validate(result)
        elif action["phase"] == "prepare" and exp["recipe_id"] == "agent_experiment_v1":
            plan = Implementation.model_validate(result)
            for name in plan.files:
                artifacts.allowed_path(self.working_directory(exp) / name, self.working_directory(exp))
        else:
            records = self.store.rows(
                "SELECT * FROM artifacts WHERE experiment_id=? AND evidence_revision=?",
                (exp["id"], exp["evidence_revision"]),
            )
            for record in records:
                if (
                    artifacts.sha256(artifacts.allowed_path(Path(record["path"]), self.root(exp["id"])))
                    != record["sha256"]
                ):
                    raise ValueError("registered evidence changed before reconciliation")
            artifacts.validate_analysis(result, [record["id"] for record in records])
        if recovering_identity:
            with self.store.tx() as db:
                before = action_obligations.expected_saved(db, action)
                cursor = db.execute(
                    "UPDATE agent_actions SET turn_id=?,state='completed',result=?,error=NULL,updated_at=? WHERE id=? AND turn_id IS NULL",
                    (turn_id, encode(result), clock.now(), action_id),
                )
                if not cursor.rowcount:
                    raise NeedsAttention("saved action changed during reconciliation; no adoption was made")
                action_obligations.sync_action(db, action_id, before=before)
                before_request = request_obligations.saved_pending(db, exp["id"])
                db.execute(
                    "UPDATE experiments SET hold_reason=NULL,error=NULL,pending_request=NULL WHERE id=?",
                    (exp["id"],),
                )
                if before_request is not None:
                    request_obligations.sync_request(db, exp["id"], before=before_request)
                self.store.emit(
                    db,
                    exp["id"],
                    "agent_turn_identity_reconciled",
                    {"action_id": action_id, "thread_id": action["thread_id"], "turn_id": turn_id},
                )
            action = {**action, "turn_id": turn_id}
        else:
            self._action_state(action_id, "completed", result=result, expected=action)
        if not recovering_identity and (not action["phase"].startswith("discuss:") or exp["pending_request"]):
            self.store.update(
                exp["id"], kind="agent_turn_reconciled", hold_reason=None, error=None, pending_request=None
            )
        return {
            "action_id": action_id,
            "thread_id": action["thread_id"],
            "turn_id": action["turn_id"],
            "state": "completed",
        }

    async def advance_reports(self):
        rows = self.store.rows(
            "SELECT r.* FROM report_versions r JOIN experiments e ON e.id=r.experiment_id "
            "WHERE e.deleted_at IS NULL AND e.phase!='cancelled' AND r.state IN ('queued','building','publishing') ORDER BY r.created_at"
        )
        rendered = False
        for row in rows:
            eid, version = row["experiment_id"], row["version"]
            exp = self.store.experiment(eid)
            try:
                if row["state"] in ("queued", "building"):
                    if rendered:
                        continue
                    rendered = True
                    if row["state"] == "queued":
                        self.store.report_state(eid, version, "building")
                    exp = self.store.experiment(eid)
                    root = self.root(eid)
                    records = self.store.rows(
                        "SELECT * FROM artifacts WHERE experiment_id=?"
                        + ("" if iteration.enabled(exp) else " AND evidence_revision=?"),
                        (eid,)
                        if iteration.enabled(exp)
                        else (eid, self.store.experiment(eid)["evidence_revision"]),
                    )
                    report_options = {}
                    if exp["contract"].get("execution_contract") == 4:
                        report_options["result_receipts"] = [
                            json.loads(event["payload"])
                            for event in self.store.rows(
                                "SELECT payload FROM events WHERE experiment_id=? AND kind='research_result_registered' "
                                "ORDER BY seq",
                                (eid,),
                            )
                        ]
                    if any(
                        receipt.get("experiment_id") != eid
                        or not receipt.get("intent_id")
                        or not receipt.get("action_id")
                        or not self.store.one(
                            "SELECT 1 FROM jobs WHERE intent_id=? AND experiment_id=?",
                            (receipt.get("intent_id"), eid),
                        )
                        for receipt in report_options.get("result_receipts", ())
                    ):
                        raise ValueError("registered research result has invalid producer scope")
                    name, files = await asyncio.to_thread(
                        reporting.build_report,
                        exp,
                        root,
                        records,
                        json.loads(row["document"]),
                        version,
                        **report_options,
                    )
                    if self.store.experiment(eid)["phase"] == "cancelled":
                        return
                    artifacts.register(
                        self.store, eid, "publish", root, files, report_version=version, report_name=name
                    )
                    row = self.store.one(
                        "SELECT * FROM report_versions WHERE experiment_id=? AND version=?", (eid, version)
                    )
                identities = json.loads(row["artifact_ids"])
                registered = self.store.rows(
                    "SELECT * FROM artifacts WHERE experiment_id=?"
                    + ("" if iteration.enabled(exp) else " AND evidence_revision=?"),
                    (eid,)
                    if iteration.enabled(exp)
                    else (eid, self.store.experiment(eid)["evidence_revision"]),
                )
                if not identities or not set(identities) <= {a["id"] for a in registered}:
                    raise ValueError("missing registered report outputs")
                sources, checked_outputs = reporting.verify_report(
                    self.store.experiment(eid),
                    self.root(eid),
                    registered,
                    version,
                    json.loads(row["document"]),
                )
                if set(checked_outputs) != set(identities):
                    raise ValueError("report outputs differ from saved registry")
                selected = [a for a in registered if a["id"] in identities + sources]
                if self.config.gateway.url and any(not a["remote_published"] for a in selected):
                    continue
                for a in selected:
                    if (
                        artifacts.sha256(
                            artifacts.allowed_path(Path(a["path"]), self.projects.base_root(eid))
                        )
                        != a["sha256"]
                    ):
                        raise ValueError("registered report output changed")
                self.store.report_state(eid, version, "ready")
            except Exception as exc:
                self.store.report_state(eid, version, "failed", str(exc)[:2000])
                self.store.notify(eid, "report_failed", f"Report version {version} could not be built: {exc}")

    async def close(self):
        # Stop admissions before any role stops: a tick that starts while shutdown runs
        # must not claim a turn whose transport this call is about to close.
        self.closing = True
        self.agent_pool.closing = True
        await self.research.close()
        await self.supervisor.close()
        await self.controller.close()
        # Threads only read/seal filesystem evidence; drain them before releasing ownership.
        # A restart re-observes the exact saved job and validates again without resubmission.
        checks = [task for task, _ in self.output_checks.values()] + list(self.evidence_checks.values())
        if checks:
            await asyncio.gather(*checks, return_exceptions=True)
        self.output_checks.clear()
        self.evidence_checks.clear()
        # Withdraw every slot's tasks first, then close each transport exactly once. A
        # cancelled turn keeps its saved uncertainty; nothing here declares a remote
        # turn absent.
        for slot in self.agent_pool.slots:
            if slot.agent_task and not slot.agent_task.done():
                slot.agent_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await slot.agent_task
            if slot.quota_task and not slot.quota_task.done():
                slot.quota_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await slot.quota_task
        for slot in self.agent_pool.slots:
            await slot.agent.close()
        if self.backup_task:
            await self.backup_task
        await self.backup_state()
