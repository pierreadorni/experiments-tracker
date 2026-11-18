import asyncio
import contextlib
import hashlib
import json
import logging
import os
import shutil
import sys
import time
from functools import partial
from pathlib import Path
from uuid import uuid4

from . import (
    agent_conventions,
    artifacts,
    clock,
    early_repair,
    iteration,
    legacy_archive,
    legacy_archive_actions,
    reporting,
    resolver,
)
from .agent_pool import EXPERIMENT_PRIORITY, TITLE_PRIORITY, AgentPool, experiment_owner
from .autopilot_controller import ResearchController
from .cancellation import process_cancellations
from .codex_client import CodexAgent, NeedsAttention, RpcRejected, SessionUnavailable, TerminalTurnError
from .config import JobResources
from .controller import Controller, ControllerDecision
from .conversation import Conversation
from .core import (
    action_obligations,
    advancement_decisions,
    job_obligations,
    liveness,
    obligation_backfill,
    request_obligations,
    review_obligations,
)
from .engine.integration_resolver import IntegrationResolver
from .errors import ExperimentFailure
from .knowledge import ProjectSupervisor
from .maintenance import Maintenance, blocked
from .models import (
    AgentStageResult,
    Analysis,
    ExperimentResult,
    Implementation,
    ResearchPlan,
    stage_dependencies,
)
from .progress import DiscussionActivity, describe_item, read_job_progress
from .projects import Projects, ProjectUnavailable
from .quota import QuotaGate
from .remote import AdmissionPending, RemoteSlurmScheduler, TransferPending
from .research import readback as research_readback
from .research.dispatch import PHASE as RESEARCH_PHASE
from .research.dispatch import ResearchDispatch
from .research.session import first_turn, follow_up_turn
from .research.settlement import complete as settle_research_turn
from .resolver import TurnResolver
from .slurm import ResourceUnavailable, SubmissionUncertain, command, wrapper
from .steering import process_restarts, queue_late_reports
from .store import encode
from .tfevents import read_previews
from .tfevents import selection as tensorboard_selection
from .title import TitleSummary, luna_title_model, title_prompt
from .watchdog import engine_watchdog
from .workspaces import Workspaces, WorkspaceUnavailable

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

    async def ensure_cluster(self, exp):
        if not self.is_remote(exp):
            return self.cluster_snapshot
        settings = exp["contract"]["execution"]
        cache = self.remote_clusters
        host = settings.get("ssh_host", "jz")
        previous = cache.get(host, {})
        if clock.now() - previous.get("checked_at", 0) >= settings.get("poll_seconds", 600):
            try:
                snapshot = await self.scheduler_for(exp).discover()
                cache[host] = {"snapshot": snapshot, "checked_at": clock.now(), "error": None}
            except Exception as exc:
                cache[host] = {"snapshot": None, "checked_at": clock.now(), "error": str(exc)}
        return cache.get(host, {}).get("snapshot")

    def cluster_for(self, exp):
        if not self.is_remote(exp):
            return self.cluster_snapshot
        return self.remote_clusters.get(exp["contract"]["execution"].get("ssh_host", "jz"), {}).get(
            "snapshot"
        )

    def execution_instruction(self, exp, phase):
        return (
            "Execution machine: "
            + ("Jean Zay via acacia owner" if self.is_remote(exp) else "acacia")
            + ". Each attempt has its own declared source copy and output directory. Never import editable "
            "working code, synchronize a whole project or seal scientific outputs. EXPERIMENT_INPUTS "
            "contains direct producer paths with host context. On Jean Zay use scripts initializing modules, "
            "omit --mem directives (cluster-assigned memory), and set the remote base using set_project_execution "
            "if needed. Transfers retrieve only bounded declared outputs/logs and selected previews. "
            "No scientific hash/membership checks or automatic cleanup. Never submit nested scheduler jobs."
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
        return {
            "experiment_cancellation": True,
            "implementation_repair": True,
            "branch_integration": True,
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
        """Steer, restart, repair, report and advance every eligible experiment."""
        self.apply_planning_steering()
        await process_restarts(self)
        await self.repair_failed_experiments()
        await self.advance_reports()
        rows = self.store.rows(
            "SELECT e.id FROM experiments e LEFT JOIN autopilot_members m ON m.experiment_id=e.id "
            "WHERE e.deleted_at IS NULL AND e.phase NOT IN ('complete','failed','cancelled') "
            "AND json_extract(e.contract,'$.execution_contract') IS 3 "
            "ORDER BY COALESCE(m.priority,0) DESC,e.created_at,e.id"
        )
        # Advance independent state machines under the same owner. Saved intents
        # and the account-wide agent slot remain the dispatch authority. Only the
        # saved contract-3 stage graph is selected here: contract 4 has its own
        # turn loop, and a widened archive predicate must not enroll it.
        for row in rows:
            eid = row["id"]
            backoff = self.error_backoff.get(eid)
            if backoff and clock.now() < backoff[1]:
                continue
            try:
                await self.advance(eid)
            except ExperimentFailure as exc:
                self.fail(eid, exc.reason, exc.message)
            except ProjectUnavailable as exc:
                self.store.update(eid, hold_reason="project_unavailable", error=str(exc))
            except Exception as exc:
                # Bugs hold one experiment; they never fail a saved research record.
                log.exception("advance failed for experiment %s", eid)
                self.hold_advance_error(eid, exc)
            else:
                self.error_backoff.pop(eid, None)

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
        await self.controller.advance()

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
            if not iteration.enabled(exp):
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
            if not iteration.enabled(exp):
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
        if exp["deleted_at"] or not iteration.enabled(exp):
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
            if not iteration.enabled(exp):
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
                not iteration.enabled(exp)
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

    async def repair_failed_experiments(self):
        """Journal one in-place repair only after all saved execution is terminal."""
        for row in self.store.rows(
            "SELECT id FROM experiments WHERE deleted_at IS NULL AND phase IN ('failed','preparing','training','evaluating') "
            "AND hold_reason IN ('compute_failed','validation_failed','automatic_repair_pending','resources_unavailable') ORDER BY created_at"
        ):
            eid = row["id"]
            exp = self.store.experiment(eid)
            if not iteration.enabled(exp):
                continue
            if self.config.mode != "live" or exp["recipe_id"] != "agent_experiment_v1" or not exp["snapshot"]:
                continue
            if exp["hold_reason"] != "resources_unavailable":
                await early_repair.queue(self, exp)
                continue
            resource_revision = exp["hold_reason"] == "resources_unavailable"
            if not resource_revision:
                continue
            if clock.now() - max(
                (stamp for (saved_eid, _, _), stamp in self.last_preflight.items() if saved_eid == eid),
                default=0,
            ) < (
                exp["contract"].get("execution", {}).get("poll_seconds", 600) if self.is_remote(exp) else 60
            ):
                continue
            if (
                exp["pending_request"]
                or self.store.one(
                    "SELECT 1 FROM steering WHERE experiment_id=? AND state IN ('pending','cancelling')",
                    (eid,),
                )
                or self.store.one(
                    "SELECT 1 FROM agent_actions WHERE experiment_id=? AND state IN ('running','dispatching','uncertain')",
                    (eid,),
                )
            ):
                continue
            if not self.workspaces.isolated(eid):
                project_id = exp["contract"].get("project", {}).get("id", "test-project")
                # Queue one editor at a time for an existing shared checkout.
                # Simultaneously reactivating two failed editors would make
                # both wait for the other's ownership without either advancing.
                peers = self.store.rows(
                    "SELECT id FROM experiments e WHERE id!=? AND deleted_at IS NULL "
                    "AND phase NOT IN ('complete','failed','cancelled') "
                    "AND (snapshot IS NOT NULL OR EXISTS (SELECT 1 FROM agent_actions a "
                    "WHERE a.experiment_id=e.id AND a.phase='prepare' AND a.attempts>0))",
                    (eid,),
                )
                if any(
                    not self.workspaces.isolated(peer["id"])
                    and self.store.experiment(peer["id"])["contract"]
                    .get("project", {})
                    .get("id", "test-project")
                    == project_id
                    for peer in peers
                ):
                    continue
            jobs = self.store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
            # Agent-stage producer records share this table but never have a
            # Slurm identity. Their turns are reconciled above, not via Slurm.
            agent_jobs = [
                j for j in jobs if json.loads(j["execution_manifest"] or "{}").get("executor") == "agent"
            ]
            scheduler_jobs = [j for j in jobs if j not in agent_jobs]
            if any(j["state"] not in ("complete", "failed", "cancelled") for j in agent_jobs):
                continue
            if (not scheduler_jobs and not resource_revision) or any(
                j["state"] in ("submitting", "submitted") or not j["job_id"] for j in scheduler_jobs
            ):
                continue
            # Fresh exact accounting is required even for an output-validation failure.
            confirmed = True
            current_timeout = False
            for job in scheduler_jobs:
                try:
                    if (
                        await self.scheduler_for(self.store.experiment(job["experiment_id"]), job).reconcile(
                            job["intent_id"]
                        )
                        != job["job_id"]
                    ):
                        confirmed = False
                        break
                    observed = await self.scheduler_for(
                        self.store.experiment(job["experiment_id"]), job
                    ).observe(job["job_id"])
                except Exception:
                    confirmed = False
                    break
                if job["attempt"] == exp["evidence_revision"] and observed.state == "TIMEOUT":
                    current_timeout = True
                if observed.state not in TERMINAL_BAD | {"COMPLETED"} or observed.exit_code is None:
                    confirmed = False
                    break
            if not confirmed:
                continue
            budget = exp["contract"].get("max_jobs", self.config.slurm.max_jobs_per_experiment)
            if budget is not None and len(scheduler_jobs) >= budget:
                if exp["hold_reason"] == "automatic_repair_pending":
                    self.fail(
                        eid, "compute_budget_exhausted", "Automatic repair would exceed the saved job budget"
                    )
                continue
            contract = exp["contract"]
            revision = exp["evidence_revision"] + 1
            contract.setdefault("working_directory", str(self.working_directory(exp)))
            contract.setdefault(
                "repair_count",
                len(contract.get("repair_history", [])) + bool(contract.get("auto_repair")),
            )
            if contract.get("auto_repair"):
                contract.setdefault("repair_history", []).append(contract["auto_repair"])
            current_jobs = [
                j
                for j in jobs
                if j["attempt"] == exp["evidence_revision"]
                or j["intent_id"] in contract.get("reused_job_intents", [])
            ]
            contract["auto_repair"] = {
                "revision": revision,
                "previous_revision": exp["evidence_revision"],
                "previous_root": str(self.root(eid)),
                "completed_stages": [
                    s["id"]
                    for s in self.store.rows(
                        "SELECT id FROM stages WHERE experiment_id=? AND state='complete'", (eid,)
                    )
                ],
                "kind": "resources"
                if resource_revision
                else "timeout"
                if current_timeout
                else "implementation",
                "reason": exp["hold_reason"],
                "error": exp["error"],
                "snapshot": exp["snapshot"],
                "jobs": current_jobs,
            }
            review = exp["plan_review"]
            if review:
                review = {
                    **review,
                    "preparation_revision": max(
                        revision, review.get("preparation_revision", review["revision"]) + 1
                    ),
                }
            with self.store.tx() as db:
                self.store.emit(
                    db,
                    eid,
                    "automatic_repair_started",
                    {
                        "evidence_revision": revision,
                        "error": exp["error"],
                        "snapshot": exp["snapshot"],
                        "stages": self.store.rows("SELECT * FROM stages WHERE experiment_id=?", (eid,)),
                    },
                )
                before_review = review_obligations.saved_review(db, eid)
                db.execute(
                    "UPDATE experiments SET contract=?,evidence_revision=?,snapshot=NULL,analysis=NULL,"
                    "scientific_outcome=NULL,phase='preparing',hold_reason='automatic_repair',error=NULL,"
                    "plan_review=? WHERE id=?",
                    (encode(contract), revision, encode(review) if review else None, eid),
                )
                review_obligations.sync_review(db, eid, before=before_review)
                db.execute(
                    "UPDATE stages SET state='pending',attempt=?,output_manifest=NULL,error=NULL,updated_at=? "
                    "WHERE experiment_id=? AND id!='plan'",
                    (revision, clock.now(), eid),
                )
                self.store.emit(db, eid, "automatic_repair_queued", {"evidence_revision": revision})

    def fail(self, eid, reason, error):
        exp = self.store.experiment(eid)
        if exp["phase"] == "cancelled":
            return
        if (
            iteration.enabled(exp)
            and early_repair.preparing(exp)
            and reason in ("compute_failed", "validation_failed")
        ):
            self.store.update(eid, phase="preparing", hold_reason="automatic_repair", error=error)
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

    def hold_advance_error(self, eid, exc):
        """Bugs hold one experiment: an inbox notice per distinct error text, then back off.

        The inbox is unique per (experiment, kind, text), so a repeating error does not
        flood the owner. The next successful `advance(eid)` clears the backoff entry, so
        a repaired bug resumes normal ticks instead of staying suppressed.
        """
        error = str(exc) or type(exc).__name__
        previous = self.error_backoff.get(eid)
        count = 0 if previous is None else previous[0] + 1
        self.error_backoff[eid] = (count, clock.now() + min(30 * 2**count, 1800))
        self.store.update(eid, hold_reason="executor_error", error=error)
        self.store.notify(eid, "executor_error", f"Experiment paused after an internal error: {error}")

    def apply_planning_steering(self):
        """Fold saved guidance into a fresh preparation revision before any job is dispatched."""
        pending = self.store.rows(
            "SELECT request_id,experiment_id,text FROM steering WHERE state='pending' ORDER BY created_at"
        )
        for item in pending:
            eid = item["experiment_id"]
            exp = self.store.experiment(eid)
            # Plan-revision feedback is part of the contract-3 planning stage; contract 4
            # steering becomes an owner message in its next turn instead.
            if not iteration.enabled(exp):
                continue
            review = exp["plan_review"]
            if (
                exp["recipe_id"] != "agent_experiment_v1"
                or exp["snapshot"]
                or not review
                or review["state"] not in ("awaiting_review", "approved", "drafting")
                or self.store.one("SELECT 1 FROM jobs WHERE experiment_id=?", (eid,))
                or self.store.one(
                    "SELECT 1 FROM agent_actions WHERE experiment_id=? AND phase='prepare'",
                    (eid,),
                )
            ):
                continue
            with self.store.tx() as db:
                current = db.execute("SELECT plan_review FROM experiments WHERE id=?", (eid,)).fetchone()
                review = json.loads(current["plan_review"])
                before_review = dict(review)
                if review["state"] != "drafting":
                    review["revision"] += 1
                    review["state"] = "drafting"
                previous = review.get("feedback", "")
                review["feedback"] = (previous + "\n" + item["text"]).strip()
                db.execute(
                    "UPDATE experiments SET plan_review=?,pause_compute=0,hold_reason=NULL WHERE id=?",
                    (encode(review), eid),
                )
                review_obligations.sync_review(db, eid, before=before_review)
                db.execute(
                    "UPDATE stages SET state='pending',updated_at=? WHERE experiment_id=? AND id='plan'",
                    (clock.now(), eid),
                )
                db.execute(
                    "UPDATE steering SET state='applied',applied_at=? WHERE request_id=?",
                    (clock.now(), item["request_id"]),
                )
                self.store.emit(db, eid, "steering_applied", {"request_id": item["request_id"]})

    async def advance(self, eid):
        exp = self.store.experiment(eid)
        # The stage graph is contract-3-only: archived legacy contracts and a future
        # contract 4 both stop here, whatever the archive predicate admits.
        if not iteration.enabled(exp):
            return
        if exp["phase"] == "cancelled" or exp["hold_reason"] in (
            "automatic_repair_pending",
            "agent_stage_blocked",
            "agent_stage_failed",
            "implementation_blocked",
        ):
            return
        restart = self.store.one(
            "SELECT 1 FROM steering WHERE experiment_id=? AND mode='restart' AND state IN ('pending','cancelling')",
            (eid,),
        )
        if restart and (
            exp["snapshot"]
            or self.store.one("SELECT 1 FROM jobs WHERE experiment_id=?", (eid,))
            or self.store.one("SELECT 1 FROM agent_actions WHERE experiment_id=? AND phase='prepare'", (eid,))
        ):
            self.store.update(eid, hold_reason="steering_restart")
            return
        stages = {
            s["id"]: s["state"] for s in self.store.rows("SELECT * FROM stages WHERE experiment_id=?", (eid,))
        }
        root = await asyncio.to_thread(self.root, eid)
        await asyncio.to_thread(root.mkdir, parents=True, exist_ok=True)
        exp = self.store.experiment(eid)
        if exp["phase"] == "cancelled":
            return
        generic = exp["recipe_id"] == "agent_experiment_v1"
        if generic and exp["contract"].get("planning_version") and stages["plan"] != "complete":
            await self.agent_stage(exp, "plan")
            return
        if generic and stages["prepare"] != "complete":
            await self.agent_stage(exp, "prepare")
            return
        await asyncio.to_thread(artifacts.verify_snapshot, root, exp["snapshot"])
        exp = self.store.experiment(eid)
        if exp["phase"] == "cancelled":
            return
        if stages["prepare"] != "complete":
            await self.agent_stage(exp, "prepare")
            return
        compute_stages = self.compute_stages(exp)
        dependencies = {stage["id"]: stage["needs"] for stage in exp["snapshot"]["plan"]["stages"]}
        # An unknown submission may already exist in Slurm. Reconcile its exact
        # identity before crossing another dispatch boundary for this experiment.
        if self.store.one(
            "SELECT 1 FROM jobs WHERE experiment_id=? AND state='submitting' AND job_id IS NULL",
            (eid,),
        ):
            self.store.update(eid, hold_reason="submission_uncertain")
            return
        requested = {
            s["id"] for s in exp["snapshot"].get("plan", {}).get("stages", []) if s.get("requested", True)
        }
        requested_done = bool(requested) and all(stages[s] == "complete" for s in requested)
        for index, stage in enumerate(compute_stages):
            if requested_done and stage not in requested:
                continue
            if stages[stage] == "complete":
                continue
            if early_repair.active_predecessor(self, exp, stage):
                continue
            job = self.store.one(
                "SELECT * FROM jobs WHERE experiment_id=? AND stage_id=? AND attempt=?",
                (eid, stage, exp["evidence_revision"]),
            )
            spec = next(
                (s for s in exp["snapshot"].get("plan", {}).get("stages", []) if s["id"] == stage), {}
            )
            if spec.get("executor") == "agent":
                if any(stages[need] != "complete" for need in dependencies[stage]):
                    continue
                if exp["pause_compute"] and not job:
                    self.store.update(eid, hold_reason="new_compute_paused")
                    continue
                if iteration.agent_job(self, exp, "agent:" + stage):
                    # An existing job already owns its exact intent and commit; never
                    # commit later worktree edits just to replay it.
                    await self.agent_stage(exp, "agent:" + stage)
                    continue
                intent = str(uuid4())
                try:
                    commit = await self.commit_branch(
                        exp, f"Job sources {stage} revision {exp['evidence_revision']}"
                    )
                except WorkspaceUnavailable as exc:
                    self.store.update(eid, hold_reason="workspace_unavailable", error=str(exc))
                    return
                self.report_skipped_commit(eid, commit, kind="job", stage=stage)
                try:
                    iteration.queue_agent(
                        self,
                        exp,
                        stage,
                        commit=commit["commit"] if commit else None,
                        intent=intent,
                    )
                except WorkspaceUnavailable as exc:
                    self.store.update(eid, hold_reason="workspace_unavailable", error=str(exc))
                    return
                await self.agent_stage(exp, "agent:" + stage)
                continue
            if job:
                if job["state"] == "complete":
                    names = await asyncio.to_thread(artifacts.validate_stage, root, stage, exp["snapshot"])
                    artifacts.register(self.store, eid, stage, root, names)
                    self.store.stage(eid, stage, "complete", outputs=names)
                elif job["state"] == "failed":
                    if stage not in requested:
                        continue
                    self.fail(eid, "compute_failed", job["error"] or "scheduler job failed")
                    return
                continue
            if any(stages[need] != "complete" for need in dependencies[stage]):
                continue
            if exp["pause_compute"]:
                self.store.update(
                    eid,
                    phase="evaluating" if index == len(compute_stages) - 1 else "training",
                    hold_reason="new_compute_paused",
                )
                return
            max_concurrent_jobs = self.config.slurm.max_concurrent_jobs
            if (
                max_concurrent_jobs is not None
                and self.store.one(
                    "SELECT COUNT(*) AS n FROM jobs WHERE state IN ('submitting','submitted') "
                    "AND NOT (scheduler_state IS 'COMPLETED' AND exit_code IS '0:0')"
                )["n"]
                >= max_concurrent_jobs
            ):
                self.store.update(
                    eid,
                    phase="evaluating" if index == len(compute_stages) - 1 else "training",
                    hold_reason="compute_busy",
                )
                return
            outcome = await self.submit_prepared_stage(exp, stage)
            if outcome and outcome.get("state") == "uncertain":
                return
        if any(stages[stage] != "complete" for stage in compute_stages if stage in requested):
            return
        self.prepare_iteration_results(exp)
        if stages["interpret"] != "complete":
            await self.agent_stage(exp, "interpret")
            return
        if stages["publish"] != "complete":
            await self.publish(exp)

    def prepare_iteration_results(self, exp):
        partial = json.loads(exp["partial_results"]) if exp.get("partial_results") else {"tasks": {}}
        metrics = {}
        evaluations = {s["id"] for s in exp["snapshot"]["plan"]["stages"] if s.get("result_file")}
        for stage, task in partial["tasks"].items():
            if stage not in evaluations:
                continue
            for metric in task["result"]["metrics"]:
                key = (metric["name"], metric["variant"])
                if key in metrics:
                    raise ExperimentFailure(
                        "validation_failed", "ambiguous evaluation metric; use distinct metric/variant names"
                    )
                metrics[key] = metric
        if not metrics:
            raise ExperimentFailure("validation_failed", "requested evaluation has no registered metrics")
        result = {
            "schema_version": 1,
            "protocol": exp["snapshot"]["plan"]["protocol"],
            "metrics": list(metrics.values()),
        }
        root = self.root(exp["id"])
        target = root / "results.json"
        if not target.exists():
            target.write_text(encode(result))
        artifacts.register(self.store, exp["id"], "evaluate", root, ["results.json"])

    async def submit_prepared_stage(self, exp, stage, request_id=None):
        async with self.submission_lock:
            return await self._submit_prepared_stage(exp, stage, request_id)

    async def _submit_prepared_stage(self, exp, stage, request_id):
        eid = exp["id"]
        exp = self.store.experiment(eid)
        if (
            not iteration.enabled(exp)
            or not exp["snapshot"]
            or exp["phase"]
            in (
                "failed",
                "cancelled",
                "complete",
            )
        ):
            raise ValueError("submission requires an active saved implementation")
        root = self.root(eid)
        held = early_repair.submission_hold(self, exp, stage)
        if held:
            return held
        compute_stages = self.compute_stages(exp)
        if stage not in compute_stages:
            raise ValueError("stage is not in the saved implementation")
        index = compute_stages.index(stage)
        spec = next((s for s in exp["snapshot"].get("plan", {}).get("stages", []) if s["id"] == stage), {})
        if spec.get("executor") == "agent":
            raise ValueError("agent stages run through the managed agent loop, not scheduler submission")
        if request_id:
            exact = self.store.one("SELECT * FROM jobs WHERE intent_id=?", (request_id,))
            if exact and (exact["experiment_id"], exact["stage_id"], exact["attempt"]) != (
                eid,
                stage,
                exp["evidence_revision"],
            ):
                raise ValueError("submission request identity belongs to different work")
        existing = self.store.one(
            "SELECT * FROM jobs WHERE experiment_id=? AND stage_id=? AND attempt=?",
            (eid, stage, exp["evidence_revision"]),
        )
        if existing:
            return {
                "intent_id": existing["intent_id"],
                "job_id": existing["job_id"],
                "state": "uncertain" if existing["state"] == "submitting" else existing["state"],
            }
        if self.store.one("SELECT 1 FROM jobs WHERE experiment_id=? AND state='submitting'", (eid,)):
            return {"state": "uncertain", "reason": "submission_uncertain"}
        plan = Implementation.model_validate(exp["snapshot"]["plan"])
        graph = stage_dependencies(plan.stages, plan.schema_version)
        if any(
            self.store.one("SELECT state FROM stages WHERE experiment_id=? AND id=?", (eid, need))["state"]
            != "complete"
            for need in graph[stage]
        ):
            return {"state": "held", "reason": "dependencies_pending"}
        if exp["plan_review"] and exp["plan_review"]["state"] != "approved":
            return {"state": "held", "reason": "plan_review"}
        if blocked(self.store):
            return {"state": "held", "reason": "executor_maintenance"}
        if exp["pause_compute"]:
            return {"state": "held", "reason": "new_compute_paused"}
        cap = self.config.slurm.max_concurrent_jobs
        if (
            cap is not None
            and self.store.one(
                "SELECT COUNT(*) AS n FROM jobs WHERE state IN ('submitting','submitted') "
                "AND NOT (scheduler_state IS 'COMPLETED' AND exit_code IS '0:0')"
            )["n"]
            >= cap
        ):
            return {"state": "held", "reason": "compute_busy"}
        artifacts.verify_snapshot(root, exp["snapshot"])
        count = self.store.one(
            "SELECT COUNT(*) AS n FROM jobs WHERE experiment_id=? AND json_extract(execution_manifest, '$.executor') IS NOT 'agent'",
            (eid,),
        )["n"]
        budget = exp["contract"].get("max_jobs", self.config.slurm.max_jobs_per_experiment)
        if budget is not None and count >= budget:
            self.fail(eid, "compute_budget_exhausted", f"{budget}-job budget exhausted")
            return
        if self.config.mode == "live" and clock.now() - self.last_preflight.get(
            (eid, exp["evidence_revision"], stage), 0
        ) < (exp["contract"].get("execution", {}).get("poll_seconds", 600) if self.is_remote(exp) else 60):
            return {"state": "held", "reason": exp["hold_reason"] or "resources_unavailable"}
        intent_id = request_id or str(uuid4())
        job_commit = None
        try:
            committed = await self.commit_branch(
                exp, f"Job sources {stage} revision {exp['evidence_revision']}"
            )
        except WorkspaceUnavailable as exc:
            self.store.update(eid, hold_reason="workspace_unavailable", error=str(exc))
            return {"state": "held", "reason": "workspace_unavailable"}
        if committed:
            job_commit = committed["commit"]
            self.report_skipped_commit(eid, committed, kind="job", stage=stage)
        python = self.experiment_python(exp)
        project = self.project_context(exp)["checkout"]
        resources = self.stage_resources(exp, stage)
        with self.store.tx() as db:
            held = self.research.service.admission(db, exp, stage, resources)
            if held:
                return held
        plan_stage = next(s for s in exp["snapshot"]["plan"]["stages"] if s["id"] == stage)
        submission_manifest = exp["snapshot"]
        refs = []
        scheduler = self.scheduler_for(exp)
        try:
            root, submission_manifest, refs = iteration.stage_attempt(
                self, exp, stage, intent_id, commit=job_commit
            )
        except WorkspaceUnavailable as exc:
            self.store.update(eid, hold_reason="workspace_unavailable", error=str(exc))
            return {"state": "held", "reason": "workspace_unavailable"}
        scheduler = self.scheduler_for(
            {**exp, "snapshot": submission_manifest, "_attempt_id": intent_id, "_attempt_root": str(root)}
        )
        project = root / "snapshot/project"
        script = wrapper(
            root,
            stage,
            intent_id,
            python,
            resources=resources,
            script=plan_stage.get("script"),
            project=project,
            iteration_mode=True,
        )
        if self.config.mode == "live":
            self.last_preflight[(eid, exp["evidence_revision"], stage)] = clock.now()
            try:
                await scheduler.preflight(intent_id, script, root, resources)
            except AdmissionPending as exc:
                self.store.update(eid, hold_reason="scheduler_admission_pending", error=str(exc))
                return {"state": "held", "reason": "scheduler_admission_pending"}
            except TransferPending as exc:
                self.store.update(eid, hold_reason="remote_transfer_pending", error=str(exc))
                return {"state": "held", "reason": "remote_transfer_pending"}
            except ResourceUnavailable as exc:
                if exp["hold_reason"] != "resources_unavailable" or exp["error"] != str(exc):
                    self.store.update(eid, hold_reason="resources_unavailable", error=str(exc))
                    self.store.notify(
                        eid,
                        "required_decision",
                        f"Slurm cannot currently accept the resource request: {exc}",
                    )
                return {"state": "held", "reason": "resources_unavailable"}
        admitted = self.store.experiment(eid)
        if (
            blocked(self.store)
            or admitted["pause_compute"]
            or admitted["phase"] in ("failed", "cancelled", "complete")
            or admitted["snapshot"]["snapshot_hash"] != exp["snapshot"]["snapshot_hash"]
        ):
            return {"state": "held", "reason": "execution_authorization_changed"}
        if job_commit is not None:
            # Pin before the intent is saved; a repeated request for this intent must
            # confirm the same commit, never rewrite an earlier pinned identity.
            try:
                await asyncio.to_thread(self.workspaces.pin_job_commit, eid, intent_id, job_commit)
            except WorkspaceUnavailable as exc:
                self.store.update(eid, hold_reason="workspace_unavailable", error=str(exc))
                return {"state": "held", "reason": "workspace_unavailable"}
        execution_manifest = dict(submission_manifest)
        if job_commit is not None:
            execution_manifest["commit"] = job_commit
        execution_manifest["command"] = iteration.bounded_bytes(script, root).decode()
        execution_manifest["parameters"] = submission_manifest["parameters"]
        if self.is_remote(exp):
            execution_manifest["execution"] = scheduler.metadata()
        record_name = f"job-source-{intent_id}.json"
        record_path = root / record_name
        record = {
            "schema_version": 1,
            "intent_id": intent_id,
            "stage_id": stage,
            "snapshot_hash": submission_manifest["snapshot_hash"],
            "script_hash": artifacts.sha256(script),
            "source_files": submission_manifest["files"],
            "project_sources": execution_manifest.get("project_sources_at_submission", {}),
            "commit": job_commit,
            "resources": resources.model_dump(),
        }
        record_path.write_text(encode(record))
        record_path.chmod(0o444)
        # Store submitting BEFORE crossing the scheduler boundary; all uncertainty is reconciled.
        with self.store.tx() as db:
            current = self.store.experiment(eid)
            if (
                blocked(self.store)
                or current["pause_compute"]
                or current["phase"] in ("failed", "cancelled", "complete")
                or current["snapshot"]["snapshot_hash"] != exp["snapshot"]["snapshot_hash"]
            ):
                return {"state": "held", "reason": "execution_authorization_changed"}
            held = self.research.service.admission(db, current, stage, resources)
            if held:
                return held
            db.execute(
                """INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,script_hash,
                resources,snapshot_hash,created_at,execution_root,execution_manifest) VALUES(?,?,?,?,'submitting',?,?,?,?,?,?)""",
                (
                    intent_id,
                    eid,
                    stage,
                    exp["evidence_revision"],
                    artifacts.sha256(script),
                    encode(resources.model_dump()),
                    submission_manifest["snapshot_hash"],
                    clock.now(),
                    str(root),
                    encode(execution_manifest),
                ),
            )
            job_obligations.sync_job(db, intent_id)
            self.research.service.admission(db, current, stage, resources, intent_id)
            for ref in refs:
                db.execute(
                    "INSERT INTO output_bindings VALUES(?,?,?)",
                    (intent_id, ref["id"], "selected dependency producer"),
                )
            db.execute(
                "UPDATE stages SET state='running',updated_at=? WHERE experiment_id=? AND id=?",
                (clock.now(), eid, stage),
            )
            db.execute(
                "UPDATE experiments SET phase=?,hold_reason=NULL,error=NULL WHERE id=?",
                ("evaluating" if index == len(compute_stages) - 1 else "training", eid),
            )
            self.store.emit(db, eid, "submission_intent", {"intent_id": intent_id, "stage": stage})
        # This caller owns the intent from the commit above until the accepted identity
        # (or the uncertain outcome) is saved: observation and the resolver decide
        # nothing about an owner whose submission is still in flight.
        self.submissions_in_flight.add(intent_id)
        try:
            job_id = await scheduler.submit(intent_id, script, root, resources=resources)
            with self.store.tx() as db:
                before = db.execute("SELECT * FROM jobs WHERE intent_id=?", (intent_id,)).fetchone()
                if before is None:
                    raise RuntimeError("saved submission intent disappeared before scheduler ID save")
                before = dict(before)
                if before["job_id"] == job_id:
                    return {"intent_id": intent_id, "job_id": job_id, "state": before["state"]}
                if before["state"] != "submitting" or before["job_id"] is not None:
                    raise RuntimeError(
                        f"saved submission changed before scheduler ID {job_id} was saved; no overwrite was made"
                    )
                db.execute(
                    "UPDATE jobs SET job_id=?,state='submitted' WHERE intent_id=?", (job_id, intent_id)
                )
                job_obligations.sync_job(db, intent_id, before=before)
                self.store.emit(db, eid, "job_submitted", {"intent_id": intent_id, "job_id": job_id})
        except SubmissionUncertain as exc:
            self.store.update(eid, hold_reason="submission_uncertain", error=str(exc))
            return {"state": "uncertain", "reason": "submission_uncertain"}
        finally:
            self.submissions_in_flight.discard(intent_id)
        return {"intent_id": intent_id, "job_id": job_id, "state": "submitted"}

    async def reconcile_outputs(self, eid):
        """Adopt a successful saved job after repaired validation; leave new compute paused."""
        exp = self.store.experiment(eid)
        previous = self.store.one(
            "SELECT payload FROM events WHERE experiment_id=? AND kind='output_validation_reconciled' ORDER BY seq DESC LIMIT 1",
            (eid,),
        )
        if previous and exp["phase"] != "failed":
            saved = json.loads(previous["payload"])
            return {
                "experiment_id": eid,
                "job_id": saved["job_id"],
                "stage": saved["stage"],
                "pause_compute": True,
            }
        if legacy_archive.execution_frozen(exp):
            raise ValueError("archived legacy output reconciliation is unavailable")
        if not iteration.enabled(exp):
            raise ValueError("stage output reconciliation requires execution contract 3")
        if exp["phase"] != "failed" or exp["hold_reason"] != "validation_failed" or exp.get("deleted_at"):
            raise ValueError("output reconciliation requires a retained experiment failed on validation")
        jobs = self.store.rows("SELECT * FROM jobs WHERE experiment_id=? AND state='failed'", (eid,))
        if len(jobs) != 1 or not jobs[0]["job_id"] or jobs[0]["error"] != exp["error"]:
            raise ValueError("output reconciliation requires one exact saved validation-failed job")
        if (
            self.store.one(
                "SELECT 1 FROM jobs WHERE experiment_id=? AND state IN ('submitting','submitted')", (eid,)
            )
            or self.store.one(
                "SELECT 1 FROM agent_actions WHERE experiment_id=? AND state IN ('dispatching','running','uncertain')",
                (eid,),
            )
            or self.store.one(
                "SELECT 1 FROM steering WHERE experiment_id=? AND state IN ('pending','cancelling')", (eid,)
            )
        ):
            raise ValueError("resolve outstanding execution or steering before reconciling outputs")
        job = jobs[0]
        stages = {
            s["id"]: s["state"]
            for s in self.store.rows("SELECT id,state FROM stages WHERE experiment_id=?", (eid,))
        }
        compute_stages = self.compute_stages(exp)
        if (
            stages.get("prepare") != "complete"
            or job["stage_id"] not in compute_stages
            or stages.get(job["stage_id"]) != "failed"
        ):
            raise ValueError("saved job must match the failed stage of a frozen implementation")
        graph = stage_dependencies(
            Implementation.model_validate(exp["snapshot"]["plan"]).stages,
            exp["snapshot"]["plan"]["schema_version"],
        )
        required = set()

        def visit(stage):
            for dependency in graph[stage]:
                if dependency not in required:
                    required.add(dependency)
                    visit(dependency)

        visit(job["stage_id"])
        preceding = [stage for stage in compute_stages if stage in required]
        if any(stages[stage] != "complete" for stage in preceding):
            raise ValueError("preceding compute stages must be complete before output recovery")
        if (
            await self.scheduler_for(self.store.experiment(job["experiment_id"]), job).reconcile(
                job["intent_id"]
            )
            != job["job_id"]
        ):
            raise ValueError("scheduler identity differs from the saved submission intent")
        observed = await self.scheduler_for(self.store.experiment(job["experiment_id"]), job).observe(
            job["job_id"]
        )
        if observed.state != "COMPLETED" or observed.exit_code != "0:0":
            raise ValueError("saved job does not have confirmed successful terminal accounting")
        # A contract-3 attempt writes into its own saved folder under its own saved
        # manifest; the observer reads that same pair instead of the experiment root.
        job_root = Path(job["execution_root"]) if job.get("execution_root") else self.root(eid)
        job_manifest = (
            json.loads(job["execution_manifest"])
            if job.get("execution_manifest")
            else self.store.experiment(eid)["snapshot"]
        )
        names = list(
            await asyncio.to_thread(artifacts.validate_stage, job_root, job["stage_id"], job_manifest)
        )
        now = clock.now()
        with self.store.tx() as db:
            iteration.complete(self, exp, job, job_manifest, job_root, db)
            before = job_obligations.locked_job(db, job)
            if before is None:
                raise ValueError("saved job identity changed during output reconciliation")
            db.execute(
                "UPDATE jobs SET state='complete',scheduler_state='COMPLETED',exit_code='0:0',error=NULL,"
                "observed_at=?,started_at=COALESCE(?,started_at),ended_at=COALESCE(?,ended_at),"
                "estimated_start_at=NULL,pending_reason=NULL WHERE intent_id=?",
                (now, observed.started_at, observed.ended_at, job["intent_id"]),
            )
            job_obligations.sync_job(db, job["intent_id"], before=before)
            db.execute(
                "UPDATE stages SET state='complete',error=NULL,output_manifest=?,updated_at=? WHERE experiment_id=? AND id=?",
                (encode(names), now, eid, job["stage_id"]),
            )
            db.execute(
                "UPDATE experiments SET phase='training',hold_reason='new_compute_paused',error=NULL,"
                "pause_compute=1,observed_at=?,updated_at=? WHERE id=?",
                (now, now, eid),
            )
            iteration.queue_working_report(self, exp, job, db)
            self.store.emit(
                db,
                eid,
                "output_validation_reconciled",
                {"intent_id": job["intent_id"], "job_id": job["job_id"], "stage": job["stage_id"]},
            )
        return {
            "experiment_id": eid,
            "job_id": job["job_id"],
            "stage": job["stage_id"],
            "pause_compute": True,
        }

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
            observed = None
            try:
                exp = self.store.experiment(eid)
                if exp["contract"].get("execution_contract") == 4:
                    # Remote output transfer has its own observation protocol.
                    if (
                        exp["deleted_at"] is not None
                        or exp["phase"] in ("complete", "failed", "cancelled")
                        or exp["contract"].get("execution", {}).get("context", "acacia") != "acacia"
                    ):
                        continue
                    if not job["job_id"]:
                        job = await resolver.resolve(self, exp, job)
                    if job:
                        from .research import job_observe

                        await job_observe.observe(self, exp, job)
                    continue
                if (
                    legacy_archive.execution_frozen(exp)
                    and exp["deleted_at"] is None
                    and exp["phase"] != "cancelled"
                ):
                    await legacy_archive.settle_job(self, exp, job)
                    continue
                check = self.output_checks.get(job["intent_id"])
                if check and not check[0].done():
                    continue
                scheduler = self.scheduler_for(exp, job)
                if isinstance(scheduler, RemoteSlurmScheduler):
                    if not scheduler.due(job, clock.now()):
                        continue
                    with self.store.tx() as db:
                        db.execute(
                            "UPDATE jobs SET remote_polled_at=? WHERE intent_id=?",
                            (clock.now(), job["intent_id"]),
                        )
                if not job["job_id"]:
                    # C5 owns every intent without a scheduler identity: adoption,
                    # quarantine or a proven absence is saved here, at the scheduler
                    # cadence, before advancement may submit for this experiment. An
                    # intent whose sbatch is still in flight has an owner that saves
                    # its own outcome, so this list entry is not observed at all.
                    if job["intent_id"] in self.submissions_in_flight:
                        continue
                    job = await resolver.resolve(self, exp, job)
                    if not job:
                        continue
                observed = check[1] if check else await scheduler.observe(job["job_id"])
                if self.store.experiment(eid)["phase"] == "cancelled":
                    continue
                self.research.service.budget.observe(
                    job, observed, exp["contract"].get("execution", {}).get("context", "acacia")
                )
                if isinstance(scheduler, RemoteSlurmScheduler):
                    try:
                        await scheduler.fetch(job, observed)
                    except TransferPending as exc:
                        with self.store.tx() as db:
                            before = job_obligations.locked_job(db, job, "state")
                            if before is None:
                                continue
                            db.execute(
                                "UPDATE jobs SET scheduler_state=?,exit_code=?,observed_at=? WHERE intent_id=?",
                                (observed.state, observed.exit_code, clock.now(), job["intent_id"]),
                            )
                            job_obligations.sync_backend(db, job["intent_id"], before=before)
                        self.store.update(
                            eid,
                            hold_reason="results_transfer_pending"
                            if observed.state == "COMPLETED"
                            else "remote_transfer_pending",
                            error=str(exc),
                        )
                        continue
                    if exp["hold_reason"] in (
                        "results_transfer_pending",
                        "remote_transfer_pending",
                        "executor_unavailable",
                    ):
                        self.store.update(eid, hold_reason=None, error=None)
                job_root = Path(job["execution_root"]) if job.get("execution_root") else self.root(eid)
                job_manifest = (
                    json.loads(job["execution_manifest"])
                    if job.get("execution_manifest")
                    else self.store.experiment(eid)["snapshot"]
                )
                now = clock.now()
                previous_progress = json.loads(job["progress"]) if job["progress"] else None
                progress = await asyncio.to_thread(
                    read_job_progress,
                    job_root,
                    {**job, "started_at": observed.started_at or job["started_at"]},
                    now=now,
                )
                if previous_progress and (
                    progress is None or progress["reported_at"] <= previous_progress["reported_at"]
                ):
                    progress = previous_progress
                # Observation freshness is saved below, but a heartbeat timestamp
                # alone is not new scientific progress worth another full view.
                progress_changed = (
                    {k: v for k, v in progress.items() if k != "reported_at"} if progress else None
                ) != (
                    {k: v for k, v in previous_progress.items() if k != "reported_at"}
                    if previous_progress
                    else None
                )
                previous_tensorboard = json.loads(job["tensorboard"]) if job.get("tensorboard") else None
                tensorboard = (
                    await asyncio.to_thread(read_previews, job_root, job, job_manifest)
                    if tensorboard_selection(job_manifest, job["stage_id"])
                    else None
                )
                if tensorboard is None:
                    tensorboard = previous_tensorboard
                elif previous_tensorboard:
                    # Keep the last saved preview across transient missing files,
                    # remote transfer failures and an executor cache restart.
                    if not tensorboard["image"]:
                        tensorboard["image"] = previous_tensorboard["image"]
                    if tensorboard["scalar"] and not tensorboard["scalar"]["points"]:
                        tensorboard["scalar"] = previous_tensorboard["scalar"]
                    tensorboard["updated_at"] = max(
                        tensorboard["updated_at"], previous_tensorboard["updated_at"]
                    )
                    if tensorboard["image"]:
                        tensorboard["image_pending"] = False
                tensorboard_changed = tensorboard != previous_tensorboard
                state = "submitted"
                error = None
                completed_names = None
                if observed.state == "COMPLETED" and observed.exit_code is not None:
                    if observed.exit_code == "0:0":
                        # Scheduler success is necessary, not sufficient: validate before recording success.
                        exp = self.store.experiment(eid)
                        register_outputs = iteration.enabled(exp)
                        names = await asyncio.to_thread(
                            artifacts.validate_stage,
                            job_root,
                            job["stage_id"],
                            job_manifest,
                        )
                        if register_outputs:
                            completed_names = names
                        state = "complete"
                    else:
                        state, error = "failed", f"job exit {observed.exit_code}"
                elif observed.state in TERMINAL_BAD:
                    state, error = "failed", f"{observed.state}: {observed.exit_code}"
                changed = (
                    (job["scheduler_state"], job["exit_code"], job["state"])
                    != (
                        observed.state,
                        observed.exit_code,
                        state,
                    )
                    or any(
                        job[field] != getattr(observed, field)
                        for field in ("estimated_start_at", "pending_reason")
                    )
                    or any(
                        getattr(observed, field) is not None and job[field] != getattr(observed, field)
                        for field in ("started_at", "ended_at")
                    )
                )
                if self.store.experiment(eid)["phase"] == "cancelled":
                    continue
                with self.store.tx() as db:
                    before = job_obligations.locked_job(db, job)
                    parent = db.execute("SELECT phase FROM experiments WHERE id=?", (eid,)).fetchone()
                    if before is None or parent is None or parent["phase"] == "cancelled":
                        continue
                    registration_started = time.monotonic()
                    selected = early_repair.selected(self, exp, job)
                    if completed_names is not None and not selected:
                        completed_names = None
                    if completed_names is not None and iteration.enabled(exp):
                        iteration.complete(self, exp, job, job_manifest, job_root, db)
                    db.execute(
                        "UPDATE jobs SET state=?,scheduler_state=?,exit_code=?,observed_at=?,error=?,"
                        "started_at=COALESCE(?,started_at),ended_at=COALESCE(?,ended_at),estimated_start_at=?,pending_reason=?,progress=?,tensorboard=? WHERE intent_id=?",
                        (
                            state,
                            observed.state,
                            observed.exit_code,
                            now,
                            error,
                            observed.started_at,
                            observed.ended_at,
                            observed.estimated_start_at,
                            observed.pending_reason,
                            encode(progress) if progress else None,
                            encode(tensorboard) if tensorboard else None,
                            job["intent_id"],
                        ),
                    )
                    job_obligations.sync_backend(db, job["intent_id"], before=before)
                    db.execute("UPDATE experiments SET observed_at=? WHERE id=?", (now, eid))
                    if completed_names is not None:
                        db.execute(
                            "UPDATE stages SET state='complete',error=NULL,output_manifest=?,updated_at=? "
                            "WHERE experiment_id=? AND id=?",
                            (encode(completed_names), now, eid, job["stage_id"]),
                        )
                        if iteration.enabled(exp):
                            iteration.queue_working_report(self, exp, job, db)
                        self.store.emit(
                            db,
                            eid,
                            "stage_changed",
                            {
                                "stage": job["stage_id"],
                                "state": "complete",
                                **(
                                    {
                                        "bookkeeping_ms": job_manifest.get("bookkeeping_ms"),
                                        "registration_ms": (time.monotonic() - registration_started) * 1000,
                                        "source_bytes": job_manifest.get("source_bytes"),
                                        "provenance_bytes_read": 0,
                                    }
                                    if iteration.enabled(exp)
                                    else {}
                                ),
                            },
                        )
                    if changed:
                        self.store.emit(
                            db, eid, "job_observed", {"job_id": job["job_id"], "state": observed.state}
                        )
                    elif progress_changed or tensorboard_changed:
                        self.store.emit(db, eid, "job_progress", {"job_id": job["job_id"]})
                self.output_checks.pop(job["intent_id"], None)
                if observed.state == "ACCOUNTING_PENDING" or (
                    observed.state == "COMPLETED" and observed.exit_code is None
                ):
                    self.store.update(eid, hold_reason="accounting_pending")
                elif self.store.experiment(eid)["hold_reason"] in (
                    "accounting_pending",
                    "executor_unavailable",
                    "submission_uncertain",
                ):
                    self.store.update(eid, hold_reason=None, error=None)
                if state == "failed" and early_repair.selected(self, self.store.experiment(eid), job):
                    restarting = self.store.one(
                        "SELECT 1 FROM steering WHERE experiment_id=? AND mode='restart' AND state IN ('pending','cancelling')",
                        (eid,),
                    )
                    self.store.stage(
                        eid, job["stage_id"], "cancelled" if restarting else "failed", error=error
                    )
                    optional = iteration.enabled(exp) and not next(
                        s for s in job_manifest["plan"]["stages"] if s["id"] == job["stage_id"]
                    ).get("requested", True)
                    if not restarting and not optional:
                        self.fail(eid, "compute_failed", error)
            except ValueError as exc:
                self.output_checks.pop(job["intent_id"], None)
                selected = early_repair.selected(self, self.store.experiment(eid), job)
                if selected:
                    self.store.stage(eid, job["stage_id"], "failed", error=str(exc))
                try:
                    with self.store.tx() as db:
                        before = job_obligations.locked_job(db, job)
                        if before is None:
                            continue
                        db.execute(
                            "UPDATE jobs SET state='failed',error=? WHERE intent_id=?",
                            (str(exc), job["intent_id"]),
                        )
                        if observed is not None:
                            db.execute(
                                "UPDATE jobs SET scheduler_state=?,exit_code=?,observed_at=? WHERE intent_id=?",
                                (observed.state, observed.exit_code, clock.now(), job["intent_id"]),
                            )
                        job_obligations.sync_backend(db, job["intent_id"], before=before)
                        self.store.emit(db, eid, "output_validation_failed", {"error": str(exc)})
                except RuntimeError as mapping_error:
                    self.store.update(eid, hold_reason="executor_unavailable", error=str(mapping_error))
                    continue
                optional = iteration.enabled(exp) and not next(
                    s for s in job_manifest["plan"]["stages"] if s["id"] == job["stage_id"]
                ).get("requested", True)
                if not optional and selected:
                    self.fail(eid, "validation_failed", str(exc))
            except Exception as exc:
                self.output_checks.pop(job["intent_id"], None)
                self.store.update(eid, hold_reason="executor_unavailable", error=str(exc))

    async def agent_stage(self, exp, phase):
        eid = exp["id"]
        # A dispatched task may have reached its quota/turn ceiling while advance
        # awaited filesystem work. Never overwrite that terminal admission hold.
        exp = self.store.experiment(eid)
        # A saved contract 4 has no plan/prepare/interpret stage to open a turn for.
        if not iteration.enabled(exp) or exp["phase"] in ("complete", "failed", "cancelled"):
            return
        self.store.update(
            eid,
            phase="training"
            if phase.startswith("agent:")
            else "preparing"
            if phase in ("plan", "prepare")
            else "interpreting",
        )
        action_revision = (
            (exp["plan_review"] or {}).get(
                "preparation_revision", (exp["plan_review"] or {}).get("revision", 1)
            )
            if phase in ("plan", "prepare") and exp["contract"].get("planning_version")
            else exp["contract"].get("interpretation_revision", exp["evidence_revision"])
            if phase == "interpret"
            else exp["evidence_revision"]
        )
        action = self.store.one(
            "SELECT * FROM agent_actions WHERE experiment_id=? AND phase=? AND evidence_revision=? ORDER BY rowid DESC LIMIT 1",
            (eid, phase, action_revision),
        )
        if not action:
            if (
                phase in ("plan", "prepare")
                and exp["recipe_id"] == "agent_experiment_v1"
                and self.config.mode == "live"
            ):
                if not await self.ensure_cluster(exp):
                    self.store.update(
                        eid,
                        hold_reason="cluster_unavailable",
                        error="Execution cluster discovery unavailable",
                    )
                    return
            if phase == "prepare" and exp["contract"].get("branch_workspace"):
                # Ensure the exact worktree and its shared environment before the prompt
                # names the interpreter this prepare turn should use.
                if not await self.prepare_workspace(exp):
                    return
            prompt = await asyncio.to_thread(self.prompt, exp, phase)
            fresh = self.store.experiment(eid)
            if fresh["phase"] == "cancelled":
                return
            action_id = str(uuid4())
            with self.store.tx() as db:
                db.execute(
                    """INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,created_at,updated_at)
                    VALUES(?,?,?,?,'pending',?,?,?)""",
                    (action_id, eid, phase, action_revision, prompt, clock.now(), clock.now()),
                )
                action_obligations.sync_action(db, action_id)
                self.store.emit(db, eid, "agent_action_queued", {"phase": phase, "action_id": action_id})
            action = self.store.one("SELECT * FROM agent_actions WHERE id=?", (action_id,))
        reason = self.research.service.authority(
            eid, "interpret" if phase == "interpret" else phase, phase.removeprefix("agent:")
        )
        if reason and action["state"] not in ("dispatching", "running", "uncertain"):
            if exp["hold_reason"] != reason:
                self.store.update(eid, hold_reason=reason)
            return
        if (
            action["state"] == "completed"
            and action.get("autopilot_control_revision") is not None
            and action["autopilot_control_revision"] != self.research.service.control_revision(eid)
        ):
            self._action_state(
                action["id"],
                "failed",
                error="Known terminal result predates current project authority",
                expected=action,
            )
            action = self.store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))
            self.store.continue_action(
                action,
                feedback="Project authority or steering changed. Re-read current project guidance and retained files; accepted jobs/results stay unchanged.",
            )
            return
        if action["state"] == "completed":
            try:
                value = json.loads(action["result"])
                if phase.startswith("agent:"):
                    iteration.finish_agent(self, exp, phase, value)
                    return
                if phase == "plan" and exp["recipe_id"] == "agent_experiment_v1":
                    plan = ResearchPlan.model_validate(value)
                    self.validate_resource_plan(exp, plan)
                    review = exp["plan_review"] or {}
                    if review.get("state") != "approved":
                        if review.get("state") != "awaiting_review":
                            self.store.update(
                                eid,
                                title=plan.title,
                                plan_review={
                                    "state": "awaiting_review",
                                    "scope": "plan",
                                    "revision": action_revision,
                                    "summary": plan.summary,
                                    "protocol": plan.protocol,
                                    "schema_version": plan.schema_version,
                                    "questions": plan.questions,
                                    "stages": [stage.model_dump() for stage in plan.stages],
                                },
                                hold_reason="plan_review",
                            )
                            self.store.notify(
                                eid,
                                "required_decision",
                                "Planning needs your answer."
                                if plan.questions
                                else "Review the experiment plan before implementation starts.",
                            )
                        return
                elif phase == "prepare" and exp["recipe_id"] == "agent_experiment_v1":
                    plan = Implementation.model_validate(value)
                    if plan.outcome == "blocked":
                        with self.store.tx() as db:
                            db.execute(
                                "UPDATE stages SET state='pending',error=?,updated_at=? WHERE experiment_id=? AND id='prepare'",
                                (plan.summary, clock.now(), eid),
                            )
                            db.execute(
                                "UPDATE experiments SET hold_reason='implementation_blocked',error=? WHERE id=?",
                                (plan.summary, eid),
                            )
                            self.store.emit(db, eid, "implementation_blocked", {"summary": plan.summary})
                        return
                    self.validate_resource_plan(exp, plan)
                    review = exp["plan_review"]
                    if review and review.get("state") != "approved":
                        self.store.update(eid, hold_reason="plan_review")
                        return
                    if review and review.get("state") == "approved":
                        implementation_hash = hashlib.sha256(encode(plan.model_dump()).encode()).hexdigest()
                        if plan.objective_change and review.get("implementation_hash") != implementation_hash:
                            self.store.update(
                                eid,
                                plan_review={
                                    "state": "awaiting_review",
                                    "scope": "implementation",
                                    "implementation_hash": implementation_hash,
                                    "objective_change": plan.objective_change,
                                    "revision": review["revision"] + 1,
                                    "preparation_revision": action_revision,
                                    "summary": plan.objective_change + "\n" + plan.summary,
                                    "protocol": plan.protocol,
                                    "schema_version": plan.schema_version,
                                    "stages": [
                                        {
                                            "id": stage.id,
                                            "title": stage.title,
                                            "executor": stage.executor,
                                            "needs": stage.needs,
                                            "resources": stage.resources.model_dump()
                                            if stage.resources
                                            else None,
                                        }
                                        for stage in plan.stages
                                    ],
                                },
                                hold_reason="plan_review",
                            )
                            self.store.notify(
                                eid,
                                "required_decision",
                                "Implementation needs a change to the approved plan. Review it before compute starts.",
                            )
                            return
                    try:
                        runtime = await asyncio.to_thread(self.workspaces.shared_runtime, eid)
                    except ProjectUnavailable as exc:
                        self.store.update(eid, hold_reason="workspace_unavailable", error=str(exc))
                        return
                    python = self.prepared_python(exp, plan, runtime)
                    for name in plan.files:
                        if name.endswith((".py", ".sh", ".sbatch")):
                            try:
                                path = str(self.working_directory(exp) / name)
                                if name.endswith((".sh", ".sbatch")):
                                    await command("/bin/bash", "-n", path)
                                else:
                                    await command(
                                        str(python),
                                        "-c",
                                        "import pathlib,sys; p=pathlib.Path(sys.argv[1]); compile(p.read_text(),str(p),'exec')",
                                        path,
                                    )
                            except RuntimeError as exc:
                                raise ValueError(f"prepared source does not compile: {name}: {exc}") from exc
                    fresh = self.store.experiment(eid)
                    if fresh["phase"] == "cancelled":
                        return
                    if early_repair.preparing(exp):
                        early_repair.validate(self, exp, plan)
                    reused = {}
                    if exp["contract"].get("auto_repair"):
                        reused = iteration.reuse(self, exp, plan)
                    elif plan.reuse_stages:
                        raise ValueError("stage reuse requires saved successful work in this experiment")
                    budget = exp["contract"].get("max_jobs", self.config.slurm.max_jobs_per_experiment)
                    # Contract-3 preparation counts scheduled work; an interactive agent stage is not a job.
                    submitted = self.store.one(
                        "SELECT COUNT(*) AS n FROM jobs WHERE experiment_id=?"
                        " AND json_extract(execution_manifest, '$.executor') IS NOT 'agent'",
                        (eid,),
                    )["n"]
                    remaining = sum(s.executor == "slurm" and s.id not in reused for s in plan.stages)
                    if early_repair.preparing(exp):
                        kept = {d.stage_id for d in plan.repair_decisions if d.decision == "keep"}
                        retained = {
                            row["stage_id"]
                            for row in self.store.rows(
                                "SELECT DISTINCT stage_id FROM jobs WHERE experiment_id=?", (eid,)
                            )
                            if row["stage_id"] in kept
                        }
                        remaining = sum(s.executor == "slurm" and s.id not in retained for s in plan.stages)
                    if budget is not None and submitted + remaining > budget:
                        raise ValueError(
                            "remaining stages exceed the total saved job budget; reuse valid completed work or request owner guidance"
                        )
                    manifest = artifacts.freeze_implementation(
                        self.root(eid),
                        plan,
                        python=python,
                        project=self.project_context(exp)["checkout"],
                        workspace=self.working_directory(exp),
                        reused=reused,
                        remote_execution=self.is_remote(exp),
                    )
                    if not await self.commit_accepted_implementation(exp):
                        return
                    if early_repair.preparing(exp):
                        early_repair.apply(self, exp, plan, manifest)
                    else:
                        self.store.resolve_implementation(eid, manifest)
                    self.adopt_reused_stages(eid, reused)
                    if review and review.get("state") == "approved":
                        self.store.update(
                            eid,
                            plan_review={
                                **review,
                                "protocol": plan.protocol,
                                "schema_version": plan.schema_version,
                                "stages": [stage.model_dump() for stage in plan.stages],
                            },
                        )
                    sources = [name for name in manifest["files"] if name.endswith(".py")]
                    lines = sum(
                        len((self.root(eid) / "snapshot" / name).read_text().splitlines()) for name in sources
                    )
                    self.store.activity(
                        action,
                        "frozen-source",
                        "fileChange",
                        f"{len(sources)} source files prepared · +{lines} lines",
                        "complete",
                    )
                    (self.root(eid) / "preparation.json").write_text(encode(value))
                    artifacts.register(self.store, eid, phase, self.root(eid), ["preparation.json"])
                else:
                    for name in value.get("analysis_files", []):
                        self.register_analysis_file(exp, name)
                    evidence_ids = [
                        a["id"]
                        for a in self.store.rows(
                            "SELECT id FROM artifacts WHERE experiment_id=? AND evidence_revision=?",
                            (eid, self.store.experiment(eid)["evidence_revision"]),
                        )
                    ]
                    analysis = artifacts.validate_analysis(value, evidence_ids)
                    if exp["contract"].get("reporting_version") and not analysis.presentation:
                        raise ValueError("a brief-driven experiment needs a composed presentation document")
                    if analysis.presentation:
                        reporting.validate_document(
                            reporting.Evidence(
                                exp,
                                self.root(eid),
                                self.store.rows(
                                    "SELECT * FROM artifacts WHERE experiment_id=? AND evidence_revision=?",
                                    (eid, self.store.experiment(eid)["evidence_revision"]),
                                ),
                            ),
                            analysis.presentation,
                        )
                    root = self.root(eid)
                    name = f"analysis-{action['id']}.json"
                    target = root / name
                    data = encode(analysis.model_dump()).encode()
                    if target.exists():
                        if iteration.bounded_bytes(target, root) != data:
                            raise ValueError("saved analysis action already has different published bytes")
                    else:
                        target.write_bytes(data)
                    artifacts.register(self.store, eid, phase, root, [name])
                    if not (root / "analysis.json").exists():
                        iteration.copy_small(target, root, root / "analysis.json")
                    artifacts.register(self.store, eid, phase, root, ["analysis.json"])
                    with self.store.tx() as db:
                        db.execute(
                            "UPDATE experiments SET analysis=?,scientific_outcome=? WHERE id=?",
                            (encode(analysis.model_dump()), analysis.scientific_outcome, eid),
                        )
                        captured = json.loads(action["input"].rsplit("\nPAYLOAD:\n", 1)[1]).get(
                            "steering_request_ids", []
                        )
                        db.executemany(
                            "UPDATE steering SET state='applied',applied_at=? WHERE experiment_id=? AND request_id=? AND mode='later' AND state='pending'",
                            [(clock.now(), eid, identity) for identity in captured],
                        )
                        self.store.emit(db, eid, "analysis_saved", {})
                self.store.stage(eid, phase, "complete")
                self.store.update(eid, hold_reason=None, error=None)
                return
            except ValueError as exc:
                self._action_state(
                    action["id"],
                    "failed",
                    error=f"agent output validation: {self.validation_feedback(exc)}",
                    expected=action,
                )
                action = self.store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))
        if action["state"] in ("running", "dispatching"):
            reservation = self.agent_pool.lookup("agent_actions", action["id"])
            slot = self.agent_pool.live_slot(reservation)
            if slot is not None:
                if exp["pause_agent"] and action["turn_id"]:
                    self.agent_pool.hold(action, "agent_paused")
                    await self.agent_pool.interrupt(reservation)
                return
            # No live task owns this saved turn: read it back on a configured
            # connection without starting another one, and settle what it proves.
            slot = self.agent_pool.read_slot(reservation)
            if action["thread_id"] and action["turn_id"] and slot is not None:
                try:
                    result = await slot.agent.read_turn(action["thread_id"], action["turn_id"])
                    self._action_state(action["id"], "completed", result=result, expected=action)
                    self.agent_pool.settle_row("agent_actions", action["id"])
                    return
                except TerminalTurnError as exc:
                    self._action_state(action["id"], "failed", error=str(exc), expected=action)
                    self.agent_pool.settle_row("agent_actions", action["id"])
                    return
                except Exception as exc:
                    self._action_state(action["id"], "uncertain", error=str(exc), expected=action)
            else:
                self._action_state(action["id"], "uncertain", error="dispatch response lost", expected=action)
            self.agent_pool.settle_row("agent_actions", action["id"])
            self.store.update(eid, hold_reason="agent_dispatch_uncertain")
            return
        if action["state"] == "uncertain" and self.input_rejected(action):
            self._action_state(action["id"], "failed", error=action["error"], expected=action)
            action = self.store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))
        if action["state"] == "uncertain":
            self.store.update(eid, hold_reason="agent_dispatch_uncertain", error=action["error"])
            return
        if action["state"] == "failed":
            if not action["turn_id"] and str(action["error"]).startswith("session resume rejected:"):
                self.store.update(eid, hold_reason="agent_session_unavailable", error=action["error"])
                return
            if exp["recipe_id"] == "agent_experiment_v1":
                if not action["turn_id"] and action["attempts"] and not self.input_rejected(action):
                    self.store.update(eid, hold_reason="agent_dispatch_uncertain", error=action["error"])
                    return
                if not action["turn_id"] and not action["attempts"]:
                    # No turn was sent. Retry a local/session prerequisite with backoff.
                    if clock.now() - action["updated_at"] < 60:
                        self.store.update(eid, hold_reason="agent_prerequisite", error=action["error"])
                        return
                    self._action_state(action["id"], "pending", error=action["error"], expected=action)
                    action = self.store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))
                else:
                    error = action["error"] or "Known terminal agent failure"
                    chain = action
                    repeats = 0
                    while chain and chain["state"] == "failed" and chain["error"] == error:
                        repeats += 1
                        chain = (
                            self.store.one(
                                "SELECT * FROM agent_actions WHERE id=?", (chain["previous_action_id"],)
                            )
                            if chain.get("previous_action_id")
                            else None
                        )
                    count = self.store.one(
                        "SELECT COUNT(*) AS n FROM agent_actions WHERE experiment_id=? AND attempts>0", (eid,)
                    )["n"]
                    if (
                        repeats >= self.config.codex.identical_error_limit
                        and action.get("interrupt_reason")
                        not in ("quota_hold", "agent_paused", "owner_ceiling")
                        and not str(action.get("interrupt_reason", "")).startswith("autopilot_")
                    ):
                        self.fail(
                            eid,
                            "agent_correction_stalled",
                            f"The same agent error repeated {repeats} times: {error}",
                        )
                        return
                    cap = self.agent_turn_ceiling(exp)
                    if cap is not None and count >= cap:
                        self.fail(
                            eid, "agent_budget_exhausted", "The configured agent turn budget is exhausted"
                        )
                        return
                    action = self.store.continue_action(action)
                    self.store.update(eid, hold_reason="agent_continuation", error=error)
        if exp["pause_agent"]:
            self.store.update(eid, hold_reason="agent_paused")
            return
        if exp["pending_request"]:
            self.store.update(eid, hold_reason="needs_attention")
            return
        if not self.agent_ready:
            self.store.update(eid, hold_reason="agent_unavailable", error=self.agent_error)
            return
        # This experiment's own unrelated turn stays its exclusion; a turn of another
        # owner consumes only a configured turn, which the pool accounts for.
        if self.store.one(
            "SELECT 1 FROM agent_actions WHERE id!=? AND experiment_id=? "
            "AND state IN ('dispatching','running','uncertain')",
            (action["id"], eid),
        ):
            self.store.update(eid, hold_reason="agent_busy")
            return
        if phase == "prepare" and exp["recipe_id"] == "agent_experiment_v1":
            if not await self.prepare_workspace(exp):
                return
        decision = self.agent_pool.quota(eid=eid)
        if not decision.admitted:
            self.store.update(eid, hold_reason="quota_hold", error=decision.reason)
            return
        self.propose_agent(
            lambda slot, action=action: self.dispatch(action, slot),
            role="stage",
            source_table="agent_actions",
            row=action,
            owner=experiment_owner(eid),
            priority=EXPERIMENT_PRIORITY,
            any_lane=True,
        )

    async def commit_branch(self, exp, message):
        """Commit this experiment's exact saved named branch; None for other workspaces."""
        if not self.workspaces.branch(exp["id"]):
            return None
        return await asyncio.to_thread(self.workspaces.commit_branch, exp["id"], message)

    def report_skipped_commit(self, eid, outcome, *, kind, stage=None):
        """Report bounded skipped untracked files from one exact new branch commit."""
        if not outcome or not outcome["skipped"] or outcome["commit"] == outcome["previous"]:
            return
        payload = {
            "kind": kind,
            "commit": outcome["commit"],
            "skipped": outcome["skipped"][:50],
            "skipped_count": len(outcome["skipped"]),
        }
        if stage is not None:
            payload["stage"] = stage
        with self.store.tx() as db:
            self.store.emit(db, eid, "workspace_commit_skipped", payload)

    async def commit_accepted_implementation(self, exp):
        """Commit accepted work to its named branch, after freezing and before saving it."""
        eid = exp["id"]
        message = f"Accept implementation revision {exp['evidence_revision']}"
        try:
            outcome = await self.commit_branch(exp, message)
        except WorkspaceUnavailable as exc:
            self.store.update(eid, hold_reason="workspace_unavailable", error=str(exc))
            return False
        if outcome and outcome["commit"] != outcome["previous"]:
            with self.store.tx() as db:
                self.store.emit(
                    db,
                    eid,
                    "workspace_committed",
                    {"kind": "implementation", "commit": outcome["commit"]},
                )
            self.report_skipped_commit(eid, outcome, kind="implementation")
        return True

    async def prepare_workspace(self, exp):
        if early_repair.preparing(exp):
            return True
        eid = exp["id"]
        if exp["contract"].get("branch_workspace"):
            # Creation saved this decision, so the named branch checkout exists before
            # the first dispatch and a busy project never gates it.
            try:
                await asyncio.to_thread(self.workspaces.ensure_branch, eid)
                # Publish the shared environment before a prepare turn names it. This
                # is a no-op for a branch without dependency manifests or fixed python.
                await asyncio.to_thread(self.workspaces.shared_runtime, eid)
                return True
            except ProjectUnavailable as exc:
                self.store.update(eid, hold_reason="workspace_unavailable", error=str(exc))
                return False
        if self.workspaces.row(eid):
            try:
                await asyncio.to_thread(self.workspaces.ensure, eid)
                return True
            except WorkspaceUnavailable as exc:
                self.store.update(eid, hold_reason="workspace_unavailable", error=str(exc))
                return False
        project_id = exp["contract"].get("project", {}).get("id", "test-project")
        busy = any(
            self.store.experiment(row["id"])["contract"].get("project", {}).get("id", "test-project")
            == project_id
            # An isolated run edits its own checkout, not this project's folder.
            and not self.workspaces.isolated(row["id"])
            for row in self.store.rows(
                "SELECT e.id FROM experiments e WHERE e.id!=? AND e.deleted_at IS NULL "
                "AND ((e.snapshot IS NOT NULL OR EXISTS (SELECT 1 FROM agent_actions a "
                "WHERE a.experiment_id=e.id AND a.phase='prepare' AND a.attempts>0)) "
                "AND e.phase NOT IN ('complete','failed','cancelled') "
                "OR EXISTS (SELECT 1 FROM jobs j WHERE j.experiment_id=e.id "
                "AND j.state IN ('submitting','submitted')))",
                (eid,),
            )
        )
        mode = exp["contract"].get("workspace_mode", "auto")
        if mode == "shared" or (not busy and mode == "auto"):
            if busy:
                self.store.update(eid, hold_reason="project_busy")
                return False
            return True
        # Never relocate an implementation whose dispatch may already have run.
        if self.store.one(
            "SELECT 1 FROM agent_actions WHERE experiment_id=? AND phase='prepare' AND attempts>0",
            (eid,),
        ):
            self.store.update(
                eid,
                hold_reason="project_busy",
                error="An existing implementation cannot be moved to another checkout",
            )
            return False
        try:
            await asyncio.to_thread(self.workspaces.ensure, eid)
            return True
        except WorkspaceUnavailable as exc:
            self.store.update(eid, hold_reason="workspace_unavailable", error=str(exc))
            return False

    def register_analysis_file(self, exp, name):
        import re

        if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._-]{0,149}", name):
            raise ValueError("analysis files require safe flat filenames")
        root = self.root(exp["id"])
        source = artifacts.allowed_path(root / "analysis" / name, root / "analysis")
        target = root / ("analysis-" + name)
        if target.exists():
            if artifacts.sha256(target) != artifacts.sha256(source):
                raise ValueError("published analysis files cannot change; choose a new filename")
        else:
            shutil.copyfile(source, target)
            target.chmod(0o444)
        artifacts.register(self.store, exp["id"], "interpret", root, ["analysis-" + name])
        return self.store.one(
            "SELECT id,name,sha256,size FROM artifacts WHERE experiment_id=? AND name=? AND evidence_revision=?",
            (exp["id"], "analysis-" + name, exp["evidence_revision"]),
        )

    def adopt_reused_stages(self, eid, reused):
        exp = self.store.experiment(eid)
        contract = exp["contract"]
        contract["reused_job_intents"] = [stage["intent_id"] for stage in reused.values()]
        with self.store.tx() as db:
            for stage, saved in reused.items():
                artifacts.register(self.store, eid, stage, self.root(eid), saved["outputs"], _db=db)
                db.execute(
                    "UPDATE stages SET state='complete',output_manifest=?,updated_at=? WHERE experiment_id=? AND id=?",
                    (encode(saved["outputs"]), clock.now(), eid, stage),
                )
            db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))
            if reused:
                self.store.emit(
                    db,
                    eid,
                    "successful_stages_reused",
                    {"stages": list(reused), "job_intents": contract["reused_job_intents"]},
                )

    @staticmethod
    def compute_stages(exp):
        return [stage["id"] for stage in exp["snapshot"]["plan"]["stages"]]

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

    def prompt_python(self, exp):
        """The interpreter a prepare turn should use; previews an unbuilt shared environment."""
        runtime = self.workspaces.shared_runtime(exp["id"], build=False)
        return str(runtime.python if runtime else self.working_directory(exp) / ".venv/bin/python")

    def stage_resources(self, exp, stage):
        resources = self.config.slurm.defaults()
        if exp["recipe_id"] == "agent_experiment_v1":
            saved = next(s for s in exp["snapshot"]["plan"]["stages"] if s["id"] == stage)
            if saved.get("resources"):
                resources = JobResources.model_validate(saved["resources"])
        if self.config.mode == "demo":
            if (
                resources.gpus_per_job
                or resources.nodes != 1
                or resources.tasks_per_node != 1
                or resources.cpus_per_job != 1
                or resources.walltime_minutes_per_job > 5
            ):
                raise ExperimentFailure(
                    "validation_failed",
                    "synthetic demonstrations retain their short single-CPU resource contract",
                )
        self.config.slurm.limits.validate_request(resources)
        cap = self.config.slurm.limits.gpu_hours_per_experiment
        if cap is not None:
            reserved = sum(
                json.loads(j["resources"]).get("gpus_per_job", 0)
                * json.loads(j["resources"]).get("walltime_minutes_per_job", 0)
                / 60
                for j in self.store.rows("SELECT resources FROM jobs WHERE experiment_id=?", (exp["id"],))
            )
            if reserved + resources.gpus_per_job * resources.walltime_minutes_per_job / 60 > cap:
                raise ExperimentFailure("validation_failed", "experiment GPU-hour budget exhausted")
        return resources

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

    @staticmethod
    def input_rejected(action):
        """A specific App Server validation reply proves no turn was created."""
        return (
            not action["turn_id"]
            and "input_too_large" in (action["error"] or "")
            and "max_chars" in (action["error"] or "")
        )

    def compact_plan(self, exp, label, plan):
        return {
            **{
                key: plan[key]
                for key in (
                    "schema_version",
                    "revision",
                    "state",
                    "title",
                    "summary",
                    "protocol",
                    "questions",
                    "stages",
                )
                if key in plan
            },
            "details": self.context_pointer(exp, label, plan),
        }

    def compact_contract(self, exp, contract):
        # Execution history is audit/recovery data, not a scientific instruction.
        summary = {
            key: contract[key]
            for key in (
                "schema_version",
                "recipe_id",
                "synthetic_demo",
                "brief_driven",
                "agent",
                "parameters",
                "project",
                "resource_policy_version",
            )
            if key in contract
        }
        if contract.get("restart_context"):
            restart = dict(contract["restart_context"])
            if isinstance(restart.get("previous_plan"), dict):
                restart["previous_plan"] = self.compact_plan(exp, "restart-plan", restart["previous_plan"])
            summary["restart_context"] = restart
        summary["details"] = self.context_pointer(exp, "contract", contract)
        return summary

    def worktree_conventions(self, exp):
        """The saved mode is available even before the prepare worktree exists."""
        return agent_conventions.worktree_conventions(bool(exp["contract"].get("branch_workspace")))

    def iteration_prompt(self, exp, phase, payload):
        root = self.root(exp["id"])
        payload.update(
            contract=self.compact_contract(exp, exp["contract"]),
            project_context=self.project_context(exp),
            project_instructions=self.projects.instructions(
                exp["contract"].get("project", {}).get("id", "test-project")
            ),
            workspace=str(self.working_directory(exp)),
            python=self.prompt_python(exp),
            execution_context=self.execution_context(exp),
            cluster=self.cluster_for(exp),
            resources=self.config.slurm.model_dump(),
            approved_plan=exp.get("plan_review"),
            plan_revision_feedback=(exp.get("plan_review") or {}).get("feedback"),
            output_refs=self.store.rows("SELECT * FROM output_refs WHERE experiment_id=?", (exp["id"],)),
            jobs=self.controller.status(exp["id"], summary_only=True),
            project_knowledge=self.supervisor.context(
                exp["contract"].get("project", {}).get("id", "test-project")
            ),
        )
        if exp.get("reference_experiment_id"):
            parent = self.store.experiment(exp["reference_experiment_id"])
            payload["reference_context"] = self.context_pointer(
                exp,
                "reference",
                {
                    "brief": parent["brief"],
                    "analysis": parent["analysis"],
                    "plan": (parent["snapshot"] or {}).get("plan"),
                    "working_directory": str(self.working_directory(parent)),
                    "outputs": self.store.rows(
                        "SELECT * FROM output_refs WHERE experiment_id=?", (parent["id"],)
                    ),
                    "artifacts": self.store.rows(
                        "SELECT id,name,path FROM artifacts WHERE experiment_id=?", (parent["id"],)
                    ),
                },
            )
        instruction = (
            "Implement the actual research brief without a model-family whitelist. Preserve requested statistics, "
            "repetitions, controls and split separation; label measured, literature and synthetic values accurately. "
            "Use managed included usage only, never API billing or credentials. The backend alone submits Slurm work. "
            "Respect project isolation: edit only project_context.checkout and workspace. Do not modify another "
            "experiment, shared environment or source_checkout belonging to another editor. Reuse this experiment's "
            "stable environment. Choose each stage's CPU/GPU/resources from fresh cluster authorization and optional "
            "owner limits. Busy nodes may queue; drained/down nodes are not usable. Follow the machine context. "
            "Use check_manifest before finishing, job_status with selected intent_ids for details and exact logs, "
            "submit_stage for prepared work, "
            "register_result for outputs, update_report for incremental working reports and schedule_wakeup for continuation. Return the requested JSON schema. "
        )
        if phase == "plan":
            instruction += (
                "This turn is read-only planning: no edits, installs, jobs or training. Return schema_version 3, "
                "title, concise summary, protocol and stages with explicit needs ([] for roots). "
                "Choose executor:'agent' for small preparation, model shape/gradient/forward checks, quick inference "
                "and debugging that fit a bounded interactive tool loop. Agent stages have resources:null. "
                "Choose executor:'slurm' with explicit resources for training, heavy extraction and sustained compute. "
                "A small check should fix and retest inside one agent turn before dependent training queues. "
                "Plan evaluation, plotting and extra analysis as separate resumable tasks. There is no universal "
                "final verification stage or all-stage dependency requirement. Dependencies express needed inputs. "
                "Initial plan approval is required. For essential missing inputs return questions and stages: []. "
                "Apply plan revision feedback. Do not add seeds, bootstrap, independent recomputation, raw prediction "
                "retention or exhaustive checks unless scientifically requested or useful for the approved task."
            )
        elif phase == "prepare":
            instruction += (
                "Return outcome='blocked' with the concrete obstacle in summary if implementation is incomplete; "
                "do not present an incomplete graph as completed. Return outcome='completed' only for a ready implementation. "
            )
            payload["repair_context"] = (
                self.context_pointer(exp, "repair-context", exp["contract"].get("auto_repair"))
                if exp["contract"].get("auto_repair")
                else None
            )
            payload["researcher_notes"] = self.store.rows(
                "SELECT author,text FROM notes WHERE experiment_id=? ORDER BY created_at DESC LIMIT 20",
                (exp["id"],),
            )
            if early_repair.preparing(exp):
                payload["early_repair"] = exp["contract"]["early_repair"]
                payload["output_refs"] = self.context_pointer(
                    exp, "repair-output-refs", payload["output_refs"]
                )
                payload["output_refs_instruction"] = (
                    "Read only needed output references from this pointer or job_status for selected intents; "
                    "do not load all output inventories into the conversation."
                )
                payload["repair_policy"] = (
                    "Diagnose now; unrelated jobs continue. Read job_status by intent only when needed. "
                    "Edit only candidate workspace/project paths, never running attempts or original sources. "
                    "Return repair_decisions for every existing stage: keep or retry with rationale. "
                    "Keep active/successful unrelated stages unchanged; reuse_stages lists kept successful stages. "
                    "For affected queued/running Slurm jobs call cancel_job with exact intent, request UUID and reason. "
                    "Retry admission waits for exact predecessor terminal accounting; agent turns cannot be cancelled by this tool. "
                    "Use the previous saved python_executable read-only if suitable; install or change packages only in a candidate environment. New files must be written in candidate paths."
                )
            payload["output_schema"] = ExperimentResult.model_json_schema()
            instruction += (
                "Preserve the plan's executor choices. Agent stages declare executor:'agent', resources:null, "
                "a concrete instruction and declared outputs; no script. Use them for small tests and in-turn debugging. "
                "They run locally beside the agent even when Slurm training uses Jean Zay; foreign-host inputs need an explicit transfer. "
                "Slurm stages declare executor:'slurm' and resources. "
                "For a readiness-only gate, use needs to wait and input_output_ids:[] when no producer files are consumed, "
                "especially when a local check gates remote training. This does not transfer data across hosts. "
                "Write schema_version 3 Implementation in workspace. files lists executable small sources with normal "
                "package paths; project_files lists explicitly needed project source/config files (16 MiB total). "
                "The tracker copies declared sources into each attempt before dispatch. Jobs never import editable "
                "working sources; include package __init__.py and helpers. No full project sync, Git commit, dependency "
                "freeze or data copying is performed. scripts run with EXPERIMENT_SOURCE and EXPERIMENT_PROJECT "
                "pointing to private source copies. Write only to EXPERIMENT_ROOT, the attempt output directory. "
                "Read EXPERIMENT_INPUTS JSON for direct input paths and IDs. Never assume producer files are in "
                "your output directory. input_output_ids can explicitly select successful output_refs instead of dependency defaults. Cross-host inputs require an explicit authorized transfer. Declare large "
                "files/directories in local_outputs; they are path references, not sealed evidence. Small displayed "
                "files belong in outputs (safe flat filenames, <=10 MiB each). An evaluation stage declares result_file "
                "in outputs, expected_metrics and sample_count when known. Result JSON follows output_schema with "
                "the exact protocol. Use unique metric/variant pairs across evaluation tasks. Compute metrics once and keep cheap shapes/units/lambda/split checks. No "
                "validation.json, scientific-data hashes, membership inventories or independent metric recomputation "
                "are required. Name results from different evaluation stages distinctly. Declare requested figures in "
                "figures. Separate requested plotting from evaluation; requested:false marks genuinely optional work. "
                "Emit ET_PROGRESS JSON (stage, completed, total, timestamp, eta_seconds) periodically for measurable "
                "work. For stages that train or fit models, encoders, decoders or probe heads, you MUST log a useful "
                "loss time series with TensorBoard and declare the stage's tensorboard selection; logging events "
                "without declaring their exact tags leaves previews invisible. When outputs have a meaningful visual "
                "representation (e.g. segmentation, depth or reconstruction), also log a representative prediction "
                "or small legible grid periodically and select its image tag. Use tensorboard: "
                "{log_dir: 'tensorboard/STAGE', scalar_tag: 'loss/train', image_tag: 'prediction'} with the exact "
                "tags actually emitted; image_tag may be null when no useful visual exists. Write event files "
                "directly under EXPERIMENT_ROOT/log_dir, use a unique directory per stage, flush periodically and "
                "close the writer. Inspect emitted tags and match the manifest, including any writer suffix. "
                "Final plots do not replace these live previews. Do not add TensorBoard to stages where it is "
                "irrelevant, such as data preparation, one-off smoke checks, metric aggregation or plotting; "
                "leave tensorboard null there. Previews are best-effort telemetry: missing previews alone must "
                "not trigger scientific compute reruns or block metric publication. "
                "Routine debugging/resource/graph corrections are autonomous. A material objective change uses "
                "objective_change for review. On repair explicitly list reuse_stages and a short reuse_rationale "
                "using available original producer IDs. [] means rerun; source edits do not invalidate results. "
                "Fix and submit only necessary tasks; do not silently reuse computation newly requested by the owner. "
                "Missing outputs are actionable: select replacements or rerun their producer, never the whole graph "
                "by default. Known-terminal retries have new identities; uncertain jobs/turns cannot be retried. "
                "Keep checkpoints and working files for later editing; no broad completion cleanup."
            )
        else:
            payload["results"] = artifacts.read_json(root / "results.json", root)
            payload["artifacts"] = self.store.rows(
                "SELECT id,name,path,sha256,size FROM artifacts WHERE experiment_id=? AND evidence_revision=?",
                (exp["id"], exp["evidence_revision"]),
            )
            payload["evidence_ids"] = [row["id"] for row in payload["artifacts"]]
            payload["analysis_directory"] = str(root / "analysis")
            payload["evidence_revision"] = exp["evidence_revision"]
            instruction += (
                "Interpret the registered measurements and available figures as a working report. Structural "
                "checks are not certification of scientific correctness or reproducibility. Cite evidence IDs. "
                "Use the writable analysis_directory for small figures, register_analysis_file for their IDs, and "
                "return analysis_files. Never repeat evaluation/training to repair a figure. Requested unfinished "
                "deliverables remain visible. Report changes create versions; published bytes stay stable.\n"
                + (
                    "This run has a named branch. Set integrate:true with integrate_reason when its code "
                    "belongs in the project's main branch; that only requests integration once this run "
                    "completes. It is a request, not a merge: the project check, approval setting and the "
                    "owner's Don't merge flag still settle whether it lands. The default integrate:false "
                    "requests nothing.\n"
                    if self.worktree_conventions(exp)
                    else ""
                )
                + reporting.skill_text()
            )
        conventions = self.worktree_conventions(exp)
        instruction += (conventions + " ") if conventions else ""
        return instruction + "\n" + self.execution_instruction(exp, phase) + "\nPAYLOAD:\n" + encode(payload)

    def prompt(self, exp, phase):
        prompt = self._prompt(exp, phase)
        member = self.research.service.member(exp["id"])
        if member:
            run = self.research.service.run(run_id=member["run_id"])
            policy = self.research.service.policy(run)
            prefix, wire = prompt.rsplit("\nPAYLOAD:\n", 1)
            payload = json.loads(wire)
            protocol = self.research.service.document(run["id"], "protocol")
            payload["autopilot_guidance"] = {
                "scope": policy.scope,
                "instructions": policy.instructions,
                "exclusions": policy.exclusions,
                "purpose": member["purpose"],
                "scope_rationale": member["rationale"],
                "handoff_at": policy.handoff_at,
                "protocol": self.context_pointer(exp, "autopilot-protocol", protocol),
                "guidance": self.store.rows(
                    "SELECT document FROM autopilot_inputs WHERE run_id=? AND kind IN ('message','answer') ORDER BY seq DESC LIMIT 20",
                    (run["id"],),
                ),
            }
            prompt = prefix + "\nPAYLOAD:\n" + encode(payload)
        return prompt

    def _prompt(self, exp, phase):
        if legacy_archive.execution_frozen(exp):
            raise ValueError("archived legacy execution prompts are read-only")
        if phase.startswith("agent:"):
            return iteration.agent_prompt(self, exp, phase)
        payload = {
            "phase": phase,
            "experiment_id": exp["id"],
            "brief": exp["brief"],
            "contract": exp["contract"],
            "manifest": exp["snapshot"],
        }
        if phase == "interpret":
            guidance = self.store.rows(
                "SELECT request_id,text FROM steering WHERE experiment_id=? AND mode='later' AND state='pending' ORDER BY created_at",
                (exp["id"],),
            )
            payload["steering_guidance"] = [row["text"] for row in guidance]
            payload["steering_request_ids"] = [row["request_id"] for row in guidance]
        return self.iteration_prompt(exp, phase, payload)

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

    async def reconcile_agent_output(self, eid):
        """Revalidate an exact completed result after correcting an output validator."""
        exp = self.store.experiment(eid)
        if self.agent_task or self.store.one(
            "SELECT 1 FROM agent_actions WHERE state IN ('dispatching','running','uncertain')"
        ):
            raise ValueError("stop and reconcile active or uncertain agent work first")
        if exp["pending_request"] or self.store.one(
            "SELECT 1 FROM steering WHERE experiment_id=? AND state IN ('pending','cancelling')", (eid,)
        ):
            raise ValueError("resolve pending input or steering first")
        action = self.store.one(
            "SELECT * FROM agent_actions WHERE experiment_id=? AND phase IN ('plan','prepare') ORDER BY rowid DESC LIMIT 1",
            (eid,),
        )
        if not action or not action["thread_id"] or not action["turn_id"] or not action["result"]:
            raise ValueError("a saved completed planning or preparation result is required")
        if action["state"] == "completed" and exp["phase"] != "failed":
            return {"experiment_id": eid, "action_id": action["id"], "state": "completed"}
        if legacy_archive.execution_frozen(exp):
            raise ValueError("archived legacy agent output reconciliation is unavailable")
        if not iteration.enabled(exp):
            raise ValueError("stage agent output reconciliation requires execution contract 3")
        if (
            exp["phase"] != "failed"
            or exp["hold_reason"] != "agent_recovery_exhausted"
            or action["state"] != "failed"
            or not str(action["error"]).startswith("agent output validation:")
            or exp["recipe_id"] != "agent_experiment_v1"
            or self.store.one(
                "SELECT 1 FROM jobs WHERE experiment_id=? AND state IN ('submitting','submitted')", (eid,)
            )
        ):
            raise ValueError("recovery requires a terminal output-validation failure without active compute")
        result = await self.agent.read_turn(action["thread_id"], action["turn_id"])
        if result != json.loads(action["result"]):
            raise ValueError("exact saved turn result differs; no adoption was made")
        plan = (ResearchPlan if action["phase"] == "plan" else Implementation).model_validate(result)
        self.validate_resource_plan(exp, plan)
        if action["phase"] == "prepare":
            runtime = await asyncio.to_thread(self.workspaces.shared_runtime, exp["id"])
            self.prepared_python(exp, plan, runtime)
            for name in plan.files:
                artifacts.allowed_path(self.working_directory(exp) / name, self.working_directory(exp))
        # Adoption, phase and outbox are one transaction. Attempts and identities
        # remain untouched; the normal owner performs full freezing/validation.
        with self.store.tx() as db:
            before = action_obligations.expected_saved(db, action, compare_result=True)
            cursor = db.execute(
                "UPDATE agent_actions SET state='completed',error=NULL,updated_at=? "
                "WHERE id=? AND state='failed' AND thread_id=? AND turn_id=?",
                (clock.now(), action["id"], action["thread_id"], action["turn_id"]),
            )
            if not cursor.rowcount:
                raise ValueError("saved action changed during output reconciliation")
            action_obligations.sync_action(db, action["id"], before=before)
            db.execute(
                "UPDATE experiments SET phase='preparing',hold_reason=NULL,error=NULL WHERE id=?", (eid,)
            )
            advancement_decisions.sync_lifetime(db, eid, exp["phase"], "preparing")
            db.execute(
                "UPDATE stages SET state='pending',error=NULL,updated_at=? WHERE experiment_id=? AND id=?",
                (clock.now(), eid, action["phase"]),
            )
            self.store.emit(
                db,
                eid,
                "agent_output_reconciled",
                {"action_id": action["id"], "thread_id": action["thread_id"], "turn_id": action["turn_id"]},
            )
        return {"experiment_id": eid, "action_id": action["id"], "state": "completed"}

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

    async def dispatch(self, action, slot):
        eid = action["experiment_id"]
        expected = action
        try:
            exp = self.store.experiment(eid)
            # Stage and monitor turns are contract-3 entrances; contract 4 dispatches
            # its own research turns.
            if not iteration.enabled(exp):
                return
            await self.refresh_quota(slot)
            decision = self.agent_pool.quota(eid=eid)
            if blocked(self.store):
                return
            if not decision.admitted:
                self.store.update(eid, hold_reason="quota_hold", error=decision.reason)
                return
            exp = self.store.experiment(eid)
            if exp["phase"] in ("complete", "failed", "cancelled"):
                self._action_state(action["id"], "superseded", expected=expected)
                return
            if exp["pause_agent"]:
                self.store.update(eid, hold_reason="agent_paused")
                return
            if action["phase"] in ("plan", "prepare") and self.config.mode == "live":
                if clock.now() - self.last_cluster_refresh >= 60:
                    await self.refresh_cluster()
                if not await self.ensure_cluster(exp):
                    self.store.update(
                        eid,
                        hold_reason="cluster_unavailable",
                        error="Execution cluster discovery unavailable",
                    )
                    return
                prompt = await asyncio.to_thread(self.prompt, exp, action["phase"])
                with self.store.tx() as db:
                    db.execute("UPDATE agent_actions SET input=? WHERE id=?", (prompt, action["id"]))
                    self.store.emit(db, eid, "agent_input_refreshed", {"action_id": action["id"]})
                action = {**action, "input": prompt}
            if action["phase"] == "interpret" and exp["recipe_id"] == "agent_experiment_v1":
                prompt = await asyncio.to_thread(self.prompt, exp, "interpret")
                with self.store.tx() as db:
                    db.execute("UPDATE agent_actions SET input=? WHERE id=?", (prompt, action["id"]))
                    self.store.emit(db, eid, "agent_input_refreshed", {"action_id": action["id"]})
                action = {**action, "input": prompt}
            cap = self.agent_turn_ceiling(exp)
            if (
                cap is not None
                and self.store.one(
                    "SELECT COUNT(*) AS n FROM agent_actions WHERE experiment_id=? AND attempts>0", (eid,)
                )["n"]
                >= cap
            ):
                self._action_state(
                    action["id"],
                    "failed",
                    error="The configured agent turn budget is exhausted",
                    expected=expected,
                )
                if action["phase"] != "monitor":
                    self.fail(eid, "agent_budget_exhausted", "The configured agent turn budget is exhausted")
                return
            selection = exp["contract"].get("agent", {})
            selected_model = selection.get("model")
            selected_effort = selection.get("reasoning_effort")
            option = None
            if selected_model or selected_effort:
                target = selected_model or getattr(slot.agent, "resolved_model", None)
                option = next(
                    (model for model in getattr(slot.agent, "models", []) if model["id"] == target), None
                )
                if not option or (selected_effort and selected_effort not in option["reasoning_efforts"]):
                    self._action_state(
                        action["id"],
                        "failed",
                        error="Selected model or reasoning effort is not advertised by the managed runtime",
                        expected=expected,
                    )
                    self.fail(
                        eid,
                        "model_unavailable",
                        "Selected model or reasoning effort is not advertised by the managed runtime; no fallback was used",
                    )
                    return
            session_options = {"model": selected_model} if selected_model else {}
            runtime_config = self.controller.session_config(eid)
            if runtime_config:
                session_options["runtime_config"] = runtime_config
            turn_options = {"effort": selected_effort} if selected_effort else {}
            if action["phase"] == "monitor" and option and "low" in option["reasoning_efforts"]:
                turn_options["effort"] = "low"
            checkout = exp["contract"].get("repair_project") or self.workspaces.checkout(eid)
            project_workspace = Path(checkout) if checkout else None
            isolated = self.workspaces.isolated(eid)
            if isolated and action["phase"] == "prepare":
                prompt = await asyncio.to_thread(self.prompt, exp, "prepare")
                with self.store.tx() as db:
                    db.execute("UPDATE agent_actions SET input=? WHERE id=?", (prompt, action["id"]))
                    self.store.emit(db, eid, "agent_input_refreshed", {"action_id": action["id"]})
                action = {**action, "input": prompt}
            if project_workspace:
                turn_options["project_workspace"] = project_workspace
            generic_prepare = exp["recipe_id"] == "agent_experiment_v1" and action["phase"] == "prepare"
            agent_execution = action["phase"].startswith("agent:")
            if agent_execution:
                job = iteration.agent_job(self, exp, action["phase"])
                workspace = Path(job["execution_root"])
                turn_options.pop("project_workspace", None)
                turn_options["additional_writable_roots"] = [self.working_directory(exp)]
                if project_workspace:
                    turn_options["additional_writable_roots"].append(project_workspace)
                thread_id, model = await slot.agent.session(
                    action["thread_id"], workspace, writable=True, **session_options
                )
            elif action["phase"] == "monitor":
                # A bounded diagnostic must never accumulate in the research
                # conversation or replace its saved session identity.
                thread_id, model = await slot.agent.session(
                    action["thread_id"], self.root(eid), **session_options
                )
            elif generic_prepare:
                workspace = self.working_directory(exp)
                workspace.mkdir(parents=True, exist_ok=True)
                runtime = await asyncio.to_thread(self.workspaces.shared_runtime, eid, build=False)
                if runtime is None and not (workspace / ".venv/bin/python").exists():
                    await command("uv", "venv", "--python", "/usr/bin/python3", str(workspace / ".venv"))
                thread_id, model = await slot.agent.session(
                    action["thread_id"]
                    or (
                        exp["thread_id"]
                        if self.store.one(
                            "SELECT id FROM agent_actions WHERE experiment_id=? AND phase='prepare' "
                            "AND thread_id IS NOT NULL AND id!=? LIMIT 1",
                            (eid, action["id"]),
                        )
                        else None
                    ),
                    project_workspace or workspace,
                    writable=True,
                    **session_options,
                )
            elif (
                action["phase"] == "interpret"
                and exp["recipe_id"] == "agent_experiment_v1"
                and self.config.mode == "live"
            ):
                workspace = self.root(eid) / "analysis"
                workspace.mkdir(parents=True, exist_ok=True)
                thread_id, model = await slot.agent.session(
                    action["thread_id"] or exp["thread_id"], workspace, writable=True, **session_options
                )
            else:
                thread_id, model = await slot.agent.session(
                    action["thread_id"] or exp["thread_id"],
                    project_workspace
                    or (self.root(eid) if action["phase"] == "plan" else self.root(eid) / "snapshot"),
                    **session_options,
                )
            if blocked(self.store) or self.store.experiment(eid)["phase"] == "cancelled":
                return
            prompt = action["input"]
            if action["error"] and not self.input_rejected(action):
                prompt = (
                    "Previous agent action failed; correct the reported error: "
                    + action["error"]
                    + "\n"
                    + prompt
                )
            with self.store.tx() as db:
                held = self.research.service.authority(
                    eid,
                    "interpret" if action["phase"] == "interpret" else action["phase"],
                    action["phase"].removeprefix("agent:"),
                )
                if held or self.store.experiment(eid)["pause_agent"]:
                    db.execute(
                        "UPDATE experiments SET hold_reason=? WHERE id=?", (held or "agent_paused", eid)
                    )
                    return
                if not self.agent_pool.quota(eid=eid).admitted:
                    db.execute("UPDATE experiments SET hold_reason='quota_hold' WHERE id=?", (eid,))
                    return
                db.execute(
                    "UPDATE agent_actions SET autopilot_control_revision=? WHERE id=?",
                    (self.research.service.control_revision(eid), action["id"]),
                )
                if action["phase"] != "monitor":
                    db.execute(
                        "UPDATE experiments SET thread_id=?,model=?,hold_reason=NULL,error=NULL WHERE id=?",
                        (thread_id, model, eid),
                    )
                expected = action_obligations.transition(
                    db, action["id"], action_obligations.dispatch_changes(thread_id), expected=expected
                )
                self.store.emit(
                    db,
                    eid,
                    "agent_dispatch_intent",
                    {
                        "action_id": action["id"],
                        "thread_id": thread_id,
                        "model": model,
                        "reasoning_effort": turn_options.get("effort", "low"),
                        "prompt_sha256": hashlib.sha256(prompt.encode()).hexdigest(),
                    },
                )
            schema = (
                AgentStageResult.model_json_schema()
                if agent_execution
                else ControllerDecision.model_json_schema()
                if action["phase"] == "monitor"
                else ResearchPlan.model_json_schema()
                if action["phase"] == "plan"
                else Implementation.model_json_schema()
                if action["phase"] == "prepare"
                else Analysis.model_json_schema()
            )
            if generic_prepare:
                # Older saved implementations omit this field. New turns must
                # explicitly distinguish a ready graph from blocked work.
                schema.setdefault("required", []).append("outcome")
                schema["properties"]["outcome"].pop("default", None)
            if (
                agent_execution
                or generic_prepare
                or (
                    action["phase"] == "interpret"
                    and exp["recipe_id"] == "agent_experiment_v1"
                    and self.config.mode == "live"
                )
            ):
                if not generic_prepare:
                    turn_options.pop("project_workspace", None)
                turn_id = await slot.agent.begin(
                    thread_id, prompt, schema, workspace=workspace, **turn_options
                )
            else:
                turn_options.pop("project_workspace", None)
                turn_id = await slot.agent.begin(thread_id, prompt, schema, **turn_options)
            with self.store.tx() as db:
                expected = action_obligations.transition(
                    db, action["id"], action_obligations.turn_changes(turn_id), expected=expected
                )
                self.store.emit(
                    db, eid, "agent_turn_started", {"action_id": action["id"], "turn_id": turn_id}
                )
            result = await slot.agent.finish(thread_id, turn_id)
            self._action_state(action["id"], "completed", result=result, expected=expected)
        except SessionUnavailable as exc:
            self._action_state(action["id"], "failed", error=str(exc), expected=expected)
            self.store.update(eid, hold_reason="agent_session_unavailable", error=str(exc))
        except asyncio.CancelledError:
            # Shutdown preserves the last durable status; next start reconciles, never blindly redispatches.
            raise
        except Exception as exc:
            current = self.store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))
            state = (
                "uncertain"
                if (current["state"] == "dispatching" and not isinstance(exc, RpcRejected))
                or isinstance(exc, NeedsAttention)
                else "failed"
            )
            error = str(exc) or (
                "turn/start response timed out; the turn may have started; reconcile its saved identity before retry"
                if current["state"] == "dispatching"
                else "agent turn outcome is unknown; reconcile the saved turn before retry"
                if current["state"] == "running"
                else f"{type(exc).__name__}: agent prerequisite failed before turn dispatch"
            )
            reason = getattr(exc, "reason", None) or self.store.experiment(eid)["hold_reason"]
            if isinstance(exc, TerminalTurnError) and reason in (
                "quota_hold",
                "agent_paused",
                "owner_ceiling",
            ):
                with self.store.tx() as db:
                    db.execute(
                        "UPDATE agent_actions SET interrupt_reason=? WHERE id=?", (reason, action["id"])
                    )
            if current["state"] == "running" and not isinstance(exc, TerminalTurnError):
                state = "uncertain"
            self._action_state(action["id"], state, error=error, expected=expected)
            self.store.update(eid, error=error)
        finally:
            pending = self.store.experiment(eid)["pending_request"]
            if pending:
                pending["expired"] = True
                self.store.update(eid, pending_request=pending)

    async def publish(self, exp):
        eid = exp["id"]
        if self.store.experiment(eid)["phase"] == "cancelled":
            return
        root = self.root(eid)
        self.store.update(eid, phase="publishing", hold_reason="publication_pending")
        records = self.store.rows(
            "SELECT * FROM artifacts WHERE experiment_id=? AND evidence_revision=?",
            (eid, self.store.experiment(eid)["evidence_revision"]),
        )
        for artifact in records:
            if artifacts.sha256(artifacts.allowed_path(Path(artifact["path"]), root)) != artifact["sha256"]:
                raise ValueError("registered evidence changed before publication")
        by_name = {a["name"]: a for a in records}
        analysis = Analysis.model_validate(exp["analysis"])
        report = root / "report.html"
        if analysis.presentation:
            version = self.store.working_report(
                eid,
                analysis.presentation.model_dump(mode="json"),
                "final-report:"
                + self.store.one(
                    "SELECT id FROM agent_actions WHERE experiment_id=? AND phase='interpret' AND state='completed' ORDER BY rowid DESC LIMIT 1",
                    (eid,),
                )["id"],
            )
            report_version = self.store.one(
                "SELECT * FROM report_versions WHERE experiment_id=? AND version=?", (eid, version)
            )
            if report_version["state"] == "failed":
                action = self.store.one(
                    "SELECT * FROM agent_actions WHERE experiment_id=? AND phase='interpret' ORDER BY rowid DESC LIMIT 1",
                    (eid,),
                )
                if action and action["state"] == "completed" and action["turn_id"]:
                    with self.store.tx() as db:
                        before = action_obligations.expected_saved(db, action)
                        feedback = "agent output validation: report rendering: " + report_version["error"]
                        cursor = db.execute(
                            "UPDATE agent_actions SET state='failed',error=?,updated_at=? WHERE id=? AND state='completed'",
                            (feedback, clock.now(), action["id"]),
                        )
                        if not cursor.rowcount:
                            raise NeedsAttention("saved report action changed during correction")
                        action_obligations.sync_action(db, action["id"], before=before)
                        db.execute(
                            "UPDATE stages SET state='pending',error=?,updated_at=? WHERE experiment_id=? AND id='interpret'",
                            (report_version["error"], clock.now(), eid),
                        )
                        db.execute(
                            "UPDATE experiments SET phase='interpreting',hold_reason='agent_continuation',error=? WHERE id=?",
                            (report_version["error"], eid),
                        )
                        self.store.emit(
                            db,
                            eid,
                            "report_correction_required",
                            {
                                "action_id": action["id"],
                                "report_version": version,
                                "error": report_version["error"],
                            },
                        )
                    return
                self.fail(eid, "report_failed", report_version["error"])
                return
            if report_version["state"] != "ready":
                return
        if not report.exists():
            report.write_text(
                artifacts.render_report(
                    exp,
                    artifacts.read_json(root / "results.json", root),
                    analysis,
                    [by_name[name]["id"] for name in exp["snapshot"]["plan"]["figures"] if name in by_name],
                    {a["id"]: a["name"] for a in records},
                )
            )
        if not self.store.one(
            "SELECT id FROM artifacts WHERE experiment_id=? AND name='report.html' AND evidence_revision=?",
            (eid, exp["evidence_revision"]),
        ):
            artifacts.register(self.store, eid, "publish", root, ["report.html"])
        if self.config.gateway.url:
            not_published = self.store.one(
                "SELECT id FROM artifacts WHERE experiment_id=? AND evidence_revision=? AND remote_published=0",
                (eid, exp["evidence_revision"]),
            )
            if not_published:
                return
        # The result inbox and readiness state are committed together.
        with self.store.tx() as db:
            db.execute(
                "UPDATE stages SET state='complete',updated_at=? WHERE experiment_id=? AND id='publish'",
                (clock.now(), eid),
            )
            previous_phase = db.execute("SELECT phase FROM experiments WHERE id=?", (eid,)).fetchone()[0]
            db.execute(
                "UPDATE experiments SET phase='complete',hold_reason=NULL,error=NULL WHERE id=?", (eid,)
            )
            advancement_decisions.sync_lifetime(db, eid, previous_phase, "complete")
            # The saved final interpretation opts in once, at completion, and only for
            # a named branch; the command queues it and never touches Git itself.
            self.store.request_completion_integration(eid, _db=db)
            db.execute(
                "INSERT OR IGNORE INTO inbox VALUES(?,?,?,?,?)",
                (str(uuid4()), eid, "result_ready", "Interpreted experiment result is ready.", clock.now()),
            )
            self.store.emit(db, eid, "result_ready")

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
