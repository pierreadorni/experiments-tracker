import asyncio
import contextlib
import hashlib
import json
import logging
import os
import shutil
import sys
import time
from pathlib import Path
from uuid import uuid4

from . import artifacts, iteration, reporting
from .codex_client import CodexAgent, NeedsAttention, RpcRejected, SessionUnavailable, TerminalTurnError
from .config import JobResources
from .controller import Controller, ControllerDecision
from .conversation import Conversation
from .knowledge import ProjectSupervisor
from .models import (
    Analysis,
    ExperimentResult,
    ExperimentValidation,
    Implementation,
    Preparation,
    ResearchPlan,
    stage_dependencies,
)
from .progress import DiscussionActivity, describe_item, read_job_progress
from .projects import Projects, ProjectUnavailable
from .quota import QuotaGate
from .remote import AdmissionPending, RemoteSlurmScheduler, TransferPending
from .remote_worker import cleanup_temporary
from .slurm import ResourceUnavailable, SubmissionUncertain, command, wrapper
from .steering import process_restarts, queue_late_reports
from .store import encode
from .tfevents import read_previews
from .tfevents import selection as tensorboard_selection
from .title import TitleSummary, luna_title_model, title_prompt
from .workspaces import Workspaces, WorkspaceUnavailable

log = logging.getLogger(__name__)
COMPUTE_STAGES = ("train_linear", "train_quadratic", "evaluate_and_plot")
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


class Workflow:
    def __init__(self, config, store, scheduler, agent):
        self.config = config
        self.store = store
        self.projects = Projects(config, store)
        self.workspaces = Workspaces(self.projects)
        self.scheduler = scheduler
        self.agent = agent
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
        self.agent_task = None
        self.quota_task = None
        self.active_action = None
        self.started = False
        self.closing = False
        self.last_quota_refresh = 0
        self.last_backup = 0
        self.backup_task = None
        self.last_scheduler = 0
        self.agent_ready = False
        self.agent_error = None
        self.last_agent_start = 0
        self.cluster_snapshot = None
        self.cluster_error = None
        self.last_cluster_refresh = 0
        self.last_preflight = {}
        self.last_cancellation = {}
        self.remote_schedulers = {}
        self.remote_clusters = {}
        self.remote_reads = {}
        self.remote_downloads = {}
        self.agent.request_handler = self._request
        self.agent.quota_handler = self._quota_notification
        self.discussion_activity = DiscussionActivity()
        self.agent.activity_handler = self._activity
        self.conversation = Conversation(self)
        self.supervisor = ProjectSupervisor(self)
        self.controller = Controller(self)
        self.submission_lock = asyncio.Lock()
        self.pool_owner = self
        self.lanes = [self]
        self.agent_assignments = {}
        self.output_checks = {}
        self.evidence_checks = {}
        self.cleanup_tasks = {}
        self.last_cleanup_attempt = {}
        self.check_slot = asyncio.Semaphore(1)

    async def check_files(self, function, *args, **kwargs):
        # Keep large sequential reads bounded without exhausting the shared I/O pool.
        async with self.check_slot:
            return await asyncio.to_thread(function, *args, **kwargs)

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
        cache = self.pool_owner.remote_schedulers
        if key not in cache:
            cache[key] = RemoteSlurmScheduler(self, exp, settings, saved=metadata)
        if not job:
            cache[key].exp = exp
        return cache[key]

    async def ensure_cluster(self, exp):
        if not self.is_remote(exp):
            return self.cluster_snapshot
        settings = exp["contract"]["execution"]
        cache = self.pool_owner.remote_clusters
        host = settings.get("ssh_host", "jz")
        previous = cache.get(host, {})
        if time.time() - previous.get("checked_at", 0) >= settings.get("poll_seconds", 600):
            try:
                snapshot = await self.scheduler_for(exp).discover()
                cache[host] = {"snapshot": snapshot, "checked_at": time.time(), "error": None}
            except Exception as exc:
                cache[host] = {"snapshot": None, "checked_at": time.time(), "error": str(exc)}
        return cache.get(host, {}).get("snapshot")

    def cluster_for(self, exp):
        if not self.is_remote(exp):
            return self.cluster_snapshot
        return self.pool_owner.remote_clusters.get(
            exp["contract"]["execution"].get("ssh_host", "jz"), {}
        ).get("snapshot")

    def execution_instruction(self, exp, phase):
        if iteration.enabled(exp):
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
        if not self.is_remote(exp):
            return "Execution machine: acacia. Agents and scheduled jobs use acacia."
        return (
            "Execution machine: Jean Zay; you are running on acacia. Read execution_context.guide and saved transfer commands. "
            "Use set_project_execution to configure the remote folder and BOTH rsync commands if empty; this tool never submits a job. "
            "Templates are expanded without a shell, one argument at a time: {local_project}, {remote_project}, {local_run}, {remote_run}, {host}. "
            "The remote project template is an isolated checkout per experiment/revision, under the configured remote folder. "
            "Code and reports remain on acacia. Tiny authorized local GPU tests and plots may use acacia; scheduled compute uses Jean Zay. "
            "Write .sbatch stage scripts (or .sh), declare them in files and each stage.script, and declare matching structured resources. "
            "The executor injects exact tracking name/comment/log paths, working directory, and structured resource arguments overriding directives. "
            "Only literal resource, name, log and working-directory SBATCH directives are supported. Arrays/dependencies/nested sbatch are prohibited. "
            "Jean Zay forbids --mem, --mem-per-cpu and --mem-per-gpu: memory follows cluster allocation policy. memory_mb_per_job is a planning estimate; choose CPU/GPU resources accordingly and omit memory directives. "
            "The .sbatch body runs from EXPERIMENT_PROJECT; source scripts are in EXPERIMENT_SOURCE. Write declared small results to EXPERIMENT_ROOT. "
            "Set up the remote runtime in the script, e.g. module load; acacia venvs are never portable. EXPERIMENT_PYTHON is only system Python; choose modules explicitly. "
            "Keep valuable large checkpoints in local_outputs relative to EXPERIMENT_ROOT on Jean Zay. The allocation seals their identities; only metadata returns. "
            "Declare regenerable feature caches in temporary_outputs under EXPERIMENT_ROOT/temporary/; they are not hashed and are deleted after the report is ready. "
            "The backend always retrieves required small declared outputs and owned logs. Configure rsync_down for additional TensorBoard events and plots. "
            "Polling and download freshness is ten minutes by default. Do not run polling loops or scheduler mutations yourself. "
        )

    def execution_context(self, exp):
        from .execution_settings import ExecutionSettings

        settings = ExecutionSettings.model_validate(exp["contract"].get("execution", {})).model_dump()
        guide = Path(__file__).with_name("machine_contexts") / (
            ("iteration-" if iteration.enabled(exp) else "") + settings["context"] + ".md"
        )
        return {
            **settings,
            "agent_host": "acacia",
            "guide": guide.read_text(),
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

    def agent_lane(self, eid):
        owner = self.pool_owner
        assigned = owner.agent_assignments.get(eid)
        if assigned and assigned.active_action:
            return assigned
        lane = next((lane for lane in owner.lanes if not lane.agent_task), assigned or self)
        owner.agent_assignments[eid] = lane
        lane.cluster_snapshot = owner.cluster_snapshot
        lane.cluster_error = owner.cluster_error
        lane.last_cluster_refresh = owner.last_cluster_refresh
        return lane

    async def start_agent_pool(self):
        for index in range(1, self.config.codex.max_sessions):
            if getattr(self.agent, "is_demo", False):
                agent = type(self.agent)()
            else:
                agent = CodexAgent(self.config.codex, self.config.state_dir / f"agent-slot-{index}")
            lane = Workflow(self.config, self.store, self.scheduler, agent)
            lane.pool_owner = self
            lane.gate = self.gate
            lane.controller = self.controller
            lane.submission_lock = self.submission_lock
            await lane.connect_agent()
            lane.started = True
            self.lanes.append(lane)

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

    async def _activity(self, message):
        if not self.active_action:
            return
        action = self.store.one("SELECT * FROM agent_actions WHERE id=?", (self.active_action,))
        params = message.get("params", {})
        if not action or action["state"] not in ("dispatching", "running"):
            return
        if (
            not action["turn_id"]
            or params.get("threadId") != action["thread_id"]
            or params.get("turnId") != action["turn_id"]
        ):
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
                        encode({"thread_total": saved["total"], "last_request": saved["last"]}),
                        time.time(),
                    ),
                )
            return
        description = (
            self.discussion_activity.describe(action["id"], message, time.monotonic())
            if action["phase"].startswith("discuss:")
            else describe_item(message)
        )
        if description:
            self.store.activity(action, *description)

    async def start(self):
        self.config.job_output_root.mkdir(parents=True, exist_ok=True)
        await self.refresh_cluster()
        await self.connect_agent()
        # A request belongs to its original connection. Never replay an obsolete RPC ID after restart.
        for row in self.store.rows(
            "SELECT id,pending_request FROM experiments WHERE pending_request IS NOT NULL"
        ):
            pending = json.loads(row["pending_request"])
            pending["expired"] = True
            self.store.update(row["id"], pending_request=pending, hold_reason="expired_agent_request")
        await self.controller.start()
        await self.start_agent_pool()
        self.started = True

    async def refresh_cluster(self):
        self.last_cluster_refresh = time.time()
        if self.config.mode == "demo":
            return
        try:
            self.cluster_snapshot = await self.scheduler.discover()
            self.cluster_error = None
        except (ResourceUnavailable, OSError, RuntimeError) as exc:
            self.cluster_snapshot = None
            self.cluster_error = str(exc)

    async def connect_agent(self):
        self.last_agent_start = time.time()
        try:
            await self.agent.start()
            self.agent_ready = True
            self.agent_error = None
            await self.refresh_quota()
        except Exception as exc:
            self.agent_ready = False
            self.agent_error = str(exc)
            log.warning("agent unavailable: %s", exc)
            await self.agent.close()

    async def refresh_quota(self):
        try:
            payload = await self.agent.quota()
            self.gate.update(payload)
            decision = self.gate.decide()
            with self.store.tx() as db:
                db.execute(
                    "INSERT INTO quota_snapshots(observed_at,payload,admitted,reason) VALUES(?,?,?,?)",
                    (time.time(), encode(payload), decision.admitted, decision.reason),
                )
            self.last_quota_refresh = time.time()
            if not decision.admitted:
                await self.pool_owner.supervisor.interrupt()
                for lane in self.pool_owner.lanes:
                    action = self.store.one("SELECT * FROM agent_actions WHERE id=?", (lane.active_action,))
                    if action and action["turn_id"]:
                        lane.hold_active_action(action, "quota_hold")
                        await lane.agent.interrupt(action["thread_id"], action["turn_id"])
            return decision
        except Exception as exc:
            log.warning("quota refresh failed: %s", exc)
            decision = self.gate.decide()
            if not decision.admitted:
                await self.pool_owner.supervisor.interrupt()
                for lane in self.pool_owner.lanes:
                    action = self.store.one("SELECT * FROM agent_actions WHERE id=?", (lane.active_action,))
                    if action and action["turn_id"]:
                        lane.hold_active_action(action, "quota_hold")
                        await lane.agent.interrupt(action["thread_id"], action["turn_id"])
            return decision

    def hold_active_action(self, action, reason):
        with self.store.tx() as db:
            db.execute("UPDATE agent_actions SET interrupt_reason=? WHERE id=?", (reason, action["id"]))
        if action["phase"].startswith("discuss:"):
            self.conversation.hold(action["phase"].removeprefix("discuss:"), reason)
        else:
            self.store.update(action["experiment_id"], hold_reason=reason)

    async def _quota_notification(self, payload):
        self.gate.update(payload)
        if (
            not self.closing
            and not self.gate.decide().admitted
            and (self.active_action or self.pool_owner.supervisor.active_id)
        ):
            # Do not await a correlated RPC from inside the protocol reader.
            if not self.quota_task or self.quota_task.done():
                self.quota_task = asyncio.create_task(self.refresh_quota())

    async def _request(self, message):
        action = self.store.one("SELECT * FROM agent_actions WHERE id=?", (self.active_action,))
        if not action:
            raise NeedsAttention("request has no active action")
        pending = {
            "id": str(uuid4()),
            "rpc_id": message["id"],
            "method": message["method"],
            "params": message.get("params", {}),
            "expired": False,
        }
        self.store.update(action["experiment_id"], pending_request=pending, hold_reason="needs_attention")
        self.store.notify(
            action["experiment_id"], "required_decision", "Codex needs an answer or permission."
        )

    async def tick(self, *, force_observe=False):
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
        if not self.started:
            await self.start()
        now = time.time()
        if self.config.mode == "live" and now - self.last_cluster_refresh >= 60:
            await self.refresh_cluster()
        if self.agent_ready and not self.agent.available:
            self.agent_ready = False
            self.agent_error = "App Server connection closed; saved turns require reconciliation"
        if not self.agent_ready and not self.agent_task and now - self.last_agent_start >= 60:
            await self.agent.close()
            await self.connect_agent()
        if self.agent_ready and now - self.last_quota_refresh >= 45:
            if not self.quota_task or self.quota_task.done():
                self.quota_task = asyncio.create_task(self.refresh_quota())
        if force_observe or now - self.last_scheduler >= self.config.scheduler_seconds:
            await self.observe_jobs()
            self.last_scheduler = now
        for row in self.store.rows("SELECT id FROM experiments WHERE pending_request IS NOT NULL"):
            exp = self.store.experiment(row["id"])
            pending = exp["pending_request"]
            if pending.get("answer") and not pending.get("expired"):
                try:
                    await self.agent.reply(pending)
                    self.store.update(exp["id"], pending_request=None, hold_reason=None)
                except Exception as exc:
                    self.store.update(exp["id"], error=str(exc), hold_reason="needs_attention")
        if self.agent_task and self.agent_task.done():
            # Dispatch handles its errors and persists them before returning.
            self.agent_task.result()
            self.agent_task = None
            self.active_action = None
        self.recover_terminal_actions()
        for lane in self.lanes[1:]:
            if lane.agent_task and lane.agent_task.done():
                lane.agent_task.result()
                lane.agent_task = None
                lane.active_action = None
            if not lane.agent_ready and not lane.agent_task and now - lane.last_agent_start >= 60:
                await lane.agent.close()
                await lane.connect_agent()
        self.apply_planning_steering()
        await process_restarts(self)
        await self.repair_failed_experiments()
        await self.advance_reports()
        rows = self.store.rows(
            "SELECT id FROM experiments WHERE deleted_at IS NULL AND phase NOT IN ('complete','failed','cancelled') ORDER BY created_at"
        )
        if self.supervisor.blocks_dispatch:
            rows = []
        # Advance independent state machines under the same owner. Saved intents
        # and the account-wide agent slot remain the dispatch authority.
        for row in rows:
            eid = row["id"]
            try:
                await self.advance(eid)
            except ProjectUnavailable as exc:
                self.store.update(eid, hold_reason="project_unavailable", error=str(exc))
            except (ValueError, KeyError, OSError) as exc:
                self.fail(eid, "validation_failed", str(exc))
            except Exception as exc:
                log.exception("workflow tick failed")
                self.store.update(eid, hold_reason="executor_error", error=str(exc))
        self.discard_failed_implementation()
        await self.advance_cleanup()
        queue_late_reports(self)
        if not self.supervisor.blocks_dispatch:
            await self.conversation.advance()
            await self.advance_title()
            await self.controller.advance()
        await self.supervisor.advance()
        if now - self.last_backup >= 60 and (not self.backup_task or self.backup_task.done()):
            self.backup_task = asyncio.create_task(self.backup_state())
            self.last_backup = now

    async def backup_state(self):
        try:
            await asyncio.to_thread(self.store.publish_backup, self.config.backup_dir)
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
            if action["state"] in ("running", "dispatching"):
                if self.agent_task or not self.agent_ready:
                    return
                if action["thread_id"] and action["turn_id"]:
                    try:
                        result = await self.agent.read_turn(action["thread_id"], action["turn_id"])
                        TitleSummary.model_validate(result)
                        self._action_state(action["id"], "completed", result=result)
                    except Exception as exc:
                        self._action_state(action["id"], "uncertain", error=str(exc))
                        self.store.update(exp["id"], hold_reason="agent_dispatch_uncertain", error=str(exc))
                else:
                    self._action_state(action["id"], "uncertain", error="title dispatch outcome is unknown")
                    self.store.update(exp["id"], hold_reason="agent_dispatch_uncertain")
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
                        self._action_state(action["id"], "failed", error=f"title validation: {exc}")
            elif action["state"] == "pending" and not self.title_applicable(exp, action, dispatch=True):
                self._action_state(action["id"], "superseded")
        choice = luna_title_model(getattr(self.agent, "models", []))
        if not choice:
            return
        for action in self.store.rows(
            "SELECT * FROM agent_actions WHERE phase='summarize_title' AND state='pending' ORDER BY created_at"
        ):
            exp = self.store.experiment(action["experiment_id"])
            if not self.title_applicable(exp, action, dispatch=True) or exp["pause_agent"]:
                continue
            if (
                self.agent_task
                or not self.agent_ready
                or not self.gate.decide().admitted
                or self.store.one(
                    "SELECT 1 FROM agent_actions WHERE state IN ('dispatching','running','uncertain') LIMIT 1"
                )
            ):
                return
            saved = json.loads(action["input"])
            if not saved.get("model"):
                saved.update(model=choice[0], effort=choice[1])
                with self.store.tx() as db:
                    db.execute(
                        "UPDATE agent_actions SET input=?,updated_at=? WHERE id=?",
                        (encode(saved), time.time(), action["id"]),
                    )
                action = self.store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))
            self.active_action = action["id"]
            self.agent_task = asyncio.create_task(self.dispatch_title(action))
            return
        for row in self.store.rows(
            "SELECT id FROM experiments WHERE recipe_id='agent_experiment_v1' AND deleted_at IS NULL "
            "AND phase='preparing' ORDER BY created_at"
        ):
            exp = self.store.experiment(row["id"])
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
            if self.agent_task or not self.agent_ready or not self.gate.decide().admitted:
                return
            if self.store.one(
                "SELECT 1 FROM agent_actions WHERE state IN ('dispatching','running','uncertain') LIMIT 1"
            ):
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
                        (action_id, exp["id"], revision, prompt, time.time(), time.time()),
                    )
                    self.store.emit(
                        db, exp["id"], "agent_action_queued", {"phase": "title", "action_id": action_id}
                    )
                action = self.store.one("SELECT * FROM agent_actions WHERE id=?", (action_id,))
            self.active_action = action["id"]
            self.agent_task = asyncio.create_task(self.dispatch_title(action))
            return

    @staticmethod
    def title_applicable(exp, action, *, dispatch=False):
        if exp["deleted_at"]:
            return False
        if action["phase"] == "summarize_title":
            return True
        review = exp["plan_review"] or {}
        return review.get("revision") == action["evidence_revision"] and review.get("state") in (
            ("awaiting_review",) if dispatch else ("awaiting_review", "approved")
        )

    async def dispatch_title(self, action):
        """Persist exact title turn identities; a lost outcome is never retried."""
        eid = action["experiment_id"]
        try:
            decision = await self.refresh_quota()
            if not decision.admitted:
                return
            exp = self.store.experiment(eid)
            if exp["pause_agent"]:
                return
            if not self.title_applicable(exp, action, dispatch=True):
                self._action_state(action["id"], "superseded")
                return
            saved = json.loads(action["input"])
            if not any(
                model["id"] == saved["model"] and saved["effort"] in model["reasoning_efforts"]
                for model in getattr(self.agent, "models", [])
            ):
                self._action_state(action["id"], "failed", error="Saved summary model is unavailable")
                return
            self.root(eid).mkdir(parents=True, exist_ok=True)
            thread_id, _ = await self.agent.session(None, self.root(eid), model=saved["model"])
            with self.store.tx() as db:
                db.execute(
                    "UPDATE agent_actions SET thread_id=?,state='dispatching',attempts=attempts+1,updated_at=? WHERE id=?",
                    (thread_id, time.time(), action["id"]),
                )
                self.store.emit(
                    db, eid, "agent_dispatch_intent", {"action_id": action["id"], "thread_id": thread_id}
                )
            turn_id = await self.agent.begin(
                thread_id, saved["prompt"], TitleSummary.model_json_schema(), effort=saved["effort"]
            )
            with self.store.tx() as db:
                db.execute(
                    "UPDATE agent_actions SET turn_id=?,state='running',updated_at=? WHERE id=?",
                    (turn_id, time.time(), action["id"]),
                )
                self.store.emit(
                    db, eid, "agent_turn_started", {"action_id": action["id"], "turn_id": turn_id}
                )
            result = await self.agent.finish(thread_id, turn_id)
            try:
                TitleSummary.model_validate(result)
            except ValueError as exc:
                self._action_state(action["id"], "failed", error=f"title validation: {exc}")
                return
            self._action_state(action["id"], "completed", result=result)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            current = self.store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))
            state = (
                "uncertain"
                if current["state"] in ("dispatching", "running") or isinstance(exc, NeedsAttention)
                else "failed"
            )
            self._action_state(action["id"], state, error=str(exc))
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
            if exp["recipe_id"] != "agent_experiment_v1" or exp["pending_request"]:
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
            if self.config.mode != "live" or exp["recipe_id"] != "agent_experiment_v1" or not exp["snapshot"]:
                continue
            resource_revision = exp["hold_reason"] == "resources_unavailable"
            if resource_revision and time.time() - max(
                (stamp for (saved_eid, _, _), stamp in self.last_preflight.items() if saved_eid == eid),
                default=0,
            ) < (
                exp["contract"].get("execution", {}).get("poll_seconds", 600) if self.is_remote(exp) else 60
            ):
                continue
            if not resource_revision and not self.repair_available(exp):
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
            if iteration.enabled(exp) and not self.workspaces.row(eid):
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
                    not self.workspaces.row(peer["id"])
                    and self.store.experiment(peer["id"])["contract"]
                    .get("project", {})
                    .get("id", "test-project")
                    == project_id
                    for peer in peers
                ):
                    continue
            jobs = self.store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
            if (not jobs and not resource_revision) or any(
                j["state"] in ("submitting", "submitted") or not j["job_id"] for j in jobs
            ):
                continue
            # Fresh exact accounting is required even for an output-validation failure.
            confirmed = True
            current_timeout = False
            for job in jobs:
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
            if budget is not None and len(jobs) >= budget:
                if exp["hold_reason"] == "automatic_repair_pending":
                    self.fail(
                        eid, "compute_budget_exhausted", "Automatic repair would exceed the saved job budget"
                    )
                continue
            contract = exp["contract"]
            revision = exp["evidence_revision"] + 1
            contract.setdefault("working_directory", str(self.working_directory(exp)))
            contract["repair_count"] = contract.get(
                "repair_count",
                len(contract.get("repair_history", [])) + bool(contract.get("auto_repair")),
            ) + (0 if resource_revision else 1)
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
                db.execute(
                    "UPDATE experiments SET contract=?,evidence_revision=?,snapshot=NULL,analysis=NULL,"
                    "scientific_outcome=NULL,phase='preparing',hold_reason='automatic_repair',error=NULL,"
                    "plan_review=? WHERE id=?",
                    (encode(contract), revision, encode(review) if review else None, eid),
                )
                db.execute(
                    "UPDATE stages SET state='pending',attempt=?,output_manifest=NULL,error=NULL,updated_at=? "
                    "WHERE experiment_id=? AND id!='plan'",
                    (revision, time.time(), eid),
                )
                self.store.emit(db, eid, "automatic_repair_queued", {"evidence_revision": revision})

    def fail(self, eid, reason, error):
        exp = self.store.experiment(eid)
        if reason == "validation_failed" and exp["phase"] == "validating":
            self.store.stage(eid, "validate", "failed", error=error)
        if (
            self.config.mode == "live"
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

    def apply_planning_steering(self):
        """Fold saved guidance into a fresh preparation revision before any job is dispatched."""
        pending = self.store.rows(
            "SELECT request_id,experiment_id,text FROM steering WHERE state='pending' ORDER BY created_at"
        )
        for item in pending:
            eid = item["experiment_id"]
            exp = self.store.experiment(eid)
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
                if review["state"] != "drafting":
                    review["revision"] += 1
                    review["state"] = "drafting"
                previous = review.get("feedback", "")
                review["feedback"] = (previous + "\n" + item["text"]).strip()
                db.execute(
                    "UPDATE experiments SET plan_review=?,pause_compute=0,hold_reason=NULL WHERE id=?",
                    (encode(review), eid),
                )
                db.execute(
                    "UPDATE stages SET state='pending',updated_at=? WHERE experiment_id=? AND id='plan'",
                    (time.time(), eid),
                )
                db.execute(
                    "UPDATE steering SET state='applied',applied_at=? WHERE request_id=?",
                    (time.time(), item["request_id"]),
                )
                self.store.emit(db, eid, "steering_applied", {"request_id": item["request_id"]})

    async def advance(self, eid):
        exp = self.store.experiment(eid)
        if exp["hold_reason"] == "automatic_repair_pending":
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
        generic = exp["recipe_id"] == "agent_experiment_v1"
        if generic and exp["contract"].get("planning_version") and stages["plan"] != "complete":
            await self.agent_stage(exp, "plan")
            return
        if generic and stages["prepare"] != "complete":
            await self.agent_stage(exp, "prepare")
            return
        if not exp["snapshot"]:
            manifest = artifacts.freeze_recipe(self.config.recipe_dir, root, exp["contract"])
            self.store.update(eid, snapshot=manifest)
            exp = self.store.experiment(eid)
        reference = exp["contract"].get("reference")
        if (
            reference
            and not generic
            and any(
                reference[field] != exp["snapshot"][field]
                for field in ("snapshot_hash", "parameters_hash", "data_hash", "test_hash")
            )
        ):
            self.fail(
                eid,
                "reference_protocol_mismatch",
                "The reference uses a different frozen protocol; no model turn or job was launched",
            )
            return
        await asyncio.to_thread(artifacts.verify_snapshot, root, exp["snapshot"])
        await asyncio.to_thread(artifacts.register_snapshot, self.store, eid, root)
        if stages["prepare"] != "complete":
            await self.agent_stage(exp, "prepare")
            return
        compute_stages = self.compute_stages(exp)
        dag = generic and exp["snapshot"]["plan"].get("schema_version", 1) >= 2
        dependencies = (
            {stage["id"]: stage["needs"] for stage in exp["snapshot"]["plan"]["stages"]} if dag else {}
        )
        # An unknown submission may already exist in Slurm. Reconcile its exact
        # identity before crossing another dispatch boundary for this experiment.
        if dag and self.store.one(
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
            if iteration.enabled(exp) and requested_done and stage not in requested:
                continue
            if stages[stage] == "complete":
                continue
            job = self.store.one(
                "SELECT * FROM jobs WHERE experiment_id=? AND stage_id=? AND attempt=?",
                (eid, stage, exp["evidence_revision"]),
            )
            if job:
                if job["state"] == "complete":
                    if not iteration.enabled(exp) and exp["contract"].get("strict_reproducibility"):
                        artifacts.verify_project_sources(
                            root, exp["snapshot"], self.project_context(exp)["checkout"]
                        )
                    names = await asyncio.to_thread(artifacts.validate_stage, root, stage, exp["snapshot"])
                    if generic:
                        names += await asyncio.to_thread(
                            artifacts.seal_local_outputs, root, stage, exp["snapshot"]
                        )
                    artifacts.register(self.store, eid, stage, root, names)
                    self.store.stage(eid, stage, "complete", outputs=names)
                elif job["state"] == "failed":
                    if iteration.enabled(exp) and stage not in requested:
                        continue
                    self.fail(eid, "compute_failed", job["error"] or "scheduler job failed")
                    return
                if dag:
                    continue
                return
            if dag and any(stages[need] != "complete" for need in dependencies[stage]):
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
            if dag:
                continue
            return
        if dag and any(
            stages[stage] != "complete"
            for stage in compute_stages
            if not iteration.enabled(exp) or stage in requested
        ):
            return
        if iteration.enabled(exp):
            self.prepare_iteration_results(exp)
        if not iteration.enabled(exp) and stages["validate"] != "complete":
            self.store.start_validation(eid)
            if self.has_local_checkpoints(exp["snapshot"]):
                key = (eid, exp["evidence_revision"])
                task = self.evidence_checks.get(key)
                if task is None:

                    def check_evidence():
                        for stage in compute_stages:
                            artifacts.validate_stage(
                                root,
                                stage,
                                exp["snapshot"],
                                verify_local_outputs=bool(exp["contract"].get("strict_reproducibility")),
                            )

                    self.evidence_checks[key] = asyncio.create_task(self.check_files(check_evidence))
                    return
                if not task.done():
                    return
                try:
                    task.result()
                finally:
                    del self.evidence_checks[key]
            else:
                for stage in compute_stages:
                    await asyncio.to_thread(
                        artifacts.validate_stage,
                        root,
                        stage,
                        exp["snapshot"],
                        verify_local_outputs=bool(exp["contract"].get("strict_reproducibility")),
                    )
            self.store.stage(eid, "validate", "complete")
            return
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
                    raise ValueError("ambiguous evaluation metric; use distinct metric/variant names")
                metrics[key] = metric
        if not metrics:
            raise ValueError("requested evaluation has no registered metrics")
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
        if not exp["snapshot"] or exp["phase"] in ("failed", "cancelled", "complete"):
            raise ValueError("submission requires an active saved implementation")
        root = self.root(eid)
        generic = exp["recipe_id"] == "agent_experiment_v1"
        compute_stages = self.compute_stages(exp)
        if stage not in compute_stages:
            raise ValueError("stage is not in the saved implementation")
        index = compute_stages.index(stage)
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
        if generic:
            plan = Implementation.model_validate(exp["snapshot"]["plan"])
            graph = stage_dependencies(plan.stages, plan.schema_version)
            if any(
                self.store.one("SELECT state FROM stages WHERE experiment_id=? AND id=?", (eid, need))[
                    "state"
                ]
                != "complete"
                for need in graph[stage]
            ):
                return {"state": "held", "reason": "dependencies_pending"}
            if exp["plan_review"] and exp["plan_review"]["state"] != "approved":
                return {"state": "held", "reason": "plan_review"}
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
        count = self.store.one("SELECT COUNT(*) AS n FROM jobs WHERE experiment_id=?", (eid,))["n"]
        budget = exp["contract"].get("max_jobs", self.config.slurm.max_jobs_per_experiment)
        if budget is not None and count >= budget:
            self.fail(eid, "compute_budget_exhausted", f"{budget}-job budget exhausted")
            return
        if self.config.mode == "live" and time.time() - self.last_preflight.get(
            (eid, exp["evidence_revision"], stage), 0
        ) < (exp["contract"].get("execution", {}).get("poll_seconds", 600) if self.is_remote(exp) else 60):
            return {"state": "held", "reason": exp["hold_reason"] or "resources_unavailable"}
        intent_id = request_id or str(uuid4())
        python = self.experiment_python(exp) if generic else "/usr/bin/python3"
        project = self.project_context(exp)["checkout"] if generic else None
        resources = self.stage_resources(exp, stage)
        plan_stage = next(s for s in exp["snapshot"]["plan"]["stages"] if s["id"] == stage) if generic else {}
        if generic and not iteration.enabled(exp) and exp["contract"].get("strict_reproducibility"):
            artifacts.verify_project_sources(root, exp["snapshot"], project)
        if generic and not self.is_remote(exp) and not iteration.enabled(exp):
            try:
                installed = await self.dependencies(root, python)
            except TimeoutError:
                self.store.update(
                    eid,
                    hold_reason="runtime_check_pending",
                    error="The environment dependency check timed out before submission; saved compute is retained",
                )
                return {"state": "held", "reason": "runtime_check_pending"}
            if installed != (root / "snapshot/requirements.txt").read_text():
                raise ValueError("prepared environment dependencies changed after freezing")
        submission_manifest = exp["snapshot"]
        refs = []
        scheduler = self.scheduler_for(exp)
        if iteration.enabled(exp):
            root, submission_manifest, refs = iteration.stage_attempt(self, exp, stage, intent_id)
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
            iteration_mode=iteration.enabled(exp),
        )
        if self.config.mode == "live":
            self.last_preflight[(eid, exp["evidence_revision"], stage)] = time.time()
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
            admitted["pause_compute"]
            or admitted["phase"] in ("failed", "cancelled", "complete")
            or admitted["snapshot"]["snapshot_hash"] != exp["snapshot"]["snapshot_hash"]
        ):
            return {"state": "held", "reason": "execution_authorization_changed"}
        execution_manifest = dict(submission_manifest)
        if iteration.enabled(exp):
            execution_manifest["command"] = iteration.bounded_bytes(script, root).decode()
            execution_manifest["parameters"] = submission_manifest["parameters"]
        if self.is_remote(exp):
            execution_manifest["execution"] = scheduler.metadata()
        if generic and project and not iteration.enabled(exp):
            execution_manifest["project_sources_at_submission"] = {
                name: exp["snapshot"]["files"][frozen]
                if self.is_remote(exp)
                else artifacts.sha256(artifacts.allowed_path(Path(project) / name, Path(project)))
                for name, frozen in exp["snapshot"]["project_sources"].items()
            }
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
            "resources": resources.model_dump(),
        }
        record_path.write_text(encode(record))
        record_path.chmod(0o444)
        # Store submitting BEFORE crossing the scheduler boundary; all uncertainty is reconciled.
        with self.store.tx() as db:
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
                    time.time(),
                    str(root),
                    encode(execution_manifest),
                ),
            )
            for ref in refs:
                db.execute(
                    "INSERT INTO output_bindings VALUES(?,?,?)",
                    (intent_id, ref["id"], "selected dependency producer"),
                )
            if not iteration.enabled(exp):
                artifacts.register(self.store, eid, stage, root, [record_name], _db=db)
            db.execute(
                "UPDATE stages SET state='running',updated_at=? WHERE experiment_id=? AND id=?",
                (time.time(), eid, stage),
            )
            db.execute(
                "UPDATE experiments SET phase=?,hold_reason=NULL,error=NULL WHERE id=?",
                ("evaluating" if index == len(compute_stages) - 1 else "training", eid),
            )
            self.store.emit(db, eid, "submission_intent", {"intent_id": intent_id, "stage": stage})
        try:
            if generic:
                job_id = await scheduler.submit(intent_id, script, root, resources=resources)
            else:
                job_id = await self.scheduler.submit(intent_id, script, root)
        except SubmissionUncertain as exc:
            self.store.update(eid, hold_reason="submission_uncertain", error=str(exc))
            return {"state": "uncertain", "reason": "submission_uncertain"}
        with self.store.tx() as db:
            db.execute("UPDATE jobs SET job_id=?,state='submitted' WHERE intent_id=?", (job_id, intent_id))
            self.store.emit(db, eid, "job_submitted", {"intent_id": intent_id, "job_id": job_id})
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
        if (
            exp["recipe_id"] == "agent_experiment_v1"
            and exp["snapshot"]["plan"].get("schema_version", 1) >= 2
        ):
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
        else:
            preceding = compute_stages[: compute_stages.index(job["stage_id"])]
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
        root = self.root(eid)
        artifacts.verify_project_sources(root, exp["snapshot"], self.project_context(exp)["checkout"])
        for stage in preceding:
            await asyncio.to_thread(artifacts.validate_stage, root, stage, exp["snapshot"])
        names = list(
            await asyncio.to_thread(artifacts.validate_stage, root, job["stage_id"], exp["snapshot"])
        )
        if exp["recipe_id"] == "agent_experiment_v1":
            names += await asyncio.to_thread(
                artifacts.seal_local_outputs, root, job["stage_id"], exp["snapshot"]
            )
        now = time.time()
        with self.store.tx() as db:
            artifacts.register(self.store, eid, job["stage_id"], root, names, _db=db)
            db.execute(
                "UPDATE jobs SET state='complete',scheduler_state='COMPLETED',exit_code='0:0',error=NULL,"
                "observed_at=?,started_at=COALESCE(?,started_at),ended_at=COALESCE(?,ended_at),"
                "estimated_start_at=NULL,pending_reason=NULL WHERE intent_id=?",
                (now, observed.started_at, observed.ended_at, job["intent_id"]),
            )
            db.execute(
                "UPDATE stages SET state='complete',error=NULL,output_manifest=?,updated_at=? WHERE experiment_id=? AND id=?",
                (encode(names), now, eid, job["stage_id"]),
            )
            db.execute(
                "UPDATE experiments SET phase='training',hold_reason='new_compute_paused',error=NULL,"
                "pause_compute=1,observed_at=?,updated_at=? WHERE id=?",
                (now, now, eid),
            )
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

    @staticmethod
    def has_local_checkpoints(manifest, stage=None):
        return (
            not iteration.manifest_mode(manifest)
            and not manifest.get("remote_execution")
            and any(
                item.get("local_outputs") and (stage is None or item["id"] == stage)
                for item in manifest.get("plan", {}).get("stages", [])
            )
        )

    async def observe_jobs(self):
        for job in self.store.rows(
            "SELECT * FROM jobs WHERE state IN ('submitting','submitted') ORDER BY created_at"
        ):
            eid = job["experiment_id"]
            try:
                check = self.output_checks.get(job["intent_id"])
                if check and not check[0].done():
                    continue
                exp = self.store.experiment(eid)
                scheduler = self.scheduler_for(exp, job)
                if isinstance(scheduler, RemoteSlurmScheduler):
                    if not scheduler.due(job, time.time()):
                        continue
                    with self.store.tx() as db:
                        db.execute(
                            "UPDATE jobs SET remote_polled_at=? WHERE intent_id=?",
                            (time.time(), job["intent_id"]),
                        )
                if not job["job_id"]:
                    job_id = await self.scheduler_for(
                        self.store.experiment(job["experiment_id"]), job
                    ).reconcile(job["intent_id"])
                    if not job_id:
                        self.store.update(
                            eid,
                            hold_reason="submission_uncertain",
                            error="No matching job found; operator reconciliation required",
                        )
                        continue
                    with self.store.tx() as db:
                        db.execute(
                            "UPDATE jobs SET job_id=?,state='submitted' WHERE intent_id=?",
                            (job_id, job["intent_id"]),
                        )
                        self.store.emit(db, eid, "job_reconciled", {"job_id": job_id})
                    job["job_id"] = job_id
                    self.store.update(eid, hold_reason=None, error=None)
                observed = check[1] if check else await scheduler.observe(job["job_id"])
                if isinstance(scheduler, RemoteSlurmScheduler):
                    try:
                        await scheduler.fetch(job, observed)
                    except TransferPending as exc:
                        with self.store.tx() as db:
                            db.execute(
                                "UPDATE jobs SET scheduler_state=?,exit_code=?,observed_at=? WHERE intent_id=?",
                                (observed.state, observed.exit_code, time.time(), job["intent_id"]),
                            )
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
                now = time.time()
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
                progress_changed = progress != previous_progress
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
                        seal_outputs = exp["recipe_id"] == "agent_experiment_v1"
                        if self.has_local_checkpoints(job_manifest, job["stage_id"]):
                            if check is None:
                                # Publish exact terminal accounting before slow filesystem checks.
                                # Keep the job submitted and its dependencies unready until validation commits.
                                with self.store.tx() as db:
                                    db.execute(
                                        "UPDATE jobs SET scheduler_state=?,exit_code=?,observed_at=?,error=NULL,"
                                        "started_at=COALESCE(?,started_at),ended_at=COALESCE(?,ended_at) "
                                        "WHERE intent_id=?",
                                        (
                                            observed.state,
                                            observed.exit_code,
                                            now,
                                            observed.started_at,
                                            observed.ended_at,
                                            job["intent_id"],
                                        ),
                                    )
                                    db.execute(
                                        "UPDATE experiments SET observed_at=?,hold_reason=NULL,error=NULL "
                                        "WHERE id=? AND (hold_reason IS NULL OR hold_reason IN "
                                        "('accounting_pending','executor_unavailable','submission_uncertain'))",
                                        (now, eid),
                                    )
                                    self.store.emit(
                                        db, eid, "job_outputs_checking", {"job_id": job["job_id"]}
                                    )
                                task = asyncio.create_task(
                                    self.check_files(
                                        artifacts.validate_stage,
                                        job_root,
                                        job["stage_id"],
                                        job_manifest,
                                        seal_outputs=seal_outputs,
                                    )
                                )
                                self.output_checks[job["intent_id"]] = (task, observed)
                                continue
                            names = check[0].result()
                        else:
                            names = await asyncio.to_thread(
                                artifacts.validate_stage,
                                job_root,
                                job["stage_id"],
                                job_manifest,
                                seal_outputs=seal_outputs,
                            )
                        if seal_outputs:
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
                with self.store.tx() as db:
                    registration_started = time.monotonic()
                    if completed_names is not None and iteration.enabled(exp):
                        iteration.complete(self, exp, job, job_manifest, job_root, db)
                    if completed_names is not None and not iteration.enabled(exp):
                        artifacts.register(
                            self.store, eid, job["stage_id"], job_root, completed_names, _db=db
                        )
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
                if state == "failed":
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
                self.store.stage(eid, job["stage_id"], "failed", error=str(exc))
                with self.store.tx() as db:
                    db.execute(
                        "UPDATE jobs SET state='failed',error=? WHERE intent_id=?",
                        (str(exc), job["intent_id"]),
                    )
                    self.store.emit(db, eid, "output_validation_failed", {"error": str(exc)})
                optional = iteration.enabled(exp) and not next(
                    s for s in job_manifest["plan"]["stages"] if s["id"] == job["stage_id"]
                ).get("requested", True)
                if not optional:
                    self.fail(eid, "validation_failed", str(exc))
            except Exception as exc:
                self.output_checks.pop(job["intent_id"], None)
                self.store.update(eid, hold_reason="executor_unavailable", error=str(exc))

    async def agent_stage(self, exp, phase):
        eid = exp["id"]
        # A dispatched task may have reached its quota/turn ceiling while advance
        # awaited filesystem work. Never overwrite that terminal admission hold.
        exp = self.store.experiment(eid)
        if exp["phase"] in ("complete", "failed", "cancelled"):
            return
        if self.pool_owner is self and exp["recipe_id"] == "agent_experiment_v1" and len(self.lanes) > 1:
            lane = self.agent_lane(eid)
            if lane is not self:
                return await lane.agent_stage(exp, phase)
        self.store.update(eid, phase="preparing" if phase in ("plan", "prepare") else "interpreting")
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
            prompt = await asyncio.to_thread(self.prompt, exp, phase)
            action_id = str(uuid4())
            with self.store.tx() as db:
                db.execute(
                    """INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,created_at,updated_at)
                    VALUES(?,?,?,?,'pending',?,?,?)""",
                    (action_id, eid, phase, action_revision, prompt, time.time(), time.time()),
                )
                self.store.emit(db, eid, "agent_action_queued", {"phase": phase, "action_id": action_id})
            action = self.store.one("SELECT * FROM agent_actions WHERE id=?", (action_id,))
        if action["state"] == "completed":
            try:
                value = json.loads(action["result"])
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
                    python = self.prepared_python(exp, plan)
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
                    requirements = (
                        "" if iteration.enabled(exp) else await self.dependencies(self.root(eid), python)
                    )
                    reused = self.reusable_stages(exp, plan)
                    budget = exp["contract"].get("max_jobs", self.config.slurm.max_jobs_per_experiment)
                    submitted = self.store.one(
                        "SELECT COUNT(*) AS n FROM jobs WHERE experiment_id=?", (eid,)
                    )["n"]
                    if budget is not None and submitted + len(plan.stages) - len(reused) > budget:
                        raise ValueError(
                            "remaining stages exceed the total saved job budget; reuse valid completed work or request owner guidance"
                        )
                    manifest = artifacts.freeze_implementation(
                        self.root(eid),
                        plan,
                        requirements,
                        python=python,
                        project=self.project_context(exp)["checkout"],
                        workspace=self.working_directory(exp),
                        reused=reused,
                        remote_execution=self.is_remote(exp),
                    )
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
                    artifacts.register_snapshot(self.store, eid, self.root(eid))
                    (self.root(eid) / "preparation.json").write_text(encode(value))
                    artifacts.register(self.store, eid, phase, self.root(eid), ["preparation.json"])
                elif phase == "prepare":
                    prep = Preparation.model_validate(value)
                    if (
                        prep.snapshot_hash != exp["snapshot"]["snapshot_hash"]
                        or prep.parameters_hash != exp["snapshot"]["parameters_hash"]
                    ):
                        raise ValueError("preparation manifest does not match immutable recipe")
                    if prep.scope_status == "unsupported":
                        reason = (
                            prep.scope_reason
                            or "The available recipe does not satisfy the requested experiment."
                        )
                        self.store.stage(eid, phase, "failed", error=reason)
                        self.store.update(
                            eid, phase="failed", hold_reason="unsupported_request", error=reason
                        )
                        self.store.notify(eid, "required_decision", f"Request not run: {reason}")
                        return
                    if exp["contract"].get("brief_must_match_recipe") and (
                        prep.scope_status != "supported" or not prep.scope_reason
                    ):
                        raise ValueError(
                            "preparation must explicitly confirm that the recipe satisfies the brief"
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
                    if iteration.enabled(exp):
                        root = self.root(eid)
                        name = f"analysis-{action['id']}.json"
                        target = root / name
                        data = encode(analysis.model_dump()).encode()
                        if target.exists():
                            if iteration.bounded_bytes(target, root) != data:
                                raise ValueError(
                                    "saved analysis action already has different published bytes"
                                )
                        else:
                            target.write_bytes(data)
                        artifacts.register(self.store, eid, phase, root, [name])
                        if not (root / "analysis.json").exists():
                            iteration.copy_small(target, root, root / "analysis.json")
                        artifacts.register(self.store, eid, phase, root, ["analysis.json"])
                    else:
                        (self.root(eid) / "analysis.json").write_text(encode(analysis.model_dump()))
                        artifacts.register(self.store, eid, phase, self.root(eid), ["analysis.json"])
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
                            [(time.time(), eid, identity) for identity in captured],
                        )
                        self.store.emit(db, eid, "analysis_saved", {})
                self.store.stage(eid, phase, "complete")
                self.store.update(eid, hold_reason=None, error=None)
                return
            except ValueError as exc:
                self._action_state(
                    action["id"], "failed", error=f"agent output validation: {self.validation_feedback(exc)}"
                )
                action = self.store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))
        if action["state"] in ("running", "dispatching"):
            if self.agent_task:
                if exp["pause_agent"] and action["turn_id"]:
                    self.hold_active_action(action, "agent_paused")
                    await self.agent.interrupt(action["thread_id"], action["turn_id"])
                return
            if action["thread_id"] and action["turn_id"] and self.agent_ready:
                try:
                    result = await self.agent.read_turn(action["thread_id"], action["turn_id"])
                    self._action_state(action["id"], "completed", result=result)
                    return
                except TerminalTurnError as exc:
                    self._action_state(action["id"], "failed", error=str(exc))
                    return
                except Exception as exc:
                    self._action_state(action["id"], "uncertain", error=str(exc))
            else:
                self._action_state(action["id"], "uncertain", error="dispatch response lost")
            self.store.update(eid, hold_reason="agent_dispatch_uncertain")
            return
        if action["state"] == "uncertain" and self.input_rejected(action):
            self._action_state(action["id"], "failed", error=action["error"])
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
                    if time.time() - action["updated_at"] < 60:
                        self.store.update(eid, hold_reason="agent_prerequisite", error=action["error"])
                        return
                    self._action_state(action["id"], "pending", error=action["error"])
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
                    if repeats >= self.config.codex.identical_error_limit and action.get(
                        "interrupt_reason"
                    ) not in ("quota_hold", "agent_paused", "owner_ceiling"):
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
            else:
                if action["attempts"] >= 2:
                    self.fail(eid, "agent_recovery_exhausted", action["error"])
                    return
                self._action_state(action["id"], "pending")
        if exp["pause_agent"]:
            self.store.update(eid, hold_reason="agent_paused")
            return
        if exp["pending_request"]:
            self.store.update(eid, hold_reason="needs_attention")
            return
        if not self.agent_ready:
            self.store.update(eid, hold_reason="agent_unavailable", error=self.agent_error)
            return
        if self.agent_task:
            self.store.update(eid, hold_reason="agent_busy")
            return
        active_sql = (
            "SELECT 1 FROM agent_actions WHERE id!=? AND state IN ('dispatching','running','uncertain')"
        )
        active_args = (action["id"],)
        if self.config.codex.max_sessions > 1:
            active_sql += " AND experiment_id=?"
            active_args += (eid,)
        if self.store.one(active_sql, active_args):
            self.store.update(eid, hold_reason="agent_busy")
            return
        if phase == "prepare" and exp["recipe_id"] == "agent_experiment_v1":
            if not await self.prepare_workspace(exp):
                return
        decision = self.gate.decide()
        if not decision.admitted:
            self.store.update(eid, hold_reason="quota_hold", error=decision.reason)
            return
        self.active_action = action["id"]
        self.agent_task = asyncio.create_task(self.dispatch(action))

    async def prepare_workspace(self, exp):
        eid = exp["id"]
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
            and not self.workspaces.row(row["id"])
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

    def reusable_stages(self, exp, plan):
        if iteration.enabled(exp) and exp["contract"].get("auto_repair"):
            return iteration.reuse(self, exp, plan)
        repair = exp["contract"].get("auto_repair")
        if not repair:
            if plan.reuse_stages:
                raise ValueError("stage reuse requires saved successful work in this experiment")
            return {}
        old = repair["snapshot"]
        prior_root = Path(
            repair.get("previous_root")
            or (
                self.projects.base_root(exp["id"])
                if repair.get("previous_revision", 1) == 1
                else self.projects.base_root(exp["id"]) / "repairs" / str(repair["previous_revision"])
            )
        )
        old_plan = Implementation.model_validate(old["plan"])
        old_graph = stage_dependencies(old_plan.stages, old_plan.schema_version)
        completed = set(repair.get("completed_stages", []))
        invalid = set(old_graph) - completed
        changed = True
        while changed:
            expanded = invalid | {stage for stage, needs in old_graph.items() if set(needs) & invalid}
            changed, invalid = expanded != invalid, expanded
        eligible = completed - invalid
        same_protocol = (
            plan.protocol == old_plan.protocol
            and json.loads(plan.parameters_json) == json.loads(old_plan.parameters_json)
            and plan.synthetic_demo == old_plan.synthetic_demo
        )
        same_sources = all(
            name in plan.files
            and artifacts.sha256(
                artifacts.allowed_path(self.working_directory(exp) / name, self.working_directory(exp))
            )
            == old["files"][name]
            for name in old_plan.files
        ) and set(plan.files) == set(old_plan.files)
        selected = (
            set(plan.reuse_stages)
            if plan.reuse_stages is not None
            else eligible
            if same_sources and same_protocol
            else set()
        )
        if selected - eligible or selected and not same_protocol:
            raise ValueError(
                "reuse requires successful stages outside the failed dependency branch and the same measured protocol/parameters"
            )
        reused = {}
        for stage in plan.stages:
            if stage.id not in selected:
                continue
            prior = next(s for s in old_plan.stages if s.id == stage.id)
            if (stage.outputs, stage.local_outputs, stage.temporary_outputs, stage.needs) != (
                prior.outputs,
                prior.local_outputs,
                prior.temporary_outputs,
                prior.needs,
            ):
                raise ValueError(f"reused stage outputs/dependencies changed: {stage.id}")
            if set(old_graph[stage.id]) - selected:
                raise ValueError(f"reused stage needs an invalidated predecessor: {stage.id}")
            job = next(
                (j for j in repair["jobs"] if j["stage_id"] == stage.id and j["state"] == "complete"), None
            )
            if not job:
                raise ValueError(f"reused stage has no successful exact job: {stage.id}")
            artifacts.validate_stage(prior_root, stage.id, old)
            names = stage.outputs + artifacts.seal_local_outputs(prior_root, stage.id, old)
            for name in names:
                record = self.store.one(
                    "SELECT * FROM artifacts WHERE experiment_id=? AND name=? AND evidence_revision=?",
                    (exp["id"], name, repair["previous_revision"]),
                )
                source = artifacts.allowed_path(prior_root / name, prior_root)
                if not record or artifacts.sha256(source) != record["sha256"]:
                    raise ValueError(f"reused output lacks unchanged registered evidence: {name}")
                target = self.root(exp["id"]) / name
                target.parent.mkdir(parents=True, exist_ok=True)
                if target.exists():
                    if (
                        artifacts.sha256(artifacts.allowed_path(target, self.root(exp["id"])))
                        != record["sha256"]
                    ):
                        raise ValueError(f"reused output copy changed: {name}")
                else:
                    shutil.copyfile(source, target)
                    target.chmod(0o444)
            reused[stage.id] = {
                **(
                    {
                        "remote_root": json.loads(job["execution_manifest"])["execution"]["layout"][
                            "remote_run"
                        ]
                    }
                    if old.get("remote_execution")
                    else {}
                ),
                "root": str(prior_root),
                "snapshot_hash": old["snapshot_hash"],
                "intent_id": job["intent_id"],
                "outputs": names,
            }
        return reused

    def adopt_reused_stages(self, eid, reused):
        exp = self.store.experiment(eid)
        contract = exp["contract"]
        contract["reused_job_intents"] = [stage["intent_id"] for stage in reused.values()]
        with self.store.tx() as db:
            for stage, saved in reused.items():
                artifacts.register(self.store, eid, stage, self.root(eid), saved["outputs"], _db=db)
                db.execute(
                    "UPDATE stages SET state='complete',output_manifest=?,updated_at=? WHERE experiment_id=? AND id=?",
                    (encode(saved["outputs"]), time.time(), eid, stage),
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
        if exp["recipe_id"] == "agent_experiment_v1":
            return [stage["id"] for stage in exp["snapshot"]["plan"]["stages"]]
        return COMPUTE_STAGES

    @staticmethod
    async def dependencies(root, python=None):
        # Query the experiment's environment; do not export direct URLs or installation credentials.
        return (
            await command(
                str(python or root / "work/.venv/bin/python"),
                "-c",
                "import importlib.metadata as m; print('\\n'.join(sorted(d.metadata['Name']+'=='+d.version for d in m.distributions())))",
            )
            + "\n"
        )

    def prepared_python(self, exp, plan):
        if self.is_remote(exp):
            if any(not stage.script for stage in plan.stages):
                raise ValueError(
                    "Jean Zay stages must declare .sbatch or .sh scripts that set up their remote runtime"
                )
            return Path(sys.executable)

        candidate = (
            Path(plan.python_executable)
            if plan.python_executable
            else self.working_directory(exp) / ".venv/bin/python"
        )
        project = self.project_context(exp)["checkout"]
        allowed = [self.working_directory(exp)] + ([Path(project)] if project else [])
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

    def stage_resources(self, exp, stage):
        resources = self.config.slurm.defaults()
        if exp["recipe_id"] == "agent_experiment_v1":
            saved = next(s for s in exp["snapshot"]["plan"]["stages"] if s["id"] == stage)
            if saved.get("resources"):
                resources = JobResources.model_validate(saved["resources"])
        if exp["contract"].get("resource_policy_version") != 2 or self.config.mode == "demo":
            if (
                resources.gpus_per_job
                or resources.nodes != 1
                or resources.tasks_per_node != 1
                or resources.cpus_per_job != 1
                or resources.walltime_minutes_per_job > 5
            ):
                raise ValueError("legacy/demo experiments retain their short single-CPU resource contract")
        self.config.slurm.limits.validate_request(resources)
        cap = self.config.slurm.limits.gpu_hours_per_experiment
        if cap is not None:
            reserved = sum(
                json.loads(j["resources"])["gpus_per_job"]
                * json.loads(j["resources"])["walltime_minutes_per_job"]
                / 60
                for j in self.store.rows("SELECT resources FROM jobs WHERE experiment_id=?", (exp["id"],))
            )
            if reserved + resources.gpus_per_job * resources.walltime_minutes_per_job / 60 > cap:
                raise ValueError("experiment GPU-hour budget exhausted")
        return resources

    def validate_resource_plan(self, exp, plan):
        if iteration.enabled(exp) and plan.schema_version != 3:
            raise ValueError("new execution contract requires schema_version 3")
        if not iteration.enabled(exp) and plan.schema_version == 3:
            raise ValueError("saved historical contracts cannot switch execution semantics")
        budget = exp["contract"].get("max_jobs", self.config.slurm.max_jobs_per_experiment)
        if budget is not None and len(plan.stages) > budget:
            raise ValueError(f"plan exceeds the {budget}-job experiment budget")
        for stage in plan.stages:
            if (
                self.config.mode == "live"
                and exp["contract"].get("resource_policy_version") == 2
                and stage.resources is None
            ):
                raise ValueError("every live stage must declare its Slurm resources")
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
            )
            > cap
        ):
            raise ValueError("plan exceeds the experiment GPU-hour budget")

    def project_context(self, exp):
        identity = exp["contract"].get("project", {"id": "test-project", "name": "Test project"})
        saved = self.projects.workspace(exp["id"])
        project = self.store.projects.get(identity["id"])
        if saved:
            checkout = self.workspaces.checkout(exp["id"])
        elif project and project.public()["available"]:
            checkout = str(project.checkout.resolve()) if project.checkout else None
        else:
            raise ValueError("selected project checkout is unavailable; restore its configuration")
        isolated = self.workspaces.row(exp["id"])
        return {
            **identity,
            "checkout": checkout,
            "access": "read-write" if saved else "read-only",
            "isolation": "worktree" if isolated else "shared",
            "source_checkout": saved["folder"] if saved else checkout,
            "base_commit": isolated["head"] if isolated else None,
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

    def iteration_prompt(self, exp, phase, payload):
        root = self.root(exp["id"])
        payload.update(
            contract=self.compact_contract(exp, exp["contract"]),
            project_context=self.project_context(exp),
            project_instructions=self.projects.instructions(
                exp["contract"].get("project", {}).get("id", "test-project")
            ),
            workspace=str(self.working_directory(exp)),
            python=str(self.working_directory(exp) / ".venv/bin/python"),
            execution_context=self.execution_context(exp),
            cluster=self.cluster_for(exp),
            resources=self.config.slurm.model_dump(),
            approved_plan=exp.get("plan_review"),
            plan_revision_feedback=(exp.get("plan_review") or {}).get("feedback"),
            output_refs=self.store.rows("SELECT * FROM output_refs WHERE experiment_id=?", (exp["id"],)),
            jobs=self.controller.status(exp["id"]),
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
            "Use check_manifest before finishing, job_status for exact logs, submit_stage for prepared work, "
            "register_result for outputs, update_report for incremental working reports and schedule_wakeup for continuation. Return the requested JSON schema. "
        )
        if phase == "plan":
            instruction += (
                "This turn is read-only planning: no edits, installs, jobs or training. Return schema_version 3, "
                "title, concise summary, protocol and stages with resources and explicit needs ([] for roots). "
                "Plan evaluation, plotting and extra analysis as separate resumable tasks. There is no universal "
                "final verification stage or all-stage dependency requirement. Dependencies express needed inputs. "
                "Initial plan approval is required. For essential missing inputs return questions and stages: []. "
                "Apply plan revision feedback. Do not add seeds, bootstrap, independent recomputation, raw prediction "
                "retention or exhaustive checks unless scientifically requested or useful for the approved task."
            )
        elif phase == "prepare":
            payload["repair_context"] = (
                self.context_pointer(exp, "repair-context", exp["contract"].get("auto_repair"))
                if exp["contract"].get("auto_repair")
                else None
            )
            payload["researcher_notes"] = self.store.rows(
                "SELECT author,text FROM notes WHERE experiment_id=? ORDER BY created_at DESC LIMIT 20",
                (exp["id"],),
            )
            payload["output_schema"] = ExperimentResult.model_json_schema()
            instruction += (
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
                "work; tensorboard may select one scalar and one image tag under tensorboard/STAGE. "
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
                + reporting.skill_text()
            )
        return instruction + "\n" + self.execution_instruction(exp, phase) + "\nPAYLOAD:\n" + encode(payload)

    def generic_prompt(self, exp, phase, payload):
        if iteration.enabled(exp):
            return self.iteration_prompt(exp, phase, payload)
        root = self.root(exp["id"])
        payload["execution_context"] = self.execution_context(exp)
        payload["project_knowledge"] = self.supervisor.context(
            exp["contract"].get("project", {}).get("id", "test-project")
        )
        payload["contract"] = self.compact_contract(exp, exp["contract"])
        payload["context_instructions"] = (
            "Context summaries preserve the scientific brief and protocol. details.path points to full "
            "saved JSON with sha256 identity. Read only the fields or source files needed for this task; "
            "do not dump whole reference records or repair histories into the conversation. Reuse existing "
            "project/reference training and evaluation code where suitable; implement the requested delta."
        )
        payload["tracker_tools"] = (
            "Use the experimenttracker MCP tools: check_manifest before finishing; job_status for exact owned logs; schedule_wakeup for continuation; submit_stage for tracked prepared work; register_analysis_file for figure evidence IDs. Never submit scheduler work outside the tracker."
        )
        if phase in ("plan", "prepare"):
            payload["project_context"] = self.project_context(exp)
            if payload["project_context"]["isolation"] == "worktree":
                payload["isolation_instructions"] = (
                    "Implement only in project_context.checkout and workspace. The original source_checkout "
                    "belongs to other experiments and is read-only context. Do not change shared Git metadata, "
                    "switch HEAD, merge, reset or remove this worktree. Use an isolated environment; never modify "
                    "the original project's environment. Reference external datasets/checkpoints read-only and "
                    "record their identities. Ignored runtime files and environments are not copied. "
                    "Embedded repositories listed in read_only_repositories remain in source_checkout as "
                    "read-only inputs. If their code needs changes, first copy the needed source into checkout, "
                    "excluding Git metadata/environments, and freeze those copies in declared project_files."
                )
            payload["project_instructions"] = self.projects.instructions(payload["project_context"]["id"])
            review = exp.get("plan_review") or {}
            if phase == "plan" and review.get("state") == "drafting" and review.get("feedback"):
                payload["plan_revision_feedback"] = review["feedback"]
                payload["previous_plan"] = self.compact_plan(exp, "previous-plan", review)
            if phase == "prepare":
                repair = exp["contract"].get("auto_repair")
                if repair:
                    previous_root = (
                        Path(repair["previous_root"])
                        if repair.get("previous_root")
                        else self.projects.base_root(exp["id"])
                    )
                    previous_revision = repair.get("previous_revision", 1)
                    if previous_revision != 1 and not repair.get("previous_root"):
                        previous_root = previous_root / "repairs" / str(previous_revision)
                    payload["repair_context"] = {
                        **repair,
                        "researcher_notes": self.store.rows(
                            "SELECT author,text FROM notes WHERE experiment_id=? ORDER BY created_at DESC LIMIT 20",
                            (exp["id"],),
                        ),
                        "previous_workspace": str(self.working_directory(exp)),
                        "reusable_stages": repair.get("completed_stages", []),
                        "previous_snapshot": str(previous_root / "snapshot"),
                        "previous_outputs": str(previous_root),
                    }
                    payload["repair_instructions"] = (
                        "Diagnose the saved failed job using its logs and previous sources. Repair the "
                        "implementation in the stable workspace; previous snapshots and registered evidence "
                        "are immutable. Preserve the brief and scientific objective. "
                        "Do not weaken validation to make a run pass. Explain the cause and fix in summary. "
                        "Do not submit untracked jobs. Routine code/resource/dependency corrections are autonomous within authorization. "
                        "Completed manifest errors receive continuing validation feedback separately from compute attempts. "
                        "For TIMEOUT, compare measured progress with the saved walltime and inspect "
                        "checkpoint/resume support. Fix code or runtime parameters without reducing "
                        "the approved scientific scope. You may propose a longer walltime or different "
                        "authorized allocation in the manifest. Reuse checked completed stages through reuse_stages; failed stages and descendants must rerun. Read prior checkpoint paths from repair_context for resume; do not copy or retrain successful models unnecessarily. "
                        "Partial outputs are diagnostic context, never validated final evidence."
                    )
                payload["approved_plan"] = self.compact_plan(exp, "approved-plan", review)
                payload["manifest_constraints"] = {
                    "reserved_stage_ids": ["plan", "prepare", "validate", "interpret", "publish"],
                    "reserved_filenames": [
                        "manifest.json",
                        "preparation.json",
                        "analysis.json",
                        "report.html",
                        "requirements.txt",
                    ],
                    "reserved_prefixes": ["local-output-", "project-source-", "report-v<integer>"],
                    "source_paths": "Safe relative package paths; no traversal or symlinks; __init__.py supported",
                    "output_paths": "Unique safe flat filenames, separate from sources, at most 10 MiB each",
                    "dependencies": "Version 2 declares needs for every stage; final stage transitively depends on all others",
                    "runtime": "A runnable interpreter in this experiment, its selected project, or an exact previously saved runtime",
                }
                payload["output_schemas"] = {
                    "results.json": ExperimentResult.model_json_schema(),
                    "validation.json": ExperimentValidation.model_json_schema(),
                }
            payload["workspace"] = str(self.working_directory(exp))
            payload["python"] = str(self.working_directory(exp) / ".venv/bin/python")
            payload["resources"] = self.config.slurm.model_dump()
            payload["cluster"] = self.cluster_for(exp)
            payload["execution_mode"] = self.config.mode
            payload["resource_policy_version"] = exp["contract"].get("resource_policy_version", 1)
            checkout = payload["project_context"]["checkout"]
            payload["project_python"] = (
                str(Path(checkout) / ".venv/bin/python")
                if checkout and (Path(checkout) / ".venv/bin/python").is_file()
                else None
            )
            if exp["reference_experiment_id"]:
                parent = self.store.experiment(exp["reference_experiment_id"])
                records = self.store.rows(
                    "SELECT * FROM artifacts WHERE experiment_id=? AND evidence_revision=?",
                    (parent["id"], parent["evidence_revision"]),
                )
                for record in records:
                    path = artifacts.allowed_path(Path(record["path"]), self.root(parent["id"]))
                    if artifacts.sha256(path) != record["sha256"]:
                        raise ValueError("reference evidence has changed")
                artifacts.verify_snapshot(self.root(parent["id"]), parent["snapshot"])
                reference = {
                    "brief": parent["brief"],
                    "contract": parent["contract"],
                    "manifest": parent["snapshot"],
                    "analysis": parent["analysis"],
                    "artifacts": [
                        {key: record[key] for key in ("name", "path", "sha256")} for record in records
                    ],
                    "source_directory": str(self.root(parent["id"]) / "snapshot"),
                }
                analysis = parent["analysis"] or {}
                payload["reference_context"] = {
                    "experiment_id": parent["id"],
                    "title": parent.get("title"),
                    "brief": parent["brief"],
                    "scientific_outcome": parent.get("scientific_outcome"),
                    "contract": self.compact_contract(exp, parent["contract"]),
                    "plan": self.compact_plan(exp, "reference-plan", parent["snapshot"].get("plan", {})),
                    "analysis": {
                        key: analysis[key]
                        for key in ("summary", "observations", "limitations", "scientific_outcome")
                        if key in analysis
                    },
                    "source_directory": reference["source_directory"],
                    "details": self.context_pointer(exp, "reference", reference),
                }
            if phase == "plan":
                instruction = (
                    "Produce a concise research plan for the researcher's brief before implementing any code. "
                    "Read the selected project and its AGENTS.md as needed, but do not edit files, install packages, "
                    "run training, or submit jobs in this turn. Follow project_instructions as researcher guidance. "
                    "If plan_revision_feedback is present, revise the previous plan to address it. "
                    "For a corrected replacement, preserve prior requests in contract.restart_context while applying the latest correction. "
                    "State the intended scientific protocol and its key assumptions explicitly, including input scale, "
                    "baselines, validation metrics, comparisons, and figures where relevant. Return schema_version 2 "
                    "with explicit needs for every compute stage: [] for a root, otherwise stage IDs whose validated "
                    "outputs it requires. Independent stages may run concurrently. The final listed stage must produce "
                    "results and validation and depend transitively on every other stage. "
                    "Use descriptive compute IDs; plan, prepare, validate, interpret and publish are executor names "
                    "and will be namespaced before review. "
                    "Give each stage a structured Slurm resource request from the fresh cluster snapshot and policy. "
                    "Prefer all currently viable GPU types when exact hardware is unnecessary so queue wait is reduced. "
                    "Do not pin prior hardware solely for comparability. If a critical input is missing, finish this turn "
                    "with questions (a list of concise question strings) and stages: []. Include title, summary and "
                    "protocol explaining what is missing. Do not use request_user_input or wait for an answer in this turn. "
                    "The tracker saves the questions and resumes planning from the researcher answer. "
                    "For a complete plan, omit questions or return questions: [] and include compute stages. "
                    "Keep discovery focused on essential inputs so the first review arrives quickly. Return a short title, "
                    "a summary of at most three short sentences (600 characters), protocol, and one-sentence stage summaries."
                )
                return (
                    instruction
                    + "\n"
                    + self.execution_instruction(exp, phase)
                    + "\nPAYLOAD:\n"
                    + encode(payload)
                )
            instruction = (
                "Implement the researcher's requested experiment in the workspace. The brief is your instruction; "
                "there is no whitelist of models, datasets, or visualizations. Read the selected project checkout and its "
                "AGENTS.md when supplied. If project_context.access is read-write, the researcher authorizes you to "
                "edit the selected project and run its tools there to implement the brief. Work from that folder, "
                "preserve unrelated changes, and keep tracking outputs in the supplied experiment workspace. "
                "If access is read-only, use the checkout only as context. Implement the actual requested project "
                "task rather than substituting a synthetic fixture. "
                "Follow project_instructions as researcher guidance for this selected project. "
                "Implement the approved scientific objective. Routine implementation, graph and authorized resource corrections are autonomous. A material objective change must be explained in objective_change for user review. "
                "If required project inputs or resources are unavailable, ask the researcher rather than inventing data. "
                "Use the reference's actual code/data/evidence "
                "as context, preserving comparability where appropriate, and perform the requested change. Do not repeat "
                "the reference experiment or move the requested work into future ideas. Explain assumptions in protocol. "
                "Write source files and a compute stage graph implementing the requested work. Use the approved "
                "schema_version and explicit dependency graph. Independent stages may run concurrently, so each "
                "stage must write only its own declared unique outputs and local checkpoint paths. Read dependencies "
                "only after their outputs are validated; never mutate a sibling's or predecessor's outputs. "
                "A stage can run recipe.py with STAGE --output ROOT, or declare script as a prepared .sh filename. "
                "Scripts run with Bash and EXPERIMENT_ROOT, EXPERIMENT_PROJECT, EXPERIMENT_PYTHON and EXPERIMENT_STAGE. "
                "For stages where learning curves or qualitative examples are useful (training, head fitting, etc.), "
                "set up normal TensorBoard logging using the project's writer (install tensorboard in the stage runtime if needed). "
                "Declare stage tensorboard: {log_dir: 'tensorboard/STAGE', scalar_tag: 'loss/train', image_tag: 'prediction'} "
                "with the exact tags you actually emit; either tag may be null. Write event files directly to "
                "EXPERIMENT_ROOT/log_dir, use a unique directory per stage, flush periodically and close the writer. "
                "Select ONE useful time series and ONE useful image; for add_images prefer a single representative example "
                "or a legible small grid. Inspect emitted tags and declare any suffix used by your writer. "
                "These are sampled progress previews, not validated scientific evidence. Keep final results/figures separately. "
                "Instrument long-running stages with measurable whole-stage progress (epochs/steps/files across ALL "
                "loops, seeds and variants, including setup/finalization where practical). Every 15-30 seconds, "
                "print a flushed standalone stdout line starting ET_PROGRESS followed by JSON: "
                '{"stage": os.environ["EXPERIMENT_STAGE"], "completed": done, "total": total, '
                '"timestamp": time.time(), "eta_seconds": remaining_seconds_or_null}. '
                "Use monotonically increasing completed work against a stable whole-stage total; do not reset "
                "at each epoch or variant. Estimate remaining seconds from measured throughput, accounting for "
                "remaining phases; use null during warmup or when ETA is unreliable. Emit the initial zero and "
                "final total as well. Only rank zero emits for distributed stages. Keep tqdm if useful, but also "
                "emit these structured lines; no custom regex is needed. Never invent progress for unmeasurable work. "
                "Use srun/torchrun inside a managed allocation for distributed tasks; never submit nested jobs. "
                "You may reuse project_python by setting python_executable, or install into the isolated workspace environment. "
                "Never change an existing shared project environment. Declare project_files for all existing project source/config "
                "files executed/imported by your stages; the backend records their bytes for each submitted revision. "
                "You may use tools to read context, write source, install ordinary dependencies in that environment, and "
                "run tiny local checks. Keep packages/caches/temporary files inside workspace; never modify other "
                "experiments, unrelated research checkouts, credentials, global environments, or services. The selected "
                "writable project is authorized. No API-billed services. "
                "Do not submit scheduler jobs: the executor runs your declared stages. For live resource_policy_version 2, "
                "inspect the supplied fresh cluster snapshot (nodes, GPU types, node states, partition limits and associations). "
                "You may run read-only scheduler queries for more context. Select resources independently for every stage: "
                "partition, account/QOS, CPUs per task (cpus_per_job), memory MiB per node, walltime minutes, total GPUs, "
                "gpu_type, nodes, tasks_per_node and optional constraint/exclusive. Choose resources based on the brief. "
                "12-hour GPU training is supported when permitted by Slurm and the optional resources.limits policy. "
                "Null limits delegate to cluster authorization; defaults are fallbacks, not maxima. There is no three-job "
                "limit for new live plans unless max_jobs_per_experiment is configured. Version 2 stages execute "
                "when all declared dependencies have validated; version 1 plans remain sequential. "
                "A busy node may queue; do not claim drained/down nodes are usable or silently substitute hardware when "
                "the requested scientific protocol needs a specific GPU. For demo or legacy contracts retain three short CPU jobs/no GPU. "
                "When several GPU types meet the stage's actual compute and memory needs, prefer a request that can use "
                "the available capable types and reduce queue time. Record any hardware-specific reproducibility tradeoff "
                "in the protocol. Do not pin a previous GPU solely for comparability when the brief does not require it. "
                "Sources may use normal nested Python package layouts, including __init__.py. Submitted revisions keep their own recorded source identity while future implementations remain editable. "
                "Declare published evidence outputs with unique flat filenames, at most 10 MiB each. Large checkpoints or "
                "retained outputs belong in local_outputs, with safe paths relative to EXPERIMENT_ROOT. Each may be a "
                "regular file or a directory: directories are recursively sealed with all file hashes, sizes and membership. "
                "Symlinks and special files are forbidden. These stay on the cluster; only their provenance is published. "
                "Never modify, add or remove anything in a previous stage's sealed output directory, including Python "
                "bytecode/cache files. Keep mutable caches in separate undeclared directories. "
                "Declare regenerable feature caches and other disposable intermediates in temporary_outputs, using paths "
                "under EXPERIMENT_ROOT/temporary/ (e.g. temporary/features). Do not put them in local_outputs. "
                "They are not hashed or published; keep them available to downstream stages and repairs. The backend "
                "deletes them only after successful interpretation and report publication. Preserve small audit measurements, "
                "predictions needed to check metrics and selected valuable checkpoints as evidence instead. "
                "Save resolved parameters and reproducible seeds; preserve raw measurements/predictions needed to audit metrics. "
                "The final stage MUST write results.json exactly with schema_version:1, protocol (exactly your returned protocol), "
                "and metrics:[{name,variant,value (finite number),unit}]. Follow output_schemas, including list and string "
                "bounds; keep exhaustive data exceeding these bounds in separately declared evidence outputs. "
                "Also write validation.json with schema_version:1, "
                "passed:boolean, checks:[{name,passed:boolean,detail}]. Implement meaningful scientific checks (e.g. held-out "
                "separation, independent metric recomputation, finite predictions) and raise on failure. A zero job exit alone "
                "does not establish validity. Declare requested plots in figures, using SVG or PNG. Label synthetic data honestly. "
                "Return the implementation JSON only after writing and checking the source. Copy approved_plan.protocol "
                "when the scientific protocol is unchanged. parameters_json encodes an object. "
                "Use your judgment for optional choices; ask only if a missing essential input makes the experiment impossible."
            )
        else:
            for stage in self.compute_stages(exp):
                artifacts.validate_stage(
                    root,
                    stage,
                    exp["snapshot"],
                    verify_local_outputs=bool(exp["contract"].get("strict_reproducibility")),
                )
            payload["project_instructions"] = self.projects.instructions(
                exp["contract"].get("project", {}).get("id", "test-project")
            )
            payload["results"] = artifacts.read_json(root / "results.json", root)
            validation = artifacts.read_json(root / "validation.json", root)
            payload["validation"] = {
                "passed": validation["passed"],
                "check_count": len(validation["checks"]),
                "failed_checks": [check["name"] for check in validation["checks"] if not check["passed"]][
                    :20
                ],
                "details": {
                    "path": str(root / "validation.json"),
                    "sha256": artifacts.sha256(root / "validation.json"),
                },
            }
            payload["evidence_ids"] = [
                row["id"]
                for row in self.store.rows(
                    "SELECT id FROM artifacts WHERE experiment_id=? AND evidence_revision=?",
                    (exp["id"], exp["evidence_revision"]),
                )
            ]
            payload["analysis_directory"] = str(root / "analysis")
            payload["evidence_revision"] = exp["evidence_revision"]
            payload["artifacts"] = self.store.rows(
                "SELECT id,name,path,sha256,size FROM artifacts WHERE experiment_id=? AND evidence_revision=?",
                (exp["id"], exp["evidence_revision"]),
            )
            instruction = (
                "Interpret the completed requested experiment from its validated evidence and frozen source. "
                "Use normal Python/scripting tools in analysis_directory to finish figures and the report. Raw results and registered evidence remain read-only. Do not submit untracked jobs or launch new experiments. Return new safe flat filenames in analysis_files. Their evidence IDs are UUID5(NAMESPACE_URL, f'{experiment_id}:{evidence_revision}:analysis-{filename}'). "
                "Cite evidence_ids for every observation. Distinguish measured outcomes, assumptions, and limitations. "
                "Follow project_instructions as researcher guidance when interpreting and presenting the evidence. "
                "Validation is summarized in the prompt; inspect validation.details.path selectively for the "
                "scientific checks relevant to a claim. Verify its sha256 before use and do not dump all checks "
                "into the conversation. "
                "Do not label synthetic results as real scientific findings. Saved next ideas are suggestions, not the original request. "
                "Address steering_guidance in the report when the saved evidence supports it; create requested figures from "
                "available evidence when possible. Explain when a request needs different training or missing data. "
                "Use the presenter skill below and return the analysis JSON including presentation.\n"
                + reporting.skill_text()
            )
        return instruction + "\n" + self.execution_instruction(exp, phase) + "\nPAYLOAD:\n" + encode(payload)

    def prompt(self, exp, phase):
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
        if exp["recipe_id"] == "agent_experiment_v1":
            return self.generic_prompt(exp, phase, payload)
        if phase == "interpret":
            # Revalidate the evidence immediately before giving it to the model.
            artifacts.validate_stage(self.root(exp["id"]), "evaluate_and_plot", exp["snapshot"])
            payload["results"] = artifacts.read_json(
                self.root(exp["id"]) / "results.json", self.root(exp["id"])
            )
            payload["evidence_ids"] = [
                a["id"]
                for a in self.store.rows(
                    "SELECT id FROM artifacts WHERE experiment_id=? AND evidence_revision=?",
                    (exp["id"], exp["evidence_revision"]),
                )
            ]
            payload["artifacts"] = self.store.rows(
                "SELECT id,name,stage_id,sha256,size FROM artifacts WHERE experiment_id=? AND evidence_revision=?",
                (exp["id"], exp["evidence_revision"]),
            )
        instruction = (
            (
                "First determine whether this runnable recipe satisfies the researcher's brief. The only "
                "available recipe fits linear and quadratic regression to the fixed one-dimensional synthetic "
                "quadratic dataset and plots their predictions. It cannot train decision trees, other model "
                "families, change data/parameters, or produce a classification decision frontier. The brief is "
                "the requested experiment, not merely context. A reference is context and provenance, not "
                "an instruction to repeat it. If the brief requests unsupported work, return scope_status "
                "'unsupported' with scope_reason explaining the mismatch; the backend will stop before compute. "
                "Do not substitute the fixed comparison, defer an explicit request into next ideas, or claim "
                "that the researcher did not authorize what they asked for. For a compatible request return "
                "scope_status 'supported' with a concrete scope_reason. In both cases return the exact "
                "snapshot_hash, parameters_hash and recipe_id from the manifest, plus a short summary."
            )
            if phase == "prepare"
            else (
                "Interpret ONLY the validated synthetic results below. Identify measured facts, limitations, "
                "and saved next ideas. Every observation must cite IDs from evidence_ids. The target function "
                "is quadratic by construction; one seed says nothing about real data. Never launch new research."
            )
        )
        return (
            instruction
            + " Return only the requested JSON schema. Do not modify files, run tools, submit jobs, "
            "use external services, or ask optional questions. The backend owns all execution.\nPAYLOAD:\n"
            + encode(payload)
        )

    def _action_state(self, action_id, state, *, error=None, result=None):
        action = self.store.one("SELECT * FROM agent_actions WHERE id=?", (action_id,))
        saved_result = encode(result) if result is not None else action["result"]
        if (action["state"], action["error"], action["result"]) == (state, error, saved_result):
            return
        with self.store.tx() as db:
            db.execute(
                "UPDATE agent_actions SET state=?,error=?,result=?,updated_at=? WHERE id=?",
                (
                    state,
                    error,
                    saved_result,
                    time.time(),
                    action_id,
                ),
            )
            self.store.emit(
                db, action["experiment_id"], "agent_action_changed", {"action_id": action_id, "state": state}
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
            self.prepared_python(exp, plan)
            for name in plan.files:
                artifacts.allowed_path(self.working_directory(exp) / name, self.working_directory(exp))
        # Adoption, phase and outbox are one transaction. Attempts and identities
        # remain untouched; the normal owner performs full freezing/validation.
        with self.store.tx() as db:
            db.execute(
                "UPDATE agent_actions SET state='completed',error=NULL,updated_at=? WHERE id=?",
                (time.time(), action["id"]),
            )
            db.execute(
                "UPDATE experiments SET phase='preparing',hold_reason=NULL,error=NULL WHERE id=?", (eid,)
            )
            db.execute(
                "UPDATE stages SET state='pending',error=NULL,updated_at=? WHERE experiment_id=? AND id=?",
                (time.time(), eid, action["phase"]),
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
        action = self.store.one("SELECT * FROM agent_actions WHERE id=?", (action_id,))
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
                or action["phase"] not in ("plan", "prepare", "interpret")
            ):
                raise NeedsAttention("missing turn recovery requires one saved research dispatch")
            if self.store.one(
                "SELECT id FROM agent_actions WHERE thread_id=? AND turn_id=?", (action["thread_id"], turn_id)
            ):
                raise NeedsAttention("turn already belongs to a saved action")
            intents = [
                json.loads(event["payload"])
                for event in self.store.rows(
                    "SELECT payload FROM events WHERE experiment_id=? AND kind='agent_dispatch_intent' ORDER BY seq",
                    (action["experiment_id"],),
                )
            ]
            intents = [event for event in intents if event.get("action_id") == action_id]
            if len(intents) != 1 or intents[0].get("thread_id") != action["thread_id"]:
                raise NeedsAttention("exact dispatch intent is missing or ambiguous")
            prompt_hash = intents[0].get("prompt_sha256")
            if not prompt_hash:
                # Historical continuations prepended their predecessor's saved
                # terminal error. Do not guess from the overwritten timeout.
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
                prompt_hash = hashlib.sha256(prompt.encode()).hexdigest()
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
            self._action_state(action_id, "completed", result=result)
            return {
                "action_id": action_id,
                "thread_id": action["thread_id"],
                "turn_id": action["turn_id"],
                "state": "completed",
            }
        elif action["phase"] in ("title", "summarize_title"):
            TitleSummary.model_validate(result)
        elif action["phase"] == "monitor":
            ControllerDecision.model_validate(result)
        elif action["phase"] == "plan" and exp["recipe_id"] == "agent_experiment_v1":
            ResearchPlan.model_validate(result)
        elif action["phase"] == "prepare" and exp["recipe_id"] == "agent_experiment_v1":
            plan = Implementation.model_validate(result)
            for name in plan.files:
                artifacts.allowed_path(self.working_directory(exp) / name, self.working_directory(exp))
        elif action["phase"] == "prepare":
            prep = Preparation.model_validate(result)
            if (
                prep.snapshot_hash != exp["snapshot"]["snapshot_hash"]
                or prep.parameters_hash != exp["snapshot"]["parameters_hash"]
            ):
                raise ValueError("saved preparation does not match the frozen experiment")
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
                db.execute(
                    "UPDATE agent_actions SET turn_id=?,state='completed',result=?,error=NULL,updated_at=? WHERE id=? AND turn_id IS NULL",
                    (turn_id, encode(result), time.time(), action_id),
                )
                db.execute(
                    "UPDATE experiments SET hold_reason=NULL,error=NULL,pending_request=NULL WHERE id=?",
                    (exp["id"],),
                )
                self.store.emit(
                    db,
                    exp["id"],
                    "agent_turn_identity_reconciled",
                    {"action_id": action_id, "thread_id": action["thread_id"], "turn_id": turn_id},
                )
            action = {**action, "turn_id": turn_id}
        else:
            self._action_state(action_id, "completed", result=result)
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

    async def dispatch(self, action):
        eid = action["experiment_id"]
        try:
            decision = await self.refresh_quota()
            if not decision.admitted:
                self.store.update(eid, hold_reason="quota_hold", error=decision.reason)
                return
            exp = self.store.experiment(eid)
            if exp["phase"] in ("complete", "failed", "cancelled"):
                self._action_state(action["id"], "superseded")
                return
            if exp["pause_agent"]:
                self.store.update(eid, hold_reason="agent_paused")
                return
            if action["phase"] in ("plan", "prepare") and self.config.mode == "live":
                if time.time() - self.last_cluster_refresh >= 60:
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
                    action["id"], "failed", error="The configured agent turn budget is exhausted"
                )
                if action["phase"] != "monitor":
                    self.fail(eid, "agent_budget_exhausted", "The configured agent turn budget is exhausted")
                return
            selection = exp["contract"].get("agent", {})
            selected_model = selection.get("model")
            selected_effort = selection.get("reasoning_effort")
            option = None
            if selected_model or selected_effort:
                target = selected_model or getattr(self.agent, "resolved_model", None)
                option = next(
                    (model for model in getattr(self.agent, "models", []) if model["id"] == target), None
                )
                if not option or (selected_effort and selected_effort not in option["reasoning_efforts"]):
                    self._action_state(
                        action["id"],
                        "failed",
                        error="Selected model or reasoning effort is not advertised by the managed runtime",
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
            checkout = self.workspaces.checkout(eid)
            project_workspace = Path(checkout) if checkout else None
            isolated = self.workspaces.row(eid)
            if isolated and action["phase"] == "prepare":
                prompt = await asyncio.to_thread(self.prompt, exp, "prepare")
                with self.store.tx() as db:
                    db.execute("UPDATE agent_actions SET input=? WHERE id=?", (prompt, action["id"]))
                    self.store.emit(db, eid, "agent_input_refreshed", {"action_id": action["id"]})
                action = {**action, "input": prompt}
            if project_workspace:
                turn_options["project_workspace"] = project_workspace
            generic_prepare = exp["recipe_id"] == "agent_experiment_v1" and action["phase"] == "prepare"
            if action["phase"] == "monitor":
                # A bounded diagnostic must never accumulate in the research
                # conversation or replace its saved session identity.
                thread_id, model = await self.agent.session(
                    action["thread_id"], self.root(eid), **session_options
                )
            elif generic_prepare:
                workspace = self.working_directory(exp)
                workspace.mkdir(parents=True, exist_ok=True)
                if not (workspace / ".venv/bin/python").exists():
                    await command("uv", "venv", "--python", "/usr/bin/python3", str(workspace / ".venv"))
                thread_id, model = await self.agent.session(
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
                thread_id, model = await self.agent.session(
                    action["thread_id"] or exp["thread_id"], workspace, writable=True, **session_options
                )
            else:
                thread_id, model = await self.agent.session(
                    action["thread_id"] or exp["thread_id"],
                    project_workspace
                    or (self.root(eid) if action["phase"] == "plan" else self.root(eid) / "snapshot"),
                    **session_options,
                )
            prompt = action["input"]
            if action["error"] and not self.input_rejected(action):
                prompt = (
                    "Previous agent action failed; correct the reported error: "
                    + action["error"]
                    + "\n"
                    + prompt
                )
            with self.store.tx() as db:
                if action["phase"] != "monitor":
                    db.execute(
                        "UPDATE experiments SET thread_id=?,model=?,hold_reason=NULL,error=NULL WHERE id=?",
                        (thread_id, model, eid),
                    )
                db.execute(
                    "UPDATE agent_actions SET thread_id=?,state='dispatching',attempts=attempts+1,updated_at=? WHERE id=?",
                    (thread_id, time.time(), action["id"]),
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
                ControllerDecision.model_json_schema()
                if action["phase"] == "monitor"
                else ResearchPlan.model_json_schema()
                if action["phase"] == "plan"
                else (Implementation if generic_prepare else Preparation).model_json_schema()
                if action["phase"] == "prepare"
                else Analysis.model_json_schema()
            )
            if generic_prepare or (
                action["phase"] == "interpret"
                and exp["recipe_id"] == "agent_experiment_v1"
                and self.config.mode == "live"
            ):
                if not generic_prepare:
                    turn_options.pop("project_workspace", None)
                turn_id = await self.agent.begin(
                    thread_id, prompt, schema, workspace=workspace, **turn_options
                )
            else:
                turn_options.pop("project_workspace", None)
                turn_id = await self.agent.begin(thread_id, prompt, schema, **turn_options)
            with self.store.tx() as db:
                db.execute(
                    "UPDATE agent_actions SET turn_id=?,state='running',updated_at=? WHERE id=?",
                    (turn_id, time.time(), action["id"]),
                )
                self.store.emit(
                    db, eid, "agent_turn_started", {"action_id": action["id"], "turn_id": turn_id}
                )
            result = await self.agent.finish(thread_id, turn_id)
            self._action_state(action["id"], "completed", result=result)
        except SessionUnavailable as exc:
            self._action_state(action["id"], "failed", error=str(exc))
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
            self._action_state(action["id"], state, error=error)
            self.store.update(eid, error=error)
        finally:
            pending = self.store.experiment(eid)["pending_request"]
            if pending:
                pending["expired"] = True
                self.store.update(eid, pending_request=pending)

    async def publish(self, exp):
        eid = exp["id"]
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
            version = 1
            if iteration.enabled(exp):
                version = self.store.working_report(
                    eid,
                    analysis.presentation.model_dump(mode="json"),
                    "final-report:"
                    + self.store.one(
                        "SELECT id FROM agent_actions WHERE experiment_id=? AND phase='interpret' AND state='completed' ORDER BY rowid DESC LIMIT 1",
                        (eid,),
                    )["id"],
                )
            else:
                self.store.initial_report(eid, analysis.presentation.model_dump(mode="json"))
            report_version = self.store.one(
                "SELECT * FROM report_versions WHERE experiment_id=? AND version=?", (eid, version)
            )
            if report_version["state"] == "failed":
                if iteration.enabled(exp):
                    action = self.store.one(
                        "SELECT * FROM agent_actions WHERE experiment_id=? AND phase='interpret' ORDER BY rowid DESC LIMIT 1",
                        (eid,),
                    )
                    if action and action["state"] == "completed" and action["turn_id"]:
                        with self.store.tx() as db:
                            feedback = "agent output validation: report rendering: " + report_version["error"]
                            db.execute(
                                "UPDATE agent_actions SET state='failed',error=?,updated_at=? WHERE id=? AND state='completed'",
                                (feedback, time.time(), action["id"]),
                            )
                            db.execute(
                                "UPDATE stages SET state='pending',error=?,updated_at=? WHERE experiment_id=? AND id='interpret'",
                                (report_version["error"], time.time(), eid),
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
                raise ValueError("initial report failed: " + report_version["error"])
            if report_version["state"] != "ready":
                return
        if not report.exists():
            report.write_text(
                artifacts.render_report(
                    exp,
                    artifacts.read_json(root / "results.json", root),
                    analysis,
                    [by_name[name]["id"] for name in exp["snapshot"]["plan"]["figures"] if name in by_name]
                    if exp["recipe_id"] == "agent_experiment_v1"
                    else by_name["predictions.svg"]["id"],
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
                (time.time(), eid),
            )
            db.execute(
                "UPDATE experiments SET phase='complete',hold_reason=NULL,error=NULL WHERE id=?", (eid,)
            )
            db.execute(
                "INSERT OR IGNORE INTO inbox VALUES(?,?,?,?,?)",
                (str(uuid4()), eid, "result_ready", "Interpreted experiment result is ready.", time.time()),
            )
            self.store.emit(db, eid, "result_ready")

    async def cleanup_outputs(self, exp):
        """Completion authorizes only declared temporary paths in saved job roots.

        A crash may repeat deletion, never broaden it. Failed runs retain caches
        for repair; cleanup failures cannot revoke an already published result.
        """
        eid = exp["id"]
        roots = {}
        for job in self.store.rows(
            "SELECT * FROM jobs WHERE experiment_id=? AND execution_manifest IS NOT NULL", (eid,)
        ):
            manifest = json.loads(job["execution_manifest"])
            if not any(s.get("temporary_outputs") for s in manifest["plan"]["stages"]):
                continue
            roots.setdefault((job["execution_root"], manifest["snapshot_hash"]), (job, manifest))
        cleaned = []
        for (directory, snapshot_hash), (job, manifest) in roots.items():
            # Revalidate the saved declaration before crossing a deletion boundary.
            root = Path(directory)

            def check_declaration(root, manifest, snapshot_hash, base):
                Implementation.model_validate(manifest["plan"])
                if root.is_symlink() or not root.resolve().is_relative_to(base.resolve()):
                    raise ValueError("temporary cleanup root does not belong to the experiment")
                saved = artifacts.read_json(root / "snapshot/manifest.json", root)
                if saved["snapshot_hash"] != snapshot_hash or saved["plan"] != manifest["plan"]:
                    raise ValueError("temporary cleanup manifest identity changed")

            await self.check_files(
                check_declaration, root, manifest, snapshot_hash, self.projects.base_root(eid)
            )
            if manifest.get("remote_execution"):
                scheduler = self.scheduler_for(exp, job)
                layout = manifest["execution"]["layout"]
                await scheduler.remote_command(
                    "/usr/bin/python3",
                    layout["remote_run"] + "/remote-worker.py",
                    "cleanup",
                    layout["remote_run"],
                    job["stage_id"],
                    snapshot_hash,
                    timeout=120,
                )
            # Remote transfer settings may also have downloaded scratch locally.
            await self.check_files(cleanup_temporary, root, manifest)
            cleaned.append({"snapshot_hash": snapshot_hash})
        with self.store.tx() as db:
            self.store.emit(db, eid, "temporary_outputs_cleaned", {"revisions": cleaned})

    async def advance_cleanup(self):
        for eid, task in list(self.cleanup_tasks.items()):
            if task.done():
                try:
                    task.result()
                except Exception:
                    log.exception("temporary output cleanup failed for %s; retained for retry", eid)
                del self.cleanup_tasks[eid]
        for row in self.store.rows(
            "SELECT id FROM experiments e WHERE phase='complete' "
            "AND NOT EXISTS (SELECT 1 FROM events WHERE experiment_id=e.id AND kind='temporary_outputs_cleaned')"
        ):
            eid = row["id"]
            exp = self.store.experiment(eid)
            if (
                iteration.enabled(exp)
                or eid in self.cleanup_tasks
                or time.time() - self.last_cleanup_attempt.get(eid, 0) < 60
                or not exp["snapshot"]
                or not any(
                    s.get("temporary_outputs")
                    for job in self.store.rows(
                        "SELECT execution_manifest FROM jobs WHERE experiment_id=? "
                        "AND execution_manifest LIKE '%temporary_outputs%'",
                        (eid,),
                    )
                    for s in json.loads(job["execution_manifest"])["plan"]["stages"]
                )
                or self.store.one(
                    "SELECT 1 FROM jobs WHERE experiment_id=? AND state IN ('submitting','submitted')", (eid,)
                )
                or self.store.one(
                    "SELECT 1 FROM agent_actions WHERE experiment_id=? AND state IN ('dispatching','running','uncertain')",
                    (eid,),
                )
            ):
                continue
            self.last_cleanup_attempt[eid] = time.time()
            self.cleanup_tasks[eid] = asyncio.create_task(self.cleanup_outputs(exp))

    async def advance_reports(self):
        rows = self.store.rows(
            "SELECT r.* FROM report_versions r JOIN experiments e ON e.id=r.experiment_id "
            "WHERE e.deleted_at IS NULL AND r.state IN ('queued','building','publishing') ORDER BY r.created_at LIMIT 1"
        )
        for row in rows:
            eid, version = row["experiment_id"], row["version"]
            exp = self.store.experiment(eid)
            try:
                if row["state"] in ("queued", "building"):
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
                    name, files = await asyncio.to_thread(
                        reporting.build_report, exp, root, records, json.loads(row["document"]), version
                    )
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
        await self.supervisor.close()
        self.closing = True
        # Threads only read/seal filesystem evidence; drain them before releasing ownership.
        # A restart re-observes the exact saved job and validates again without resubmission.
        checks = (
            [task for task, _ in self.output_checks.values()]
            + list(self.evidence_checks.values())
            + list(self.cleanup_tasks.values())
        )
        if checks:
            await asyncio.gather(*checks, return_exceptions=True)
        self.output_checks.clear()
        self.evidence_checks.clear()
        for lane in self.lanes[1:]:
            if lane.agent_task and not lane.agent_task.done():
                lane.agent_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await lane.agent_task
            if lane.quota_task and not lane.quota_task.done():
                lane.quota_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await lane.quota_task
            await lane.agent.close()
        if self.agent_task and not self.agent_task.done():
            self.agent_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.agent_task
        if self.quota_task and not self.quota_task.done():
            self.quota_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.quota_task
        await self.controller.close()
        await self.agent.close()
        if self.backup_task:
            await self.backup_task
        await self.backup_state()
