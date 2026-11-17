"""Synthetic scenario fakes: engine clock, scheduler and managed-agent surfaces.

Adapters mirror the production method signatures and raise the real exception types;
external facts live in a backend that survives workflow (adapter) recreation. No fake
starts Slurm, a model runtime or an agent socket, and no scenario claims research.
"""

import asyncio
import hashlib
import json
from dataclasses import dataclass

from experimenttracker import clock as engine_clock
from experimenttracker.codex_client import NeedsAttention, TerminalTurnError
from experimenttracker.slurm import ResourceUnavailable, SubmissionUncertain


class FakeClock:
    """Engine clock only: real epoch wall time plus an independent monotonic counter."""

    def __init__(self):
        self.wall = engine_clock.now()
        self.monotonic = 0.0

    def now(self):
        return self.wall

    def advance(self, seconds):
        self.wall += seconds
        self.monotonic += seconds

    def install(self, monkeypatch):
        monkeypatch.setattr(engine_clock, "now", self.now)
        monkeypatch.setattr(engine_clock, "monotonic", lambda: self.monotonic)


class FakeSlurmBackend:
    def __init__(self):
        self.matches = {}  # intent_id -> accepted job IDs, including hidden ones
        self.accepted = []  # {"intent_id", "job_id", "intent_saved"}
        self.submit_calls = []
        self.reconcile_calls = []
        self.refusals = []  # errors raised by an sbatch attempt before any acceptance
        self.lose_response = False  # accept, then lose the sbatch response
        self.lag = False  # accepted identities stay invisible until reveal()
        self.hidden = set()
        self.duplicate = False  # accept a second job for one submission intent
        self.unreachable_intents = {}  # intent_id -> raw error raised by reconcile
        self.unreachable_jobs = {}  # job_id -> raw error raised by observe
        self.observations, self.cancel_requests = {}, []
        self.next_job_id = 4200

    def accept(self, intent_id, intent_saved):
        self.next_job_id += 1
        job_id = str(self.next_job_id)
        self.matches.setdefault(intent_id, []).append(job_id)
        self.accepted.append({"intent_id": intent_id, "job_id": job_id, "intent_saved": intent_saved})
        return job_id

    def reveal(self):
        self.hidden.clear()


class FakeSlurm:
    """Scheduler surface mirroring ``SlurmScheduler``; external facts live in ``backend``."""

    def __init__(self, backend=None):
        self.backend = backend or FakeSlurmBackend()
        self.saved_intent = None  # per-adapter probe: the owner saved the intent row
        self.submit_gate = None  # optional FakeGate that parks a submission inside sbatch

    async def submit(self, intent_id, script, root, *, resources=None):
        backend = self.backend
        backend.submit_calls.append(intent_id)
        if self.submit_gate is not None:
            await self.submit_gate.wait()
        if backend.refusals:
            error = backend.refusals.pop(0)
            if isinstance(error, (RuntimeError, OSError)):
                # Live submit holds every reported failure as an uncertain submission.
                raise SubmissionUncertain(str(error)) from error
            raise error  # ValueError: resource validation refused before sbatch
        intent_saved = bool(self.saved_intent and self.saved_intent(intent_id))
        job_id = backend.accept(intent_id, intent_saved)
        if backend.duplicate:
            backend.accept(intent_id, intent_saved)
        if backend.lag:
            backend.hidden.add(intent_id)
        if backend.lose_response:
            raise SubmissionUncertain("synthetic sbatch response lost after acceptance")
        return job_id

    async def reconcile(self, intent_id):
        self.backend.reconcile_calls.append(intent_id)
        error = self.backend.unreachable_intents.get(intent_id)
        if error is not None:
            raise error
        visible = [] if intent_id in self.backend.hidden else self.backend.matches.get(intent_id, [])
        if len(visible) > 1:
            raise SubmissionUncertain("multiple scheduler jobs match one submission intent")
        return visible[0] if visible else None

    async def observe(self, job_id):
        error = self.backend.unreachable_jobs.get(job_id)
        if error is not None:
            raise error
        observation = self.backend.observations.get(job_id)
        assert observation is not None, f"fake scheduler has no observation for job {job_id}"
        return observation

    async def cancel(self, job_id, intent_id):
        """Cancel only the exact saved identity; record the request, assume no outcome."""
        if not job_id.isdecimal():
            raise ValueError("synthetic cancellation needs a numeric saved job ID")
        intents = [entry["intent_id"] for entry in self.backend.accepted if entry["job_id"] == job_id]
        if not intents:
            return False
        if intents != [intent_id]:
            raise ResourceUnavailable("saved job ID no longer matches its scheduler intent")
        self.backend.cancel_requests.append((job_id, intent_id))
        return True

    async def discover(self):
        raise AssertionError("demo scenarios never discover a live scheduler")

    async def preflight(self, *args, **kwargs):
        raise AssertionError("demo scenarios never preflight a live scheduler")


class FakeCodexBackend:
    def __init__(self):
        self.scripts = {}  # phase -> {"turn", "begin_error", "expected", "seen"}
        self.starts = []  # exact dispatched prompts, threads and turn IDs
        self.turns = {}  # (thread_id, turn_id) -> {"record", "result", "state"}
        self.unreadable = {}  # thread_id -> raw error raised by any thread read
        self.models_observed_at = None
        self.threads = self.turns_started = 0


class FakeCodex:
    """Managed-agent surface mirroring ``CodexAgent``; scripted facts live in ``backend``."""

    is_demo = True
    available = True
    resolved_model = "scenario-fake"
    models = [
        {
            "id": "scenario-fake",
            "name": "Synthetic scenario model",
            "reasoning_efforts": ["low", "medium"],
            "default_effort": "low",
        }
    ]

    def __init__(self, backend=None):
        self.backend = backend or FakeCodexBackend()
        self.request_handler = self.quota_handler = self.activity_handler = None

    @property
    def models_observed_at(self):
        return self.backend.models_observed_at

    def script(self, phase, *, turn=None, begin_error=None, turns=1, started=True):
        """Program one phase: ``turn(payload, workspace)`` runs at begin; ``turns`` bounds dispatches.

        ``started=False`` scripts a dispatch the runtime refused before it created the
        turn, so no thread read can find it.
        """
        scripts = self.backend.scripts
        scripts[phase] = {
            "turn": turn,
            "begin_error": begin_error,
            "expected": turns,
            "seen": 0,
            "started": started,
        }
        return self

    def complete(self, thread_id, turn_id, result):
        """Record the terminal result a later read or finish observes."""
        self.backend.turns[(thread_id, turn_id)].update(result=result, state="completed")

    def fail(self, thread_id, turn_id, state="failed"):
        """Record a terminal non-completed turn."""
        assert state in ("failed", "interrupted"), state
        self.backend.turns[(thread_id, turn_id)]["state"] = state

    async def start(self):
        self.backend.models_observed_at = engine_clock.now()

    async def quota(self):
        """Synthetic admitted quota: no purchased allowance and no model usage."""
        window = {"usedPercent": 0, "windowDurationMins": 300}
        weekly = {"usedPercent": 0, "windowDurationMins": 10080}
        return {
            "rateLimitsByLimitId": {"codex": {"primary": window, "secondary": weekly}},
            "ordinaryUsageAllowed": True,
        }

    async def session(
        self,
        thread_id,
        workspace,
        *,
        writable=False,
        model=None,
        approval_policy="on-request",
        runtime_config=None,
    ):
        if thread_id:
            return thread_id, self.resolved_model
        self.backend.threads += 1
        return f"fake-thread-{self.backend.threads}", self.resolved_model

    async def begin(
        self,
        thread_id,
        prompt,
        schema,
        *,
        workspace=None,
        effort="low",
        project_workspace=None,
        additional_writable_roots=(),
    ):
        backend = self.backend
        # The engine appends one payload; rsplit keeps an earlier "PAYLOAD:" in prose intact.
        payload = json.loads(prompt.rsplit("\nPAYLOAD:\n", 1)[1])
        scripted = backend.scripts.get(payload["phase"])
        assert scripted is not None, f"unexpected agent phase {payload['phase']}"
        assert scripted["seen"] < scripted["expected"], f"unexpected repeat of agent phase {payload['phase']}"
        scripted["seen"] += 1
        backend.turns_started += 1
        turn_id = f"fake-turn-{backend.turns_started}"
        record = {
            "phase": payload["phase"],
            "prompt": prompt,
            "prompt_sha256": hashlib.sha256(prompt.encode()).hexdigest(),
            "thread_id": thread_id,
            "turn_id": turn_id,
            "writable_roots": [str(root) for root in additional_writable_roots],
        }
        backend.starts.append(record)
        if scripted["begin_error"] and not scripted["started"]:
            raise scripted["begin_error"]
        saved = backend.turns[(thread_id, turn_id)] = {"record": record, "result": None, "state": "accepted"}
        if scripted["begin_error"]:
            # The runtime accepted the turn before its response was lost.
            raise scripted["begin_error"]
        result = scripted["turn"](payload, workspace) if scripted["turn"] else None
        saved.update(result=result, state="completed")
        return turn_id

    async def finish(self, thread_id, turn_id):
        saved = self.backend.turns.get((thread_id, turn_id))
        if saved is None:
            raise NeedsAttention("saved turn not found; operator reconciliation required")
        if saved["state"] in ("failed", "interrupted"):
            raise TerminalTurnError(f"turn {saved['state']}")
        if saved["state"] != "completed":
            raise NeedsAttention(f"saved turn is {saved['state']}; operator reconciliation required")
        if saved["result"] is None:
            raise TerminalTurnError("agent output validation: completed turn has no structured result")
        return saved["result"]

    async def read_thread(self, thread_id):
        """Turns one thread retains, oldest first; an unreadable thread raises."""
        if thread_id in self.backend.unreadable:
            raise self.backend.unreadable[thread_id]
        return [
            {
                "id": saved["record"]["turn_id"],
                "status": "inProgress" if saved["state"] == "accepted" else saved["state"],
                "prompt_sha256": saved["record"]["prompt_sha256"],
            }
            for (thread, _), saved in self.backend.turns.items()
            if thread == thread_id
        ]

    async def read_turn(self, thread_id, turn_id, *, expected_prompt_hash=None):
        if thread_id in self.backend.unreadable:
            raise self.backend.unreadable[thread_id]
        saved = self.backend.turns.get((thread_id, turn_id))
        if saved is None:
            raise NeedsAttention("saved turn not found; operator reconciliation required")
        if expected_prompt_hash is not None and saved["record"]["prompt_sha256"] != expected_prompt_hash:
            raise NeedsAttention("saved turn input does not match the exact dispatched prompt")
        if saved["state"] == "completed" and saved["result"] is not None:
            return saved["result"]
        if saved["state"] in ("failed", "interrupted"):
            raise TerminalTurnError(f"turn {saved['state']}")
        raise NeedsAttention(f"saved turn is {saved['state']}; operator reconciliation required")

    async def interrupt(self, thread_id, turn_id):
        pass  # best effort, like the real client: state changes only on a real outcome

    async def reply(self, pending):
        pass

    async def close(self):
        pass


class FakeGate:
    """An intentional wait the engine's own task blocks on, observed without pre-registering it."""

    def __init__(self):
        self.task = None
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def wait(self):
        self.task = asyncio.current_task()
        self.entered.set()
        await self.release.wait()


DEFAULT_STEP_SECONDS = 5.0
DEFAULT_TIMEOUT_SECONDS = 20.0


class ScenarioTimeout(RuntimeError):
    """The real-time failure bound expired; it is a detector, never a progress condition."""


@dataclass(frozen=True)
class SettleReport:
    """Engine tasks a settle left parked at a registered fake gate instead of awaiting."""

    waiting: tuple = ()


def engine_tasks(workflow):
    """The engine's own unfinished task handles, read from the real workflow seams."""
    handles = [task for lane in workflow.lanes for task in (lane.agent_task, lane.quota_task)]
    handles.append(workflow.backup_task)
    handles.extend(task for task, _ in workflow.output_checks.values())
    handles.extend(workflow.evidence_checks.values())
    return [task for task in handles if task is not None and not task.done()]


async def settle(workflow, *, gates=()):
    """Await engine task completion; report tasks parked at a registered gate instead.

    Handles are re-read after every completion, so a task created by a tick or by
    another task is settled without preregistration. A parked gate is observed
    through its ``entered`` event with FIRST_COMPLETED, so no real time passes and
    no yield count drives progress.
    """
    while True:
        waiting = [
            gate.task
            for gate in gates
            if gate.task is not None and gate.entered.is_set() and not gate.release.is_set()
        ]
        tasks = [handle for handle in engine_tasks(workflow) if handle not in waiting]
        if not tasks:
            return SettleReport(waiting=tuple(waiting))
        observers = [
            asyncio.ensure_future(gate.entered.wait()) for gate in gates if not gate.entered.is_set()
        ]
        try:
            await asyncio.wait([*tasks, *observers], return_when=asyncio.FIRST_COMPLETED)
        finally:
            for observer in observers:
                observer.cancel()


def _describe(task):
    if task is None:
        return "none"
    coroutine = task.get_coro()
    return getattr(coroutine, "__qualname__", repr(coroutine))


def _counter(adapter, field):
    value = getattr(getattr(adapter, "backend", None), field, None)
    return len(value) if isinstance(value, list) else value


def diagnostics(workflow, *, gates=()):
    """Concise saved state for a scenario timeout, read without mutating anything."""
    saved = (
        ("experiments", "SELECT id,phase,hold_reason FROM experiments WHERE deleted_at IS NULL"),
        ("actions", "SELECT phase,state FROM agent_actions ORDER BY created_at"),
        ("jobs", "SELECT intent_id,stage_id,state,job_id FROM jobs"),
    )
    parts = []
    store = getattr(workflow, "store", None)
    if store is not None:
        try:
            for label, sql in saved:
                parts.append(f"{label}={[tuple(row.values()) for row in store.rows(sql)]}")
        except Exception as exc:  # diagnostics never mask the timeout they describe
            parts.append(f"saved state unreadable: {exc!r}")
    parts.append(f"submitted={_counter(workflow.scheduler, 'submit_calls')}")
    parts.append(f"begin={_counter(workflow.agent, 'turns_started')}")
    parts.append(f"pending={[_describe(task) for task in engine_tasks(workflow)]}")
    parts.append(f"gated={[_describe(gate.task) for gate in gates if gate.task is not None]}")
    return " ".join(parts)


async def run_until(
    workflow,
    predicate,
    *,
    clock,
    step=DEFAULT_STEP_SECONDS,
    gates=(),
    timeout=DEFAULT_TIMEOUT_SECONDS,
):
    """Advance fake time and settle engine work until ``predicate()`` returns truthy.

    Each step advances the fake clock, calls only ``tick(force_observe=True)`` and
    awaits the workflow's own task handles. ``timeout`` bounds a tick and its
    settling together as a real-time failure detector only.
    """
    limit = asyncio.timeout(timeout)
    try:
        async with limit:
            while True:
                value = predicate()
                if value:
                    return value
                await workflow.tick(force_observe=True)
                await settle(workflow, gates=gates)
                clock.advance(step)
    except TimeoutError:
        if not limit.expired():
            raise
        detail = diagnostics(workflow, gates=gates)
        raise ScenarioTimeout(f"scenario made no progress within {timeout}s: {detail}") from None
