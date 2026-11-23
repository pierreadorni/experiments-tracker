"""Isolated periodic engine tasks over the shared workflow steps (T1.5c).

The API used to run every step of the engine in one sequential loop. Each task here
keeps the cadence that loop used, so a stuck or failing task no longer stalls the
others: ordinary exceptions stay inside the task that raised them and are reported
through the existing ``/healthz`` heartbeat, while cancellation propagates. The
workflow starts once before the loops; only the manual ``Workflow.tick`` adapter
keeps lazy startup.
"""

import asyncio
import contextlib
import logging

from .. import clock
from ..maintenance import blocked
from ..workflow import (
    BACKUP_SECONDS,
    CONTROL_SERVICE,
    HEALTH_COUNTED_STEPS,
    OBSERVE_SERVICE,
    RETENTION_SECONDS,
)
from . import integration_runtime

log = logging.getLogger(__name__)
ENGINE_ERROR = "executor loop failed; inspect daemon logs"
INTEGRATION_SECONDS = 15  # Between drained integration requests (Git and checks)
# The maintenance gate keeps the sequential positions: observation, input replies and
# agent draining continue before it, and these steps stay skipped after it. Integration
# joins them because it fast-forwards the project base checkout. The control task
# applies the same gate to its own advancement region.
GATED = ("watchdog", "supervisor", "integration", "retention", "backup")
# Cancelling a task that awaits ``asyncio.to_thread`` leaves its thread running, so the
# retention and integration loops are awaited instead of cancelled: the store must never
# close while a pass they own can still write or fast-forward a checkout.
THREADED = ("retention", "integration")


class Runner:
    """Owns the named periodic loops, their stop event and their in-memory health.

    ``control`` and ``supervisor`` hold one primary-agent lock: they
    mutate dispatch state, so their check-then-act regions stay exclusive across
    awaits. Observation, the watchdog, integration, retention and backup never take it.
    """

    def __init__(self, workflow):
        self.workflow = workflow
        self.lock = asyncio.Lock()
        self.started = False
        self.stopping = False
        self.tasks = {}
        self.last_success = {}
        self.last_error = {}  # task -> (failed at, message)
        self._stop = asyncio.Event()
        self.steps = {
            "control": self.control,
            "observe": self.observe,
            "watchdog": self.watchdog,
            "supervisor": self.supervisor,
            "integration": self.integration,
            "retention": self.retention,
            "backup": self.backup,
        }

    def periods(self):
        """Each task's wait between passes, from the existing settings and constants."""
        pacing = min(self.workflow.config.scheduler_seconds, 1)
        return {
            "control": pacing,
            "observe": self.workflow.config.scheduler_seconds,
            "watchdog": pacing,
            "supervisor": pacing,
            "integration": INTEGRATION_SECONDS,
            "retention": RETENTION_SECONDS,
            "backup": BACKUP_SECONDS,
        }

    def health(self):
        """The oldest last-success across the counted tasks, plus the latest failure.

        ``last_tick`` stays ``None`` until every task the sequential tick ran has
        succeeded once, so no single healthy task can hide a stuck one. ``backup`` is
        excluded because it was already a background task.
        """
        counted = HEALTH_COUNTED_STEPS
        tick = (
            min(self.last_success[name] for name in counted)
            if all(name in self.last_success for name in counted)
            else None
        )
        failures = [self.last_error[name] for name in counted if name in self.last_error]
        return tick, max(failures)[1] if failures else None

    async def run_once(self, name):
        """One pass of a task: gated, exception-isolated and health-recorded."""
        try:
            if not (name in GATED and blocked(self.workflow.store)):
                await self.steps[name]()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("%s engine task failed", name)
            self.last_error[name] = (clock.now(), ENGINE_ERROR)
            return False
        self.last_error.pop(name, None)
        self.last_success[name] = clock.now()
        return True

    async def loop(self, name, seconds):
        while not self.stopping:
            await self.run_once(name)
            if await self.wait(seconds):
                break

    async def wait(self, seconds):
        """Wait one period, or return at once once shutdown starts."""
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        return self.stopping

    async def start(self):
        """Start the workflow once, then every loop; a second runner is an error."""
        if self.started:
            raise RuntimeError("workflow engine already has a runner")
        self.started = True
        await self.workflow.start()
        for name, seconds in self.periods().items():
            self.tasks[name] = asyncio.create_task(self.loop(name, seconds), name=f"engine-{name}")

    async def stop(self):
        """Stop scheduling, finish the passes already running, and return.

        Threaded passes are awaited rather than cancelled so no owned write can reach
        the store after its owner closes it.
        """
        self.stopping = True
        self._stop.set()
        for name, task in self.tasks.items():
            if name not in THREADED:
                task.cancel()
        results = await asyncio.gather(*self.tasks.values(), return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                log.error("engine task did not stop cleanly: %r", result)
        self.tasks.clear()

    async def control(self):
        """Intake, draining, resolution and advancement without observation."""
        async with self.lock:
            await self.workflow.step_control()
            # The pass returned, so the families it drives were really serviced.
            self.workflow.record_service(CONTROL_SERVICE)
            # The resolver fragment runs inside this pass, after agent draining and
            # before advancement, so the pass that ran it records its health.
            self.last_success["resolver"] = clock.now()
        self.workflow.maintenance.launch()

    async def observe(self):
        await self.workflow.step_observe(clock.now())
        self.workflow.record_service(OBSERVE_SERVICE)

    async def watchdog(self):
        await self.workflow.step_watchdog(clock.now())

    async def supervisor(self):
        async with self.lock:
            await self.workflow.step_supervisor()

    async def integration(self):
        """Drive already queued integration requests, one repository at a time."""
        await asyncio.to_thread(integration_runtime.run, self.workflow.workspaces)

    async def retention(self):
        await self.workflow.step_retention(clock.now())

    async def backup(self):
        await self.workflow.step_backup(clock.now())
