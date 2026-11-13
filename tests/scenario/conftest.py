"""Local scenario fixture: real SQLite state, fake engine clock and fake external backends.

One demo-mode session runs against the engine's own command and tick boundaries.
The scheduler and managed-agent adapters are rebuilt on demand while their saved
external facts and the authoritative SQLite state survive; no live resource starts.
"""

import contextlib
import sqlite3
from uuid import uuid4

import pytest

from experimenttracker.store import Store
from experimenttracker.workflow import Workflow
from scenario import FakeClock, FakeCodex, FakeCodexBackend, FakeSlurm, FakeSlurmBackend, run_until


class Scenario:
    """Saved workflow state plus the fake scheduler/agent facts across restarts."""

    def __init__(self, config, store, clock, slurm_backend, agent_backend):
        self.config = config
        self.store = store
        self.clock = clock
        self.slurm_backend = slurm_backend
        self.agent_backend = agent_backend
        self.workflow = None
        self.step = 5.0
        self.build()

    def build(self):
        """Fresh adapters over the shared backends, and a workflow on the same Store."""
        slurm = FakeSlurm(self.slurm_backend)
        slurm.saved_intent = self.intent_committed
        self.workflow = Workflow(self.config, self.store, slurm, FakeCodex(self.agent_backend))
        assert self.workflow.pool_owner is self.workflow
        assert self.workflow.config.codex.max_sessions == 1
        return self.workflow

    @property
    def slurm(self):
        return self.workflow.scheduler

    @property
    def agent(self):
        return self.workflow.agent

    def intent_committed(self, intent_id):
        """Prove the dispatch intent row is durable before the scheduler sees it.

        A separate read-only connection only sees committed rows, so this rejects
        both an open source transaction and an intent saved after dispatch.
        """
        assert not self.store.db.in_transaction, "submission ran inside an open source transaction"
        with contextlib.closing(sqlite3.connect(f"{self.store.path.as_uri()}?mode=ro", uri=True)) as reader:
            row = reader.execute("SELECT state FROM jobs WHERE intent_id=?", (intent_id,)).fetchone()
        return bool(row)

    async def run(self, predicate, **options):
        """Drive the current workflow with the scenario clock."""
        return await run_until(self.workflow, predicate, clock=self.clock, step=self.step, **options)

    async def approve_plan(self, eid):
        """Wait for review with the driver, then approve with the ordinary command."""
        await self.run(
            lambda: (self.store.experiment(eid)["plan_review"] or {}).get("state") == "awaiting_review"
        )
        review = self.store.experiment(eid)["plan_review"]
        self.store.command(
            uuid4(), "plan_review", {"decision": "approve", "plan_revision": review["revision"]}, eid=eid
        )

    async def restart(self):
        """Stop the current workflow, then rebuild adapters on the saved state."""
        await self.close()
        return self.build()

    def one_session(self):
        """The pool never grows a second lane under the one-session contract."""
        owner = self.workflow.pool_owner
        assert owner.config.codex.max_sessions == 1
        assert owner.lanes == [owner]

    async def close(self):
        """Close the current workflow once; the engine's own flag marks it closed."""
        if not self.workflow.closing:
            await self.workflow.close()


@pytest.fixture
async def scenario(config, monkeypatch):
    clock = FakeClock()
    clock.install(monkeypatch)
    store = Store(config.state_dir / "workflow.sqlite")
    state = Scenario(config, store, clock, FakeSlurmBackend(), FakeCodexBackend())
    try:
        yield state
    finally:
        try:
            await state.close()
        finally:
            store.close()
