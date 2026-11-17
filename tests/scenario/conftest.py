"""Local scenario fixture: real SQLite state, fake engine clock and fake external backends.

One demo-mode session runs against the engine's own command and tick boundaries.
The scheduler and managed-agent adapters are rebuilt on demand while their saved
external facts and the authoritative SQLite state survive; no live resource starts.
"""

import contextlib
import sqlite3
from uuid import uuid4

import pytest

from experimenttracker.codex_client import CodexAgent
from experimenttracker.store import Store
from experimenttracker.workflow import Workflow
from scenario import (
    FakeClock,
    FakeCodex,
    FakeCodexBackend,
    FakeGate,
    FakeSlurm,
    FakeSlurmBackend,
    run_until,
    shared_fake_slots,
)


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

    def gate(self, phase):
        """Park the fake turn of this phase after begin, as an explicit event gate."""
        gate = FakeGate()
        self.agent_backend.gates[phase] = gate
        return gate

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


class MultiScenario(Scenario):
    """The same saved engine state behind several distinct synthetic connections.

    ``Scenario`` keeps its one-slot assertion; this fixture reaches the engine's own
    pool through a per-test no-argument fake class whose clones share one explicit
    backend, so two roles can be live at once without a second queue simulation.
    """

    def __init__(self, config, store, clock, slurm_backend, sessions=2):
        self.sessions = sessions
        self.slots = shared_fake_slots()
        config.codex.max_sessions = sessions
        super().__init__(config, store, clock, slurm_backend, self.slots.backend)

    def build(self):
        slurm = FakeSlurm(self.slurm_backend)
        slurm.saved_intent = self.intent_committed
        self.workflow = Workflow(self.config, self.store, slurm, self.slots.agent())
        assert self.workflow.pool_owner is self.workflow
        assert self.workflow.config.codex.max_sessions == self.sessions
        return self.workflow

    async def started(self):
        """Start the engine's pool and label every synthetic clone with its slot."""
        await self.workflow.start()
        assert len(self.workflow.lanes) == self.sessions
        for index, lane in enumerate(self.workflow.lanes):
            lane.agent.slot = index
            # Every clone is this per-test synthetic class, never a managed client.
            assert type(lane.agent) is self.slots.agent and not isinstance(lane.agent, CodexAgent)
        return self.workflow

    async def restart(self, *, sessions=None):
        """Stop the pool, then rebuild adapters on the saved state and same backend.

        ``sessions`` shrinks or keeps the configured connection count: logical
        reservations survive either way, because they come from the source rows.
        """
        if sessions is not None:
            self.sessions = sessions
            self.config.codex.max_sessions = sessions
        await self.close()
        return self.build()


@pytest.fixture
async def multi_scenario(config, monkeypatch):
    clock = FakeClock()
    clock.install(monkeypatch)
    store = Store(config.state_dir / "workflow.sqlite")
    state = MultiScenario(config, store, clock, FakeSlurmBackend())
    try:
        yield state
    finally:
        try:
            await state.close()
        finally:
            store.close()
