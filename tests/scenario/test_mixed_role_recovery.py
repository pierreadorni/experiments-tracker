"""Real-engine restart, capacity debt and callback routing (T1.6c-2).

The shared multi-slot fixture keeps one explicit fake backend and the saved SQLite
state across a workflow restart, so a lost begin, an unreadable outcome, a shrunken
pool and a late old-generation callback are decided by the production selector,
resolver and role readback instead of a second queue simulation. The callback case
drives a real ``CodexAgent`` whose pinned transport is answered from memory, so the
generation counter and the request/notification receipt are production code with no
process, socket or model. Direct-SQL source rows are labelled where they appear: they
prove saved-state accounting and readback, never dispatch integration. Synthetic
mechanism acceptance only: no live scheduler, managed account or research.
"""

import asyncio
from itertools import count
from uuid import uuid4

import pytest
from test_knowledge import completed

import experimenttracker.workflow as workflow_module
from experimenttracker import clock
from experimenttracker.agent_pool import experiment_owner, project_owner
from experimenttracker.codex_client import CodexAgent, NeedsAttention, TransportError
from experimenttracker.store import Store
from experimenttracker.workflow import Workflow
from scenario import FakeClock, FakeSlurm, settle
from scenario.test_iteration_scenario_healthy_job import plan_turn
from scenario.test_mixed_role_overlap import (
    connection_evidence,
    create,
    saved,
    synthesize_turn,
    turn_task,
)

STUB_CATALOG = {
    "data": [
        {
            "model": "stub-model",
            "displayName": "Synthetic stub transport model",
            "supportedReasoningEfforts": [{"reasoningEffort": "low"}],
            "defaultReasoningEffort": "low",
            "hidden": False,
            "isDefault": True,
        }
    ]
}


def quota_payload(used_percent=0):
    """One synthetic admitted quota observation for both required windows."""
    bucket = {
        "primary": {"usedPercent": used_percent, "windowDurationMins": 300},
        "secondary": {"usedPercent": used_percent, "windowDurationMins": 10080},
    }
    return {"rateLimitsByLimitId": {"codex": bucket}, "ordinaryUsageAllowed": True}


STUB_QUOTA = quota_payload()
THREADS, TURNS = count(1), count(1)


def actions(store, eid):
    return store.rows("SELECT * FROM agent_actions WHERE experiment_id=? ORDER BY created_at", (eid,))


def insert_action(store, eid, state, *, phase, thread, turn):
    """A direct-SQL source row: it proves accounting and readback, not dispatch."""
    action_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,input,created_at,updated_at) "
            "VALUES(?,?,?,1,?,?,?,'synthetic prompt',?,0)",
            (action_id, eid, phase, state, thread, turn, clock.now()),
        )
    return store.one("SELECT * FROM agent_actions WHERE id=?", (action_id,))


def save_dispatch_intent(store, action, prompt_sha256="synthetic-prompt"):
    """The saved dispatch intent a readback requires; a direct fixture for a direct-SQL row."""
    with store.tx() as db:
        store.emit(
            db,
            action["experiment_id"],
            "agent_dispatch_intent",
            {"action_id": action["id"], "thread_id": action["thread_id"], "prompt_sha256": prompt_sha256},
        )


def seed_turn(backend, thread, turn, result):
    """Program the external runtime's saved completed turn for a direct-SQL source row."""
    backend.turns[(thread, turn)] = {
        "record": {"phase": "title", "thread_id": thread, "turn_id": turn, "prompt_sha256": ""},
        "result": result,
        "state": "completed",
    }


async def steps(scenario, ticks, *, gates=()):
    """Advance through explicit ticks only; no success condition is inferred from a yield."""
    for _ in range(ticks):
        await scenario.workflow.tick(force_observe=True)
        await settle(scenario.workflow, gates=gates)
        scenario.clock.advance(scenario.step)


async def lost_begin(scenario, brief):
    """Create an experiment whose plan turn the runtime accepted without a response."""
    scenario.agent.script(
        "plan", turn=plan_turn, begin_error=TransportError("turn/start response lost"), turns=1
    )
    eid = create(scenario.store, brief)
    await scenario.run(lambda: scenario.store.experiment(eid)["hold_reason"] == "agent_dispatch_uncertain")
    [action] = actions(scenario.store, eid)
    return eid, action


async def test_lost_begin_survives_a_restart_of_the_same_pool_size(multi_scenario):
    scenario = multi_scenario
    store, backend, slots = scenario.store, scenario.agent_backend, scenario.slots
    wf = await scenario.started()
    eid, lost = await lost_begin(scenario, "Synthetic lost begin.")
    [sent] = backend.starts  # the runtime created this exact turn before its response was lost
    assert (lost["state"], lost["turn_id"], lost["attempts"]) == ("uncertain", None, 1)
    assert lost["thread_id"] == sent["thread_id"]
    assert len(slots.named("begin", thread=sent["thread_id"])) == 1
    assert (len(wf.lanes), backend.turns_started) == (2, 1)
    assert [lane.agent.instance for lane in wf.lanes] == ["synthetic-agent-1", "synthetic-agent-2"]
    # One of two configured turns is owed to the unknown action, and its owner is excluded.
    assert wf.agent_pool.turn_capacity() == 1
    assert not wf.agent_pool.owner_free(experiment_owner(eid))

    # A restart reuses the saved database and the same external backend, never the process.
    await scenario.restart()
    wf = await scenario.started()
    assert (len(wf.lanes), backend.turns_started) == (2, 1)
    assert [lane.agent.instance for lane in wf.lanes] == ["synthetic-agent-3", "synthetic-agent-4"]
    kept = wf.agent_pool.lookup("agent_actions", lost["id"])
    assert (kept.connection, kept.thread_id, kept.turn_id) == (None, sent["thread_id"], None)
    assert wf.agent_pool.turn_capacity() == 1
    # An unknown outcome is not a recovery: no driver pass begins a replacement action.
    await steps(scenario, 2)
    assert [action["id"] for action in actions(store, eid)] == [lost["id"]]
    assert (backend.turns_started, len(slots.named("begin", thread=sent["thread_id"]))) == (1, 1)

    # The accepted turn really completes: exact readback adopts it on a restarted connection.
    backend.turns[(sent["thread_id"], sent["turn_id"])].update(
        result=plan_turn(None, None), state="completed"
    )
    await scenario.run(lambda: (store.experiment(eid)["plan_review"] or {}).get("state") == "awaiting_review")
    [adopted] = actions(store, eid)
    assert (adopted["id"], adopted["state"], adopted["thread_id"], adopted["turn_id"]) == (
        lost["id"],
        "completed",
        sent["thread_id"],
        sent["turn_id"],
    )
    assert (backend.turns_started, len(slots.named("begin", thread=sent["thread_id"]))) == (1, 1)
    reads = slots.named("read_thread", thread=sent["thread_id"])
    assert (reads[0]["instance"], reads[-1]["instance"]) == ("synthetic-agent-1", "synthetic-agent-3")
    assert {
        (call["instance"], call["generation"]) for call in slots.named("read_turn", turn=sent["turn_id"])
    } == {("synthetic-agent-3", 1)}
    assert wf.agent_pool.turn_capacity() == 2

    # A project source row has no lost-turn adoption: the saved thread alone is not an
    # identity, so the supervisor keeps exactly the uncertainty and sends no second turn.
    completed(store, summary="Synthetic completed source.")
    memory = wf.supervisor.memory
    memory.refresh()
    identity = memory.enqueue(
        "test-project", {"request_id": str(uuid4()), "kind": "synthesize"}, origin="weekly"
    )["request_id"]
    scenario.agent.script(
        "project_synthesize", begin_error=TransportError("turn/start response lost"), turns=1
    )
    await scenario.run(lambda: saved(store, "project_turns", id=identity, state="uncertain"))
    project = saved(store, "project_turns", id=identity)
    [project_sent] = [start for start in backend.starts if start["phase"] == "project_synthesize"]
    assert (project["thread_id"], project["turn_id"]) == (project_sent["thread_id"], None)
    assert len(slots.named("begin", thread=project["thread_id"])) == 1
    await steps(scenario, 2)
    assert saved(store, "project_turns", id=identity, state="uncertain")["turn_id"] is None
    assert len(slots.named("begin", thread=project["thread_id"])) == 1
    # The unknown project turn keeps its owner exclusive and one configured turn owed.
    assert wf.agent_pool.turn_capacity() == 1
    assert not wf.agent_pool.owner_free(project_owner("test-project"))
    await wf.close()


async def test_two_unknown_begins_survive_a_shrunken_pool_without_a_duplicate(multi_scenario):
    scenario = multi_scenario
    store, backend, slots = scenario.store, scenario.agent_backend, scenario.slots
    wf = await scenario.started()
    ends = []
    for brief in ("Synthetic lost begin one.", "Synthetic lost begin two."):
        eid, lost = await lost_begin(scenario, brief)
        ends.append((eid, lost))
    sent = list(backend.starts)
    [first, second] = ends
    assert [(lost["state"], lost["turn_id"]) for _, lost in ends] == [("uncertain", None)] * 2
    assert (len(sent), backend.turns_started) == (2, 2)
    assert wf.agent_pool.turn_capacity() == 0

    # Two configured sessions shrink to one while both turns stay unknown: the pool owes
    # more possibly-live turns than it can host, and no configured server is hidden.
    await scenario.restart(sessions=1)
    wf = await scenario.started()
    assert [lane.agent.instance for lane in wf.lanes] == ["synthetic-agent-3"]
    assert wf.agent_pool.turn_capacity() == -1
    third = create(store, "Synthetic third experiment after shrink.")
    gate = scenario.gate("plan")
    scenario.agent.script("plan", turn=plan_turn, turns=1)
    await steps(scenario, 2, gates=[gate])
    assert not any(action["turn_id"] for action in actions(store, third))
    assert (backend.turns_started, len(slots.named("begin"))) == (2, 2)

    # Exact readback settles one saved turn, never starts one, and frees exactly one unit.
    backend.turns[(sent[0]["thread_id"], sent[0]["turn_id"])].update(
        result=plan_turn(None, None), state="completed"
    )
    await scenario.run(
        lambda: (store.experiment(first[0])["plan_review"] or {}).get("state") == "awaiting_review",
        gates=[gate],
    )
    assert wf.agent_pool.turn_capacity() == 0
    await steps(scenario, 1, gates=[gate])
    assert not any(action["turn_id"] for action in actions(store, third))

    backend.turns[(sent[1]["thread_id"], sent[1]["turn_id"])].update(
        result=plan_turn(None, None), state="completed"
    )
    await scenario.run(
        lambda: (store.experiment(second[0])["plan_review"] or {}).get("state") == "awaiting_review",
        gates=[gate],
    )
    started = await scenario.run(
        lambda: saved(store, "agent_actions", experiment_id=third, phase="plan", state="running"),
        gates=[gate],
    )
    # Both pre-restart identities settled exactly; the freed configured turn serves the
    # next source once, on the single restarted connection, and admits nothing more.
    assert connection_evidence(scenario, wf, "agent_actions", started).slot == 0
    assert [call["instance"] for call in slots.named("begin", turn=started["turn_id"])] == [
        "synthetic-agent-3"
    ]
    assert [(row["id"], row["thread_id"], row["turn_id"]) for row in actions(store, second[0])] == [
        (second[1]["id"], sent[1]["thread_id"], sent[1]["turn_id"])
    ]
    assert (backend.turns_started, wf.agent_pool.turn_capacity()) == (3, 0)
    await steps(scenario, 1, gates=[gate])
    assert (backend.turns_started, len(slots.named("begin"))) == (3, 3)
    gate.release.set()
    await turn_task(wf, "agent_actions", started)
    await wf.close()


async def test_unreadable_outcomes_keep_debt_while_another_owner_progresses(multi_scenario):
    scenario = multi_scenario
    store, backend, slots = scenario.store, scenario.agent_backend, scenario.slots
    # A direct-SQL source row: this case proves saved-state capacity and readback.
    blocked_eid = create(store, "Synthetic unreadable owner.")
    blocked = insert_action(
        store, blocked_eid, "uncertain", phase="plan", thread="hidden-thread", turn="hidden-turn"
    )
    save_dispatch_intent(store, blocked)
    backend.unreadable["hidden-thread"] = TransportError("thread/read transport closed")
    wf = await scenario.started()
    assert wf.agent_pool.turn_debt() == {("agent_actions", blocked["id"])}
    assert wf.agent_pool.turn_capacity() == 1
    assert not wf.agent_pool.owner_free(experiment_owner(blocked_eid))
    assert len(slots.named("begin")) == 0  # the saved row is a fixture, not a dispatch

    # Another typed owner uses the one remaining configured turn.
    completed(store, summary="Synthetic completed source.")
    memory = wf.supervisor.memory
    memory.refresh()
    identity = memory.enqueue(
        "test-project", {"request_id": str(uuid4()), "kind": "synthesize"}, origin="weekly"
    )["request_id"]
    gate = scenario.gate("project_synthesize")
    scenario.agent.script("project_synthesize", turn=synthesize_turn, turns=1)
    live = await scenario.run(
        lambda: saved(store, "project_turns", id=identity, state="running"), gates=[gate]
    )
    assert connection_evidence(scenario, wf, "project_turns", live).slot == 0
    assert wf.agent_pool.turn_capacity() == 0

    # Interrupt leaves through the owning live process and needs no configured turn.
    reservation = wf.agent_pool.lookup("project_turns", identity)
    assert await wf.agent_pool.interrupt(reservation) is True
    assert [(call["slot"], call["turn"]) for call in slots.named("interrupt")] == [(0, live["turn_id"])]
    assert wf.agent_pool.turn_capacity() == 0

    # The live turn settles independently while the unreadable turn keeps its debt; a
    # closed connection never proves that the remote turn ended.
    gate.release.set()
    await turn_task(wf, "project_turns", live)
    await scenario.run(lambda: saved(store, "project_turns", id=identity, state="completed"), gates=[gate])
    assert wf.agent_pool.turn_capacity() == 1
    assert wf.agent_pool.turn_debt() == {("agent_actions", blocked["id"])}

    # Two saved outcomes whose exit is unproven exhaust both configured turns, and a
    # received result excludes its owner without consuming a turn of its own.
    third = create(store, "Synthetic waiting experiment.")
    ready = insert_action(store, third, "running", phase="title", thread="ready-thread", turn="ready-turn")
    seed_turn(backend, "ready-thread", "ready-turn", {"title": "Synthetic readback title"})
    wf.agent_pool.reconstruct()
    received_eid = create(store, "Synthetic received owner.")
    insert_action(
        store, received_eid, "received", phase="prepare", thread="received-thread", turn="received-turn"
    )
    plan_gate = scenario.gate("plan")
    scenario.agent.script("plan", turn=plan_turn, turns=1)
    assert wf.agent_pool.turn_capacity() == 0
    assert not wf.agent_pool.owner_free(experiment_owner(received_eid))
    assert wf.agent_pool.turn_debt() == {("agent_actions", blocked["id"]), ("agent_actions", ready["id"])}

    # A shrunken pool owes more possibly-live turns than it can host; no server hides.
    await scenario.restart(sessions=1)
    wf = await scenario.started()
    assert [lane.agent.instance for lane in wf.lanes] == ["synthetic-agent-3"]
    assert wf.agent_pool.turn_capacity() == -1
    assert (backend.turns_started, len(slots.named("begin"))) == (1, 1)
    # Role readback at negative capacity settles exactly the saved ready turn.
    await wf.advance_title()
    assert saved(store, "agent_actions", id=ready["id"], state="completed")["result"]
    assert wf.agent_pool.turn_capacity() == 0
    assert (backend.turns_started, len(slots.named("begin"))) == (1, 1)
    assert [call["instance"] for call in slots.named("read_turn", turn="ready-turn")] == ["synthetic-agent-3"]
    # No free configured turn: the received owner is excluded and the experiment's plan
    # waits for capacity, so neither begins.
    await steps(scenario, 2, gates=[plan_gate])
    assert not any(
        action["turn_id"]
        for eid in (third, received_eid)
        for action in actions(store, eid)
        if action["phase"] == "plan"
    )
    assert (backend.turns_started, len(slots.named("begin"))) == (1, 1)
    assert not wf.agent_pool.owner_free(experiment_owner(received_eid))

    # The unreadable thread becomes readable with the exact saved turn completed: the
    # resolver adopts that identity and frees the unit that the next source then uses.
    del backend.unreadable["hidden-thread"]
    backend.turns[("hidden-thread", "hidden-turn")] = {
        "record": {
            "phase": "plan",
            "thread_id": "hidden-thread",
            "turn_id": "hidden-turn",
            "prompt_sha256": "",
        },
        "result": plan_turn(None, None),
        "state": "completed",
    }
    scenario.clock.advance(310)  # the production retry backoff expires in fake time only
    started = await scenario.run(
        lambda: saved(store, "agent_actions", experiment_id=third, phase="plan", state="running"),
        gates=[plan_gate],
    )
    adopted = saved(store, "agent_actions", id=blocked["id"])
    assert (adopted["state"], adopted["thread_id"], adopted["turn_id"]) == (
        "completed",
        "hidden-thread",
        "hidden-turn",
    )
    assert len(actions(store, blocked_eid)) == 1
    assert slots.named("begin", turn="hidden-turn") == []  # readback never dispatched it
    # The waiting source uses the freed turn once; the received owner still begins nothing.
    assert connection_evidence(scenario, wf, "agent_actions", started).slot == 0
    assert [call["instance"] for call in slots.named("begin", turn=started["turn_id"])] == [
        "synthetic-agent-3"
    ]
    assert not any(action["turn_id"] for action in actions(store, received_eid) if action["phase"] == "plan")
    assert (backend.turns_started, wf.agent_pool.turn_capacity()) == (2, 0)
    plan_gate.release.set()
    await turn_task(wf, "agent_actions", started)
    await wf.close()


class StubTransport:
    """The production App Server surface answered from memory: no process, socket or model."""

    def __init__(self):
        self.on_notification = self.on_request = None
        self.available = False
        self.starts = 0
        self.sent = []

    async def start(self):
        self.starts += 1
        self.available = True

    async def rpc(self, method, params, *, timeout=None):
        if method == "account/read":
            return {"account": {"type": "chatgpt"}}
        if method == "model/list":
            return STUB_CATALOG
        if method == "thread/start":
            return {"thread": {"id": f"stub-thread-{next(THREADS)}"}, "model": "stub-model"}
        if method == "turn/start":
            return {"turn": {"id": f"stub-turn-{next(TURNS)}"}}
        if method == "account/rateLimits/read":
            return STUB_QUOTA
        if method == "turn/interrupt":
            return {}
        raise AssertionError(f"unexpected synthetic App Server call {method}")

    async def send(self, message):
        self.sent.append(message)

    async def close(self):
        self.available = False

    async def deliver(self, message):
        """Hand one runtime message to the production receipt, as ``AppServer._read`` does."""
        if "id" in message:
            await self.on_request(message)
        else:
            await self.on_notification(message)


class StubCodexAgent(CodexAgent):
    """A real CodexAgent whose pinned JSONL transport is replaced before any socket starts."""

    def __init__(self, config, state_dir):
        super().__init__(config, state_dir)
        self.server = StubTransport()


async def routed_turn(store, lane, eid, phase):
    """A saved turn whose exact thread/turn identity came from the production agent."""
    thread, _ = await lane.agent.session(None, store.path.parent, writable=False)
    turn = await lane.agent.begin(thread, "Synthetic callback prompt", {})
    row = insert_action(store, eid, "running", phase=phase, thread=thread, turn=turn)
    lane.agent_pool.claim(
        role="stage",
        source_table="agent_actions",
        row=row,
        owner=experiment_owner(eid),
        workflow=lane,
    )
    return row


async def test_production_generation_routes_callbacks_to_the_exact_live_process(config, monkeypatch):
    fake_clock = FakeClock()
    fake_clock.install(monkeypatch)
    config.codex.max_sessions = 2
    monkeypatch.setattr(workflow_module, "CodexAgent", StubCodexAgent)
    store = Store(config.state_dir / "workflow.sqlite")
    wf = Workflow(config, store, FakeSlurm(), StubCodexAgent(config.codex, config.state_dir))
    await wf.start()
    try:
        first, second = wf.lanes
        assert [(lane.agent.start_count, lane.agent.server.starts) for lane in wf.lanes] == [(1, 1), (1, 1)]
        eid, other = create(store, "Synthetic callback owner."), create(store, "Synthetic other owner.")
        # Direct-SQL saved rows: routing and generation are what this case proves.
        left = await routed_turn(store, first, eid, "prepare")
        right = await routed_turn(store, second, other, "prepare")
        assert [
            first.agent_pool.lookup("agent_actions", row["id"]).connection.slot for row in (left, right)
        ] == [0, 1]
        activity = {
            "method": "item/started",
            "params": {
                "threadId": left["thread_id"],
                "turnId": left["turn_id"],
                "item": {"id": "1", "type": "reasoning"},
            },
        }
        request = {
            "id": 7,
            "method": "item/tool/requestUserInput",
            "params": {
                "threadId": left["thread_id"],
                "turnId": left["turn_id"],
                "questions": [{"id": "dataset"}],
            },
        }

        # Activity reaches only the process whose saved turn it belongs to.
        await first.agent.server.deliver(activity)
        await second.agent.server.deliver(
            {
                **activity,
                "params": {**activity["params"], "threadId": right["thread_id"], "turnId": right["turn_id"]},
            }
        )
        assert store.projection(eid)["stage_activity"] and store.projection(other)["stage_activity"]
        # A request for the left turn on the other live connection is not its owner.
        with pytest.raises(NeedsAttention):
            await second.agent.server.deliver(request)
        assert store.experiment(other)["pending_request"] is None
        await first.agent.server.deliver(request)
        pending = store.experiment(eid)["pending_request"]
        assert (pending["action_id"], pending["rpc_id"]) == (left["id"], 7)
        assert store.experiment(other)["pending_request"] is None

        # A quota event on the other connection updates the one shared decision only.
        quota = quota_payload(5)
        await second.agent.server.deliver({"method": "account/rateLimits/updated", "params": quota})
        await asyncio.gather(*second.agent.notification_tasks)  # the reader never awaits this handler
        assert (wf.gate.windows[300]["usedPercent"], wf.gate.decide().admitted) == (5, True)
        assert [
            (store.one("SELECT state FROM agent_actions WHERE id=?", (row["id"],)))["state"]
            for row in (left, right)
        ] == ["running", "running"]

        # The normal answer command replies once, through the asking process only.
        store.command(
            uuid4(), "answer", {"pending_request_id": pending["id"], "answer": "synthetic A"}, eid=eid
        )
        await wf.step_control_after_observe(clock.now())
        replied = {"id": 7, "result": {"answers": {"dataset": {"answers": ["synthetic A"]}}}}
        assert first.agent.server.sent == [replied] and second.agent.server.sent == []
        assert store.experiment(eid)["pending_request"] is None
        await wf.step_control_after_observe(clock.now())
        assert first.agent.server.sent == [replied]

        # The next answer is saved against this process generation, then the process
        # restarts: a late callback of the old turn fails closed instead of answering it.
        await first.agent.server.deliver({**request, "id": 8})
        waiting = store.experiment(eid)["pending_request"]
        assert waiting["rpc_id"] == 8
        await first.connect_agent()
        assert (first.agent.start_count, first.agent.server.starts) == (2, 2)
        assert first.agent_pool.live_lane(first.agent_pool.lookup("agent_actions", left["id"])) is None
        with pytest.raises(NeedsAttention):
            await first.agent.server.deliver({**request, "id": 8})
        await first.agent.server.deliver(
            {**activity, "params": {**activity["params"], "item": {"id": "2", "type": "reasoning"}}}
        )
        assert len(store.projection(eid)["stage_activity"]) == 1
        store.command(
            uuid4(), "answer", {"pending_request_id": waiting["id"], "answer": "synthetic late"}, eid=eid
        )
        await wf.step_control_after_observe(clock.now())
        expired = store.experiment(eid)["pending_request"]
        assert (expired["expired"], expired["answer"]) == (True, "synthetic late")
        assert store.experiment(eid)["hold_reason"] == "expired_agent_request"
        assert first.agent.server.sent == [replied] and second.agent.server.sent == []
        # The other owner's saved turn and activity stay untouched.
        assert store.one("SELECT state FROM agent_actions WHERE id=?", (right["id"],))["state"] == "running"
        assert len(store.projection(other)["stage_activity"]) == 1
        assert not store.rows("SELECT * FROM experiments WHERE pending_request IS NOT NULL AND id<>?", (eid,))
        await wf.close()
    finally:
        store.close()
