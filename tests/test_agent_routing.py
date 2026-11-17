"""Callback routing through derived reservations (T1.6a-2).

Every callback carries the lane and start count of the process that received it.
A request, activity item or saved answer reaches only the reservation that its
saved thread/turn proves on that same process; nothing else is guessed, and the
retired research fallback stays gone.
"""

import json
from uuid import uuid4

import pytest
from test_iteration_autopilot import activated, command

from experimenttracker import clock
from experimenttracker.agent_pool import experiment_owner, project_owner
from experimenttracker.codex_client import DemoAgent, NeedsAttention
from experimenttracker.workflow import Workflow


class LaneAgent(DemoAgent):
    """Managed-agent double that records answers instead of writing a protocol reply."""

    def __init__(self):
        super().__init__()
        self.replies = []

    async def reply(self, pending):
        self.replies.append(pending)


async def pooled(runtime):
    """One pool over two lanes; each lane has its own started agent process."""
    store, scheduler, _, old = runtime
    old.config.codex.max_sessions = 2
    wf = Workflow(old.config, store, scheduler, LaneAgent())
    await wf.connect_agent()
    await wf.start_agent_pool()
    return wf


def create(store):
    return store.command(
        uuid4(), "create", {"brief": "Route synthetic callbacks.", "recipe_id": "agent_experiment_v1"}
    )["experiment_id"]


def live_action(store, eid, *, phase="prepare", thread="thread", turn="turn"):
    with store.tx() as db:
        action_id = str(uuid4())
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,input,created_at,updated_at) "
            "VALUES(?,?,?,1,'running',?,?,'prompt',0,0)",
            (action_id, eid, phase, thread, turn),
        )
    return store.one("SELECT * FROM agent_actions WHERE id=?", (action_id,))


def claim(wf, lane, row, role, *, source_table="agent_actions", owner=None):
    return wf.agent_pool.claim(
        role=role,
        source_table=source_table,
        row=row,
        owner=owner or experiment_owner(row["experiment_id"]),
        workflow=lane,
    )


def activity(thread, turn):
    params = {"threadId": thread, "turnId": turn}
    return {"method": "item/started", "params": {**params, "item": {"id": "1", "type": "reasoning"}}}


def request(thread, turn):
    questions = [{"id": "dataset"}]
    return {
        "id": 7,
        "method": "item/tool/requestUserInput",
        "params": {"threadId": thread, "turnId": turn, "questions": questions},
    }


def project_pending(service, action_id):
    row = service.store.one("SELECT pending_request FROM autopilot_actions WHERE id=?", (action_id,))
    return json.loads(row["pending_request"])


async def test_each_lane_receives_only_its_own_turn_s_callbacks(runtime):
    wf = await pooled(runtime)
    store, first, second = wf.store, wf.lanes[0], wf.lanes[1]
    left, right = create(store), create(store)
    left_reservation = claim(wf, first, live_action(store, left, thread="left", turn="left-turn"), "stage")
    right_action = live_action(store, right, thread="right", turn="right-turn")
    claim(wf, second, right_action, "stage")
    assert left_reservation.connection.slot == 0 and left_reservation.connection.start >= 1

    # The left turn's activity is only owned by the lane that runs it.
    await second._activity(activity("left", "left-turn"), second.agent.start_count)
    assert not store.projection(left)["stage_activity"]
    await first._activity(activity("left", "left-turn"), first.agent.start_count)
    assert store.projection(left)["stage_activity"]
    assert not store.projection(right)["stage_activity"]

    # The right turn's request is only answered by its own lane and experiment.
    with pytest.raises(NeedsAttention):
        await first._request(request("right", "right-turn"), first.agent.start_count)
    assert store.experiment(right)["pending_request"] is None
    await second._request(request("right", "right-turn"), second.agent.start_count)
    pending = store.experiment(right)["pending_request"]
    assert (pending["action_id"], pending["rpc_id"]) == (right_action["id"], 7)
    assert store.experiment(left)["pending_request"] is None

    # The answer is delivered by the asking lane, and only once.
    answer = {"pending_request_id": pending["id"], "answer": "dataset A"}
    store.command(uuid4(), "answer", answer, eid=right)
    await wf.step_control_after_observe(clock.now())
    assert [entry["id"] for entry in second.agent.replies] == [pending["id"]]
    assert first.agent.replies == []
    assert store.experiment(right)["pending_request"] is None
    await wf.close()


@pytest.mark.parametrize(
    ("phase", "role"),
    [("prepare", "stage"), ("title", "title"), ("monitor", "monitor"), ("discuss:seeded", "conversation")],
)
async def test_every_saved_role_routes_its_request_to_the_owning_experiment(runtime, phase, role):
    wf = await pooled(runtime)
    lane = wf.lanes[1]
    eid = create(wf.store)
    action = live_action(wf.store, eid, phase=phase, thread="own", turn="own-turn")
    claim(wf, lane, action, role)
    await lane._request(request("own", "own-turn"), lane.agent.start_count)
    pending = wf.store.experiment(eid)["pending_request"]
    assert (pending["action_id"], pending["expired"]) == (action["id"], False)
    assert wf.store.experiment(eid)["hold_reason"] == "needs_attention"
    assert wf.store.rows("SELECT * FROM inbox WHERE kind='required_decision'")
    await wf.close()


async def test_managed_project_request_routes_to_its_saved_run_turn(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    wf = await pooled(runtime)
    lane, store = wf.lanes[1], wf.store
    action_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO autopilot_actions(id,run_id,cursor,control_revision,state,thread_id,turn_id,created_at,updated_at) "
            "VALUES(?,?,0,?,'running','project-thread','project-turn',0,0)",
            (action_id, run["id"], run["control_revision"]),
        )
    row = store.one("SELECT * FROM autopilot_actions WHERE id=?", (action_id,))
    claim(wf, lane, row, "research", source_table="autopilot_actions", owner=project_owner("test-project"))
    await lane._request(request("project-thread", "project-turn"), lane.agent.start_count)
    pending = project_pending(service, action_id)
    assert pending["rpc_id"] == 7 and lane.agent.replies == []
    command(service, "answer", input_id=pending["id"], text="Synthetic fixture only.")
    await wf.research.reply_inputs()
    assert [entry["id"] for entry in lane.agent.replies] == [pending["id"]]
    assert project_pending(service, action_id)["expired"] is True
    await wf.close()


async def test_unmatched_old_and_supervisor_requests_fail_closed(runtime, monkeypatch):
    wf = await pooled(runtime)
    lane, store, asked = wf.lanes[0], wf.store, []

    async def research_request(*args):
        asked.append(args)

    monkeypatch.setattr(wf.research, "request", research_request)
    # The retired fallback read this holder; it must not steer any request now.
    wf.research.active_id = "saved-project-turn"
    eid = create(store)
    old = live_action(store, eid, thread="old", turn="old-turn")
    with pytest.raises(NeedsAttention):
        await lane._request(request("unknown", "unknown-turn"), lane.agent.start_count)
    store.db.execute("UPDATE agent_actions SET state='completed' WHERE id=?", (old["id"],))
    with pytest.raises(NeedsAttention):
        await lane._request(request("old", "old-turn"), lane.agent.start_count)
    supervisor = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO project_turns(id,project_id,payload,origin,state,thread_id,turn_id,created_at,updated_at) "
            "VALUES(?,?,'{}','local','running','supervisor','supervisor-turn',0,0)",
            (supervisor, "test-project"),
        )
    row = store.one("SELECT * FROM project_turns WHERE id=?", (supervisor,))
    claim(wf, lane, row, "supervisor", source_table="project_turns", owner=project_owner("test-project"))
    with pytest.raises(NeedsAttention):
        await lane._request(request("supervisor", "supervisor-turn"), lane.agent.start_count)
    assert asked == []
    assert store.rows("SELECT * FROM autopilot_notices") == []
    assert not store.rows("SELECT * FROM experiments WHERE pending_request IS NOT NULL")
    await wf.close()


async def test_answer_after_a_restart_expires_instead_of_replaying(runtime):
    wf = await pooled(runtime)
    lane, store = wf.lanes[1], wf.store
    eid = create(store)
    claim(wf, lane, live_action(store, eid, thread="asked", turn="asked-turn"), "stage")
    await lane._request(request("asked", "asked-turn"), lane.agent.start_count)
    pending = store.experiment(eid)["pending_request"]
    store.command(uuid4(), "answer", {"pending_request_id": pending["id"], "answer": "dataset A"}, eid=eid)
    # A restarted process starts a new count: the obsolete RPC ID is never replayed.
    await lane.connect_agent()
    await wf.step_control_after_observe(clock.now())
    assert lane.agent.replies == []
    saved = store.experiment(eid)["pending_request"]
    assert (saved["expired"], saved["answer"]) == (True, "dataset A")
    assert store.experiment(eid)["hold_reason"] == "expired_agent_request"
    await wf.close()


async def test_late_activity_after_settlement_cannot_attach_to_a_successor(runtime):
    wf = await pooled(runtime)
    lane, store = wf.lanes[1], wf.store
    eid = create(store)
    action = live_action(store, eid, thread="settled", turn="settled-turn")
    reservation = claim(wf, lane, action, "stage")
    store.db.execute("UPDATE agent_actions SET state='uncertain' WHERE id=?", (action["id"],))
    wf.agent_pool.settle(reservation)
    # Saved ownership survives without a connection; the callback has no owner.
    assert wf.agent_pool.lookup("agent_actions", action["id"]).connection is None
    claim(wf, lane, live_action(store, eid, thread="next", turn="next-turn"), "stage")
    await lane._activity(activity("settled", "settled-turn"), lane.agent.start_count)
    await lane._activity(activity("next", "next-turn"), lane.agent.start_count)
    assert len(store.projection(eid)["stage_activity"]) == 1
    await wf.close()
