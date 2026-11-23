"""One quota policy decides every role's selection filter and binding admission (T1.6b-2).

A selection decision is a filter, never authorization: each dispatch path asks the
same policy again after `refresh_quota()`/session setup, immediately before it saves
dispatch intent. A denial there launches nothing, leaves the source row unclaimed and
writes the hold the role already used. These tests drive fake owners over temporary
SQLite, so no managed turn is sent.
"""

import asyncio
from uuid import uuid4

from helpers import prepare_branch, role_step, seed_saved_contract3
from test_iteration_autopilot import activated, command

from experimenttracker.agent_pool import WEEKLY_PLENTY_REASON
from experimenttracker.autopilot_policy import lead_id
from experimenttracker.research.session import first_turn


def snapshot(five=0, week=0):
    """Ordinary and weekly usage percentages for the managed subscription bucket."""
    return {
        "ordinaryUsageAllowed": True,
        "rateLimitsByLimitId": {
            "codex": {
                "primary": {"windowDurationMins": 300, "usedPercent": five},
                "secondary": {"windowDurationMins": 10080, "usedPercent": week},
            }
        },
    }


def hold_session(agent, entered, release):
    """Gate one session setup so a decision can change after the cached admission."""
    original = agent.session

    async def session(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    agent.session = session


def member(store, run):
    """Save the member row an activated run uses to own an experiment."""
    eid = seed_saved_contract3(store, "Synthetic member.")
    with store.tx() as db:
        db.execute(
            "INSERT INTO autopilot_members VALUES(?,?,?,'question','exploration','test',0,1,0,NULL,'[]',?)",
            (eid, run["id"], run["project_id"], 1000000),
        )
    return eid


async def test_binding_denial_after_a_cached_admit_launches_nothing(runtime):
    store, _, agent, wf = runtime
    wf.agent_ready = True
    wf.gate.update(await agent.quota())
    identity = wf.supervisor.memory.enqueue(
        "test-project", {"request_id": str(uuid4()), "kind": "chat", "text": "What next?"}
    )["request_id"]
    entered, release = asyncio.Event(), asyncio.Event()
    hold_session(agent, entered, release)
    # The cached selection filter admits and claims before the session setup waits.
    await role_step(wf.supervisor)
    await entered.wait()
    assert agent.calls == 0 and wf.supervisor.task is not None
    # A fresh denial after that admission is binding: the turn is never sent.
    wf.gate.update({"ordinaryUsageAllowed": False})
    release.set()
    await wf.supervisor.task
    row = store.one("SELECT * FROM project_turns WHERE id=?", (identity,))
    assert (row["state"], row["hold_reason"], row["turn_id"]) == ("waiting", "quota_hold", None)
    assert agent.calls == 0
    # The never-sent claim is released with the busy flag it published.
    assert wf.agent_pool.lookup("project_turns", identity) is None
    assert wf.supervisor.task is None


def test_autopilot_member_turns_take_their_run_reserves(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    store, _, _, wf = runtime
    member_eid = member(store, run)
    other = seed_saved_contract3(store, "Unrelated.")
    # 45% of the five-hour window is left: the ordinary threshold admits, the run's
    # 50% reserve does not.
    wf.gate.update(snapshot(five=55))
    assert wf.gate.decide().admitted
    denied = wf.agent_pool.quota(eid=member_eid)
    assert not denied.admitted and denied.reason == "autopilot subscription reserve reached"
    assert not wf.agent_pool.quota(run=run).admitted
    assert wf.agent_pool.quota(eid=other).admitted
    # The run policy keeps its own hysteresis: clearing the reserve needs a margin.
    wf.gate.update(snapshot(five=52))
    assert not wf.agent_pool.quota(eid=member_eid).admitted
    wf.gate.update(snapshot(five=44))
    assert wf.agent_pool.quota(eid=member_eid).admitted


def test_weekly_synthesis_needs_plenty_while_owner_turns_do_not(runtime):
    _, _, _, wf = runtime
    # 35% of the five-hour window is left: admitted everywhere, plentiful nowhere.
    wf.gate.update(snapshot(five=65))
    assert wf.gate.decide().admitted
    weekly = wf.agent_pool.quota(origin="weekly")
    assert not weekly.admitted and weekly.reason == WEEKLY_PLENTY_REASON
    assert wf.agent_pool.quota(origin="local").admitted
    wf.gate.update(snapshot(five=45))
    assert wf.agent_pool.quota(origin="weekly").admitted


async def test_a_run_paused_between_selection_and_binding_refuses_the_lead(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    store, _, agent, wf = runtime
    wf.agent_ready = True
    wf.gate.update(await agent.quota())
    lead = lead_id(run["id"])
    await prepare_branch(wf, lead)
    action = first_turn(store, lead)
    entered, release = asyncio.Event(), asyncio.Event()
    hold_session(agent, entered, release)
    # The cached selection filter admits and claims the lead's saved first turn.
    await role_step(wf.research_dispatch)
    await entered.wait()
    reservation = wf.agent_pool.lookup("agent_actions", action["id"])
    task = wf.agent_pool.slots[reservation.slot].agent_task
    command(service, "pause")
    release.set()
    await task
    await asyncio.sleep(0)  # drain the done callback that releases the never-sent claim
    # The run's current status is re-checked after session setup, before saved intent.
    row = store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))
    assert (row["state"], row["thread_id"], row["turn_id"]) == ("pending", None, None)
    assert wf.agent_pool.quota(eid=lead).reason == "autopilot_paused"
    assert agent.calls == 0
    assert wf.agent_pool.lookup("agent_actions", action["id"]) is None
    await wf.close()
