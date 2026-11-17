"""Mixed-role overlap and serialization through the real engine (T1.6c-1).

Several configured slots and one explicit shared fake backend prove that an
experiment, a research decision, weekly supervision and a discussion run at the same
time on distinct connections and process generations, while one slot and one typed
owner serialize. Synthetic mechanism acceptance only: no live scheduler, managed
account, socket runtime or research.
"""

import json
from uuid import uuid4

from legacy_fixture import load_legacy_completed
from test_iteration_autopilot import command, policy
from test_knowledge import completed

from experimenttracker.agent_pool import experiment_owner, project_owner
from experimenttracker.autopilot import Autopilot
from scenario.test_iteration_scenario_healthy_job import plan_turn, prepare_turn

DECISION = {
    "summary": "Synthetic checkpoint.",
    "central_argument": "No real science in this fixture.",
    "uncertainty": "Real retrieval remains a live prerequisite.",
    "outcome": "wait",
}
LUNA = {
    "id": "gpt-6-luna",
    "name": "Synthetic Luna title model",
    "reasoning_efforts": ["low"],
    "default_effort": "low",
}


def title_turn(payload, workspace):
    return {"title": "Synthetic mixed-role title"}


def research_turn(payload, workspace):
    return DECISION


def synthesize_turn(payload, workspace):
    """A weekly working report that organizes exactly the saved sources it was given."""
    ids = list(payload["sources"])
    return {
        "reply": "Synthetic weekly synthesis of completed synthetic demonstrations.",
        "experiment_ids": ids,
        "report": {
            "title": "Synthetic working report",
            "overview": "Synthetic demonstrations only; no research conclusion.",
            "sections": [
                {
                    "title": "Synthetic sources",
                    "body": "Mechanism acceptance only.",
                    "experiment_ids": ids,
                    "blocks": [],
                }
            ],
            "open_questions": ["Which real question should be tested next?"],
        },
    }


def discuss_turn(payload, workspace):
    return {"schema_version": 1, "reply": "Synthetic discussion reply.", "evidence_ids": []}


def create(store, brief="Synthetic mixed-role overlap."):
    return store.command(uuid4(), "create", {"brief": brief, "recipe_id": "agent_experiment_v1"})[
        "experiment_id"
    ]


def saved(store, table, **where):
    return store.one(
        f"SELECT * FROM {table} WHERE {' AND '.join(f'{key}=?' for key in where)}", tuple(where.values())
    )


def activated(scenario):
    """Configure and activate the synthetic research run through its own commands."""
    service = Autopilot(scenario.store, scenario.config)
    command(service, "configure", policy=policy(now=scenario.clock.now()).model_dump())
    command(service, "activate")
    return service, service.run("test-project")


def connection_evidence(scenario, wf, source_table, row):
    """Prove this saved turn runs on the one process generation its transport logged."""
    reservation = wf.agent_pool.lookup(source_table, row["id"])
    assert reservation is not None and reservation.connection is not None, f"{row['id']} is not live"
    connection = reservation.connection
    lane = wf.slots[connection.slot]
    call = scenario.slots.begin(row["turn_id"])  # exactly one begin for this saved turn
    assert (call["slot"], call["instance"], call["generation"]) == (
        connection.slot,
        lane.agent.instance,
        connection.start,
    )
    # The registry's generation is the transport's own start count on this clone.
    assert lane.agent.start_count == call["generation"] == connection.start
    return connection


def live_both(scenario, wf, left, right):
    """Both saved turns run at the same time on different connections."""
    first = connection_evidence(scenario, wf, *left)
    second = connection_evidence(scenario, wf, *right)
    assert first.slot != second.slot
    return first, second


def turn_task(wf, source_table, row):
    """The dispatch task that holds this saved turn's live connection."""
    lane = wf.slots[wf.agent_pool.lookup(source_table, row["id"]).connection.slot]
    if source_table == "autopilot_actions":
        return wf.research.task
    if source_table == "project_turns":
        return wf.supervisor.task
    return lane.agent_task


def begin_turns(scenario, phase):
    return [
        call["turn"] for call in scenario.slots.calls if call["call"] == "begin" and call["phase"] == phase
    ]


async def test_two_slots_overlap_an_experiment_and_a_research_decision(multi_scenario):
    scenario = multi_scenario
    wf = await scenario.started()
    store, backend = scenario.store, scenario.agent_backend
    service, run = activated(scenario)
    with store.tx() as db:
        service.wake(db, run["id"], "synthetic-input", "experiment_change", {"synthetic": True})
    plan_gate, research_gate = scenario.gate("plan"), scenario.gate("research")
    scenario.agent.script("plan", turn=plan_turn, turns=1)
    scenario.agent.script("research", turn=research_turn, turns=1)
    eid = create(store)

    def both_running():
        plan = saved(store, "agent_actions", experiment_id=eid, phase="plan", state="running")
        decision = saved(store, "autopilot_actions", run_id=run["id"], state="running")
        return (plan, decision) if plan and decision else None

    plan, decision = await scenario.run(both_running, gates=[plan_gate, research_gate])
    live_both(scenario, wf, ("agent_actions", plan), ("autopilot_actions", decision))
    assert backend.turns_started == 2

    # The experiment's own saved turn finishes while research stays live.
    plan_gate.release.set()
    await turn_task(wf, "agent_actions", plan)
    await scenario.run(
        lambda: (store.experiment(eid)["plan_review"] or {}).get("state") == "awaiting_review",
        gates=[research_gate],
    )
    assert saved(store, "autopilot_actions", id=decision["id"])["state"] == "running"

    # Then the research decision applies its own saved result to its own run.
    research_gate.release.set()
    await turn_task(wf, "autopilot_actions", decision)
    applied = await scenario.run(
        lambda: saved(store, "autopilot_actions", id=decision["id"], state="completed"),
        gates=[research_gate],
    )
    assert json.loads(applied["result"])["summary"] == DECISION["summary"]
    assert saved(store, "autopilot_runs", id=run["id"])["central_argument"] == DECISION["central_argument"]
    assert begin_turns(scenario, "research") == [applied["turn_id"]]

    # A closed socket is not a finished turn: the next generation is a new process,
    # and the next admitted turn records exactly that generation.
    wf.agent_ready = False
    scenario.clock.advance(120)
    scenario.agent.script("plan", turn=plan_turn, turns=1)
    gate = scenario.gate("plan")
    second = create(store, "Synthetic second experiment.")
    plan2 = await scenario.run(
        lambda: saved(store, "agent_actions", experiment_id=second, phase="plan", state="running"),
        gates=[plan_gate, gate],
    )
    assert connection_evidence(scenario, wf, "agent_actions", plan2).start == 2
    gate.release.set()
    await turn_task(wf, "agent_actions", plan2)
    await wf.close()


async def test_two_slots_overlap_an_experiment_and_a_weekly_synthesis(multi_scenario):
    scenario = multi_scenario
    wf = await scenario.started()
    store, backend = scenario.store, scenario.agent_backend
    completed(store, summary="Synthetic completed source.")
    memory = wf.supervisor.memory
    memory.refresh()
    identity = memory.enqueue(
        "test-project", {"request_id": str(uuid4()), "kind": "synthesize"}, origin="weekly"
    )["request_id"]
    # Weekly synthesis needs plentiful quota, not merely an admitted window.
    backend.weekly_used_percent = 60
    waiting = await scenario.run(lambda: saved(store, "project_turns", id=identity, state="waiting"))
    assert waiting["hold_reason"] == "quota_hold" and not begin_turns(scenario, "project_synthesize")

    backend.weekly_used_percent = 0
    plan_gate, synthesis_gate = scenario.gate("plan"), scenario.gate("project_synthesize")
    scenario.agent.script("plan", turn=plan_turn, turns=1)
    scenario.agent.script("project_synthesize", turn=synthesize_turn, turns=1)
    eid = create(store)

    def both_running():
        plan = saved(store, "agent_actions", experiment_id=eid, phase="plan", state="running")
        weekly = saved(store, "project_turns", id=identity, state="running")
        return (plan, weekly) if plan and weekly else None

    plan, weekly = await scenario.run(both_running, gates=[plan_gate, synthesis_gate])
    live_both(scenario, wf, ("agent_actions", plan), ("project_turns", weekly))
    # One project holds an experiment turn and a project turn: distinct typed owners.
    assert wf.agent_pool.lookup("agent_actions", plan["id"]).owner == experiment_owner(eid)
    assert wf.agent_pool.lookup("project_turns", identity).owner == project_owner("test-project")

    # The weekly synthesis applies its saved report while the experiment stays live.
    synthesis_gate.release.set()
    await turn_task(wf, "project_turns", weekly)
    done = await scenario.run(
        lambda: saved(store, "project_turns", id=identity, state="completed"), gates=[plan_gate]
    )
    assert done["report_version"] == 1
    report = memory.report("test-project")
    assert (report["version"], report["document"]["source_scope"]) == (1, "synthetic")
    assert saved(store, "agent_actions", id=plan["id"])["state"] == "running"

    plan_gate.release.set()
    await turn_task(wf, "agent_actions", plan)
    await scenario.run(
        lambda: (store.experiment(eid)["plan_review"] or {}).get("state") == "awaiting_review",
        gates=[synthesis_gate],
    )
    assert saved(store, "agent_actions", id=plan["id"])["state"] == "completed"
    await wf.close()


async def test_two_slots_overlap_a_discussion_and_an_unrelated_experiment(multi_scenario):
    scenario = multi_scenario
    wf = await scenario.started()
    store = scenario.store
    parent = load_legacy_completed(store, wf)
    identity = store.command(uuid4(), "discuss", {"text": "Explain the synthetic result"}, eid=parent)[
        "request_id"
    ]
    discussion_gate, plan_gate = scenario.gate("discuss"), scenario.gate("plan")
    scenario.agent.script("discuss", turn=discuss_turn, turns=1)
    scenario.agent.script("plan", turn=plan_turn, turns=1)
    eid = create(store, "Synthetic unrelated experiment.")
    action = "discuss:" + identity

    def both_running():
        discussion = saved(store, "agent_actions", experiment_id=parent, phase=action, state="running")
        plan = saved(store, "agent_actions", experiment_id=eid, phase="plan", state="running")
        return (discussion, plan) if discussion and plan else None

    discussion, plan = await scenario.run(both_running, gates=[discussion_gate, plan_gate])
    live_both(scenario, wf, ("agent_actions", discussion), ("agent_actions", plan))

    # The discussion keeps its queued identity and evidence, and completes alone.
    discussion_gate.release.set()
    await turn_task(wf, "agent_actions", discussion)
    done = await scenario.run(
        lambda: saved(store, "discussions", id=identity, state="completed"), gates=[plan_gate]
    )
    assert (done["action_id"], done["report_version"]) == (discussion["id"], None)
    assert (done["reply"], json.loads(done["evidence_ids"])) == ("Synthetic discussion reply.", [])
    assert saved(store, "agent_actions", id=plan["id"])["state"] == "running"

    plan_gate.release.set()
    await turn_task(wf, "agent_actions", plan)
    await scenario.run(
        lambda: (store.experiment(eid)["plan_review"] or {}).get("state") == "awaiting_review",
        gates=[discussion_gate],
    )
    assert saved(store, "agent_actions", id=discussion["id"])["state"] == "completed"
    await wf.close()


async def test_one_slot_serializes_mixed_roles_without_a_hidden_agent(scenario):
    store, backend = scenario.store, scenario.agent_backend
    completed(store, summary="Synthetic completed source.")
    memory = scenario.workflow.supervisor.memory
    memory.refresh()
    identity = memory.enqueue(
        "test-project", {"request_id": str(uuid4()), "kind": "synthesize"}, origin="weekly"
    )["request_id"]
    plan_gate, synthesis_gate = scenario.gate("plan"), scenario.gate("project_synthesize")
    scenario.agent.script("plan", turn=plan_turn, turns=1)
    scenario.agent.script("project_synthesize", turn=synthesize_turn, turns=1)
    eid = create(store)
    await scenario.run(
        lambda: saved(store, "agent_actions", experiment_id=eid, phase="plan", state="running"),
        gates=[plan_gate, synthesis_gate],
    )
    # One configured connection: the weekly turn keeps its saved place, nothing else starts.
    scenario.one_session()
    assert saved(store, "project_turns", id=identity)["state"] in ("queued", "waiting")
    assert scenario.workflow.supervisor.task is None and scenario.workflow.research.task is None
    assert scenario.agent_backend.turns_started == 1 and scenario.agent.start_count == 1

    plan_gate.release.set()
    await scenario.workflow.agent_task
    weekly = await scenario.run(
        lambda: saved(store, "project_turns", id=identity, state="running"),
        gates=[plan_gate, synthesis_gate],
    )
    # The next admitted source keeps its exact saved identity on the freed connection.
    reservation = scenario.workflow.agent_pool.lookup("project_turns", weekly["id"])
    assert (reservation.connection.slot, reservation.connection.start) == (0, 1)
    assert [start["phase"] for start in backend.starts] == ["plan", "project_synthesize"]
    assert [(weekly["thread_id"], weekly["turn_id"])] == [
        (start["thread_id"], start["turn_id"]) for start in backend.starts[1:]
    ]
    await scenario.close()


async def test_same_owner_holds_implementation_while_a_spare_slot_serves_another(multi_scenario):
    scenario = multi_scenario
    wf = await scenario.started()
    store = scenario.store
    scenario.slots.agent.models = [*scenario.agent.models, LUNA]
    completed(store, summary="Synthetic completed source.")
    memory = wf.supervisor.memory
    memory.refresh()
    title_gate, synthesis_gate = scenario.gate("title"), scenario.gate("project_synthesize")
    scenario.agent.script("plan", turn=plan_turn, turns=1)
    scenario.agent.script("title", turn=title_turn, turns=1)
    scenario.agent.script("project_synthesize", turn=synthesize_turn, turns=1)
    scenario.agent.script("prepare", turn=prepare_turn, turns=1)
    eid = create(store)
    title = await scenario.run(
        lambda: saved(store, "agent_actions", experiment_id=eid, phase="title", state="running"),
        gates=[title_gate, synthesis_gate],
    )
    assert connection_evidence(scenario, wf, "agent_actions", title).slot == 0
    review = store.experiment(eid)["plan_review"]
    assert review["state"] == "awaiting_review"
    # Approving by the normal command makes implementation eligible for this owner.
    store.command(
        uuid4(), "plan_review", {"decision": "approve", "plan_revision": review["revision"]}, eid=eid
    )
    identity = memory.enqueue(
        "test-project", {"request_id": str(uuid4()), "kind": "synthesize"}, origin="weekly"
    )["request_id"]
    weekly = await scenario.run(
        lambda: saved(store, "project_turns", id=identity, state="running"),
        gates=[title_gate, synthesis_gate],
    )
    # The spare slot serves another typed owner of the same project, so the
    # experiment's own wait is an owner exclusion rather than a stuck engine.
    assert connection_evidence(scenario, wf, "project_turns", weekly).slot == 1
    assert wf.agent_pool.lookup("project_turns", identity).owner == project_owner("test-project")
    assert saved(store, "agent_actions", experiment_id=eid, phase="prepare")["turn_id"] is None
    assert begin_turns(scenario, "prepare") == []

    # Once the title settles, implementation for that experiment begins exactly once.
    def prepared():
        row = saved(store, "agent_actions", experiment_id=eid, phase="prepare")
        return row if row and row["turn_id"] else None

    title_gate.release.set()
    await turn_task(wf, "agent_actions", title)
    prepare = await scenario.run(prepared, gates=[title_gate, synthesis_gate])
    assert prepare["phase"] == "prepare" and begin_turns(scenario, "prepare") == [prepare["turn_id"]]
    assert saved(store, "experiments", id=eid)["title"] == "Synthetic mixed-role title"
    await wf.close()


async def test_one_typed_project_owner_serializes_research_and_weekly_synthesis(multi_scenario):
    scenario = multi_scenario
    wf = await scenario.started()
    store = scenario.store
    completed(store, summary="Synthetic completed source.")
    memory = wf.supervisor.memory
    memory.refresh()
    synthesis_gate, research_gate = scenario.gate("project_synthesize"), scenario.gate("research")
    scenario.agent.script("project_synthesize", turn=synthesize_turn, turns=1)
    scenario.agent.script("research", turn=research_turn, turns=1)
    identity = memory.enqueue(
        "test-project", {"request_id": str(uuid4()), "kind": "synthesize"}, origin="weekly"
    )["request_id"]
    weekly = await scenario.run(
        lambda: saved(store, "project_turns", id=identity, state="running"), gates=[synthesis_gate]
    )
    connection_evidence(scenario, wf, "project_turns", weekly)

    # Research and supervision of one project share the typed project owner, so the
    # free connection may not start the pending research action on the same owner.
    service, run = activated(scenario)
    with store.tx() as db:
        service.wake(db, run["id"], "synthetic-input", "experiment_change", {"synthetic": True})
    pending = await scenario.run(
        lambda: saved(store, "autopilot_actions", run_id=run["id"], state="pending"),
        gates=[synthesis_gate],
    )
    await wf.tick(force_observe=True)  # the pass that prepares the research proposal
    [proposal] = wf.agent_pool.proposals
    assert proposal.key == ("autopilot_actions", pending["id"])
    wf.agent_pool.admit()  # the selection that sees one free turn and one busy owner
    assert wf.agent_pool.turn_capacity() == 1 and wf.research.task is None
    assert wf.agent_pool.lookup("autopilot_actions", pending["id"]) is None
    assert saved(store, "project_turns", id=identity)["state"] == "running"

    # Settling the weekly turn hands the shared owner's next turn to research, once.
    synthesis_gate.release.set()
    await turn_task(wf, "project_turns", weekly)
    assert (
        await scenario.run(
            lambda: saved(store, "project_turns", id=identity, state="completed"), gates=[synthesis_gate]
        )
    )["report_version"] == 1
    research = await scenario.run(
        lambda: saved(store, "autopilot_actions", id=pending["id"], state="running"),
        gates=[research_gate],
    )
    connection_evidence(scenario, wf, "autopilot_actions", research)
    assert begin_turns(scenario, "research") == [research["turn_id"]]
    research_gate.release.set()
    await turn_task(wf, "autopilot_actions", research)
    await wf.close()
