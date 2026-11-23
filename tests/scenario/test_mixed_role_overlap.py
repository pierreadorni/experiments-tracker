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

from experimenttracker import clock
from experimenttracker.agent_pool import experiment_owner, project_owner
from experimenttracker.autopilot import Autopilot
from experimenttracker.autopilot_policy import lead_id
from experimenttracker.workspaces import initialize_project

DECISION = {
    "summary": "Synthetic checkpoint.",
    "central_argument": "No real science in this fixture.",
    "uncertainty": "Real retrieval remains a live prerequisite.",
    "outcome": "wait",
}
EXPERIMENT_PHASE = "experiment_research"


def experiment_turn(payload, workspace):
    return {"status": "wait", "summary": "Synthetic experiment turn.", "next": "timer:3600"}


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


def research_project(store):
    """The test project with a saved Git checkout that contract 4 work and the lead need."""
    project = store.projects["test-project"]
    if project.checkout is None:
        checkout = store.path.parent.parent / "scenario-project"
        checkout.mkdir()
        initialize_project(checkout)
        info = checkout.stat()
        # This saved project identity also works when the fixture reopens its
        # workflow and rebuilds the in-memory project catalog.
        with store.tx() as db:
            db.execute(
                "INSERT INTO projects(id,name,folder,device,inode,non_empty,created_at) VALUES(?,?,?,?,?,1,?)",
                (project.id, project.name, str(checkout), info.st_dev, info.st_ino, clock.now()),
            )
        store.projects[project.id] = project.model_copy(update={"checkout": checkout, "research_loop": True})
    return store.projects[project.id]


def create(store, brief="Synthetic mixed-role overlap."):
    project = research_project(store)
    result = store.command(
        uuid4(),
        "create",
        {
            "brief": brief,
            "recipe_id": "agent_experiment_v1",
            "project_id": project.id,
            "execution_contract": 4,
        },
    )
    eid = result["experiment_id"]
    assert store.experiment(eid)["contract"]["execution_contract"] == 4
    return eid


def saved(store, table, **where):
    return store.one(
        f"SELECT * FROM {table} WHERE {' AND '.join(f'{key}=?' for key in where)}", tuple(where.values())
    )


def activated(scenario):
    """Configure and activate the synthetic research run through its own commands."""
    research_project(scenario.store)
    service = Autopilot(scenario.store, scenario.config)
    command(service, "configure", policy=policy(now=scenario.clock.now()).model_dump())
    command(service, "activate")
    run = service.run("test-project")
    # Hold the lead: these scenarios exercise the project-turn engine until T5.1e.
    scenario.store.update(lead_id(run["id"]), pause_agent=True)
    return service, run


def connection_evidence(scenario, wf, source_table, row):
    """Prove this saved turn runs on the one process generation its transport logged."""
    reservation = wf.agent_pool.lookup(source_table, row["id"])
    assert reservation is not None and reservation.connection is not None, f"{row['id']} is not live"
    connection = reservation.connection
    slot = wf.agent_pool.slots[connection.slot]
    call = scenario.slots.begin(row["turn_id"])  # exactly one begin for this saved turn
    assert (call["slot"], call["instance"], call["generation"]) == (
        connection.slot,
        slot.agent.instance,
        connection.start,
    )
    # The registry's generation is the transport's own start count on this slot.
    assert slot.agent.start_count == call["generation"] == connection.start
    return connection


def live_both(scenario, wf, left, right):
    """Both saved turns run at the same time on different connections."""
    first = connection_evidence(scenario, wf, *left)
    second = connection_evidence(scenario, wf, *right)
    assert first.slot != second.slot
    return first, second


def turn_task(wf, source_table, row):
    """The dispatch task that holds this saved turn's live connection."""
    slot = wf.agent_pool.slots[wf.agent_pool.lookup(source_table, row["id"]).connection.slot]
    if source_table == "autopilot_actions":
        return wf.research.task
    if source_table == "project_turns":
        return wf.supervisor.task
    return slot.agent_task


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
        service.legacy_input(db, run["id"], "synthetic-input", "experiment_change", {"synthetic": True})
    experiment_gate, research_gate = scenario.gate(EXPERIMENT_PHASE), scenario.gate("research")
    scenario.agent.script(EXPERIMENT_PHASE, turn=experiment_turn, turns=1)
    scenario.agent.script("research", turn=research_turn, turns=1)
    eid = create(store)

    def both_running():
        turn = saved(store, "agent_actions", experiment_id=eid, phase="research", state="running")
        decision = saved(store, "autopilot_actions", run_id=run["id"], state="running")
        return (turn, decision) if turn and decision else None

    turn, decision = await scenario.run(both_running, gates=[experiment_gate, research_gate])
    live_both(scenario, wf, ("agent_actions", turn), ("autopilot_actions", decision))
    assert backend.turns_started == 2

    # The experiment's own saved turn finishes while research stays live.
    experiment_gate.release.set()
    await turn_task(wf, "agent_actions", turn)
    await scenario.run(
        lambda: saved(store, "agent_actions", id=turn["id"], state="completed"),
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
    scenario.agent.script(EXPERIMENT_PHASE, turn=experiment_turn, turns=1)
    gate = scenario.gate(EXPERIMENT_PHASE)
    second = create(store, "Synthetic second experiment.")
    second_turn = await scenario.run(
        lambda: saved(store, "agent_actions", experiment_id=second, phase="research", state="running"),
        gates=[experiment_gate, gate],
    )
    assert connection_evidence(scenario, wf, "agent_actions", second_turn).start == 2
    gate.release.set()
    await turn_task(wf, "agent_actions", second_turn)
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
    experiment_gate, synthesis_gate = scenario.gate(EXPERIMENT_PHASE), scenario.gate("project_synthesize")
    scenario.agent.script(EXPERIMENT_PHASE, turn=experiment_turn, turns=1)
    scenario.agent.script("project_synthesize", turn=synthesize_turn, turns=1)
    eid = create(store)

    def both_running():
        turn = saved(store, "agent_actions", experiment_id=eid, phase="research", state="running")
        weekly = saved(store, "project_turns", id=identity, state="running")
        return (turn, weekly) if turn and weekly else None

    turn, weekly = await scenario.run(both_running, gates=[experiment_gate, synthesis_gate])
    live_both(scenario, wf, ("agent_actions", turn), ("project_turns", weekly))
    # One project holds an experiment turn and a project turn: distinct typed owners.
    assert wf.agent_pool.lookup("agent_actions", turn["id"]).owner == experiment_owner(eid)
    assert wf.agent_pool.lookup("project_turns", identity).owner == project_owner("test-project")

    # The weekly synthesis applies its saved report while the experiment stays live.
    synthesis_gate.release.set()
    await turn_task(wf, "project_turns", weekly)
    done = await scenario.run(
        lambda: saved(store, "project_turns", id=identity, state="completed"), gates=[experiment_gate]
    )
    assert done["report_version"] == 1
    report = memory.report("test-project")
    assert (report["version"], report["document"]["source_scope"]) == (1, "synthetic")
    assert saved(store, "agent_actions", id=turn["id"])["state"] == "running"

    experiment_gate.release.set()
    await turn_task(wf, "agent_actions", turn)
    await scenario.run(
        lambda: saved(store, "agent_actions", id=turn["id"], state="completed"),
        gates=[synthesis_gate],
    )
    assert saved(store, "agent_actions", id=turn["id"])["state"] == "completed"
    await wf.close()


async def test_two_slots_overlap_a_discussion_and_an_unrelated_experiment(multi_scenario):
    scenario = multi_scenario
    wf = await scenario.started()
    store = scenario.store
    parent = load_legacy_completed(store, wf)
    identity = store.command(uuid4(), "discuss", {"text": "Explain the synthetic result"}, eid=parent)[
        "request_id"
    ]
    discussion_gate, experiment_gate = scenario.gate("discuss"), scenario.gate(EXPERIMENT_PHASE)
    scenario.agent.script("discuss", turn=discuss_turn, turns=1)
    scenario.agent.script(EXPERIMENT_PHASE, turn=experiment_turn, turns=1)
    eid = create(store, "Synthetic unrelated experiment.")
    action = "discuss:" + identity

    def both_running():
        discussion = saved(store, "agent_actions", experiment_id=parent, phase=action, state="running")
        turn = saved(store, "agent_actions", experiment_id=eid, phase="research", state="running")
        return (discussion, turn) if discussion and turn else None

    discussion, turn = await scenario.run(both_running, gates=[discussion_gate, experiment_gate])
    live_both(scenario, wf, ("agent_actions", discussion), ("agent_actions", turn))

    # The discussion keeps its queued identity and evidence, and completes alone.
    discussion_gate.release.set()
    await turn_task(wf, "agent_actions", discussion)
    done = await scenario.run(
        lambda: saved(store, "discussions", id=identity, state="completed"), gates=[experiment_gate]
    )
    assert (done["action_id"], done["report_version"]) == (discussion["id"], None)
    assert (done["reply"], json.loads(done["evidence_ids"])) == ("Synthetic discussion reply.", [])
    assert saved(store, "agent_actions", id=turn["id"])["state"] == "running"

    experiment_gate.release.set()
    await turn_task(wf, "agent_actions", turn)
    await scenario.run(
        lambda: saved(store, "agent_actions", id=turn["id"], state="completed"),
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
    experiment_gate, synthesis_gate = scenario.gate(EXPERIMENT_PHASE), scenario.gate("project_synthesize")
    scenario.agent.script(EXPERIMENT_PHASE, turn=experiment_turn, turns=1)
    scenario.agent.script("project_synthesize", turn=synthesize_turn, turns=1)
    eid = create(store)
    turn = await scenario.run(
        lambda: saved(store, "agent_actions", experiment_id=eid, phase="research", state="running"),
        gates=[experiment_gate, synthesis_gate],
    )
    # One configured connection: the weekly turn keeps its saved place, nothing else starts.
    scenario.one_session()
    assert saved(store, "project_turns", id=identity)["state"] in ("queued", "waiting")
    assert scenario.workflow.supervisor.task is None and scenario.workflow.research.task is None
    assert scenario.agent_backend.turns_started == 1 and scenario.agent.start_count == 1

    experiment_gate.release.set()
    await turn_task(scenario.workflow, "agent_actions", turn)
    weekly = await scenario.run(
        lambda: saved(store, "project_turns", id=identity, state="running"),
        gates=[experiment_gate, synthesis_gate],
    )
    # The next admitted source keeps its exact saved identity on the freed connection.
    reservation = scenario.workflow.agent_pool.lookup("project_turns", weekly["id"])
    assert (reservation.connection.slot, reservation.connection.start) == (0, 1)
    assert [start["phase"] for start in backend.starts] == [EXPERIMENT_PHASE, "project_synthesize"]
    assert [(weekly["thread_id"], weekly["turn_id"])] == [
        (start["thread_id"], start["turn_id"]) for start in backend.starts[1:]
    ]
    await scenario.close()


async def test_same_experiment_serializes_steering_while_a_spare_slot_serves_supervision(multi_scenario):
    scenario = multi_scenario
    wf = await scenario.started()
    store = scenario.store
    completed(store, summary="Synthetic completed source.")
    memory = wf.supervisor.memory
    memory.refresh()
    first_gate = scenario.gate(EXPERIMENT_PHASE)
    synthesis_gate = scenario.gate("project_synthesize")
    scenario.agent.script(EXPERIMENT_PHASE, turn=experiment_turn, turns=2)
    scenario.agent.script("project_synthesize", turn=synthesize_turn, turns=1)
    eid = create(store)
    first = await scenario.run(
        lambda: saved(store, "agent_actions", experiment_id=eid, phase="research", state="running"),
        gates=[first_gate, synthesis_gate],
    )
    assert connection_evidence(scenario, wf, "agent_actions", first).slot == 0

    # A steering command wakes this saved experiment without starting another
    # turn while its exact thread is still occupied.
    guidance = "Use the held-out synthetic examples."
    steer_id = str(uuid4())
    store.command(steer_id, "steer", {"text": guidance, "mode": "later"}, eid=eid)
    identity = memory.enqueue(
        "test-project", {"request_id": str(uuid4()), "kind": "synthesize"}, origin="weekly"
    )["request_id"]
    weekly = await scenario.run(
        lambda: saved(store, "project_turns", id=identity, state="running"),
        gates=[first_gate, synthesis_gate],
    )
    assert connection_evidence(scenario, wf, "project_turns", weekly).slot == 1
    assert wf.agent_pool.lookup("project_turns", identity).owner == project_owner("test-project")
    assert store.rows("SELECT id,state FROM agent_actions WHERE experiment_id=?", (eid,)) == [
        {"id": first["id"], "state": "running"}
    ]
    assert begin_turns(scenario, EXPERIMENT_PHASE) == [first["turn_id"]]

    first_gate.release.set()
    await turn_task(wf, "agent_actions", first)
    next_gate = scenario.gate(EXPERIMENT_PHASE)
    next_turn = await scenario.run(
        lambda: saved(store, "agent_actions", experiment_id=eid, phase="research", state="running"),
        gates=[next_gate, synthesis_gate],
    )
    assert next_turn["id"] != first["id"]
    assert next_turn["thread_id"] == first["thread_id"]
    assert begin_turns(scenario, EXPERIMENT_PHASE) == [first["turn_id"], next_turn["turn_id"]]
    assert guidance in next_turn["input"]
    assert saved(store, "steering", request_id=steer_id)["state"] == "applied"
    assert saved(store, "project_turns", id=identity)["state"] == "running"
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
        service.legacy_input(db, run["id"], "synthetic-input", "experiment_change", {"synthetic": True})
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
