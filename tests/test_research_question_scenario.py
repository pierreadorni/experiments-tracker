"""C12: a saved research question waits without blocking another experiment."""

from uuid import uuid4

import pytest
from test_research_create import create, git_project
from test_research_dispatch import ScriptedAgent, run_pass, saved_action, scripted
from test_research_follow_up import fake_clock

from experimenttracker.research.session import first_turn, follow_up_turn


@pytest.mark.asyncio
async def test_waiting_owner_question_does_not_block_another_research_thread(
    runtime, config, tmp_path, monkeypatch
):
    store, scheduler, _, workflow = runtime
    git_project(config, store, tmp_path, research_loop=True)
    now = fake_clock(monkeypatch)
    first = create(store)[2]["experiment_id"]
    second = create(store)[2]["experiment_id"]
    for eid in (first, second):
        workflow.workspaces.ensure_branch(eid)
    question = str(uuid4())

    class QuestionAgent(ScriptedAgent):
        async def finish(self, thread_id, turn_id):
            await super().finish(thread_id, turn_id)
            if len(self.calls) == 2:
                await workflow.controller.call(
                    first, "ask_owner", {"request_id": question, "question": "Which held-out split?"}
                )
                return {"status": "needs_owner", "summary": "Waiting for split", "next": "Use answer"}
            return {"status": "wait", "summary": "Ready for another check", "next": "timer:120"}

    agent = scripted(workflow, QuestionAgent())
    first_action = first_turn(store, first)
    await run_pass(workflow, workflow.research_dispatch)
    assert saved_action(store, first_action["id"])["state"] == "completed"
    assert store.experiment(first)["pending_request"]["id"] == question
    assert follow_up_turn(store, first) is None

    agent.session_thread = "thread-2"
    second_action = first_turn(store, second)
    await run_pass(workflow, workflow.research_dispatch)
    assert saved_action(store, second_action["id"])["state"] == "completed"
    assert store.experiment(second)["thread_id"] == "thread-2"
    assert store.experiment(first)["pending_request"]["id"] == question
    assert follow_up_turn(store, first) is None

    store.command(str(uuid4()), "answer", {"pending_request_id": question, "answer": "Split 7"}, eid=first)
    assert follow_up_turn(store, first) is None
    now.advance(60)
    follow_up = follow_up_turn(store, first)
    assert "Split 7" in follow_up["input"]
    assert follow_up_turn(store, second) is None
    assert follow_up["thread_id"] == "thread-1"
    agent.session_thread = None
    await run_pass(workflow, workflow.research_dispatch)

    assert saved_action(store, follow_up["id"])["state"] == "completed"
    begins = [call for call in agent.calls if call[0] == "begin"]
    assert [call[1] for call in begins] == ["thread-1", "thread-2", "thread-1"]
    assert ["Split 7" in call[2] for call in begins] == [False, False, True]
    assert scheduler.submissions == 0
