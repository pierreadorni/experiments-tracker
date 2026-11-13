"""Fake-control tests: they prove the scenario fakes, not engine or resolver behavior."""

import asyncio
import time
from pathlib import Path

import pytest

from experimenttracker import clock as engine_clock
from experimenttracker.codex_client import NeedsAttention, TerminalTurnError, TransportError
from experimenttracker.slurm import Observation, ResourceUnavailable, SubmissionUncertain
from scenario import FakeClock, FakeCodex, FakeCodexBackend, FakeGate, FakeSlurm, FakeSlurmBackend


def prompt(phase):
    return f'Synthetic mechanism brief.\nPAYLOAD:\n{{"phase": "{phase}"}}'


def test_fake_clock_controls_only_the_engine_clock(monkeypatch):
    fake = FakeClock()
    wall = fake.wall
    fake.install(monkeypatch)
    assert engine_clock.now() == wall and engine_clock.monotonic() == 0.0
    assert abs(time.time() - wall) < 300 and time.monotonic() > 0.0  # real clocks are independent
    fake.advance(3600)
    assert engine_clock.now() == wall + 3600 and engine_clock.monotonic() == 3600.0
    assert time.time() < wall + 3600 - 60  # engine time moved; real time did not


async def test_submissions_refused_lost_lagged_duplicated_or_unreachable():
    slurm = FakeSlurm()
    backend = slurm.backend
    backend.refusals += [
        RuntimeError("synthetic sbatch rejected"),
        OSError("synthetic sbatch transport"),
        TimeoutError("synthetic sbatch timeout"),
    ]
    for message in ("rejected", "transport", "timeout"):
        with pytest.raises(SubmissionUncertain, match=message):
            await slurm.submit("intent-refused", None, None)
    backend.refusals.append(ValueError("synthetic resource validation refused"))
    with pytest.raises(ValueError, match="resource validation"):
        await slurm.submit("intent-invalid", None, None)  # validation precedes submission
    assert backend.accepted == [] and backend.matches == {}
    assert backend.submit_calls == ["intent-refused"] * 3 + ["intent-invalid"]

    slurm.saved_intent = lambda intent: True
    backend.lose_response = backend.lag = True
    with pytest.raises(SubmissionUncertain, match="lost after acceptance"):
        await slurm.submit("intent-lost", None, None)
    [accepted] = backend.accepted
    assert accepted["intent_saved"] is True
    assert await slurm.reconcile("intent-lost") is None  # accounting lag, not absence
    backend.reveal()
    assert await slurm.reconcile("intent-lost") == accepted["job_id"]
    assert not backend.hidden

    backend.lose_response = backend.lag = False
    backend.duplicate = True
    duplicate_job = await slurm.submit("intent-dup", None, None)
    assert backend.matches["intent-dup"][0] == duplicate_job
    assert len(backend.matches["intent-dup"]) == 2  # duplicate exact matches stay visible
    with pytest.raises(SubmissionUncertain, match="multiple"):
        await slurm.reconcile("intent-dup")
    backend.unreachable_intents["intent-lost"] = TimeoutError("synthetic sacct timed out")
    with pytest.raises(TimeoutError, match="sacct timed out"):
        await slurm.reconcile("intent-lost")
    assert backend.submit_calls[4:] == ["intent-lost", "intent-dup"]


async def test_cancel_and_observe_require_the_exact_saved_identity():
    slurm = FakeSlurm()
    backend = slurm.backend
    job_id = await slurm.submit("intent-cancel", None, None)
    backend.observations[job_id] = Observation("RUNNING", started_at=100.0)
    assert (await slurm.observe(job_id)).started_at == 100.0
    assert await slurm.cancel(job_id, "intent-cancel") is True
    assert backend.cancel_requests == [(job_id, "intent-cancel")]
    assert (await slurm.observe(job_id)).state == "RUNNING"  # request recorded, not assumed terminal
    assert await slurm.cancel("9999", "intent-cancel") is False  # absent job
    with pytest.raises(ResourceUnavailable, match="no longer matches"):
        await slurm.cancel(job_id, "intent-other")  # wrong intent
    with pytest.raises(ValueError, match="numeric"):
        await slurm.cancel("demo-intent-cancel", "intent-cancel")
    assert backend.cancel_requests == [(job_id, "intent-cancel")]
    with pytest.raises(AssertionError, match="no observation for job 9999"):
        await slurm.observe("9999")
    backend.unreachable_jobs[job_id] = TransportError("synthetic squeue transport closed")
    with pytest.raises(TransportError, match="transport closed"):
        await slurm.observe(job_id)
    with pytest.raises(AssertionError, match="never discover"):
        await slurm.discover()
    with pytest.raises(AssertionError, match="never preflight"):
        await slurm.preflight("intent-cancel", None, None, None)


async def test_lost_begin_keeps_accepted_turn_until_explicit_completion():
    agent = FakeCodex()
    agent.script("prepare", begin_error=TransportError("turn/start response lost"))
    thread, _ = await agent.session(None, Path("work"))
    with pytest.raises(TransportError, match="response lost"):
        await agent.begin(thread, prompt("prepare"), {})
    [start] = agent.backend.starts
    saved = agent.backend.turns[(thread, start["turn_id"])]
    assert saved["state"] == "accepted" and saved["result"] is None
    with pytest.raises(NeedsAttention, match="saved turn is accepted"):
        await agent.finish(thread, start["turn_id"])
    with pytest.raises(NeedsAttention, match="saved turn is accepted"):
        await agent.read_turn(thread, start["turn_id"])
    agent.complete(thread, start["turn_id"], {"summary": "recovered"})
    recovered = await agent.read_turn(thread, start["turn_id"], expected_prompt_hash=start["prompt_sha256"])
    assert recovered == {"summary": "recovered"}
    with pytest.raises(NeedsAttention, match="does not match"):
        await agent.read_turn(thread, start["turn_id"], expected_prompt_hash="stale")
    assert await agent.finish(thread, start["turn_id"]) == {"summary": "recovered"}
    agent.script("monitor", begin_error=TimeoutError("turn/start timed out"))
    with pytest.raises(TimeoutError, match="timed out"):
        await agent.begin(thread, prompt("monitor"), {})
    agent.backend.unreadable[thread] = TransportError("thread/read transport closed")
    with pytest.raises(TransportError, match="transport closed"):
        await agent.read_turn(thread, start["turn_id"])


async def test_terminal_unfinished_and_scripted_turns_raise_real_errors():
    agent = FakeCodex()
    thread, _ = await agent.session(None, Path("work"))
    agent.script("plan", turn=None)  # a completed turn without a structured result
    turn = await agent.begin(thread, prompt("plan"), {})
    with pytest.raises(TerminalTurnError, match="no structured result"):
        await agent.finish(thread, turn)
    with pytest.raises(NeedsAttention, match="saved turn is completed"):
        await agent.read_turn(thread, turn)
    agent.script("prepare", begin_error=TransportError("turn/start response lost"))
    with pytest.raises(TransportError):
        await agent.begin(thread, prompt("prepare"), {})
    failed = agent.backend.starts[-1]["turn_id"]
    agent.fail(thread, failed, state="failed")
    with pytest.raises(TerminalTurnError, match="turn failed"):
        await agent.read_turn(thread, failed)
    with pytest.raises(TerminalTurnError, match="turn failed"):
        await agent.finish(thread, failed)
    agent.script("monitor", begin_error=TimeoutError("turn/start timed out"))
    with pytest.raises(TimeoutError):
        await agent.begin(thread, prompt("monitor"), {})
    interrupted = agent.backend.starts[-1]["turn_id"]
    agent.fail(thread, interrupted, state="interrupted")
    with pytest.raises(TerminalTurnError, match="turn interrupted"):
        await agent.read_turn(thread, interrupted)
    with pytest.raises(TerminalTurnError, match="turn interrupted"):
        await agent.finish(thread, interrupted)
    with pytest.raises(NeedsAttention, match="not found"):
        await agent.read_turn(thread, "fake-turn-unknown")

    agent.script("summarize", turn=lambda payload, workspace: {"summary": payload["phase"]})
    noted = 'Synthetic brief.\nPAYLOAD:\n{"note": "PAYLOAD:"}\nPAYLOAD:\n{"phase": "summarize"}'
    summarized = await agent.begin(thread, noted, {})
    assert await agent.finish(thread, summarized) == {"summary": "summarize"}
    with pytest.raises(AssertionError, match="unexpected repeat of agent phase summarize"):
        await agent.begin(thread, prompt("summarize"), {})
    with pytest.raises(AssertionError, match="unexpected agent phase interpret"):
        await agent.begin(thread, prompt("interpret"), {})


async def test_fresh_adapters_share_external_backend_state(monkeypatch):
    clock = FakeClock()
    clock.install(monkeypatch)
    slurm_backend = FakeSlurmBackend()
    first, second = FakeSlurm(slurm_backend), FakeSlurm(slurm_backend)
    assert first is not second and first.backend is second.backend is slurm_backend
    first.saved_intent = lambda intent: True
    job_id = await first.submit("intent-shared", None, None)
    assert await second.reconcile("intent-shared") == job_id
    slurm_backend.observations[job_id] = Observation("RUNNING", started_at=clock.now())
    assert (await second.observe(job_id)).state == "RUNNING"
    second.saved_intent = lambda intent: False
    next_job = await second.submit("intent-next", None, None)
    assert next_job != job_id and slurm_backend.matches["intent-next"] == [next_job]
    assert [entry["intent_saved"] for entry in slurm_backend.accepted] == [True, False]

    agent_backend = FakeCodexBackend()
    first_agent, second_agent = FakeCodex(agent_backend), FakeCodex(agent_backend)
    assert first_agent is not second_agent and first_agent.backend is agent_backend
    assert first_agent.models[0]["default_effort"] == "low"
    await first_agent.start()
    assert second_agent.models_observed_at == clock.now() == agent_backend.models_observed_at
    first_agent.script("plan", turn=lambda payload, workspace: {"summary": "synthetic"})
    first_thread, _ = await first_agent.session(None, Path("first"))
    second_thread, _ = await second_agent.session(None, Path("second"))
    assert (first_thread, second_thread) == ("fake-thread-1", "fake-thread-2")
    turn = await second_agent.begin(
        first_thread, prompt("plan"), {}, additional_writable_roots=(Path("shared"),)
    )
    assert await first_agent.read_turn(first_thread, turn) == {"summary": "synthetic"}
    assert first_agent.backend.starts[0]["writable_roots"] == ["shared"]  # exact dispatch retained


async def test_gate_records_the_task_that_blocks_then_releases_it():
    gate = FakeGate()
    assert gate.task is None and not gate.entered.is_set()
    waiter = asyncio.create_task(gate.wait())
    await asyncio.wait_for(gate.entered.wait(), 2)  # real time only guards a broken gate
    assert gate.task is waiter and gate.entered.is_set() and not waiter.done()
    gate.release.set()
    await asyncio.wait_for(waiter, 2)
    assert waiter.done() and not waiter.cancelled()
