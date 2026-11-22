"""Synthetic cancellation acceptance: no live SSH, Slurm or managed model calls."""

import asyncio
from uuid import uuid4

import pytest
from helpers import seed_saved_contract3

from experimenttracker.agent_pool import experiment_owner
from experimenttracker.cancellation import process_cancellations
from experimenttracker.codex_client import NeedsAttention, TerminalTurnError
from experimenttracker.core import action_obligations, job_obligations, transfer_obligations
from experimenttracker.maintenance import blockers
from experimenttracker.models import ActionCommand
from experimenttracker.remote import RemoteSlurmScheduler, TransferCancelled, TransferPending
from experimenttracker.slurm import Observation
from experimenttracker.store import Conflict, Store


def create(store):
    return seed_saved_contract3(store, "Synthetic cancellation test.")


def cancel(store, eid, request_id=None):
    payload = ActionCommand(request_id=request_id or uuid4(), action="cancel").model_dump(mode="json")
    return store.command(payload["request_id"], "action", payload, eid=eid)


@pytest.mark.usefixtures("contract_three_fixture")
async def test_cancel_supersedes_pending_action_obligation_atomically_and_replays(runtime, monkeypatch):
    store, _, _, wf = runtime
    eid = create(store)
    action_id, request_id = str(uuid4()), uuid4()
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,created_at,updated_at) "
            "VALUES(?,?,'monitor',1,'pending','prompt',0,0)",
            (action_id, eid),
        )
        action_obligations.sync_action(db, action_id)
    expected = store.one("SELECT * FROM agent_actions WHERE id=?", (action_id,))
    with store.tx() as db:
        db.execute("UPDATE agent_actions SET turn_id='replacement' WHERE id=?", (action_id,))
    with pytest.raises(NeedsAttention, match="saved action changed"):
        wf._action_state(action_id, "failed", expected=expected)
    with store.tx() as db:
        db.execute("UPDATE agent_actions SET turn_id=NULL WHERE id=?", (action_id,))

    def fail_sync(*args, **kwargs):
        raise RuntimeError("synthetic obligation failure")

    with monkeypatch.context() as patcher:
        patcher.setattr(action_obligations, "sync_action", fail_sync)
        with pytest.raises(RuntimeError, match="synthetic obligation failure"):
            cancel(store, eid)
    assert store.one("SELECT state FROM agent_actions WHERE id=?", (action_id,))["state"] == "pending"
    assert store.one("SELECT phase FROM experiments WHERE id=?", (eid,))["phase"] != "cancelled"

    first = cancel(store, eid, request_id)
    latest = store.one(
        "SELECT state FROM obligations WHERE id=?",
        (f"agent-action:{action_id}:0",),
    )
    assert store.one("SELECT state FROM agent_actions WHERE id=?", (action_id,))["state"] == "superseded"
    assert latest["state"] == "abandoned"
    count = store.one(
        "SELECT COUNT(*) AS n FROM obligations WHERE id LIKE ?", (f"agent-action:{action_id}:%",)
    )["n"]
    assert cancel(store, eid, request_id) == first
    assert (
        store.one(
            "SELECT COUNT(*) AS n FROM obligations WHERE id LIKE ?",
            (f"agent-action:{action_id}:%",),
        )["n"]
        == count
    )


@pytest.mark.parametrize(
    "phase", ["preparing", "training", "evaluating", "validating", "interpreting", "publishing", "failed"]
)
@pytest.mark.usefixtures("contract_three_fixture")
async def test_cancel_any_phase_retires_only_its_queue_and_survives_restart(runtime, phase, monkeypatch):
    from experimenttracker import creation_policy

    if phase == "failed":
        monkeypatch.setattr(creation_policy, "MODE", "legacy")
    store, _, _, wf = runtime
    eid, other = create(store), create(store)
    store.update(eid, phase=phase, pending_request={"id": "approval"})
    for identity, experiment in (("mine", eid), ("other", other)):
        with store.tx() as db:
            db.execute(
                "INSERT INTO execution_transfers VALUES(?,?, 'up','{}','pending',NULL,0)",
                (identity, experiment),
            )
    store.command(
        uuid4(), "steer", {"text": "Do later", "mode": "restart" if phase == "failed" else "later"}, eid=eid
    )
    request = uuid4()
    assert cancel(store, eid, request) == cancel(store, eid, request)
    assert store.experiment(eid)["pending_request"] is None
    assert store.one("SELECT state FROM execution_transfers WHERE id='mine'")["state"] == "cancelled"
    assert store.one("SELECT state FROM execution_transfers WHERE id='other'")["state"] == "pending"
    assert not store.one(
        "SELECT 1 FROM steering WHERE experiment_id=? AND state IN ('pending','cancelling')", (eid,)
    )
    # Late worker responses cannot clear cancellation or make dependencies ready.
    store.update(eid, phase="complete", hold_reason=None)
    store.stage(eid, "prepare", "complete")
    assert store.experiment(eid)["phase"] == "cancelled"
    assert (
        store.one("SELECT state FROM stages WHERE experiment_id=? AND id='prepare'", (eid,))["state"]
        == "cancelled"
    )
    with pytest.raises(Conflict, match="cancelled"):
        store.command(uuid4(), "action", {"action": "resume_agent"}, eid=eid)
    reopened = Store(store.path)
    try:
        assert reopened.experiment(eid)["phase"] == "cancelled"
    finally:
        reopened.close()
    await process_cancellations(wf)
    assert store.experiment(eid)["hold_reason"] is None
    await wf.close()


async def test_inflight_transfer_is_stopped_and_never_retried(runtime):
    store, _, _, wf = runtime
    eid = create(store)
    remote = RemoteSlurmScheduler(
        wf, store.experiment(eid), {"context": "jean-zay", "remote_project": "/synthetic"}
    )
    remote.layout = {"host": "synthetic", "remote_run": "/synthetic/run"}
    started, stopped = asyncio.Event(), asyncio.Event()

    async def upload():
        started.set()
        try:
            await asyncio.Future()
        finally:
            stopped.set()

    task = asyncio.create_task(remote.transfer("up", upload, identity="upload"))
    await started.wait()
    cancel(store, eid)
    with pytest.raises(TransferCancelled):
        await asyncio.wait_for(task, 2)
    assert stopped.is_set()
    with pytest.raises(TransferCancelled):
        await remote.transfer("up", upload, identity="upload")
    assert store.one("SELECT state FROM execution_transfers WHERE id='upload'")["state"] == "cancelled"
    assert "a transfer is unfinished" not in blockers(wf)
    await wf.close()


async def test_failed_transfer_cancellation_needs_no_server_access(runtime):
    store, _, _, wf = runtime
    eid = create(store)
    remote = RemoteSlurmScheduler(
        wf, store.experiment(eid), {"context": "jean-zay", "remote_project": "/synthetic"}
    )
    remote.layout = {"host": "synthetic"}

    async def broken():
        raise OSError("access revoked")

    with pytest.raises(TransferPending):
        await remote.transfer("up", broken, identity="broken")
    cancel(store, eid)
    await process_cancellations(wf)
    assert store.experiment(eid)["hold_reason"] is None
    assert not store.one("SELECT 1 FROM jobs WHERE experiment_id=?", (eid,))
    await wf.close()


async def test_cancel_abandons_exact_pending_transfer_obligations_atomically_and_replays(
    runtime, monkeypatch
):
    store, _, _, wf = runtime
    eid = create(store)
    for identity, state in (("mine", "pending"), ("done", "complete")):
        with store.tx() as db:
            db.execute(
                "INSERT INTO execution_transfers VALUES(?,?,'up','{}',?,NULL,0)", (identity, eid, state)
            )
    with store.tx() as db:
        transfer_obligations.sync_transfer(db, "mine")

    def fail_sync(*args, **kwargs):
        raise RuntimeError("synthetic obligation failure")

    with monkeypatch.context() as patcher:
        patcher.setattr(transfer_obligations, "sync_transfer", fail_sync)
        with pytest.raises(RuntimeError, match="synthetic obligation failure"):
            cancel(store, eid)
    assert store.experiment(eid)["phase"] != "cancelled"
    assert store.one("SELECT state FROM execution_transfers WHERE id='mine'")["state"] == "pending"
    assert store.one("SELECT state FROM obligations WHERE id='transfer:mine:0'")["state"] == "open"
    assert not store.one("SELECT 1 FROM events WHERE kind='experiment_cancel_requested'")

    request = uuid4()
    first = cancel(store, eid, request)
    assert store.one("SELECT state,result FROM execution_transfers WHERE id='mine'") == {
        "state": "cancelled",
        "result": "Experiment cancelled",
    }
    assert store.one("SELECT state FROM obligations WHERE id='transfer:mine:0'")["state"] == "abandoned"
    # Saved terminal rows keep their history: cancellation settles open work only.
    assert store.one("SELECT state FROM execution_transfers WHERE id='done'")["state"] == "complete"
    assert not store.one("SELECT 1 FROM obligations WHERE id LIKE 'transfer:done:%'")
    assert cancel(store, eid, request) == first
    assert store.one("SELECT COUNT(*) n FROM obligations WHERE id LIKE 'transfer:mine:%'")["n"] == 1
    await wf.close()


async def test_cancel_during_transfer_replay_abandons_the_episode_and_refuses_late_outcome(runtime):
    store, _, _, wf = runtime
    eid = create(store)
    remote = RemoteSlurmScheduler(
        wf, store.experiment(eid), {"context": "jean-zay", "remote_project": "/synthetic"}
    )
    remote.layout = {"host": "synthetic"}
    assert await remote.transfer("up", lambda: asyncio.sleep(0, result="first"), identity="copy") == "first"
    assert store.one("SELECT state FROM obligations WHERE id='transfer:copy:0'")["state"] == "satisfied"
    started, release = asyncio.Event(), asyncio.Event()

    async def delayed():
        started.set()
        await release.wait()
        return "late"

    replay = asyncio.create_task(remote.transfer("up", delayed, identity="copy"))
    await started.wait()
    assert store.one("SELECT state FROM execution_transfers WHERE id='copy'")["state"] == "pending"
    assert store.one("SELECT state FROM obligations WHERE id='transfer:copy:1'")["state"] == "open"
    cancel(store, eid)
    assert store.one("SELECT state FROM obligations WHERE id='transfer:copy:1'")["state"] == "abandoned"
    with pytest.raises(TransferCancelled):
        remote._save_transfer_outcome("copy", "transfer:copy:1", "complete")
    assert store.one("SELECT state FROM obligations WHERE id='transfer:copy:1'")["state"] == "abandoned"
    release.set()
    with pytest.raises(TransferCancelled):
        await replay
    assert store.one("SELECT state FROM execution_transfers WHERE id='copy'")["state"] == "cancelled"
    assert not store.one("SELECT 1 FROM obligations WHERE id='transfer:copy:2'")
    # A cancelled row is never dispatched again.
    with pytest.raises(TransferCancelled):
        await remote.transfer("up", lambda: asyncio.sleep(0, result="third"), identity="copy")
    await wf.close()


class Scheduler:
    def __init__(self, *, identity="123", unreachable=False):
        self.identity, self.unreachable = identity, unreachable
        self.cancelled = []

    async def reconcile(self, intent_id):
        return self.identity

    async def observe(self, job_id):
        if self.unreachable:
            raise OSError("server unreachable")
        return Observation("CANCELLED" if self.cancelled else "RUNNING")

    async def cancel(self, job_id, intent_id):
        self.cancelled.append((job_id, intent_id))


def job(store, eid, job_id="123", scheduler_state=None):
    identity = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,script_hash,resources,snapshot_hash,created_at,job_id,scheduler_state) VALUES(?,?,'prepare',1,?,'source','{}','snapshot',0,?,?)",
            (identity, eid, "submitted" if job_id else "submitting", job_id, scheduler_state),
        )
    return identity


async def test_cancel_saved_job_without_downloading_outputs_or_starting_replacement(runtime):
    store, _, _, wf = runtime
    eid = create(store)
    scheduler = Scheduler()
    wf.scheduler = scheduler
    intent = job(store, eid, None)
    with store.tx() as db:
        job_obligations.sync_job(db, intent)
    cancel(store, eid)
    await process_cancellations(wf)
    assert scheduler.cancelled == [("123", intent)]
    episodes = store.rows(
        "SELECT id,owner,wait,state FROM obligations WHERE id LIKE ? ORDER BY id", (f"job:{intent}:%",)
    )
    assert [(row["owner"], row["wait"], row["state"]) for row in episodes] == [
        ("backend", f"reconcile:{intent}", "abandoned"),
        ("external", "job:123", "open"),
    ]
    assert store.one("SELECT payload FROM events WHERE kind='job_cancellation_intent'")
    wf.last_cancellation.clear()
    await process_cancellations(wf)
    assert store.one("SELECT state FROM jobs WHERE intent_id=?", (intent,))["state"] == "cancelled"
    assert store.one("SELECT state FROM obligations WHERE id=?", (episodes[1]["id"],))["state"] == "abandoned"
    assert store.experiment(eid)["hold_reason"] is None
    assert len(store.rows("SELECT id FROM experiments")) == 1
    await wf.close()


@pytest.mark.parametrize("unknown", [True, False])
async def test_uncertain_submission_or_unreachable_server_retains_identity(runtime, unknown):
    store, _, _, wf = runtime
    eid = create(store)
    wf.scheduler = Scheduler(identity=None, unreachable=True)
    intent = job(store, eid, None if unknown else "123")
    cancel(store, eid)
    await process_cancellations(wf)
    assert wf.scheduler.cancelled == []
    assert store.one("SELECT state FROM jobs WHERE intent_id=?", (intent,))["state"] in (
        "submitting",
        "submitted",
    )
    assert store.experiment(eid)["hold_reason"] == "cancellation_unconfirmed"
    await wf.close()


async def test_completed_job_with_missing_download_settles_without_contacting_server(runtime):
    store, _, _, wf = runtime
    eid = create(store)
    wf.scheduler = Scheduler(unreachable=True)
    intent = job(store, eid, scheduler_state="COMPLETED")
    cancel(store, eid)
    await process_cancellations(wf)
    assert store.one("SELECT state FROM jobs WHERE intent_id=?", (intent,))["state"] == "cancelled"
    assert wf.scheduler.cancelled == []
    assert store.experiment(eid)["hold_reason"] is None
    await wf.close()


async def test_cancel_interrupts_exact_agent_turn_and_retains_unknown_outcomes(runtime):
    store, _, _, wf = runtime
    eid = create(store)
    wf.agent_ready = True
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,thread_id,turn_id,created_at,updated_at) VALUES('action',?,'prepare',1,'uncertain','prompt','thread','turn',0,0)",
            (eid,),
        )
    # The dispatch task that saved this turn still owns its process.
    wf.agent_pool.claim(
        role="stage",
        source_table="agent_actions",
        row=store.one("SELECT * FROM agent_actions WHERE id='action'"),
        owner=experiment_owner(eid),
        slot=wf.agent_pool.primary,
    )
    calls = []

    async def interrupt(thread, turn):
        calls.append((thread, turn))

    async def read(thread, turn):
        raise TerminalTurnError("turn interrupted")

    wf.agent.interrupt, wf.agent.read_turn = interrupt, read
    cancel(store, eid)
    await process_cancellations(wf)
    assert calls == [("thread", "turn")]
    assert store.one("SELECT state FROM agent_actions WHERE id='action'")["state"] == "failed"
    assert store.experiment(eid)["hold_reason"] is None
    await wf.close()


async def test_cancellation_during_report_render_cannot_revive_publication(runtime, monkeypatch):
    import threading

    from experimenttracker import reporting

    store, _, _, wf = runtime
    eid = create(store)
    root = wf.root(eid)
    root.mkdir(parents=True)
    version = store.working_report(eid, {}, "synthetic-report")
    started, finish = threading.Event(), threading.Event()

    def render(*args):
        started.set()
        assert finish.wait(5)
        (root / "late.html").write_text("Synthetic report")
        return "late.html", ["late.html"]

    monkeypatch.setattr(reporting, "build_report", render)
    task = asyncio.create_task(wf.advance_reports())
    assert await asyncio.to_thread(started.wait, 5)
    cancel(store, eid)
    finish.set()
    await task
    store.report_state(eid, version, "ready")
    assert store.one("SELECT state FROM report_versions WHERE experiment_id=?", (eid,))["state"] == "failed"
    assert not store.one("SELECT 1 FROM artifacts WHERE experiment_id=?", (eid,))
    assert not store.one("SELECT 1 FROM inbox WHERE experiment_id=? AND kind='report_ready'", (eid,))
    await wf.close()
