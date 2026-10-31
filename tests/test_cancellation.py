"""Synthetic cancellation acceptance: no live SSH, Slurm or managed model calls."""

import asyncio
from uuid import uuid4

import pytest

from experimenttracker.cancellation import process_cancellations
from experimenttracker.codex_client import TerminalTurnError
from experimenttracker.maintenance import blockers
from experimenttracker.models import ActionCommand
from experimenttracker.remote import RemoteSlurmScheduler, TransferCancelled, TransferPending
from experimenttracker.slurm import Observation
from experimenttracker.store import Conflict, Store


def create(store):
    return store.command(
        uuid4(), "create", {"brief": "Synthetic cancellation test", "recipe_id": "agent_experiment_v1"}
    )["experiment_id"]


def cancel(store, eid, request_id=None):
    payload = ActionCommand(request_id=request_id or uuid4(), action="cancel").model_dump(mode="json")
    return store.command(payload["request_id"], "action", payload, eid=eid)


@pytest.mark.parametrize(
    "phase", ["preparing", "training", "evaluating", "validating", "interpreting", "publishing", "failed"]
)
async def test_cancel_any_phase_retires_only_its_queue_and_survives_restart(runtime, phase):
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
    cancel(store, eid)
    await process_cancellations(wf)
    assert scheduler.cancelled == [("123", intent)]
    assert store.one("SELECT payload FROM events WHERE kind='job_cancellation_intent'")
    wf.last_cancellation.clear()
    await process_cancellations(wf)
    assert store.one("SELECT state FROM jobs WHERE intent_id=?", (intent,))["state"] == "cancelled"
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
