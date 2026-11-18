import asyncio
import json
import shlex
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from test_remote_execution import Machines
from test_research_admission import payload
from test_research_follow_up import fake_clock
from test_research_job_intent import PREPARED, setup_intent
from test_research_job_source import setup_request

from experimenttracker import remote, resolver
from experimenttracker.research import job_observe, remote_dispatch
from experimenttracker.research.job_dispatch import record_pre_send_failure, record_submission
from experimenttracker.research.job_intent import save
from experimenttracker.slurm import SubmissionUncertain
from experimenttracker.store import Conflict


class DispatchStore:
    def __init__(self):
        self.jobs = {}

    def one(self, query, params):
        assert query == "SELECT 1 FROM jobs WHERE intent_id=?"
        return {"intent_id": params[0]} if params[0] in self.jobs else None

    def experiment(self, _):
        return {"id": "experiment"}


class DispatchWorkflow:
    def __init__(self, scheduler):
        self.store = DispatchStore()
        self.submissions_in_flight = set()
        self.scheduler = scheduler
        self.selected_job = None

    def scheduler_for(self, _experiment, job=None):
        self.selected_job = job
        return self.scheduler


def request_for(intent):
    return {**payload(intent, "synthetic"), "request_id": intent}


def harness(monkeypatch, *, send):
    intent = str(uuid4())
    events = []
    job = {"intent_id": intent, "state": "submitting"}
    receipt = {"intent_id": intent}
    scheduler = SimpleNamespace()

    async def prepare(*_):
        events.append("prepare")
        assert workflow.submissions_in_flight == {intent}
        return SimpleNamespace(prepared="prepared", execution={"context": "jean-zay"})

    def save(*_args, **_kwargs):
        events.append("save")
        workflow.store.jobs[intent] = job
        return job, True

    async def stage(_job):
        events.append("stage")
        return receipt

    async def preflight(_job, _receipt):
        events.append("preflight")

    async def submit(_job, _receipt):
        events.append("send")
        assert workflow.submissions_in_flight == {intent}
        return await send()

    def record(store, eid, saved_job, identity):
        events.append(("record", identity))
        saved_job.update(state="submitted", job_id=identity)
        return saved_job

    def fail(store, eid, saved_job, error):
        events.append(("not-sent", str(error)))
        saved_job.update(state="failed", job_id=None, error="not_submitted: pre-send failure")
        return saved_job

    monkeypatch.setattr(remote_dispatch, "prepare_remote", prepare)
    monkeypatch.setattr(remote_dispatch, "save", save)
    monkeypatch.setattr(remote_dispatch, "record_submission", record)
    monkeypatch.setattr(remote_dispatch, "record_pre_send_failure", fail)
    scheduler.stage_research_source = stage
    scheduler.preflight_research_job = preflight
    scheduler.submit_research_job = submit
    workflow = DispatchWorkflow(scheduler)
    return workflow, intent, events, job


def test_dispatch_uses_saved_job_route_persists_exact_identity_and_rejects_replay(monkeypatch):
    async def send():
        return "remote-id"

    workflow, intent, events, job = harness(monkeypatch, send=send)

    async def run():
        result = await remote_dispatch.dispatch(workflow, "experiment", {}, request_for(intent))
        assert workflow.selected_job is job
        assert result["job"]["job_id"] == "remote-id"
        assert intent not in workflow.submissions_in_flight
        with pytest.raises(Conflict, match="not permission to send or replay"):
            await remote_dispatch.dispatch(workflow, "experiment", {}, request_for(intent))

    asyncio.run(run())
    assert events == ["prepare", "save", "stage", "preflight", "send", ("record", "remote-id")]


@pytest.mark.parametrize("failure_point", ["stage", "preflight"])
def test_proven_pre_send_failure_is_durable_and_not_replayed(monkeypatch, failure_point):
    async def send():
        return "unexpected"

    workflow, intent, events, job = harness(monkeypatch, send=send)
    if failure_point == "stage":

        async def fail_stage(_job):
            events.append("stage")
            raise ValueError("source staging failed")

        workflow.scheduler.stage_research_source = fail_stage
    else:

        async def fail_preflight(_job, _receipt):
            events.append("preflight")
            raise ValueError("compute paused")

        workflow.scheduler.preflight_research_job = fail_preflight

    async def run():
        with pytest.raises(ValueError):
            await remote_dispatch.dispatch(workflow, "experiment", {}, request_for(intent))
        assert job["state"] == "failed" and job["error"].startswith("not_submitted:")
        with pytest.raises(Conflict, match="not permission to send or replay"):
            await remote_dispatch.dispatch(workflow, "experiment", {}, request_for(intent))
        assert intent not in workflow.submissions_in_flight

    asyncio.run(run())
    assert events.count("send") == 0


def test_saved_exact_identity_and_obligation_survive_restart(runtime):
    store, _scheduler, _agent, workflow = runtime
    eid, action, request = setup_intent(store)
    store.command(
        uuid4(), "answer", {"pending_request_id": request["approval_request_id"], "answer": "accept"}, eid=eid
    )
    job, created = save(workflow, eid, action, request, PREPARED)
    assert created

    saved = record_submission(store, eid, job, "12345")
    assert saved["state"] == "submitted" and saved["job_id"] == "12345"
    obligation = store.one(
        "SELECT owner,kind,wait FROM obligations WHERE target=? ORDER BY id DESC LIMIT 1", (job["intent_id"],)
    )
    assert obligation == {"owner": "external", "kind": "job", "wait": "job:12345"}

    restarted = DispatchWorkflow(SimpleNamespace())
    restarted.store = store
    with pytest.raises(Conflict, match="not permission to send or replay"):
        asyncio.run(remote_dispatch.dispatch(restarted, eid, action, request))


def test_pre_send_failure_is_durable_with_no_external_obligation(runtime):
    store, _scheduler, _agent, workflow = runtime
    eid, action, request = setup_intent(store)
    store.command(
        uuid4(), "answer", {"pending_request_id": request["approval_request_id"], "answer": "accept"}, eid=eid
    )
    job, created = save(workflow, eid, action, request, PREPARED)
    assert created

    failed = record_pre_send_failure(store, eid, job, RuntimeError("agent dispatch paused"))
    assert failed["state"] == "failed" and failed["job_id"] is None
    assert failed["error"].startswith("not_submitted: pre-send failure:")
    assert (
        store.one(
            "SELECT state FROM obligations WHERE target=? ORDER BY id DESC LIMIT 1", (job["intent_id"],)
        )["state"]
        == "abandoned"
    )


@pytest.mark.parametrize("error", [SubmissionUncertain("send outcome unknown"), RuntimeError("lost reply")])
def test_uncertain_send_restart_adopts_exact_identity_without_replay(monkeypatch, error):
    async def send():
        raise error

    workflow, intent, events, job = harness(monkeypatch, send=send)

    async def run():
        with pytest.raises(SubmissionUncertain):
            await remote_dispatch.dispatch(workflow, "experiment", {}, request_for(intent))
        assert job["state"] == "submitting" and job.get("job_id") is None
        assert intent not in workflow.submissions_in_flight

        # Simulate process restart and exact reconciliation adopting one identity.
        restarted = DispatchWorkflow(workflow.scheduler)
        restarted.store.jobs[intent] = job
        job.update(state="submitted", job_id="adopted-id")
        with pytest.raises(Conflict, match="not permission to send or replay"):
            await remote_dispatch.dispatch(restarted, "experiment", {}, request_for(intent))
        assert job["job_id"] == "adopted-id"

    asyncio.run(run())
    assert events.count("send") == 1


@pytest.mark.asyncio
async def test_controller_refuses_invalid_saved_remote_route_without_send(runtime, tmp_path, monkeypatch):
    store, _, _, workflow = runtime
    _, eid, action, request = setup_request(workflow, tmp_path)
    store.command(
        uuid4(), "answer", {"pending_request_id": request["approval_request_id"], "answer": "accept"}, eid=eid
    )
    contract = store.experiment(eid)["contract"]
    contract["execution"] = {"context": "jean-zay"}
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (json.dumps(contract), eid))
    pool = workflow.agent_pool
    pool.claim(
        role="research",
        source_table="agent_actions",
        row=action,
        owner=("experiment", eid),
        slot=pool.primary,
    )
    calls = []

    async def forbidden(*args, **kwargs):
        calls.append(args)
        pytest.fail("invalid saved remote route must not reach SSH/Slurm")

    monkeypatch.setattr(remote, "command", forbidden)
    with pytest.raises((ValueError, KeyError)):
        await workflow.controller.call(eid, "submit_job", request)
    assert calls == []
    assert store.one("SELECT * FROM jobs WHERE intent_id=?", (request["request_id"],)) is None


@pytest.mark.asyncio
async def test_scripted_remote_send_recovery_and_result_wakeup_once(runtime, tmp_path, monkeypatch):
    """Exercise the saved remote boundary through SSH, accounting, and result delivery."""
    store, _, _, workflow = runtime
    _, eid, action, request = setup_request(workflow, tmp_path)
    request["result_file"] = "results/result.json"
    store.command(
        uuid4(), "answer", {"pending_request_id": request["approval_request_id"], "answer": "accept"}, eid=eid
    )
    worktree = workflow.workspaces.branch_checkout(eid)
    Path(worktree, "comparison.py").write_text(  # noqa: ASYNC240
        "import json, os\nfrom pathlib import Path\n"
        "Path(os.environ['EXPERIMENT_ROOT'], 'results/result.json').write_text(json.dumps({'metrics': []}))\n"
    )
    workflow.config.mode = "live"
    contract = store.experiment(eid)["contract"]
    contract["execution"] = {
        "context": "jean-zay",
        "ssh_host": "jz",
        "remote_project": str(tmp_path / "remote"),
        "accounting_lag_seconds": 60,
        "poll_seconds": 600,
    }
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (json.dumps(contract), eid))

    now = fake_clock(monkeypatch)
    machines = Machines(monkeypatch, store)
    # C4 saves the intent before running this read-only sbatch check.
    machines.lost_submit = True
    pool = workflow.agent_pool
    pool.claim(
        role="research",
        source_table="agent_actions",
        row=action,
        owner=("experiment", eid),
        slot=pool.primary,
    )
    hide_accounting = True
    original_run = machines.run

    async def scripted_run(*args, **kwargs):
        if hide_accounting and args[0] == "ssh" and "--starttime" in args[-1]:
            machines.calls.append(args)
            return ""
        return await original_run(*args, **kwargs)

    monkeypatch.setattr(remote, "command", scripted_run)
    with pytest.raises(SubmissionUncertain):
        await workflow.controller.call(eid, "submit_job", request)
    job = store.one("SELECT * FROM jobs WHERE intent_id=?", (request["request_id"],))
    manifest = json.loads(job["execution_manifest"])
    assert job["state"] == "submitting" and job["job_id"] is None
    assert manifest["execution"]["context"] == "jean-zay"
    sbatch = [call[-1] for call in machines.calls if call[0] == "ssh" and "sbatch" in call[-1]]
    assert len([call for call in sbatch if "--test-only" not in call]) == 1
    command = shlex.split(sbatch[-1])[2]
    if "; done; " in command:
        command = command.split("; done; ", 1)[1]
    sent = shlex.split(command)
    assert sent[sent.index("--comment") + 1] == request["request_id"]
    assert "--test-only" not in sent
    assert "--no-requeue" in sent
    # The scripted sbatch boundary records acceptance from this exact comment.
    replay = await workflow.controller.call(eid, "submit_job", request)
    assert replay["state"] == "submitting" and replay["intent_id"] == request["request_id"]
    assert len(machines.jobs) == 1

    # The actual resolver must preserve the uncertain intent during accounting lag.
    with store.tx() as db:
        db.execute("UPDATE jobs SET created_at=? WHERE intent_id=?", (now.now() - 1, request["request_id"]))
    assert await resolver.resolve(workflow, store.experiment(eid), job) is None
    assert (
        store.one("SELECT state FROM jobs WHERE intent_id=?", (request["request_id"],))["state"]
        == "submitting"
    )
    now.advance(601)  # pass the saved remote poll interval before refreshed accounting
    workflow.remote_reads.clear()
    hide_accounting = False
    adopted = await resolver.resolve(workflow, store.experiment(eid), job)
    assert adopted is not None, (
        store.experiment(eid)["error"],
        machines.jobs,
        [call[-1] for call in machines.calls if call[0] == "ssh"],
    )
    assert adopted["job_id"] == "101" and adopted["state"] == "submitted"
    assert len(machines.jobs) == 1

    layout = manifest["execution"]["layout"]
    remote_result = Path(layout["remote_run"]) / "results/result.json"
    remote_result.parent.mkdir(parents=True, exist_ok=True)
    remote_result.write_text('{"metrics": []}')
    existing_wakeups = {
        row["id"] for row in store.rows("SELECT id FROM agent_wakeups WHERE experiment_id=?", (eid,))
    }
    machines.fail_download = True
    await job_observe.observe_remote(workflow, store.experiment(eid), adopted)
    pending = store.one("SELECT * FROM jobs WHERE intent_id=?", (request["request_id"],))
    assert pending["state"] == "submitted" and pending["scheduler_state"] == "COMPLETED"
    assert {
        row["id"] for row in store.rows("SELECT id FROM agent_wakeups WHERE experiment_id=?", (eid,))
    } == existing_wakeups
    machines.fail_download = False
    await job_observe.observe_remote(workflow, store.experiment(eid), pending)
    settled = store.one("SELECT state FROM jobs WHERE intent_id=?", (request["request_id"],))
    assert settled["state"] == "complete"
    wakeups = store.rows("SELECT id,state,reason FROM agent_wakeups WHERE experiment_id=?", (eid,))
    job_wakeups = [
        wake for wake in wakeups if json.loads(wake["reason"]).get("source_id") == request["request_id"]
    ]
    assert len(job_wakeups) == 1 and job_wakeups[0]["state"] == "pending"
    calls = len(machines.calls)
    await job_observe.observe_remote(
        workflow,
        store.experiment(eid),
        store.one("SELECT * FROM jobs WHERE intent_id=?", (request["request_id"],)),
    )
    assert (
        len(
            [
                wake
                for wake in store.rows("SELECT id,reason FROM agent_wakeups WHERE experiment_id=?", (eid,))
                if json.loads(wake["reason"]).get("source_id") == request["request_id"]
            ]
        )
        == 1
    )
    assert len(machines.jobs) == 1 and len(machines.calls) == calls
