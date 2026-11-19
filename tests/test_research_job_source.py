import asyncio
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from test_research_admission import payload
from test_research_first_turn import create, research_project

from experimenttracker.research import approval
from experimenttracker.research.job_intent import save
from experimenttracker.research.job_source import prepare, prepare_remote
from experimenttracker.research.result_tool import register_result
from experimenttracker.research.session import first_turn
from experimenttracker.store import Conflict
from experimenttracker.workspaces import WorkspaceUnavailable, git


def setup_request(workflow, tmp_path):
    store = workflow.store
    repo = research_project(workflow, store, tmp_path)
    eid = create(store)
    workflow.workspaces.ensure_branch(eid)
    action = first_turn(store, eid)
    exp = store.experiment(eid)
    identity = str(uuid4())
    objective = "Compare synthetic inputs"
    store.update(
        eid,
        thread_id="thread",
        pending_request={
            "id": identity,
            "action_id": action["id"],
            "kind": "approval",
            "method": "item/commandExecution/requestApproval",
            "params": {},
        },
    )
    with store.tx() as db:
        db.execute(
            "UPDATE agent_actions SET state='running',thread_id='thread',turn_id='turn' WHERE id=?",
            (action["id"],),
        )
        store.emit(
            db,
            eid,
            approval.REQUEST_EVENT,
            {
                "experiment_id": eid,
                "request_id": identity,
                "objective": objective,
                "brief": exp["brief"],
                "project_id": exp["contract"]["project"]["id"],
            },
        )
    return (
        repo,
        eid,
        store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],)),
        payload(identity, objective),
    )


def test_prepare_requires_approval_and_exports_exact_pinned_argv_job(runtime, tmp_path):
    store, scheduler, _, workflow = runtime
    repo, eid, action, request = setup_request(workflow, tmp_path)
    root = workflow.projects.base_root(eid) / "attempts" / request["request_id"]
    with pytest.raises(ValueError, match="exact owner approval"):
        prepare(workflow, eid, action, request)
    assert not root.exists()
    store.command(
        uuid4(), "answer", {"pending_request_id": request["approval_request_id"], "answer": "accept"}, eid=eid
    )
    worktree = workflow.workspaces.branch_checkout(eid)
    from pathlib import Path

    Path(worktree, "comparison.py").write_text(
        "import json, os, sys\nfrom pathlib import Path\n"
        "Path(os.environ['EXPERIMENT_ROOT'], 'result.json').write_text(json.dumps(sys.argv[1:]))\n"
    )
    args = ["two words", "$(false)", "x'y"]
    request["command"] = [sys.executable, "comparison.py", *args]
    prepared = prepare(workflow, eid, action, request)
    assert prepared.root == root
    assert hashlib.sha256(prepared.script.read_bytes()).hexdigest() == prepared.prepared.script_hash
    assert git(repo, "rev-parse", "refs/et/jobs/" + request["request_id"]).decode().strip() == prepared.commit
    Path(worktree, "comparison.py").unlink()
    subprocess.run(["/bin/sh", str(prepared.script)], check=True, timeout=10)
    assert json.loads((root / "result.json").read_text()) == args
    with pytest.raises(WorkspaceUnavailable, match="already occupied"):
        prepare(workflow, eid, action, request)
    assert scheduler.submissions == 0


@pytest.mark.asyncio
async def test_dispatch_wrapper_consumes_registered_result_and_rejects_tampering(
    runtime, tmp_path, monkeypatch
):
    store, _, _, workflow = runtime
    _, eid, action, request = setup_request(workflow, tmp_path)
    producer_id = str(uuid4())
    producer_root = workflow.projects.base_root(eid) / "attempts" / producer_id
    producer_root.mkdir(parents=True)
    with store.tx() as db:
        db.execute(
            "INSERT INTO stages(experiment_id,id,state,attempt,updated_at) VALUES(?,'producer','complete',1,0)",
            (eid,),
        )
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,job_id,script_hash,resources,"
            "snapshot_hash,created_at,execution_root,execution_manifest,ended_at) "
            "VALUES(?,?,'producer',1,'complete','123','s','{}','h',0,?,?,1)",
            (producer_id, eid, str(producer_root), json.dumps({"executor": "slurm"})),
        )
    (producer_root / "results.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "protocol": "synthetic dispatch receipt",
                "metrics": [{"name": "loss", "variant": "v1", "value": 2, "unit": "unit"}],
            }
        )
    )
    registration_id = uuid4()
    receipt = register_result(
        workflow,
        store.experiment(eid),
        action,
        {"request_id": str(registration_id), "intent_id": producer_id, "path": "results.json"},
    )

    def accept():
        store.command(
            uuid4(),
            "answer",
            {"pending_request_id": request["approval_request_id"], "answer": "accept"},
            eid=eid,
        )

    accept()
    request["inputs"] = [str(registration_id)]
    request["command"] = [
        sys.executable,
        "-c",
        "import json,os; from pathlib import Path; "
        "p=json.loads(os.environ['EXPERIMENT_INPUTS']); "
        "Path(os.environ['EXPERIMENT_ROOT'],'consumed.json').write_text(Path(p[next(iter(p))]).read_text())",
    ]

    class WrapperScheduler:
        submissions = 0

        async def preflight(self, *args):
            pass

        async def submit(self, _intent, script, _root, **_kwargs):
            self.submissions += 1
            process = await asyncio.create_subprocess_exec("/bin/sh", str(script))
            await asyncio.wait_for(process.wait(), timeout=10)
            assert process.returncode == 0
            return "4321"

    scheduler = WrapperScheduler()
    monkeypatch.setattr(workflow, "scheduler_for", lambda *_: scheduler)
    from experimenttracker.research.job_dispatch import dispatch

    await dispatch(workflow, eid, action, request)
    job = store.one("SELECT execution_root FROM jobs WHERE intent_id=?", (request["request_id"],))
    copied = Path(job["execution_root"]) / "inputs" / f"{registration_id}.json"
    registered_path = Path(workflow.projects.base_root(eid)) / receipt["name"]
    registered_bytes = registered_path.read_bytes()
    assert copied.read_bytes() == registered_bytes
    assert json.loads((Path(job["execution_root"]) / "consumed.json").read_text())["protocol"] == (
        "synthetic dispatch receipt"
    )
    assert scheduler.submissions == 1
    with store.tx() as db:
        db.execute("UPDATE jobs SET state='complete',ended_at=2 WHERE intent_id=?", (request["request_id"],))

    # A fresh intent cannot stage bytes that no longer match the registered receipt.
    tampered_request = {**request, "request_id": str(uuid4())}
    changed = Path(workflow.projects.base_root(eid)) / receipt["name"]
    changed.chmod(0o600)
    changed.write_text('{"metrics":[{"value":3}]}')
    with pytest.raises(ValueError, match="registered immutable result changed"):
        await dispatch(workflow, eid, action, tampered_request)
    assert scheduler.submissions == 1
    assert store.one("SELECT 1 FROM jobs WHERE intent_id=?", (tampered_request["request_id"],)) is None

    registered_path.write_bytes(registered_bytes)
    registered_path.unlink()
    external = tmp_path / "external-result.json"
    external.write_bytes(registered_bytes)
    registered_path.symlink_to(external)
    linked_request = {**request, "request_id": str(uuid4())}
    with pytest.raises(ValueError, match="registered-result artifact"):
        await dispatch(workflow, eid, action, linked_request)
    assert scheduler.submissions == 1
    assert store.one("SELECT 1 FROM jobs WHERE intent_id=?", (linked_request["request_id"],)) is None


async def test_remote_unregistered_result_refusal_precedes_dispatch(runtime, tmp_path, monkeypatch):
    store, scheduler, _, workflow = runtime
    _, eid, action, request = setup_request(workflow, tmp_path)
    request["result_file"] = "results/result.json"
    request["inputs"] = [str(uuid4())]
    contract = store.experiment(eid)["contract"]
    contract["execution"] = {"context": "jean-zay"}
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (json.dumps(contract), eid))
    root = workflow.projects.base_root(eid) / "attempts" / request["request_id"]
    with pytest.raises(ValueError, match="inputs must name registered results"):
        await prepare_remote(workflow, eid, action, request)
    assert not root.exists()
    assert scheduler.submissions == 0


async def test_prepare_remote_rejects_unsafe_result_transport_before_filesystem_effects(runtime, tmp_path):
    store, _, _, workflow = runtime
    _, eid, action, request = setup_request(workflow, tmp_path)
    request["result_file"] = "results/unsafe name.json"
    experiment = store.experiment(eid)
    contract = experiment["contract"]
    contract["execution"] = {"context": "jean-zay"}
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (json.dumps(contract), eid))
    root = workflow.projects.base_root(eid) / "attempts" / request["request_id"]
    with pytest.raises(ValueError, match="unsafe for rsync transport"):
        await prepare_remote(workflow, eid, action, request)
    assert not root.exists()
    assert not store.rows("SELECT * FROM jobs")


@pytest.mark.asyncio
async def test_prepare_remote_enforces_source_ceiling_before_remote_route_or_intent(runtime, tmp_path):
    store, scheduler, _, workflow = runtime
    _, eid, action, request = setup_request(workflow, tmp_path)
    request["result_file"] = "results/result.json"
    store.command(
        uuid4(), "answer", {"pending_request_id": request["approval_request_id"], "answer": "accept"}, eid=eid
    )
    experiment = store.experiment(eid)
    contract = experiment["contract"]
    contract["execution"] = {
        "context": "jean-zay",
        "ssh_host": "jz-test",
        "remote_project": "/scratch/project",
        "poll_seconds": 600,
    }
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (json.dumps(contract), eid))
    worktree = workflow.workspaces.branch_checkout(eid)
    oversized = Path(worktree, "shared.py")
    await asyncio.to_thread(oversized.write_bytes, b"x" * (16 * 1024 * 1024 + 1))
    await asyncio.to_thread(git, worktree, "add", "shared.py")

    with pytest.raises(WorkspaceUnavailable, match="source exceeds its size limit"):
        await prepare_remote(workflow, eid, action, request)

    assert not store.rows("SELECT * FROM jobs")
    assert scheduler.submissions == 0


@pytest.mark.asyncio
async def test_prepare_remote_resolves_per_intent_jean_zay_route_without_dispatch(
    runtime, tmp_path, monkeypatch
):
    store, scheduler, _, workflow = runtime
    _, eid, action, request = setup_request(workflow, tmp_path)
    request["result_file"] = "results/result.json"
    producer_id = str(uuid4())
    producer_root = workflow.projects.base_root(eid) / "attempts" / producer_id
    producer_root.mkdir(parents=True)
    with store.tx() as db:
        db.execute(
            "INSERT INTO stages(experiment_id,id,state,attempt,updated_at) "
            "VALUES(?,'producer','complete',1,0)",
            (eid,),
        )
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,job_id,script_hash,resources,"
            "snapshot_hash,created_at,execution_root,execution_manifest,ended_at) "
            "VALUES(?,?,'producer',1,'complete','123','s','{}','h',0,?,?,1)",
            (producer_id, eid, str(producer_root), json.dumps({"executor": "slurm"})),
        )
    (producer_root / "result.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "protocol": "registered remote input",
                "metrics": [{"name": "loss", "variant": "baseline", "value": 1, "unit": "unit"}],
            }
        )
    )
    input_id = uuid4()
    register_result(
        workflow,
        store.experiment(eid),
        action,
        {"request_id": str(input_id), "intent_id": producer_id, "path": "result.json"},
    )
    request["inputs"] = [str(input_id)]
    request["command"] = [
        sys.executable,
        "-c",
        "import json,os; from pathlib import Path; "
        "inputs=json.loads(os.environ['EXPERIMENT_INPUTS']); "
        "Path(os.environ['EXPERIMENT_ROOT'],'consumed.json').write_text("
        "Path(inputs[next(iter(inputs))]).read_text())",
    ]
    request["request_id"] = request["request_id"].upper()
    canonical_intent = str(UUID(request["request_id"]))
    store.command(
        uuid4(), "answer", {"pending_request_id": request["approval_request_id"], "answer": "accept"}, eid=eid
    )
    experiment = store.experiment(eid)
    contract = experiment["contract"]
    contract["execution"] = {
        "context": "jean-zay",
        "ssh_host": "jz-test",
        "remote_project": "/scratch/project",
        "poll_seconds": 600,
    }
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (json.dumps(contract), eid))

    class FakeRemote:
        settings = SimpleNamespace(model_dump=lambda: contract["execution"])

        async def resolve(self):
            assert scheduler.submissions == 0
            route = {
                "host": "jz-test",
                "remote_base": "/scratch/project",
                "remote_project": f"/scratch/project/.experimenttracker/executions/{eid}/{canonical_intent}/project",
                "remote_run": f"/scratch/project/.experimenttracker/executions/{eid}/{canonical_intent}/run",
                "local_project": "/local/project",
                "local_run": str(workflow.projects.base_root(eid) / "attempts" / canonical_intent),
            }
            self.route = route
            return route

    fake = FakeRemote()
    captured = {}

    def scheduler_for(exp):
        captured.update(exp)
        return fake

    monkeypatch.setattr(workflow, "scheduler_for", scheduler_for)
    worktree = workflow.workspaces.branch_checkout(eid)
    await asyncio.to_thread(Path(worktree, "comparison.py").write_text, "print('pinned source')\n")
    prepared = await prepare_remote(workflow, eid, action, request)

    assert prepared.execution["context"] == "jean-zay"
    assert prepared.execution["settings"] == contract["execution"]
    assert prepared.execution["layout"] == fake.route
    assert captured["_attempt_id"] == canonical_intent
    assert captured["_attempt_root"] == str(prepared.root)
    assert prepared.script.is_file() and prepared.prepared.commit == prepared.commit
    assert store.one("SELECT 1 FROM jobs WHERE intent_id=?", (canonical_intent,)) is None
    job, created = save(workflow, eid, action, request, prepared.prepared, execution=prepared.execution)
    assert created
    manifest = json.loads(job["execution_manifest"])
    assert manifest["execution"] == prepared.execution
    with pytest.raises(Conflict, match="another submission"):
        save(
            workflow,
            eid,
            action,
            request,
            prepared.prepared,
            execution={**prepared.execution, "layout": {**prepared.execution["layout"], "host": "other"}},
        )
    assert scheduler.submissions == 0

    from experimenttracker.remote import RemoteSlurmScheduler

    remote_scheduler = RemoteSlurmScheduler(workflow, store.experiment(eid), saved=prepared.execution)
    staged = {}
    remote_calls = []
    rsync_calls = []
    attempt_source = Path(job["execution_root"]) / "source"
    attempt_source.mkdir(exist_ok=True)
    expected_source = (attempt_source / "comparison.py").read_text()
    (attempt_source / "comparison.py").write_text("tampered attempt source")

    async def scripted_remote_command(*args, **kwargs):
        remote_calls.append(args)
        if args[0] == "mkdir":
            return ""
        if args[0] == "bash" and "mv -Tn" in args[2]:
            staged[args[-3]] = staged.pop(args[-2])
            return ""
        if args[0] == "bash" and "cat" in args[2]:
            return staged.get(args[-1], "absent")
        raise AssertionError(args)

    staged_input = Path(job["execution_root"]) / "inputs" / f"{input_id}.json"
    saved_input = staged_input.read_bytes()
    staged_input.chmod(0o600)
    staged_input.write_bytes(b"conflicting bytes")
    with pytest.raises(ValueError, match="staged input changed"):
        await remote_scheduler.stage_research_source(job)
    assert remote_calls == [] and rsync_calls == []
    staged_input.write_bytes(saved_input)
    staged_input.chmod(0o444)

    fail_upload = True

    async def scripted_rsync(*args, **kwargs):
        nonlocal fail_upload
        rsync_calls.append(args)
        local = Path(args[-2].rstrip("/"))
        assert (local / "source" / "comparison.py").read_text() == expected_source
        remote_input = local / "inputs" / f"{input_id}.json"
        assert remote_input.read_bytes() == saved_input
        wrapper = (local / "job.sh").read_text()
        paths = json.loads(
            next(
                line.split("=", 1)[1].strip("'\"")
                for line in wrapper.splitlines()
                if line.startswith("export EXPERIMENT_INPUTS=")
            )
        )
        remote_run = prepared.execution["layout"]["remote_run"]
        assert paths == {str(input_id): remote_run + "/c4-source/inputs/" + str(input_id) + ".json"}
        local_run = tmp_path / "scripted-remote"
        local_run.mkdir(exist_ok=True)
        rewritten = wrapper.replace(remote_run + "/c4-source/source", str(local / "source"))
        rewritten = rewritten.replace(remote_run + "/c4-source/inputs", str(local / "inputs"))
        rewritten = rewritten.replace(remote_run, str(local_run))
        script = local / "scripted-job.sh"
        script.write_text(rewritten)
        process = await asyncio.create_subprocess_exec("/bin/sh", str(script))
        assert await process.wait() == 0
        assert json.loads((local_run / "consumed.json").read_text())["protocol"] == "registered remote input"
        pending_path = args[-1].split(":", 1)[1].rstrip("/")
        if fail_upload:
            fail_upload = False
            raise OSError("interrupted upload")
        staged[pending_path] = (local / "intent.json").read_text().strip()

    remote_scheduler.remote_command = scripted_remote_command
    remote_scheduler.rsync = scripted_rsync
    from experimenttracker.remote import TransferPending

    fabricated = {
        "state": "staged",
        "intent_id": job["intent_id"],
        "remote_path": prepared.execution["layout"]["remote_run"] + "/c4-source",
    }
    with pytest.raises(ValueError, match="completed saved source upload"):
        await remote_scheduler.submit_research_job(job, fabricated)

    with pytest.raises(TransferPending, match="interrupted upload"):
        await remote_scheduler.stage_research_source(job)
    first_pending = rsync_calls[0][-1]
    receipt = await remote_scheduler.stage_research_source(job)
    assert receipt == {
        "state": "staged",
        "intent_id": job["intent_id"],
        "remote_path": prepared.execution["layout"]["remote_run"] + "/c4-source",
    }
    assert rsync_calls[1][-1] != first_pending
    assert len(rsync_calls) == 2

    sends = []

    async def scripted_send(*args, **kwargs):
        sends.append((args, kwargs))
        return "" if len(sends) < 2 else "4321;cluster\n"

    remote_scheduler.remote_command = scripted_send
    remote_scheduler.command = scripted_send
    await remote_scheduler.preflight_research_job(job, receipt)
    assert await remote_scheduler.submit_research_job(job, receipt) == "4321"
    assert len(sends) == 2
    assert sends[0][0][0] == "sbatch"
    assert sends[0][0][-1] == prepared.execution["layout"]["remote_run"] + "/c4-source/job.sh"
    assert sends[1][0][0] == "sbatch"
    assert sends[1][0][sends[1][0].index("--comment") + 1] == job["intent_id"]

    original_remote_command = remote_scheduler.command

    async def pause_during_preflight(*args, **kwargs):
        with store.tx() as db:
            db.execute("UPDATE experiments SET pause_compute=1 WHERE id=?", (eid,))
        return await original_remote_command(*args, **kwargs)

    remote_scheduler.command = pause_during_preflight
    with pytest.raises(ValueError, match="paused"):
        await remote_scheduler.preflight_research_job(job, receipt)
    assert len(sends) == 3  # the awaited scheduler preflight ran; no submission followed it
    store.update(eid, pause_compute=False)
    remote_scheduler.command = original_remote_command

    with pytest.raises(ValueError, match="exact pinned staging receipt"):
        await remote_scheduler.submit_research_job(job, {**receipt, "intent_id": "other"})
    assert len(sends) == 3
    remote_scheduler.remote_command = scripted_remote_command
    remote_scheduler.command = scripted_remote_command

    save_outcome = remote_scheduler._save_transfer_outcome
    interrupted = True

    def lose_completion(*args, **kwargs):
        nonlocal interrupted
        if interrupted:
            interrupted = False
            raise RuntimeError("interrupted after remote rename")
        return save_outcome(*args, **kwargs)

    remote_scheduler._save_transfer_outcome = lose_completion
    with pytest.raises(RuntimeError, match="after remote rename"):
        await remote_scheduler.stage_research_source(job)
    remote_scheduler._save_transfer_outcome = save_outcome
    await remote_scheduler.stage_research_source(job)
    assert len(rsync_calls) == 2
    remote_count = len(remote_calls)
    store.db.execute("UPDATE jobs SET state='submitted' WHERE intent_id=?", (job["intent_id"],))
    with pytest.raises(ValueError, match="submitting job intent"):
        await remote_scheduler.stage_research_source(job)
    assert len(remote_calls) == remote_count and len(rsync_calls) == 2
    store.db.execute("UPDATE jobs SET state='submitting' WHERE intent_id=?", (job["intent_id"],))
    store.db.execute("UPDATE experiments SET phase='complete' WHERE id=?", (eid,))
    with pytest.raises(ValueError, match="non-terminal parent"):
        await remote_scheduler.stage_research_source(job)
    assert len(remote_calls) == remote_count and len(rsync_calls) == 2
