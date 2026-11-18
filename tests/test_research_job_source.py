import hashlib
import json
import subprocess
import sys
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from test_research_admission import payload
from test_research_first_turn import create, research_project

from experimenttracker.research import approval
from experimenttracker.research.job_intent import save
from experimenttracker.research.job_source import prepare, prepare_remote
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
async def test_prepare_remote_resolves_per_intent_jean_zay_route_without_dispatch(
    runtime, tmp_path, monkeypatch
):
    store, scheduler, _, workflow = runtime
    _, eid, action, request = setup_request(workflow, tmp_path)
    request["result_file"] = "results/result.json"
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
    prepared = await prepare_remote(workflow, eid, action, request)

    assert prepared.execution["context"] == "jean-zay"
    assert prepared.execution["settings"] == contract["execution"]
    assert prepared.execution["layout"] == fake.route
    assert captured["_attempt_id"] == canonical_intent
    assert captured["_attempt_root"] == str(prepared.root)
    assert prepared.script.is_file() and prepared.prepared.commit == prepared.commit
    assert not store.rows("SELECT * FROM jobs")
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
