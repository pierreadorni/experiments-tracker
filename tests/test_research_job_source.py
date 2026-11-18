import hashlib
import json
import subprocess
import sys
from uuid import uuid4

import pytest
from test_research_admission import payload
from test_research_first_turn import create, research_project

from experimenttracker.research import approval
from experimenttracker.research.job_source import prepare
from experimenttracker.research.session import first_turn
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
