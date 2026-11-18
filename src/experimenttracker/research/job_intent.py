"""Persist one local research job intent without sending it to the scheduler."""

import json
from pathlib import Path

from pydantic import Field

from .. import clock
from ..config import StrictModel
from ..core import action_obligations, job_obligations
from ..store import Conflict, encode
from .admission import SubmitJobRequest, preflight
from .tools import require_live_research


class PreparedJob(StrictModel):
    """Identity from the caller that exported the exact commit and prepared its script."""

    commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    script_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


def save(workflow, eid, action, request, prepared):
    """Return (saved job, created). A replay never grants permission to submit."""
    request = SubmitJobRequest.model_validate(request)
    prepared = PreparedJob.model_validate(prepared)
    intent = str(request.request_id)
    wire = request.saved_request()
    store = workflow.store
    with store.tx() as db:
        prior = db.execute("SELECT * FROM jobs WHERE intent_id=?", (intent,)).fetchone()
        if prior is not None:
            require_live_research(store.experiment(eid), action_obligations.expected_saved(db, action))
            manifest = json.loads(prior["execution_manifest"] or "{}")
            if (
                prior["experiment_id"] != eid
                or manifest.get("research_request") != wire
                or manifest.get("research_action_id") != action["id"]
                or manifest.get("commit") != prepared.commit
                or prior["script_hash"] != prepared.script_hash
            ):
                raise Conflict("job intent identity already belongs to another submission")
            return dict(prior), False

        preflight(workflow, eid, action, request, db=db)
        branch = db.execute(
            "SELECT head_commit FROM experiment_branches WHERE experiment_id=? AND state='active'",
            (eid,),
        ).fetchone()
        if branch is None or branch["head_commit"] != prepared.commit:
            raise Conflict("submission requires the exact saved worktree commit")
        root = Path(workflow.projects.base_root(eid)) / "attempts" / intent
        stage = "research-" + intent
        now = clock.now()
        # Compatibility identity only: jobs still have a composite FK to stages.
        # Contract 4 never walks or advances these rows; remove the FK seam in T4.8.
        db.execute("INSERT INTO stages(experiment_id,id,updated_at) VALUES(?,?,?)", (eid, stage, now))
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,state,script_hash,resources,"
            "snapshot_hash,created_at,execution_root,execution_manifest) VALUES(?,?,?,'submitting',?,?,?,?,?,?)",
            (
                intent,
                eid,
                stage,
                prepared.script_hash,
                encode(wire["resources"]),
                prepared.commit,
                now,
                str(root),
                encode(
                    {
                        "executor": "slurm",
                        "commit": prepared.commit,
                        "research_request": wire,
                        "research_action_id": action["id"],
                    }
                ),
            ),
        )
        job_obligations.sync_job(db, intent)
        store.emit(db, eid, "submission_intent", {"intent_id": intent, "commit": prepared.commit})
        return dict(db.execute("SELECT * FROM jobs WHERE intent_id=?", (intent,)).fetchone()), True
