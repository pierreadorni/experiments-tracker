"""Bounded local preparation of an approved research job source."""

import hashlib
import json
import os
import shlex
import stat
from pathlib import Path

from pydantic import Field

from ..config import StrictModel
from ..engine import gitworkspace
from ..workspaces import WorkspaceUnavailable
from .admission import MAX_INPUT_BYTES, preflight
from .job_intent import PreparedJob


class PreparedSource(StrictModel):
    prepared: PreparedJob
    root: Path
    source: Path
    script: Path
    commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    execution: dict | None = None


def prepare(workflow, experiment_id, action, request, *, remote_source=False):
    """Commit, pin and export one approved source and create its argv-safe script."""
    request = preflight(workflow, experiment_id, action, request, remote_source=remote_source)
    intent = str(request.request_id)
    # Admission precedes all filesystem/Git effects, including path inspection.
    root = Path(workflow.projects.base_root(experiment_id)) / "attempts" / intent
    if root.exists() or root.is_symlink():
        raise WorkspaceUnavailable("The job preparation root is already occupied")
    branch = workflow.workspaces.branch(experiment_id)
    if not branch:
        raise WorkspaceUnavailable("The named research worktree is unavailable")
    worktree = workflow.workspaces.branch_checkout(experiment_id)
    if Path(branch["worktree"]) != Path(worktree):
        raise WorkspaceUnavailable("The named research worktree is unavailable")
    root.parent.mkdir(parents=True, exist_ok=True)
    if root.parent.resolve() != root.parent:
        raise WorkspaceUnavailable("The job preparation parent is not the exact saved path")
    root.mkdir(mode=0o700)
    committed = workflow.workspaces.commit_branch(experiment_id, "Prepare research job " + intent)
    commit = committed["commit"]
    workflow.workspaces.pin_job_commit(experiment_id, intent, commit)
    source = root / "source"
    script = root / "job.sh"
    gitworkspace.export(
        branch["repo"],
        commit,
        source,
        **(
            {"max_archive_bytes": 32 * 1024 * 1024, "max_source_bytes": 16 * 1024 * 1024}
            if remote_source
            else {}
        ),
    )
    inputs = {}
    if request.inputs:
        input_root = root / "inputs"
        input_root.mkdir(mode=0o700)
        for input_id in request.inputs:
            receipt = workflow.store.one(
                "SELECT payload FROM events WHERE kind='research_result_registered' "
                "AND json_extract(payload,'$.request_id')=? AND experiment_id=? LIMIT 1",
                (str(input_id), experiment_id),
            )
            if receipt is None:
                raise ValueError("registered-result input receipt disappeared during preparation")
            item = json.loads(receipt["payload"])
            artifact = workflow.store.one(
                "SELECT size,sha256 FROM artifacts WHERE id=? AND experiment_id=?",
                (item["artifact_id"], experiment_id),
            )
            if artifact is None or item.get("name") != f"result-{input_id}.json":
                raise ValueError("registered-result artifact identity is invalid")
            base_root = Path(workflow.projects.base_root(experiment_id))
            source_result = base_root / item["name"]
            try:
                if source_result.resolve(strict=True).parent != base_root.resolve(strict=True):
                    raise ValueError("registered-result artifact is outside its root")
                if not stat.S_ISREG(source_result.lstat().st_mode):
                    raise ValueError("registered-result artifact must be a regular file")
                descriptor = os.open(source_result, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            except OSError as exc:
                raise ValueError("registered-result artifact is missing or not a regular file") from exc
            target = input_root / f"{input_id}.json"
            with os.fdopen(descriptor, "rb") as src:
                if not stat.S_ISREG(os.fstat(src.fileno()).st_mode):
                    raise ValueError("registered-result artifact must be a regular file")
                data = src.read(MAX_INPUT_BYTES + 1)
            if (
                len(data) != artifact["size"]
                or len(data) > MAX_INPUT_BYTES
                or hashlib.sha256(data).hexdigest() != artifact["sha256"]
            ):
                raise ValueError("registered immutable result changed")
            with target.open("xb") as dst:
                dst.write(data)
            target.chmod(0o444)
            inputs[str(input_id)] = str(target)
    exports = {
        "EXPERIMENT_ROOT": str(root),
        "EXPERIMENT_SOURCE": str(source),
        **(
            {"EXPERIMENT_INPUTS": json.dumps(inputs, sort_keys=True, separators=(",", ":"))} if inputs else {}
        ),
        "OMP_NUM_THREADS": str(request.resources.cpus_per_job),
        "OPENBLAS_NUM_THREADS": str(request.resources.cpus_per_job),
        "MKL_NUM_THREADS": str(request.resources.cpus_per_job),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONUNBUFFERED": "1",
    }
    body = (
        "#!/bin/sh\nset -eu\n"
        + "".join(f"export {name}={shlex.quote(value)}\n" for name, value in exports.items())
        + f"cd {shlex.quote(str(source))}\n"
        + "exec "
        + shlex.join(request.command)
        + "\n"
    ).encode()
    with script.open("xb") as stream:
        stream.write(body)
    script.chmod(0o444)
    digest = hashlib.sha256(body).hexdigest()
    return PreparedSource(
        prepared=PreparedJob(commit=commit, script_hash=digest),
        root=root,
        source=source,
        script=script,
        commit=commit,
    )


async def prepare_remote(workflow, experiment_id, action, request):
    """Prepare the pinned C4 source and resolve its saved Jean Zay intent route.

    This deliberately stops before remote staging, scheduler preflight, or submission.
    """
    experiment = workflow.store.experiment(experiment_id)
    if experiment["contract"].get("execution", {}).get("context") != "jean-zay":
        raise ValueError("remote source preparation requires the saved Jean Zay context")
    request = preflight(workflow, experiment_id, action, request, remote_source=True)
    request_id = str(request.request_id)
    prepared = prepare(workflow, experiment_id, action, request, remote_source=True)
    experiment = dict(experiment)
    experiment["_attempt_id"] = request_id
    experiment["_attempt_root"] = str(prepared.root)
    scheduler = workflow.scheduler_for(experiment)
    layout = await scheduler.resolve()
    prepared.execution = {
        "context": "jean-zay",
        "settings": scheduler.settings.model_dump(),
        "layout": layout,
        "memory_allocation": (
            "cluster-assigned; --mem is not submitted; memory_mb_per_job is a planning estimate"
        ),
    }
    return prepared
