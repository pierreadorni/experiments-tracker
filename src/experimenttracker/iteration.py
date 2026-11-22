"""Version 3 execution: bounded control records, direct producers and small results.

Scientific output paths are declarations, never byte identities. All executable
sources are copied before dispatch into an attempt-private package tree; agents
may edit the stable workspace while allocations use their own copies.
"""

import json
import os
import stat
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from . import artifacts, clock, legacy_archive
from .models import ExperimentResult
from .store import encode

SOURCE_BUDGET = 16 * 1024 * 1024


def enabled(exp):
    return exp["contract"].get("execution_contract") == 3


def runnable(exp):
    """The stage engine also requires that this saved format is still active."""
    return enabled(exp) and not legacy_archive.execution_frozen(exp)


def manifest_mode(manifest):
    return manifest.get("plan", {}).get("schema_version") == 3


def metadata(root, name):
    """O(path depth) lstat only. Do not resolve a foreign-host path locally."""
    from .remote_worker import checked

    root = Path(root)
    if root.is_symlink() or root.parent.is_symlink():
        raise ValueError("output root must retain its owned attempt directory")
    try:
        path = checked(root.resolve(), name)
        info = path.lstat()
    except OSError as exc:
        raise ValueError(
            f"missing or inaccessible declared output {name}; replace it or rerun only its producer"
        ) from exc
    return {
        "name": name,
        "kind": "directory" if stat.S_ISDIR(info.st_mode) else "file",
        "size": info.st_size if stat.S_ISREG(info.st_mode) else None,
        "mtime_ns": info.st_mtime_ns,
    }


def bounded_bytes(path, root, limit=artifacts.MAX_ARTIFACT_BYTES):
    path = artifacts.allowed_path(path, root, max_bytes=limit)
    with path.open("rb") as stream:
        value = stream.read(limit + 1)
    if len(value) > limit:
        raise ValueError("bounded control/source file grew beyond its limit")
    return value


def agent_job(wf, exp, phase):
    return wf.store.one(
        "SELECT * FROM jobs WHERE experiment_id=? AND stage_id=? AND attempt=?",
        (exp["id"], phase.removeprefix("agent:"), exp["evidence_revision"]),
    )


def agent_prompt(wf, exp, phase):
    job = agent_job(wf, exp, phase)
    manifest = json.loads(job["execution_manifest"])
    spec = next(s for s in manifest["plan"]["stages"] if s["id"] == job["stage_id"])
    payload = {
        "phase": phase,
        "brief": exp["brief"],
        "stage": spec,
        "protocol": manifest["plan"]["protocol"],
        "attempt_directory": job["execution_root"],
        "source_directory": str(Path(job["execution_root"]) / "snapshot"),
        "inputs": manifest["inputs"],
        "python": manifest["python_executable"],
        "workspace": str(wf.working_directory(exp)),
        "project_context": wf.project_context(exp),
        "project_instructions": wf.projects.instructions(
            exp["contract"].get("project", {}).get("id", "test-project")
        ),
    }
    conventions = wf.worktree_conventions(exp)
    return (
        "Execute this small agent stage with shell/tool calls in the local attempt_directory. "
        "Diagnose failures and correct/retest within this same turn; do not turn a failed small check into a Slurm job. "
        "Use the declared source copies as starting context; write any corrected scripts, diagnostics and declared outputs "
        "inside attempt_directory, preserving snapshot and inputs.json. You may also fix declared source/config files "
        "in workspace and the authorized project_context.checkout, preserving unrelated edits. Copy changed candidate sources "
        "into this attempt's scratch area and test them there. Dependent Slurm attempts copy the corrected declared sources "
        "at dispatch; already dispatched source copies stay unchanged. Do not modify producer inputs, shared environments "
        "or other experiments. Dependencies are direct paths in inputs, never files assumed under your output root. "
        "Keep work bounded to the approved instruction: no long training, background processes, scheduler submission, "
        "API billing or credentials. Local GPU access requires existing runtime/host authorization; a plan grants none. "
        "Return outcome='blocked' with a concrete reason and needed action in summary if required inputs or resources "
        "are unavailable; return outcome='failed' if the work cannot be completed for another reason. "
        "Return outcome='completed' only after successful work and all declared outputs exist. "
        "For evaluation, result_file follows the supplied "
        "ExperimentResult schema and exact protocol. Use check_agent_stage to check outputs and fix errors before finishing."
        + (f" {conventions}" if conventions else "")
        + "\nPAYLOAD:\n"
        + encode({**payload, "result_schema": ExperimentResult.model_json_schema()})
    )


def check_stage(root, stage, manifest):
    spec = next(s for s in manifest["plan"]["stages"] if s["id"] == stage)
    for name in spec["outputs"]:
        artifacts.allowed_path(root / name, root)
    if not manifest.get("remote_execution"):
        for name in spec.get("local_outputs", []):
            metadata(root, name)
    if spec.get("result_file"):
        result = ExperimentResult.model_validate(artifacts.read_json(root / spec["result_file"], root))
        if result.protocol != manifest["plan"]["protocol"]:
            raise ValueError("evaluation protocol differs from the requested protocol")
        keys = [(m.name, m.variant) for m in result.metrics]
        if len(keys) != len(set(keys)):
            raise ValueError("evaluation contains duplicate metrics")
        if set(spec.get("expected_metrics", [])) - {m.name for m in result.metrics}:
            raise ValueError("evaluation is missing expected metrics")
        if spec.get("sample_count") is not None and result.sample_count != spec["sample_count"]:
            raise ValueError("evaluation sample count differs from the requested count")
    return spec["outputs"]


def copy_small(source, source_root, destination):
    data = bounded_bytes(source, source_root)
    if destination.is_symlink():
        raise ValueError("publication destination cannot be a symlink")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if bounded_bytes(destination, destination.parent) != data:
            raise ValueError("small published output already exists with different bytes; use a new name")
        return
    pending = destination.with_name("." + destination.name + ".pending")
    if pending.exists():
        artifacts.allowed_path(pending, destination.parent)
        pending.unlink()
    with pending.open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    pending.chmod(0o444)
    pending.replace(destination)


def complete(wf, exp, job, manifest, root, db):
    """Commit output IDs, selected producer, results and readiness with job success."""
    stage = job["stage_id"]
    spec = next(s for s in manifest["plan"]["stages"] if s["id"] == stage)
    remote = manifest.get("execution")
    if remote:
        receipt_name = f"remote-receipt-{stage}-{job['intent_id']}.json"
        receipt = artifacts.read_json(root / receipt_name, root)
        records = receipt["outputs"] + receipt["local_outputs"]
        context, host = "jean-zay", remote["layout"]["host"]
        producer_root = remote["layout"]["remote_run"]
    else:
        records = [metadata(root, name) for name in spec["outputs"] + spec.get("local_outputs", [])]
        context, host, producer_root = "acacia", wf.config.executor_id, str(root)
    for record in records:
        identity = str(uuid5(NAMESPACE_URL, job["intent_id"] + ":" + record["name"]))
        db.execute(
            "INSERT OR IGNORE INTO output_refs VALUES(?,?,?,?,?,?,?,?,?)",
            (
                identity,
                exp["id"],
                job["intent_id"],
                record["name"],
                context,
                host,
                str(Path(producer_root) / record["name"]),
                record["kind"],
                clock.now(),
            ),
        )
    db.execute(
        "INSERT INTO stage_producers VALUES(?,?,?,?) ON CONFLICT(experiment_id,stage_id) DO UPDATE SET intent_id=excluded.intent_id,rationale=excluded.rationale",
        (exp["id"], stage, job["intent_id"], "successful declared task outputs"),
    )
    publication_root = wf.root(exp["id"])
    for name in spec["outputs"]:
        copy_small(root / name, root, publication_root / name)
    artifacts.register(wf.store, exp["id"], stage, publication_root, spec["outputs"], _db=db)
    if spec.get("result_file"):
        result = artifacts.read_json(root / spec["result_file"], root)
        previous = wf.store.experiment(exp["id"]).get("partial_results")
        partial = json.loads(previous) if previous else {"tasks": {}}
        partial["tasks"][stage] = {"intent_id": job["intent_id"], "result": result}
        metric_keys = [
            (m["name"], m["variant"]) for task in partial["tasks"].values() for m in task["result"]["metrics"]
        ]
        if len(metric_keys) != len(set(metric_keys)):
            raise ValueError(
                "evaluation metric/variant pairs must be unique across tasks; qualify dataset/split/variant names"
            )
        if (
            sum(len(t["result"]["metrics"]) for t in partial["tasks"].values()) > 10000
            or len(encode(partial).encode()) > artifacts.MAX_ARTIFACT_BYTES
        ):
            raise ValueError(
                "combined structured results exceed the bounded 10000-metric/10 MiB budget; retain detailed rows as separate small outputs"
            )
        db.execute("UPDATE experiments SET partial_results=? WHERE id=?", (encode(partial), exp["id"]))


def reuse(wf, exp, plan):
    if plan.reuse_stages is None or (plan.reuse_stages and not (plan.reuse_rationale or "").strip()):
        raise ValueError(
            "repair must explicitly select reuse_stages and explain reuse_rationale; [] requests no reuse"
        )
    reused = {}
    for stage in plan.stages:
        if stage.id not in plan.reuse_stages:
            continue
        producer = wf.store.one(
            "SELECT * FROM stage_producers WHERE experiment_id=? AND stage_id=?", (exp["id"], stage.id)
        )
        if not producer:
            raise ValueError(f"no successful selected producer for {stage.id}")
        job = wf.store.one(
            "SELECT * FROM jobs WHERE intent_id=? AND state='complete'", (producer["intent_id"],)
        )
        if not job:
            raise ValueError("reuse requires an exact known-successful job")
        refs = wf.store.rows("SELECT * FROM output_refs WHERE intent_id=?", (job["intent_id"],))
        if {r["name"] for r in refs} != set(stage.outputs + stage.local_outputs):
            raise ValueError("reused output declarations differ; choose the producer's original declarations")
        # Paths are used directly. No prior revision, receipt or source traversal.
        for ref in refs:
            if ref["context"] == "acacia" and not Path(ref["path"]).exists():
                raise ValueError(
                    f"missing output {ref['id']} ({ref['name']}); choose a replacement or rerun only its producer"
                )
        for name in stage.outputs:
            copy_small(
                Path(job["execution_root"]) / name, Path(job["execution_root"]), wf.root(exp["id"]) / name
            )
        reused[stage.id] = {
            "intent_id": job["intent_id"],
            "outputs": stage.outputs,
            "output_ids": [r["id"] for r in refs],
            "rationale": plan.reuse_rationale,
        }
    return reused


def queue_working_report(wf, exp, job, db):
    """Cheap deterministic working document; no extra model/research work."""
    spec = next(s for s in exp["snapshot"]["plan"]["stages"] if s["id"] == job["stage_id"])
    figures = exp["snapshot"]["plan"]["figures"]
    if not spec.get("result_file") and not set(spec["outputs"]) & set(figures):
        return
    saved = wf.store.experiment(exp["id"])
    partial = json.loads(saved["partial_results"]) if saved.get("partial_results") else {"tasks": {}}
    metrics = [m for task in partial["tasks"].values() for m in task["result"]["metrics"]]
    if not metrics:
        return
    rows = [
        [
            {"text": m["name"]},
            {"text": m["variant"]},
            {"metric": {"name": m["name"], "variant": m["variant"]}},
            {"text": m["unit"]},
        ]
        for m in metrics[:12]
    ]
    title = "Synthetic working results" if exp["snapshot"]["plan"]["synthetic_demo"] else "Working results"
    blocks = [
        {
            "kind": "table",
            "caption": "Available evaluation measurements; full measurements are in Evidence.",
            "columns": ["Metric", "Variant", "Value", "Unit"],
            "rows": rows,
        }
    ]
    records = wf.store.rows(
        "SELECT id,name FROM artifacts WHERE experiment_id=? AND evidence_revision=?",
        (exp["id"], exp["evidence_revision"]),
    )
    for record in records:
        if record["name"] in figures:
            blocks.append(
                {
                    "kind": "figure",
                    "artifact_id": record["id"],
                    "alt": record["name"],
                    "caption": record["name"],
                }
            )
    states = {
        s["id"]: s["state"]
        for s in wf.store.rows("SELECT id,state FROM stages WHERE experiment_id=?", (exp["id"],))
    }
    pending = [
        s["title"]
        for s in exp["snapshot"]["plan"]["stages"]
        if s.get("requested", True) and states[s["id"]] != "complete"
    ]
    if pending:
        blocks.append({"kind": "paragraph", "text": "Requested work remaining: " + "; ".join(pending)[:5000]})
    document = {
        "title": title,
        "standfirst": "Available results from the saved producing tasks. Interpretation and requested deliverables may still be in progress.",
        "blocks": blocks,
    }
    wf.store.working_report(exp["id"], document, "working-results:" + job["intent_id"], _db=db)
