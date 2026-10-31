"""Version 3 execution: bounded control records, direct producers and small results.

Scientific output paths are declarations, never byte identities. All executable
sources are copied before dispatch into an attempt-private package tree; agents
may edit the stable workspace while allocations use their own copies.
"""

import json
import os
import stat
import time
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from . import artifacts
from .models import ExperimentResult
from .store import encode

SOURCE_BUDGET = 16 * 1024 * 1024


def enabled(exp):
    return exp["contract"].get("execution_contract") == 3


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


def capture_sources(root, plan, workspace, project):
    """Copy only explicitly declared small sources, preserving package paths."""
    snapshot = root / "snapshot"
    snapshot.mkdir(parents=True, exist_ok=True)
    files, project_sources, total = {}, {}, 0
    for name, base, target_name in [
        *[(name, workspace, name) for name in plan.files],
        *[(name, project, "project/" + name) for name in plan.project_files],
    ]:
        if not base:
            raise ValueError("declared executable project sources need a project checkout")
        value = bounded_bytes(
            Path(base) / name, Path(base), min(SOURCE_BUDGET - total, artifacts.MAX_ARTIFACT_BYTES)
        )
        total += len(value)
        if total > SOURCE_BUDGET:
            raise ValueError("declared executable sources exceed the 16 MiB submission budget")
        target = snapshot / target_name
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            if bounded_bytes(target, snapshot) != value:
                raise ValueError("attempt source staging already exists with different bytes")
        else:
            target.write_bytes(value)
            target.chmod(0o444)
        files[target_name] = artifacts.sha256(target)
        if target_name.startswith("project/"):
            project_sources[name] = target_name
    return files, project_sources, total


def input_refs(store, exp, stage):
    plan = exp["snapshot"]["plan"]
    spec = next(s for s in plan["stages"] if s["id"] == stage)
    if spec.get("input_output_ids") is not None:
        refs = []
        for identity in spec["input_output_ids"]:
            ref = store.one(
                "SELECT r.* FROM output_refs r JOIN jobs j ON j.intent_id=r.intent_id WHERE r.id=? AND j.state='complete'",
                (str(identity),),
            )
            if ref and ref["experiment_id"] not in (exp["id"], exp.get("reference_experiment_id")):
                ref = None
            if not ref:
                raise ValueError(
                    "selected input must identify this experiment or its selected reference's successful original producer"
                )
            refs.append(ref)
        return refs
    needs = spec["needs"]
    refs = []
    for need in needs:
        selected = store.one(
            "SELECT intent_id FROM stage_producers WHERE experiment_id=? AND stage_id=?", (exp["id"], need)
        )
        if not selected:
            raise ValueError(
                f"missing selected producer for dependency {need}; select completed work or rerun that producer"
            )
        rows = store.rows(
            "SELECT * FROM output_refs WHERE intent_id=? ORDER BY name", (selected["intent_id"],)
        )
        refs.extend(rows)
    if len({r["name"] for r in refs}) != len(refs):
        raise ValueError("ambiguous logical input names; explicitly name distinct producer outputs")
    return refs


def stage_attempt(wf, exp, stage, intent):
    from .models import Implementation

    started = time.monotonic()
    root = wf.projects.base_root(exp["id"]) / "attempts" / intent
    if root.is_symlink() or root.parent.is_symlink():
        raise ValueError("attempt folder must be owned and not a symlink")
    root.mkdir(parents=True, exist_ok=True)
    plan = Implementation.model_validate(exp["snapshot"]["plan"])
    files, project_sources, source_bytes = capture_sources(
        root, plan, wf.working_directory(exp), wf.project_context(exp)["checkout"]
    )
    (root / "snapshot/project").mkdir(exist_ok=True)
    refs = input_refs(wf.store, exp, stage)
    spec = next(s for s in exp["snapshot"]["plan"]["stages"] if s["id"] == stage)
    if not wf.is_remote(exp) or spec.get("executor") == "agent":
        for ref in refs:
            if ref["context"] != "acacia" or ref["host"] != wf.config.executor_id:
                raise ValueError("selected input belongs to another host; arrange an explicit transfer")
            if not Path(ref["path"]).exists():
                raise ValueError(f"missing output {ref['id']}; replace or rerun only its producer")
    else:
        host = exp["contract"]["execution"]["ssh_host"]
        if any(ref["context"] != "jean-zay" or ref["host"] != host for ref in refs):
            raise ValueError(
                "selected input belongs to another host; arrange an explicit transfer or use input_output_ids:[] for an ordering-only gate"
            )
    saved_path = root / "snapshot/manifest.json"
    if saved_path.exists():
        saved = artifacts.read_json(saved_path, root)
        if saved["files"] != files or saved["inputs"] != refs or saved["plan"] != exp["snapshot"]["plan"]:
            raise ValueError(
                "pre-dispatch attempt identity already has different sources/inputs; use a new request ID"
            )
        return root, saved, refs
    source_context = {}
    project = wf.project_context(exp)["checkout"]
    if project:
        import subprocess

        try:
            result = subprocess.run(
                ["git", "-C", str(project), "rev-parse", "--verify", "HEAD"],
                capture_output=True,
                text=True,
                timeout=2,
                check=True,
            )
            source_context["git_commit"] = result.stdout.strip()[:80]
        except (OSError, subprocess.SubprocessError):
            source_context["note"] = "optional Git context unavailable"
    manifest = {
        **exp["snapshot"],
        **({"executor": "agent", "remote_execution": False} if spec.get("executor") == "agent" else {}),
        "files": files,
        "project_sources": project_sources,
        "reused_stages": {},
        "inputs": refs,
        "intent_id": intent,
        "source_mode": "declared-copies",
        "source_context": source_context,
        "source_bytes": source_bytes,
        "provenance_bytes_read": 0,
        "bookkeeping_ms": (time.monotonic() - started) * 1000,
    }
    # No Git traversal or dependency/environment freeze. This identity protects
    # the small command payload, not scientific bytes or later checkout equality.
    import hashlib

    manifest.pop("snapshot_hash", None)
    manifest["snapshot_hash"] = hashlib.sha256(encode(manifest).encode()).hexdigest()
    (root / "snapshot/manifest.json").write_text(encode(manifest))
    (root / "inputs.json").write_text(encode(refs))
    return root, manifest, refs


def agent_job(wf, exp, phase):
    return wf.store.one(
        "SELECT * FROM jobs WHERE experiment_id=? AND stage_id=? AND attempt=?",
        (exp["id"], phase.removeprefix("agent:"), exp["evidence_revision"]),
    )


def queue_agent(wf, exp, stage):
    """Record the local attempt and input bindings before any managed turn."""
    from uuid import uuid4

    existing = agent_job(wf, exp, "agent:" + stage)
    if existing:
        return existing
    intent = str(uuid4())
    root, manifest, refs = stage_attempt(wf, exp, stage, intent)
    with wf.store.tx() as db:
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,script_hash,resources,snapshot_hash,created_at,execution_root,execution_manifest) VALUES(?,?,?,?,'agent',?,'{}',?,?,?,?)",
            (
                intent,
                exp["id"],
                stage,
                exp["evidence_revision"],
                manifest["snapshot_hash"],
                manifest["snapshot_hash"],
                time.time(),
                str(root),
                encode(manifest),
            ),
        )
        for ref in refs:
            db.execute(
                "INSERT INTO output_bindings VALUES(?,?,?)",
                (intent, ref["id"], "selected dependency producer"),
            )
        db.execute(
            "UPDATE stages SET state='running',updated_at=? WHERE experiment_id=? AND id=?",
            (time.time(), exp["id"], stage),
        )
        wf.store.emit(db, exp["id"], "agent_stage_intent", {"intent_id": intent, "stage": stage})
    return agent_job(wf, exp, "agent:" + stage)


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
        "If required inputs/resources are unavailable, request user input through the runtime. "
        "Write all declared outputs before returning a concise summary. For evaluation, result_file follows the supplied "
        "ExperimentResult schema and exact protocol. Use check_agent_stage to check outputs and fix errors before finishing.\nPAYLOAD:\n"
        + encode({**payload, "result_schema": ExperimentResult.model_json_schema()})
    )


def finish_agent(wf, exp, phase, value):
    from .models import AgentStageResult

    AgentStageResult.model_validate(value)
    job = agent_job(wf, exp, phase)
    if job["state"] == "complete":
        return
    manifest = json.loads(job["execution_manifest"])
    root = Path(job["execution_root"])
    check_stage(root, job["stage_id"], manifest)
    with wf.store.tx() as db:
        complete(wf, exp, job, manifest, root, db)
        db.execute(
            "UPDATE jobs SET state='complete',ended_at=? WHERE intent_id=?", (time.time(), job["intent_id"])
        )
        db.execute(
            "UPDATE stages SET state='complete',error=NULL,updated_at=? WHERE experiment_id=? AND id=?",
            (time.time(), exp["id"], job["stage_id"]),
        )
        db.execute("UPDATE experiments SET hold_reason=NULL,error=NULL WHERE id=?", (exp["id"],))
        wf.store.emit(
            db,
            exp["id"],
            "agent_stage_complete",
            {"stage": job["stage_id"], "intent_id": job["intent_id"], "summary": value["summary"]},
        )
        queue_working_report(wf, exp, job, db)


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
                time.time(),
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


def prepare_manifest(root, plan, *, python, project, workspace, reused, remote_execution):
    import hashlib

    target = root / "snapshot/manifest.json"
    if target.exists():
        saved = artifacts.read_json(target, root)
        if saved["plan"] != plan.model_dump():
            raise ValueError("implementation record differs from saved plan")
        return saved
    # Only a complete manifest makes this bounded source record immutable.
    # A failed pre-dispatch capture must not strand subsequent output corrections.
    snapshot = root / "snapshot"
    if snapshot.exists() or snapshot.is_symlink():
        import shutil

        if snapshot.is_symlink():
            raise ValueError("source staging directory cannot be a symlink")
        shutil.rmtree(snapshot)
    files, project_sources, _ = capture_sources(root, plan, workspace, project)
    (root / "snapshot/requirements.txt").write_text("")
    manifest = {
        "schema_version": 1,
        "recipe_id": "agent_experiment_v1",
        "files": files,
        "plan": plan.model_dump(),
        "parameters": json.loads(plan.parameters_json),
        "parameters_hash": hashlib.sha256(plan.parameters_json.encode()).hexdigest(),
        "expected_outputs": {s.id: s.outputs for s in plan.stages},
        "python_executable": str(python),
        "project_sources": project_sources,
        "reused_stages": reused or {},
        "remote_execution": remote_execution,
    }
    manifest["snapshot_hash"] = hashlib.sha256(encode(manifest).encode()).hexdigest()
    target.write_text(encode(manifest))
    return manifest


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
