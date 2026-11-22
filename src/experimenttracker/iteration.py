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
