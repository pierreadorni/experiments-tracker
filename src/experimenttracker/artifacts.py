import contextlib
import hashlib
import html
import json
import math
import mimetypes
import runpy
import shutil
import stat
import time
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from .models import Analysis, ExperimentResult, ExperimentValidation, Implementation
from .store import encode

MAX_ARTIFACT_BYTES = 10 * 1024 * 1024
MEDIA_TYPES = {
    "text/html",
    "image/svg+xml",
    "application/json",
    "application/octet-stream",
    "image/png",
    "image/jpeg",
    "text/plain",
    "text/csv",
    "text/x-python",
}


def media_type(name):
    value = mimetypes.guess_type(name)[0] if name != "recipe.py" else None
    return value if value in MEDIA_TYPES else "application/octet-stream"


def sha256(path: Path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def allowed_path(path: Path, root: Path, *, max_bytes=MAX_ARTIFACT_BYTES):
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise ValueError(f"missing or inaccessible evidence: {path.name}") from exc
    if not resolved.is_relative_to(root.resolve()) or path.is_symlink() or not resolved.is_file():
        raise ValueError("artifact must be a regular file inside the registered root")
    if max_bytes is not None and resolved.stat().st_size > max_bytes:
        raise ValueError("artifact exceeds 10 MiB limit")
    return resolved


def read_json(path: Path, root: Path):
    def reject_constant(value):
        raise ValueError(f"non-finite JSON constant: {value}")

    value = json.loads(allowed_path(path, root).read_text(), parse_constant=reject_constant)
    ensure_finite_json(value)
    return value


def ensure_finite_json(value):
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("non-finite JSON number")
    if isinstance(value, dict):
        for item in value.values():
            ensure_finite_json(item)
    elif isinstance(value, list):
        for item in value:
            ensure_finite_json(item)


def freeze_implementation(
    root: Path,
    plan: Implementation,
    requirements: str,
    *,
    python=None,
    project=None,
    workspace=None,
    reused=None,
    remote_execution=False,
):
    if plan.schema_version == 3:
        from .iteration import prepare_manifest

        return prepare_manifest(
            root,
            plan,
            python=python,
            project=project,
            workspace=workspace,
            reused=reused,
            remote_execution=remote_execution,
        )
    parameters = json.loads(plan.parameters_json)
    ensure_finite_json(parameters)
    snapshot = root / "snapshot"
    if snapshot.exists():
        manifest = read_json(snapshot / "manifest.json", root)
        if Implementation.model_validate(manifest.get("plan")) != plan:
            raise ValueError("prepared plan differs from saved immutable implementation")
        verify_snapshot(root, manifest)
        if (snapshot / "requirements.txt").read_text() != requirements:
            raise ValueError("prepared dependencies differ from the saved immutable implementation")
        return manifest
    pending = root / "snapshot.pending"
    if pending.exists():
        if pending.is_symlink():
            raise ValueError("snapshot staging directory is a symlink")
        shutil.rmtree(pending)
    pending.mkdir()
    files = {}
    for name in plan.files:
        work = workspace or root / "work"
        source = allowed_path(work / name, work)
        # The workflow checks syntax with a selected interpreter before passing python.
        if name.endswith(".py") and python is None:
            try:
                compile(source.read_text(), name, "exec")
            except SyntaxError as exc:
                raise ValueError(f"prepared source does not compile: {name}: {exc.msg}") from exc
        target = pending / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source.read_bytes())
        target.chmod(0o444)
        files[name] = sha256(target)
    project_sources = {}
    if plan.project_files and not project:
        raise ValueError("project source files require a selected project checkout")
    for index, name in enumerate(plan.project_files):
        source = allowed_path(Path(project) / name, Path(project))
        frozen = f"project-source-{index:04d}{source.suffix}"
        target = pending / frozen
        shutil.copyfile(source, target)
        target.chmod(0o444)
        files[frozen] = sha256(target)
        project_sources[name] = frozen
    lock = pending / "requirements.txt"
    lock.write_text(requirements)
    lock.chmod(0o444)
    files["requirements.txt"] = sha256(lock)
    manifest = {
        "schema_version": 1,
        "recipe_id": "agent_experiment_v1",
        "files": files,
        "plan": plan.model_dump(),
        "parameters": parameters,
        "parameters_hash": hashlib.sha256(encode(parameters).encode()).hexdigest(),
        "expected_outputs": {stage.id: stage.outputs for stage in plan.stages},
        "python_relative_path": "work/.venv/bin/python",
        "python_executable": str(python) if plan.python_executable or workspace else None,
        "project_sources": project_sources,
        "reused_stages": reused or {},
    }
    if remote_execution:
        manifest["remote_execution"] = True
    manifest["snapshot_hash"] = hashlib.sha256(encode(manifest).encode()).hexdigest()
    (pending / "manifest.json").write_text(json.dumps(manifest, indent=2))
    (pending / "manifest.json").chmod(0o444)
    pending.chmod(0o555)
    pending.rename(snapshot)
    return manifest


def freeze_recipe(source: Path, root: Path, contract: dict):
    snapshot = root / "snapshot"
    snapshot.mkdir(parents=True, exist_ok=True)
    target = snapshot / "recipe.py"
    if not target.exists():
        shutil.copyfile(source / "recipe.py", target)
        target.chmod(0o444)
    module = runpy.run_path(str(target))
    if module["PARAMETERS"] != contract["parameters"]:
        raise ValueError("recipe parameters do not match contract")
    dataset = module["data"]()
    manifest = {
        "schema_version": 1,
        "recipe_id": contract["recipe_id"],
        "files": {"recipe.py": sha256(target)},
        "parameters": contract["parameters"],
        "parameters_hash": module["digest"](contract["parameters"]),
        "data_hash": module["digest"](dataset),
        "test_hash": module["digest"](dataset["test"]),
        "expected_outputs": {
            "train_linear": ["linear.json"],
            "train_quadratic": ["quadratic.json"],
            "evaluate_and_plot": ["results.json", "predictions.json", "predictions.svg"],
        },
    }
    manifest["snapshot_hash"] = hashlib.sha256(encode(manifest).encode()).hexdigest()
    manifest_path = snapshot / "manifest.json"
    if manifest_path.exists() and json.loads(manifest_path.read_text()) != manifest:
        raise ValueError("snapshot manifest changed")
    if not manifest_path.exists():
        manifest_path.write_text(json.dumps(manifest, indent=2))
        manifest_path.chmod(0o444)
    snapshot.chmod(0o555)
    return manifest


def verify_snapshot(root: Path, manifest):
    if manifest.get("plan", {}).get("schema_version") == 3:
        return
    for filename, expected in manifest["files"].items():
        if sha256(allowed_path(root / "snapshot" / filename, root)) != expected:
            raise ValueError("immutable snapshot hash mismatch")


def verify_project_sources(root: Path, manifest, project):
    for name, frozen in manifest.get("project_sources", {}).items():
        if (
            not project
            or sha256(allowed_path(Path(project) / name, Path(project))) != manifest["files"][frozen]
        ):
            raise ValueError(f"project source changed after preparation: {name}")


def local_output_identity(root: Path, name: str):
    """Keep file identities compatible; seal directory membership and every file's bytes."""
    root = root.resolve()
    path = root / name
    if not path.is_relative_to(root) or ".." in path.parts:
        raise ValueError(f"local output must be inside the registered root: {name}")
    for parent in (path, *path.parents):
        if parent == root:
            break
        if parent.is_symlink():
            raise ValueError(f"local output must not contain symlinks: {name}")

    def inspect(entry):
        relative = entry.relative_to(root).as_posix()
        try:
            mode = entry.lstat().st_mode
        except OSError as exc:
            raise ValueError(f"missing or inaccessible local output: {relative}") from exc
        if stat.S_ISLNK(mode) or not (stat.S_ISREG(mode) or stat.S_ISDIR(mode)):
            raise ValueError(f"local output must be a regular file or directory without symlinks: {relative}")
        return stat.S_ISDIR(mode)

    def file_record(entry):
        file = allowed_path(entry, root, max_bytes=None)
        return {
            "name": entry.relative_to(root).as_posix(),
            "size": file.stat().st_size,
            "sha256": sha256(file),
        }

    if not inspect(path):
        return file_record(path)
    files, directories, pending = [], [], [path]
    while pending:
        directory = pending.pop()
        directories.append(directory.relative_to(root).as_posix())
        try:
            children = sorted(directory.iterdir())
        except OSError as exc:
            raise ValueError(f"inaccessible local output directory: {directory.relative_to(root)}") from exc
        for child in children:
            if inspect(child):
                pending.append(child)
            else:
                files.append(file_record(child))
    contents = {"directories": sorted(directories), "files": sorted(files, key=lambda record: record["name"])}
    return {
        "name": name,
        "type": "directory",
        "size": sum(record["size"] for record in files),
        "sha256": hashlib.sha256(encode(contents).encode()).hexdigest(),
        **contents,
    }


def local_output_manifest(root: Path, stage: str, manifest):
    if manifest.get("plan", {}).get("schema_version") == 3:
        raise ValueError("version 3 uses direct output_refs; scientific output verification is unavailable")
    reused = manifest.get("reused_stages", {}).get(stage)
    if reused:
        prior_root = Path(reused["root"])
        prior = read_json(prior_root / "snapshot/manifest.json", prior_root)
        if prior["snapshot_hash"] != reused["snapshot_hash"]:
            raise ValueError("reused checkpoint source identity changed")
        return local_output_manifest(prior_root, stage, prior)
    plan = next(s for s in manifest["plan"]["stages"] if s["id"] == stage)
    names = plan.get("local_outputs", [])
    if not names:
        return None
    # Checkpoints stay on the compute-visible filesystem; publish their identities only.
    if manifest.get("remote_execution"):
        value = read_json(root / f"local-output-{stage}.json", root)
        if (
            value.get("storage") != "jean-zay"
            or value.get("stage") != stage
            or value.get("snapshot_hash") != manifest["snapshot_hash"]
            or [record["name"] for record in value.get("outputs", [])] != names
        ):
            raise ValueError("remote checkpoint provenance mismatch")
        return value
    outputs = [local_output_identity(root, name) for name in names]
    value = {"schema_version": 1, "stage": stage, "storage": "executor-only", "outputs": outputs}
    if len(encode(value).encode()) > MAX_ARTIFACT_BYTES:
        raise ValueError("local output provenance exceeds 10 MiB; declare smaller output directories")
    path = root / f"local-output-{stage}.json"
    if path.exists() and read_json(path, root) != value:
        raise ValueError(f"registered local checkpoint changed: {stage}")
    return value


def seal_local_outputs(root: Path, stage: str, manifest):
    if manifest.get("plan", {}).get("schema_version") == 3:
        return []
    value = local_output_manifest(root, stage, manifest)
    if value is None:
        return []
    name = f"local-output-{stage}.json"
    path = root / name
    if not path.exists():
        pending = root / f".{name}.pending"
        if pending.is_symlink():
            raise ValueError("local-output staging path is a symlink")
        pending.write_text(encode(value))
        pending.chmod(0o444)
        pending.replace(path)
    return [name]


def register_snapshot(store, eid, root: Path):
    """Publish legacy source evidence; v3 records bounded context per attempt."""
    if read_json(root / "snapshot/manifest.json", root).get("plan", {}).get("schema_version") == 3:
        return
    missing = []
    for name in (*read_json(root / "snapshot/manifest.json", root)["files"], "manifest.json"):
        if store.one(
            "SELECT id FROM artifacts WHERE experiment_id=? AND name=? AND evidence_revision=?",
            (eid, name, store.experiment(eid)["evidence_revision"]),
        ):
            continue
        source = allowed_path(root / "snapshot" / name, root)
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.is_symlink():
            raise ValueError("frozen source copies must not be symlinks")
        if target.exists():
            if sha256(allowed_path(target, root)) != sha256(source):
                raise ValueError("frozen source copy changed")
        else:
            target.write_bytes(source.read_bytes())
            target.chmod(0o444)
        missing.append(name)
    if missing:
        register(store, eid, "prepare", root, missing)


def finite_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def validate_stage(root: Path, stage: str, manifest: dict, *, seal_outputs=False, verify_local_outputs=True):
    if manifest.get("plan", {}).get("schema_version") == 3:
        from .iteration import check_stage

        return check_stage(root, stage, manifest)
    verify_snapshot(root, manifest)
    reused = manifest.get("reused_stages", {}).get(stage)
    if reused:
        prior_root = Path(reused["root"])
        prior = read_json(prior_root / "snapshot/manifest.json", prior_root)
        if prior["snapshot_hash"] != reused["snapshot_hash"]:
            raise ValueError("reused stage source identity changed")
        validate_stage(prior_root, stage, prior, verify_local_outputs=verify_local_outputs)
        for name in manifest["expected_outputs"][stage]:
            if sha256(allowed_path(root / name, root)) != sha256(allowed_path(prior_root / name, prior_root)):
                raise ValueError("reused stage output changed")
        return manifest["expected_outputs"][stage]
    outputs = manifest["expected_outputs"][stage]
    for name in outputs:
        allowed_path(root / name, root)
    if manifest["recipe_id"] == "agent_experiment_v1":
        sealed = []
        if seal_outputs:
            sealed = seal_local_outputs(root, stage, manifest)
        elif verify_local_outputs:
            local_output_manifest(root, stage, manifest)
        for name in outputs:
            if name.endswith(".json"):
                read_json(root / name, root)
        if stage == manifest["plan"]["stages"][-1]["id"]:
            result = ExperimentResult.model_validate(read_json(root / "results.json", root))
            if result.protocol != manifest["plan"]["protocol"]:
                raise ValueError("results protocol does not match the frozen plan")
            validation = ExperimentValidation.model_validate(read_json(root / "validation.json", root))
            if not validation.passed or not all(check.passed for check in validation.checks):
                raise ValueError("experiment's scientific validation checks failed")
        return outputs + sealed
    if stage.startswith("train_"):
        variant = stage.removeprefix("train_")
        model = read_json(root / f"{variant}.json", root)
        if model.get("schema_version") != 1 or model.get("variant") != variant:
            raise ValueError("wrong checkpoint variant or schema")
        if len(model.get("coefficients", [])) != (2 if variant == "linear" else 3):
            raise ValueError("wrong number of coefficients")
        if not all(finite_number(v) for v in model["coefficients"]):
            raise ValueError("invalid coefficients")
        for field in ("parameters_hash", "data_hash", "test_hash", "parameters"):
            if model.get(field) != manifest[field]:
                raise ValueError(f"checkpoint {field} mismatch")
    else:
        result = read_json(root / "results.json", root)
        if result.get("schema_version") != 1 or result.get("synthetic_demo") is not True:
            raise ValueError("invalid result schema or demo identity")
        for field in ("parameters_hash", "data_hash", "test_hash", "parameters"):
            if result.get(field) != manifest[field]:
                raise ValueError(f"results {field} mismatch")
        if set(result.get("metrics", {})) != {"linear", "quadratic"}:
            raise ValueError("wrong result variants")
        for variant, metrics in result["metrics"].items():
            if not all(finite_number(metrics.get(k)) and metrics[k] >= 0 for k in ("mse", "rmse")):
                raise ValueError("invalid metrics")
            if metrics.get("n_test") != manifest["parameters"]["n_test"]:
                raise ValueError("wrong evaluation size")
            if metrics.get("n_parameters") != (2 if variant == "linear" else 3):
                raise ValueError("wrong parameter count")
            if not math.isclose(metrics["rmse"] ** 2, metrics["mse"], rel_tol=1e-9, abs_tol=1e-12):
                raise ValueError("inconsistent metrics")
        predictions = read_json(root / "predictions.json", root)
        if predictions.get("schema_version") != 1 or predictions.get("test_hash") != manifest["test_hash"]:
            raise ValueError("prediction provenance mismatch")
        if len(predictions.get("points", [])) != manifest["parameters"]["n_test"]:
            raise ValueError("missing predictions")
        for row in predictions["points"]:
            if not all(finite_number(row.get(k)) for k in ("x", "target", "linear", "quadratic")):
                raise ValueError("non-finite prediction")
        for variant in ("linear", "quadratic"):
            mse = sum((row[variant] - row["target"]) ** 2 for row in predictions["points"]) / len(
                predictions["points"]
            )
            if not math.isclose(mse, result["metrics"][variant]["mse"], rel_tol=1e-9, abs_tol=1e-12):
                raise ValueError("predictions and metrics disagree")
    return outputs


def register(
    store, eid, stage, root: Path, names: list[str], *, report_version=None, report_name=None, _db=None
):
    exp = store.experiment(eid)
    with contextlib.nullcontext(_db) if _db is not None else store.tx() as db:
        for name in names:
            path = allowed_path(root / name, root)
            artifact_id = str(uuid5(NAMESPACE_URL, f"{eid}:{exp['evidence_revision']}:{name}"))
            existing = db.execute("SELECT sha256 FROM artifacts WHERE id=?", (artifact_id,)).fetchone()
            digest = sha256(path)
            if existing and existing[0] != digest:
                raise ValueError("registered immutable artifact changed")
            db.execute(
                """INSERT OR IGNORE INTO artifacts(
                id,experiment_id,stage_id,path,name,sha256,size,media_type,evidence_revision,local_valid)
                VALUES(?,?,?,?,?,?,?,?,?,1)""",
                (
                    artifact_id,
                    eid,
                    stage,
                    str(path),
                    name,
                    digest,
                    path.stat().st_size,
                    media_type(name),
                    exp["evidence_revision"],
                ),
            )
        if report_version is not None:
            from .reporting import artifact_id

            db.execute(
                "UPDATE report_versions SET state='publishing',artifact_ids=?,report_artifact_id=?,updated_at=? WHERE experiment_id=? AND version=?",
                (
                    encode([artifact_id(exp, name) for name in names]),
                    artifact_id(exp, report_name),
                    time.time(),
                    eid,
                    report_version,
                ),
            )
        store.emit(db, eid, "artifacts_registered", {"names": names})


def validate_analysis(value, evidence_ids):
    analysis = Analysis.model_validate(value)
    for observation in analysis.observations:
        if not set(observation.evidence_ids) <= set(evidence_ids):
            raise ValueError("analysis cites unknown evidence")
    if any(not text.strip() or len(text) > 4000 for text in analysis.limitations + analysis.next_ideas):
        raise ValueError("invalid analysis text")
    return analysis


def render_report(exp, result, analysis: Analysis, figure_id, artifact_ids):
    esc = html.escape
    rows = "".join(
        f"<tr><th scope='row'>{v.title()}</th><td>{m['mse']:.6g}</td>"
        f"<td>{m['rmse']:.6g}</td><td>{m['n_parameters']}</td><td>{m['n_test']}</td></tr>"
        for v, m in (result["metrics"].items() if isinstance(result["metrics"], dict) else [])
    )
    generic = exp["recipe_id"] == "agent_experiment_v1"
    if generic:
        rows = "".join(
            f"<tr><th scope='row'>{esc(m['variant'])}</th><td>{esc(m['name'])}</td>"
            f"<td>{m['value']:.6g}</td><td>{esc(m['unit'])}</td></tr>"
            for m in result["metrics"]
        )
    header = (
        "<tr><th>Variant</th><th>Metric</th><th>Value</th><th>Unit</th></tr>"
        if generic
        else "<tr><th>Variant</th><th>MSE (y²)</th><th>RMSE (y)</th><th>Parameters</th><th>Test cases</th></tr>"
    )
    figures = figure_id if isinstance(figure_id, list) else [figure_id]
    figure_html = "".join(
        f'<figure><img src="/api/artifacts/{esc(i)}" alt="{esc(artifact_ids.get(i, "Experiment figure"))}"><figcaption>{esc(artifact_ids.get(i, ""))}</figcaption></figure>'
        for i in figures
    )
    label = "SYNTHETIC EXPERIMENT" if exp["contract"].get("synthetic_demo") else "EXPERIMENT"
    observations = "".join(
        f"<li>{esc(o.text)} <small>Evidence: "
        + ", ".join(
            f"<a href='/api/artifacts/{esc(i)}'>{esc(artifact_ids.get(i, i))}</a>" for i in o.evidence_ids
        )
        + "</small></li>"
        for o in analysis.observations
    )
    limitations = "".join(f"<li>{esc(t)}</li>" for t in analysis.limitations)
    ideas = "".join(f"<li>{esc(t)}</li>" for t in analysis.next_ideas)
    provenance = esc(
        json.dumps(
            {
                "experiment_id": exp["id"],
                "contract": exp["contract"],
                "snapshot": exp["snapshot"],
                "model": exp["model"],
                "generated_at": time.time(),
            },
            indent=2,
        )
    )
    return f"""<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
    <title>{esc(analysis.title)}</title><style>
    body{{font:17px/1.6 system-ui,sans-serif;color:#172c35;background:#f8faf9;margin:0 auto;padding:32px 20px;max-width:900px}}
    h1{{line-height:1.2}}small{{display:block;color:#536976}}table{{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}}
    th,td{{border-bottom:1px solid #dce5e5;text-align:left;padding:10px}}img{{width:100%;background:white;border-radius:12px}}
    pre{{white-space:pre-wrap;overflow-wrap:anywhere;font-size:12px}}a{{color:#087a70}}li{{margin-bottom:12px}}
    </style><body><p>{label} · Evidence revision {exp["evidence_revision"]}</p>
    <h1>{esc(analysis.title)}</h1><p>{esc(analysis.summary)}</p><h2>Question</h2><p>{esc(exp["brief"])}</p>
    <h2>Measured results</h2><p>Values rendered from validated result JSON. Scientific checks are recorded in the evidence.</p>
    <table><thead>{header}</thead>
    <tbody>{rows}</tbody></table><h2>Figures</h2>{figure_html}
    <h2>Interpretation</h2><ul>{observations}</ul><h2>Limitations</h2><ul>{limitations}</ul>
    <h2>Saved next ideas</h2><p>These suggestions do not launch jobs.</p><ul>{ideas}</ul>
    <h2>Runnable provenance</h2><pre>{provenance}</pre></body></html>"""
