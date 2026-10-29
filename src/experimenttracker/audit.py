"""Read-only verification for restored state and downloaded evidence bundles."""

import hashlib
import json
import re
from pathlib import Path
from zipfile import ZipFile

from . import artifacts
from .models import Implementation, Preparation
from .store import encode


def verify_experiment(store, eid: str, root: Path):
    exp = store.experiment(eid)
    if exp["contract"].get("execution_contract") == 3:
        rows = store.rows("SELECT * FROM artifacts WHERE experiment_id=?", (eid,))
        base = root.parent.parent if root.parent.name == "records" else root
        for row in rows:
            if artifacts.sha256(artifacts.allowed_path(Path(row["path"]), base)) != row["sha256"]:
                raise ValueError("published object changed")
        return {
            "experiment_id": eid,
            "artifacts_verified": len(rows),
            "scope": "small published object transport checks only; scientific data and reproducibility are not verified",
        }
    if exp["phase"] != "complete":
        raise ValueError("full scientific verification requires a completed experiment")
    manifest = artifacts.read_json(root / "snapshot/manifest.json", root)
    if manifest != exp["snapshot"]:
        raise ValueError("frozen manifest differs from authoritative provenance")
    core = {key: value for key, value in manifest.items() if key != "snapshot_hash"}
    if hashlib.sha256(encode(core).encode()).hexdigest() != manifest["snapshot_hash"]:
        raise ValueError("invalid snapshot identity")
    if manifest["parameters"] != exp["contract"]["parameters"]:
        raise ValueError("run contract and frozen parameters differ")
    reference = exp["contract"].get("reference")
    generic = exp["recipe_id"] == "agent_experiment_v1"
    if (
        reference
        and not generic
        and any(
            reference[field] != manifest[field]
            for field in (
                "snapshot_hash",
                "parameters_hash",
                "data_hash",
                "test_hash",
            )
        )
    ):
        raise ValueError("reference protocol differs")
    stages = (
        [stage["id"] for stage in manifest["plan"]["stages"]]
        if generic
        else ("train_linear", "train_quadratic", "evaluate_and_plot")
    )
    for stage in stages:
        artifacts.validate_stage(root, stage, manifest)
    rows = store.rows(
        "SELECT * FROM artifacts WHERE experiment_id=? AND evidence_revision=?",
        (eid, exp["evidence_revision"]),
    )
    expected = {
        "preparation.json",
        "linear.json",
        "quadratic.json",
        "results.json",
        "predictions.json",
        "predictions.svg",
        "analysis.json",
        "report.html",
    }
    if generic:
        expected = {
            "preparation.json",
            "manifest.json",
            "analysis.json",
            "report.html",
            *manifest["files"],
            *[name for stage in manifest["plan"]["stages"] for name in stage["outputs"]],
            *[
                f"local-output-{stage['id']}.json"
                for stage in manifest["plan"]["stages"]
                if stage.get("local_outputs")
            ],
        }
    if not expected <= {row["name"] for row in rows}:
        raise ValueError("required registered evidence is missing")
    for row in rows:
        data = artifacts.allowed_path(Path(row["path"]), root).read_bytes()
        if len(data) != row["size"] or hashlib.sha256(data).hexdigest() != row["sha256"]:
            raise ValueError(f"registered artifact changed: {row['name']}")
    if generic:
        plan = Implementation.model_validate(artifacts.read_json(root / "preparation.json", root))
        if plan != Implementation.model_validate(manifest["plan"]):
            raise ValueError("preparation plan differs from frozen provenance")
    else:
        prep = Preparation.model_validate(artifacts.read_json(root / "preparation.json", root))
        if (
            prep.snapshot_hash != manifest["snapshot_hash"]
            or prep.parameters_hash != manifest["parameters_hash"]
        ):
            raise ValueError("preparation does not match frozen provenance")
    analysis = artifacts.validate_analysis(
        artifacts.read_json(root / "analysis.json", root), [row["id"] for row in rows]
    )
    if (
        analysis.model_dump()
        != artifacts.validate_analysis(exp["analysis"], [row["id"] for row in rows]).model_dump()
    ):
        raise ValueError("analysis file differs from authoritative interpretation")
    from .reporting import verify_report

    reports = store.rows("SELECT * FROM report_versions WHERE experiment_id=? AND state='ready'", (eid,))
    for report in reports:
        _, identities = verify_report(exp, root, rows, report["version"], json.loads(report["document"]))
        if set(identities) != set(json.loads(report["artifact_ids"])):
            raise ValueError("report version output identities differ")
    return {
        "experiment_id": eid,
        "revision": exp["revision"],
        "evidence_revision": exp["evidence_revision"],
        "artifacts_verified": len(rows),
        "reports_verified": len(reports),
        "scientific_checks": "passed",
        "reference_experiment_id": exp["reference_experiment_id"],
    }


def _json(data):
    def reject(value):
        raise ValueError(f"non-finite JSON: {value}")

    return json.loads(data, parse_constant=reject)


def verify_bundle(path: Path):
    """Check internal integrity without extracting files or executing report/recipe content."""
    if path.stat().st_size > 160 * 1024 * 1024:
        raise ValueError("bundle exceeds verification limits")
    with ZipFile(path) as archive:
        entries = archive.infolist()
        if len(entries) > 2048 or any(entry.flag_bits & 1 for entry in entries):
            raise ValueError("too many ZIP entries or encrypted evidence")
        if any(entry.compress_type not in (0, 8) for entry in entries):
            raise ValueError("unsupported ZIP compression")
        names = [entry.filename for entry in entries]
        if len(names) != len(set(names)):
            raise ValueError("duplicate ZIP entries")
        if any(
            not re.fullmatch(
                r"(?:evidence/)?[a-zA-Z0-9_][a-zA-Z0-9._-]*(?:/[a-zA-Z0-9_][a-zA-Z0-9._-]*)*", name
            )
            for name in names
        ):
            raise ValueError("unsafe or unsupported ZIP path")
        if (
            any(entry.file_size > artifacts.MAX_ARTIFACT_BYTES for entry in entries)
            or sum(entry.file_size for entry in entries) > 160 * 1024 * 1024
        ):
            raise ValueError("bundle exceeds verification limits")
        try:
            checks = _json(archive.read("checksums.json"))
            exp = _json(archive.read("experiment.json"))
        except KeyError as exc:
            raise ValueError("bundle metadata is missing") from exc
        if not isinstance(checks, dict) or not isinstance(exp, dict):
            raise ValueError("invalid bundle metadata objects")
        if checks.get("schema_version") != 1 or exp.get("schema_version") != 1:
            raise ValueError("unsupported bundle schema")
        if (checks.get("experiment_id"), checks.get("revision"), checks.get("evidence_revision")) != (
            exp.get("id"),
            exp.get("revision"),
            exp.get("evidence_revision"),
        ):
            raise ValueError("bundle and experiment identities differ")
        records = checks.get("artifacts")
        if not isinstance(records, list) or not 1 <= len(records) <= (
            1000 if exp.get("contract", {}).get("execution_contract") == 3 else 50
        ):
            raise ValueError("invalid bundle artifact manifest")
        expected = {"report.html", "experiment.json", "checksums.json", "README.txt"}
        for record in records:
            if not isinstance(record, dict):
                raise ValueError("invalid bundle artifact record")
            filename = record.get("path", "")
            if (
                not isinstance(filename, str)
                or not re.fullmatch(
                    r"evidence/[a-zA-Z0-9_][a-zA-Z0-9._-]*(?:/[a-zA-Z0-9_][a-zA-Z0-9._-]*)*", filename
                )
                or filename in expected
            ):
                raise ValueError("invalid or duplicate evidence path")
            expected.add(filename)
            if filename not in names:
                raise ValueError("declared evidence is missing")
            data = archive.read(filename)
            if len(data) != record.get("size") or hashlib.sha256(data).hexdigest() != record.get("sha256"):
                raise ValueError(f"bundle evidence changed: {filename}")
        if set(names) != expected:
            raise ValueError("bundle contains missing or unlisted files")
        report = archive.read("report.html")
        if hashlib.sha256(report).hexdigest() != checks.get("portable_report_sha256"):
            raise ValueError("portable report checksum mismatch")
        registry = exp.get("artifacts")
        if not isinstance(registry, list) or any(
            not isinstance(item, dict)
            or not isinstance(item.get("name"), str)
            or not isinstance(item.get("sha256"), str)
            or type(item.get("size")) is not int
            for item in registry
        ):
            raise ValueError("invalid exported artifact registry")
        iteration = exp.get("contract", {}).get("execution_contract") == 3
        exported = {
            "evidence/" + (item["id"] + "/" if iteration else "") + item["name"]: (
                item["sha256"],
                item["size"],
            )
            for item in registry
        }
        if len(exported) != len(registry):
            raise ValueError("duplicate exported artifact registry entry")
        declared = {item["path"]: (item["sha256"], item["size"]) for item in records}
        if exported != declared:
            raise ValueError("exported registry and bundle manifest differ")
        source_status = "not included in this historical export"
        if "evidence/recipe.py" in names or "evidence/manifest.json" in names:
            if "evidence/manifest.json" not in names:
                raise ValueError("frozen source pair is incomplete")
            manifest = _json(archive.read("evidence/manifest.json"))
            if not isinstance(manifest, dict) or manifest != exp.get("snapshot"):
                raise ValueError("exported frozen manifest differs")
            if not isinstance(manifest.get("files"), dict):
                raise ValueError("invalid frozen source registry")
            core = {key: value for key, value in manifest.items() if key != "snapshot_hash"}
            if hashlib.sha256(encode(core).encode()).hexdigest() != manifest.get("snapshot_hash"):
                raise ValueError("invalid exported snapshot identity")
            if "recipe.py" in manifest["files"] and (
                "evidence/recipe.py" not in names
                or hashlib.sha256(archive.read("evidence/recipe.py")).hexdigest()
                != manifest["files"]["recipe.py"]
            ):
                raise ValueError("exported recipe differs from frozen protocol")
            for name, digest in manifest["files"].items():
                if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._-]*", name) or "evidence/" + name not in names:
                    raise ValueError("declared frozen source is missing or unsafe")
                if hashlib.sha256(archive.read("evidence/" + name)).hexdigest() != digest:
                    raise ValueError("exported source or dependency lock changed")
            source_status = "verified against exported frozen protocol"
        return {
            "experiment_id": exp["id"],
            "revision": exp["revision"],
            "artifacts_verified": len(records),
            "portable_report": "verified",
            "frozen_source": "not promised by iteration-first downloads" if iteration else source_status,
            "scope": "internal bundle consistency; no source or report content was executed",
        }
