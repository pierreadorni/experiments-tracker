import hashlib
import json
from zipfile import ZipFile

import pytest
from test_workflow import submit, until

from experimenttracker.audit import verify_bundle, verify_experiment


async def test_restored_evidence_audit_is_read_only_and_detects_content_changes(runtime):
    store, scheduler, agent, workflow = runtime
    eid = submit(store)
    await until(workflow, store, eid, lambda e: e["phase"] == "complete")
    revision = store.experiment(eid)["revision"]
    result = verify_experiment(store, eid, workflow.root(eid))
    assert result["scientific_checks"] == "passed" and result["artifacts_verified"] == 13
    assert store.experiment(eid)["revision"] == revision
    assert scheduler.submissions == 3 and agent.calls == 2
    checkpoint = workflow.root(eid) / "linear.json"
    checkpoint.write_bytes(checkpoint.read_bytes() + b" ")  # Valid JSON; a changed immutable file.
    with pytest.raises(ValueError, match="registered artifact changed"):
        verify_experiment(store, eid, workflow.root(eid))
    await workflow.close()


def bundle_fixture(path):
    original = b"<html>original report</html>"
    portable = b"<html>portable report</html>"
    digest = hashlib.sha256(original).hexdigest()
    checks = {
        "schema_version": 1,
        "experiment_id": "test",
        "revision": 7,
        "evidence_revision": 1,
        "artifacts": [{"path": "evidence/report.html", "sha256": digest, "size": len(original)}],
        "portable_report_sha256": hashlib.sha256(portable).hexdigest(),
    }
    exp = {
        "schema_version": 1,
        "id": "test",
        "revision": 7,
        "evidence_revision": 1,
        "artifacts": [{"name": "report.html", "sha256": digest, "size": len(original)}],
    }
    files = {
        "checksums.json": json.dumps(checks).encode(),
        "experiment.json": json.dumps(exp).encode(),
        "README.txt": b"Extract and open report.html",
        "report.html": portable,
        "evidence/report.html": original,
    }
    with ZipFile(path, "w") as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    return files


def test_bundle_verification_checks_bytes_without_extraction_and_rejects_tampering(tmp_path):
    path = tmp_path / "evidence.zip"
    files = bundle_fixture(path)
    assert verify_bundle(path)["artifacts_verified"] == 1
    assert not (tmp_path / "report.html").exists()
    files["report.html"] = b"changed portable report"
    with ZipFile(path, "w") as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    with pytest.raises(ValueError, match="portable report checksum"):
        verify_bundle(path)
    files = bundle_fixture(path)
    with ZipFile(path, "a") as archive:
        archive.writestr("../outside.txt", b"must not be extracted")
    with pytest.raises(ValueError, match="ZIP path"):
        verify_bundle(path)
    assert not (tmp_path.parent / "outside.txt").exists()


@pytest.mark.parametrize("mutation", ["missing_hash", "duplicate", "invalid_size"])
def test_bundle_rejects_malformed_or_ambiguous_registry(tmp_path, mutation):
    path = tmp_path / "evidence.zip"
    files = bundle_fixture(path)
    exp = json.loads(files["experiment.json"])
    if mutation == "missing_hash":
        del exp["artifacts"][0]["sha256"]
    elif mutation == "duplicate":
        exp["artifacts"].append(exp["artifacts"][0].copy())
    else:
        exp["artifacts"][0]["size"] = True
    files["experiment.json"] = json.dumps(exp).encode()
    with ZipFile(path, "w") as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    with pytest.raises(ValueError, match="artifact registry"):
        verify_bundle(path)
