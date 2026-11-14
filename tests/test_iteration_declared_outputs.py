"""Contract-3 declared-output safety: the guards that exist today, measured.

Published ``outputs`` pass ``artifacts.allowed_path`` and ``local_outputs``
pass ``iteration.metadata`` -> ``remote_worker.checked``. Each case asserts the
exact refusal reason, so an unexpected message is a finding to report rather
than an assertion to relax.
"""

import copy
import os

import pytest
from helpers import PLAN

from experimenttracker import artifacts, iteration
from experimenttracker.models import Implementation


def manifest():
    """A valid schema-3 plan whose training stage declares only a local cache."""
    plan = copy.deepcopy(PLAN)
    train = plan["stages"][0]
    assert train["id"] == "train"
    train["outputs"] = []
    train["local_outputs"] = ["cache"]
    implementation = Implementation.model_validate(plan)
    assert implementation.schema_version == 3
    return {"plan": implementation.model_dump(), "remote_execution": False}


def stage(document, stage_id):
    return next(s for s in document["plan"]["stages"] if s["id"] == stage_id)


@pytest.mark.parametrize(
    ("case", "reason"),
    [
        ("final_link", "regular file inside the registered root"),
        ("outside_link", "regular file inside the registered root"),
        ("escape", "regular file inside the registered root"),
        ("fifo", "regular file inside the registered root"),
        ("directory", "regular file inside the registered root"),
        ("missing", "missing or inaccessible evidence: plot.svg"),
        ("oversize", "exceeds 10 MiB"),
    ],
)
def test_published_output_is_a_bounded_regular_file_inside_the_root(tmp_path, case, reason):
    root = tmp_path / "attempt"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.write_text("outside")
    target = root / "plot.svg"
    document = manifest()
    if case == "final_link":
        inside = root / "inside.svg"
        inside.write_text("inside")
        target.symlink_to(inside)
    elif case == "outside_link":
        target.symlink_to(outside)
    elif case == "escape":
        stage(document, "plot")["outputs"] = ["../outside"]
    elif case == "fifo":
        os.mkfifo(target)
    elif case == "directory":
        target.mkdir()
    elif case == "oversize":
        with target.open("wb") as stream:
            stream.truncate(artifacts.MAX_ARTIFACT_BYTES + 1)
    with pytest.raises(ValueError, match=reason):
        iteration.check_stage(root, "plot", document)


@pytest.mark.parametrize(
    ("case", "reason"),
    [
        ("escape", "output outside execution directory"),
        ("final_link", "symlink in execution evidence"),
        ("parent_link", "symlink in execution evidence"),
        ("root_link", "output root must retain its owned attempt directory"),
        ("missing", "missing or inaccessible declared output cache"),
        ("fifo", "regular file or directory"),
    ],
)
def test_local_output_keeps_its_owned_directory_and_no_symlink(tmp_path, case, reason):
    root = tmp_path / "attempt"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    cache = root / "cache"
    document = manifest()
    if case == "escape":
        stage(document, "train")["local_outputs"] = ["../outside"]
    elif case == "final_link":
        cache.symlink_to(outside)
    elif case == "parent_link":
        stage(document, "train")["local_outputs"] = ["cache/link/data"]
        cache.mkdir()
        (cache / "link").symlink_to(outside)
    elif case == "root_link":
        real = tmp_path / "real"
        real.mkdir()
        root.rmdir()
        root.symlink_to(real)
    elif case == "fifo":
        os.mkfifo(cache)
    with pytest.raises(ValueError, match=reason):
        iteration.check_stage(root, "train", document)


def test_declared_outputs_pass_for_a_plain_cache_and_published_file(tmp_path):
    root = tmp_path / "attempt"
    root.mkdir()
    (root / "cache").mkdir()
    (root / "plot.svg").write_text("<svg xmlns='http://www.w3.org/2000/svg'/>")
    document = manifest()
    assert iteration.manifest_mode(document)
    assert document["remote_execution"] is False
    assert iteration.check_stage(root, "plot", document) == ["plot.svg"]
    assert iteration.metadata(root, "cache")["kind"] == "directory"
    assert iteration.check_stage(root, "train", document) == []
