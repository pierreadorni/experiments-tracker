import base64
import io
import json
import struct
import sys
from uuid import uuid4

import pytest
from helpers import seed_saved_contract3
from PIL import Image
from tensorboard.compat.proto.event_pb2 import Event
from tensorboard.compat.proto.summary_pb2 import Summary
from tensorboard.compat.proto.tensor_pb2 import TensorProto
from tensorboard.compat.tensorflow_stub.pywrap_tensorflow import masked_crc32c

from experimenttracker import artifacts
from experimenttracker.models import Implementation, TensorBoardDisplay
from experimenttracker.tfevents import _CACHE, read_previews


def record(event):
    payload = event.SerializeToString()
    size = struct.pack("<Q", len(payload))
    return size + struct.pack("<I", masked_crc32c(size)) + payload + struct.pack("<I", masked_crc32c(payload))


def png(color="red"):
    stream = io.BytesIO()
    Image.new("RGB", (800, 600), color).save(stream, format="PNG")
    return stream.getvalue()


def tensorboard_experiment(store, workflow):
    eid = seed_saved_contract3(store, "Synthetic TensorBoard fixture")
    assert store.experiment(eid)["contract"]["execution_contract"] == 3
    root = workflow.root(eid)
    work = root / "work"
    work.mkdir(parents=True, exist_ok=True)
    (work / "recipe.py").write_text("pass\n")
    plan = Implementation.model_validate(
        {
            "schema_version": 3,
            "summary": "Synthetic TensorBoard publication fixture.",
            "protocol": "Infrastructure fixture, not scientific research.",
            "synthetic_demo": True,
            "parameters_json": "{}",
            "files": ["recipe.py"],
            "figures": [],
            "stages": [
                {
                    "id": "train_linear",
                    "title": "Synthetic training",
                    "needs": [],
                    "tensorboard": {"log_dir": "tensorboard/fit", "scalar_tag": "loss"},
                },
                {
                    "id": "evaluate",
                    "title": "Synthetic evaluation",
                    "needs": ["train_linear"],
                    "outputs": ["metrics.json"],
                    "result_file": "metrics.json",
                    "expected_metrics": ["score"],
                    "sample_count": 12,
                },
            ],
        }
    )
    manifest = artifacts.freeze_implementation(
        root, plan, python=sys.executable, project=None, workspace=work
    )
    store.resolve_implementation(eid, manifest)
    return eid


@pytest.fixture
def events(tmp_path):
    directory = tmp_path / "tensorboard/fit"
    directory.mkdir(parents=True)
    path = directory / "events.out.tfevents.test"
    job = {"stage_id": "fit", "job_id": "123", "intent_id": str(uuid4()), "started_at": 100}
    manifest = {
        "plan": {
            "stages": [
                {
                    "id": "fit",
                    "tensorboard": {"log_dir": "tensorboard/fit", "scalar_tag": "loss", "image_tag": "mask"},
                }
            ]
        }
    }
    return tmp_path, job, manifest, path


def event(step, *values, wall_time=None):
    return Event(step=step, wall_time=wall_time or 101 + step, summary=Summary(value=values))


def test_legacy_and_tensor_summaries_selection_and_incremental_flush(events):
    root, job, manifest, path = events
    first = record(
        event(
            1,
            Summary.Value(tag="loss", simple_value=2),
            Summary.Value(tag="private", simple_value=99),
            Summary.Value(tag="mask", image=Summary.Image(encoded_image_string=png())),
        )
    )
    second = record(
        event(
            2,
            Summary.Value(tag="loss", tensor=TensorProto(dtype=1, float_val=[1])),
            Summary.Value(
                tag="mask",
                tensor=TensorProto(dtype=7, string_val=[b"800", b"600", png("blue"), png("green")]),
            ),
        )
    )
    path.write_bytes(first + second[:20])
    preview = read_previews(root, job, manifest)
    assert preview["scalar"] == {"tag": "loss", "points": [[1, 2.0]]}
    assert preview["image"]["step"] == 1
    with Image.open(io.BytesIO(base64.b64decode(preview["image"]["src"].split(",")[1]))) as image:
        assert image.size == (640, 480)
    with path.open("ab") as stream:
        stream.write(second[20:])
    preview = read_previews(root, job, manifest)
    assert preview["scalar"]["points"] == [[1, 2.0], [2, 1.0]]
    assert preview["image"]["step"] == 2
    assert not preview["image_pending"]
    assert read_previews(root, job, manifest) == preview
    _CACHE.clear()  # restart: reconstruct from the retained event file
    assert read_previews(root, job, manifest) == preview
    assert "private" not in json.dumps(preview)


def test_research_request_tensorboard_selection_reads_from_attempt_root(events):
    root, job, _, path = events
    path.write_bytes(record(event(1, Summary.Value(tag="loss", simple_value=2))))
    manifest = {
        "research_request": {
            "tensorboard": {
                "log_dir": "tensorboard/fit",
                "scalar_tag": "loss",
                "image_tag": None,
            }
        }
    }
    preview = read_previews(root, job, manifest)
    assert preview["scalar"] == {"tag": "loss", "points": [[1, 2.0]]}
    assert preview["image_pending"] is False


def test_bad_records_symlinks_and_old_attempts_are_not_published(events, tmp_path):
    root, job, manifest, path = events
    path.write_bytes(
        record(event(1, Summary.Value(tag="loss", simple_value=8), wall_time=1))
        + record(event(2, Summary.Value(tag="loss", simple_value=float("nan"))))
        + b"broken record"
    )
    assert read_previews(root, job, manifest)["scalar"]["points"] == []
    path.unlink()
    outside = tmp_path / "secret"
    outside.write_bytes(record(event(3, Summary.Value(tag="loss", simple_value=99))))
    path.symlink_to(outside)
    assert read_previews(root, job, manifest)["scalar"]["points"] == []
    assert read_previews(root, job, {"plan": {"stages": []}}) is None


def test_curve_is_bounded_and_keeps_endpoints(events):
    root, job, manifest, path = events
    path.write_bytes(
        b"".join(record(event(i, Summary.Value(tag="loss", simple_value=i))) for i in range(1, 3000))
    )
    preview = read_previews(root, job, manifest)
    assert len(preview["scalar"]["points"]) <= 160
    assert preview["scalar"]["points"][0] == [1, 1]
    assert preview["scalar"]["points"][-1] == [2999, 2999]


def test_curve_preserves_history_as_events_arrive(events):
    root, job, manifest, path = events
    with path.open("wb") as stream:
        for start in range(0, 16000, 1000):
            stream.write(
                b"".join(
                    record(event(i, Summary.Value(tag="loss", simple_value=i)))
                    for i in range(start, start + 1000)
                )
            )
            stream.flush()
            preview = read_previews(root, job, manifest)
    points = preview["scalar"]["points"]
    assert len(points) <= 160
    assert points[0] == [0, 0]
    assert points[-1] == [15999, 15999]
    assert all(
        sum(start <= step < start + 2000 for step, _ in points) >= 10 for start in range(0, 16000, 2000)
    )
    _CACHE.clear()
    assert read_previews(root, job, manifest)["scalar"]["points"] == points


@pytest.mark.parametrize(
    "directory", ["../private", "/tmp/events", "tensorboard/../secret", "tensorboard/fit/subdir"]
)
def test_event_directory_is_confined(directory):
    with pytest.raises(ValueError):
        TensorBoardDisplay(log_dir=directory, scalar_tag="loss")


def test_public_preview_budget_prefers_active_jobs_without_mutating_saved_data():
    from experimenttracker.store import public_projection

    jobs = [
        {"state": "complete", "created_at": 1, "tensorboard": {"image": "x" * 200_000}},
        {"state": "submitted", "created_at": 2, "tensorboard": {"image": "y" * 200_000}},
    ]
    view = public_projection({"contract": {}, "jobs": jobs})
    assert view["jobs"][0]["tensorboard"] is None
    assert view["jobs"][1]["tensorboard"] == jobs[1]["tensorboard"]
    assert jobs[0]["tensorboard"] is not None
