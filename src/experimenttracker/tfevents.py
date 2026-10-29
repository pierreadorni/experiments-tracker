"""Small, best-effort TensorBoard previews; never evidence or scheduler authority."""

import base64
import hashlib
import heapq
import io
import math
import os
import stat
import struct
from collections import OrderedDict

from google.protobuf.message import DecodeError
from PIL import Image
from tensorboard.compat.proto.event_pb2 import Event
from tensorboard.compat.tensorflow_stub.pywrap_tensorflow import masked_crc32c
from tensorboard.util.tensor_util import make_ndarray

MAX_READ = 64 * 1024 * 1024
MAX_RECORD = 8 * 1024 * 1024
MAX_POINTS = 160
# Incremental offsets and small previews only, scoped to the exact saved submission.
_CACHE = OrderedDict()


def selection(manifest, stage_id):
    return next(
        (
            s.get("tensorboard")
            for s in (manifest or {}).get("plan", {}).get("stages", [])
            if s["id"] == stage_id
        ),
        None,
    )


def thin(points, limit=MAX_POINTS):
    if len(points) <= limit:
        return points
    return [points[round(i * (len(points) - 1) / (limit - 1))] for i in range(limit)]


def sample_scalar(state, step, value):
    """Keep a deterministic, bounded sample without favoring recent events."""
    points = state["points"]
    if state["first"] is None or step <= state["first"][0]:
        state["first"] = (step, value)
    if state["last"] is None or step >= state["last"][0]:
        state["last"] = (step, value)
    if step in points:
        points[step] = value
        return
    priority = int.from_bytes(hashlib.blake2b(str(step).encode(), digest_size=8).digest(), "big")
    item = (-priority, -step)
    heap = state["heap"]
    if len(heap) < 2048:
        heapq.heappush(heap, item)
    elif item > heap[0]:
        _, removed = heapq.heapreplace(heap, item)
        del points[-removed]
    else:
        return
    points[step] = value


def image_preview(encoded):
    with Image.open(io.BytesIO(encoded)) as image:
        if image.width * image.height > 16_000_000:
            raise ValueError("image too large")
        image.thumbnail((640, 480))
        output = io.BytesIO()
        image.convert("RGB").save(output, format="JPEG", quality=75)
    if output.tell() > 160_000:
        raise ValueError("preview too large")
    return "data:image/jpeg;base64," + base64.b64encode(output.getvalue()).decode("ascii")


def read_previews(root, job, manifest):
    config = selection(manifest, job["stage_id"])
    if not config or not job.get("job_id"):
        return None
    key = (str(root), job["intent_id"], job["job_id"], tuple(sorted(config.items())))
    state = _CACHE.setdefault(
        key, {"files": {}, "points": {}, "heap": [], "first": None, "last": None, "image": None, "updated": 0}
    )
    _CACHE.move_to_end(key)
    while len(_CACHE) > 32:
        _CACHE.popitem(last=False)
    result = {
        "scalar": None,
        "image": None,
        "image_pending": bool(config.get("image_tag")),
        "updated_at": state["updated"],
        "limited": False,
    }
    directory = root / config["log_dir"]
    try:
        if directory.is_symlink() or not directory.resolve().is_relative_to(root.resolve()):
            return None
        # A single SummaryWriter directory, without recursive traversal or symlink following.
        paths = []
        with os.scandir(directory) as entries:
            for entry in entries:
                if entry.name.startswith("events.out.tfevents.") and entry.is_file(follow_symlinks=False):
                    paths.append(directory / entry.name)
                if len(paths) >= 64:
                    result["limited"] = True
                    break
        budget = MAX_READ
        for path in sorted(paths):
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(fd, "rb") as stream:
                meta = os.fstat(stream.fileno())
                if not stat.S_ISREG(meta.st_mode):
                    continue
                identity = (meta.st_dev, meta.st_ino)
                saved = state["files"].get(path.name)
                offset = saved[1] if saved and saved[0] == identity and meta.st_size >= saved[1] else 0
                stream.seek(offset)
                while budget > 12:
                    header = stream.read(12)
                    if len(header) < 12:
                        break
                    size, checksum = struct.unpack("<QI", header)
                    if checksum != masked_crc32c(header[:8]) or size > MAX_RECORD:
                        result["limited"] = True
                        break
                    if size + 16 > budget:
                        result["limited"] = True
                        break
                    payload, footer = stream.read(size), stream.read(4)
                    if len(payload) != size or len(footer) != 4:
                        break  # writer has not flushed this record yet
                    if struct.unpack("<I", footer)[0] != masked_crc32c(payload):
                        break
                    budget -= size + 16
                    event = Event.FromString(payload)
                    offset = stream.tell()
                    # Avoid including earlier attempts if a writer reused its directory.
                    if job.get("started_at") and event.wall_time < job["started_at"] - 60:
                        continue
                    if not math.isfinite(event.wall_time) or event.step < 0:
                        continue
                    for value in event.summary.value:
                        try:
                            if value.tag == config.get("scalar_tag"):
                                number = None
                                if value.HasField("simple_value"):
                                    number = float(value.simple_value)
                                elif value.HasField("tensor"):
                                    tensor = value.tensor
                                    # Only a true numeric scalar; never allocate a declared huge tensor.
                                    if not tensor.tensor_shape.dim and len(tensor.tensor_content) <= 8:
                                        number = float(make_ndarray(tensor).item())
                                if number is not None and math.isfinite(number):
                                    sample_scalar(state, event.step, number)
                                    state["updated"] = max(state["updated"], event.wall_time)
                            if value.tag == config.get("image_tag"):
                                encoded = (
                                    value.image.encoded_image_string if value.HasField("image") else None
                                )
                                if value.HasField("tensor") and value.tensor.dtype == 7:
                                    # TensorBoard images plugin: width, height, encoded examples.
                                    strings = value.tensor.string_val
                                    encoded = strings[2] if len(strings) > 2 else None
                                prior = state["image"]
                                if encoded and (not prior or event.wall_time >= prior["wall_time"]):
                                    state["image"] = {
                                        "tag": value.tag,
                                        "step": event.step,
                                        "wall_time": event.wall_time,
                                        "src": image_preview(encoded),
                                    }
                                    state["updated"] = max(state["updated"], event.wall_time)
                        except (
                            ValueError,
                            TypeError,
                            OSError,
                            OverflowError,
                            KeyError,
                            Image.DecompressionBombError,
                        ):
                            continue
                state["files"][path.name] = (identity, offset)
                if offset < meta.st_size and budget <= MAX_RECORD + 16:
                    result["limited"] = True
    except (OSError, ValueError, DecodeError, Image.DecompressionBombError):
        pass  # optional telemetry cannot fail a scientific job
    if config.get("scalar_tag"):
        points = dict(state["points"])
        for endpoint in (state["first"], state["last"]):
            if endpoint is not None:
                points[endpoint[0]] = endpoint[1]
        result["scalar"] = {
            "tag": config["scalar_tag"],
            "points": [list(point) for point in thin(sorted(points.items()))],
        }
    result["image"] = state["image"]
    result["image_pending"] = bool(config.get("image_tag")) and not state["image"]
    result["updated_at"] = state["updated"]
    return result
