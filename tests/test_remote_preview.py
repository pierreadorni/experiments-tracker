import base64
import json
import os
import subprocess
import sys

import pytest

from experimenttracker.remote_preview import REMOTE_EVENT_READER


def invoke_reader(tmp_path, run, log="logs"):
    return subprocess.run(
        [sys.executable, "-c", REMOTE_EVENT_READER, str(run), log],
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )


def test_remote_event_reader_returns_only_bounded_prefix(tmp_path):
    run = tmp_path / "run"
    log = run / "logs"
    log.mkdir(parents=True)
    (log / "events.out.tfevents.big").write_bytes(b"x" * (2 * 1024 * 1024 + 20))

    result = invoke_reader(tmp_path, run)

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert set(payload) == {"data"}
    assert base64.b64decode(payload["data"]) == b"x" * (2 * 1024 * 1024)


@pytest.mark.parametrize("kind", ["symlink", "fifo"])
def test_remote_event_reader_refuses_symlink_and_fifo(tmp_path, kind):
    run = tmp_path / "run"
    log = run / "logs"
    log.mkdir(parents=True)
    entry = log / "events.out.tfevents.unsafe"
    if kind == "symlink":
        target = tmp_path / "target"
        target.write_text("not an event")
        entry.symlink_to(target)
    else:
        os.mkfifo(entry)

    result = invoke_reader(tmp_path, run)

    assert result.returncode != 0
    assert not result.stdout
