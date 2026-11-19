"""Engine wall-clock time comes from the clock seam; this test replaces that seam."""

import time

from helpers import seed_saved_contract3

from experimenttracker import clock


def test_now_and_monotonic_read_the_standard_clocks():
    assert time.time() <= clock.now() <= time.time()
    started = clock.monotonic()
    assert clock.monotonic() >= started


def test_patching_now_controls_stored_timestamps(runtime, monkeypatch):
    store = runtime[0]
    eid = seed_saved_contract3(store, "Synthetic clock test")
    monkeypatch.setattr(clock, "now", lambda: 1234.0)
    store.notify(eid, "required_decision", "synthetic")
    rows = store.rows("SELECT created_at FROM inbox WHERE experiment_id=?", (eid,))
    assert rows == [{"created_at": 1234.0}]
