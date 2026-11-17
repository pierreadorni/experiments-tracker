"""Map saved report versions to backend publication responsibility episodes.

Call ``sync_report`` inside the transaction that changes a report version. It
tracks saved readiness only; artifact delivery keeps its own saved identity.
"""

import json

from . import obligation_sources

_SOURCE = "report-version"
_LIVE_STATES = frozenset({"queued", "building", "publishing"})
_SATISFIED_STATES = frozenset({"ready"})
_ABANDONED_STATES = frozenset({"failed"})


def sync_report(db, experiment_id, version):
    """Synchronize one saved report version inside its caller's transaction.

    Queued, building and publishing are progress of one publication
    responsibility; ready satisfies it, and failure or cancellation abandons it.
    A ready or failed version with no history stays without history.
    """
    if not db.in_transaction:
        raise RuntimeError("report obligation sync requires the caller's active transaction")
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise ValueError("report version must be a positive integer")
    saved = db.execute(
        "SELECT state,request_id FROM report_versions WHERE experiment_id=? AND version=?",
        (experiment_id, version),
    ).fetchone()
    if saved is None:
        raise ValueError(f"saved report version does not exist: {experiment_id}:{version}")
    experiment = db.execute("SELECT contract FROM experiments WHERE id=?", (experiment_id,)).fetchone()
    if experiment is None:
        raise ValueError(f"saved report experiment does not exist: {experiment_id}")
    project_id = (json.loads(experiment["contract"]).get("project") or {}).get("id")
    return obligation_sources.sync_episode(
        db,
        _SOURCE,
        f"{experiment_id}:{version}",
        state=saved["state"],
        live_states=_LIVE_STATES,
        satisfied_states=_SATISFIED_STATES,
        abandoned_states=_ABANDONED_STATES,
        experiment_id=experiment_id,
        project_id=project_id,
        kind="publication",
        owner="backend",
        wait=f"report:{experiment_id}:{version}",
        detail=f"request={saved['request_id']}",
    )
