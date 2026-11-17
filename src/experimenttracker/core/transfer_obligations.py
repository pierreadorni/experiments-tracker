"""Map saved execution transfers to backend responsibility episodes."""

import json

from . import obligation_sources

_SOURCE = "transfer"


def sync_transfer(db, identity, *, before=None):
    """Synchronize one transfer row inside its caller's transaction.

    Only a saved complete-to-pending replay starts a new responsibility episode; a
    pending retry reuses its open episode and age. A saved terminal row settles only
    an already-open episode, and a terminal row with no history stays without history.
    This mapper records ownership and exact outcomes; it never performs transport.
    """
    if not db.in_transaction:
        raise RuntimeError("transfer obligation sync requires the caller's active transaction")
    if not isinstance(identity, str) or not identity:
        raise ValueError("transfer identity must be a non-empty string")
    saved = db.execute("SELECT * FROM execution_transfers WHERE id=?", (identity,)).fetchone()
    if saved is None:
        raise ValueError(f"saved transfer does not exist: {identity}")
    transfer = dict(saved)
    previous = None if before is None else dict(before)
    if transfer["id"] != identity:
        raise ValueError("saved transfer identity changed")
    if previous is not None and any(
        previous.get(field) != transfer[field] for field in ("id", "experiment_id", "direction", "payload")
    ):
        raise ValueError("saved transfer scope or immutable payload changed")
    experiment = db.execute(
        "SELECT contract FROM experiments WHERE id=?", (transfer["experiment_id"],)
    ).fetchone()
    if experiment is None:
        raise ValueError(f"saved transfer experiment does not exist: {transfer['experiment_id']}")
    contract = json.loads(experiment["contract"])
    project_id = (contract.get("project") or {}).get("id")
    detail = f"direction={transfer['direction']};payload={transfer['payload']}"
    latest = obligation_sources.latest_episode(db, _SOURCE, identity)
    _check_history(db, identity, transfer, project_id, detail)

    state = transfer["state"]
    if state == "pending":
        source_transition = bool(previous is not None and previous["state"] == "complete")
        obligation = obligation_sources.open_episode(
            db,
            _SOURCE,
            identity,
            source_transition=source_transition,
            experiment_id=transfer["experiment_id"],
            project_id=project_id,
            kind="transfer",
            target=identity,
            owner="backend",
            wait=f"transfer:{identity}",
            detail=detail,
        )
        if obligation["state"] != "open":
            raise ValueError("pending saved transfer conflicts with terminal obligation history")
        return obligation

    if state not in {"complete", "failed", "cancelled"}:
        raise ValueError(f"unsupported saved transfer state: {state}")
    if previous is not None and previous["state"] != "pending":
        return latest
    if latest is None or latest["state"] != "open":
        return latest
    episode = int(latest["id"].rsplit(":", 1)[1])
    terminalize = (
        obligation_sources.satisfy_episode if state == "complete" else obligation_sources.abandon_episode
    )
    return terminalize(db, _SOURCE, identity, episode)


def _check_history(db, identity, transfer, project_id, detail):
    prefix = f"{_SOURCE}:{identity}:"
    source_key = f"{_SOURCE}:{identity}"
    rows = [
        dict(row)
        for row in db.execute(
            "SELECT * FROM obligations WHERE substr(id,1,?)=? ORDER BY id", (len(prefix), prefix)
        )
        if row["id"].rsplit(":", 1)[0] == source_key
    ]
    for row in rows:
        if (
            row["experiment_id"] != transfer["experiment_id"]
            or row["project_id"] != project_id
            or row["kind"] != "transfer"
            or row["target"] != identity
            or row["owner"] != "backend"
            or row["wait"] != f"transfer:{identity}"
            or row["detail"] != detail
        ):
            raise ValueError("saved transfer obligation identity or responsibility conflicts")
