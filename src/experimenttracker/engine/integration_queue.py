"""Bounded durable integration queue state (T3.4b-1).

Each caller request is one C2 ``integration`` obligation episode whose JSON
``detail`` saves ``repo``, raw ``request``, raw ``candidate`` (or None), ``stage``
and owner/agent ``status`` text. Candidate identity never comes from status text.

Stages are ``queued``, ``claimed``, ``conflict``, ``check_failed``,
``awaiting_approval``, ``ready`` and terminal ``applied``/``abandoned``. At most
one request per canonical repository is active, claims are FIFO, the three wait
stages cannot be applied, and terminal marking releases the repository. Callers
own the transaction; this module never touches Git, runs a command or emits. The
public API is ``enqueue``, ``pending``, ``active``, ``saved``, ``claim``,
``checkpoint`` and ``finish``.
"""

import json
import os

from ..core import obligation_sources

SOURCE = "integration"
VERSION = 1
MAX_QUEUED = 32
MAX_EPISODES = 16
MAX_REPO = 4096
MAX_REQUEST = 4096
MAX_CANDIDATE = 16384
MAX_STATUS = 512
_TERMINAL = frozenset({"applied", "abandoned"})
_ACTIVE = frozenset({"claimed", "conflict", "check_failed", "awaiting_approval", "ready"})
_RESPONSIBILITY = {
    "queued": ("backend", "integration_queue"),
    "claimed": ("backend", "integration_prepare"),
    "conflict": ("owner", "integration_conflict"),
    "check_failed": ("agent", "integration_check"),
    "awaiting_approval": ("owner", "integration_approval"),
    "ready": ("backend", "integration_apply"),
}
_STAGES = frozenset(_RESPONSIBILITY) | _TERMINAL
_FIELDS = frozenset({"version", "repo", "request", "candidate", "stage", "status"})
_TRANSITIONS = {
    "claimed": frozenset({"conflict", "check_failed", "awaiting_approval", "ready"}),
    "conflict": frozenset({"claimed"}),
    "check_failed": frozenset({"claimed"}),
    "awaiting_approval": frozenset({"ready"}),
    "ready": frozenset({"claimed"}),
}


def canonical_repo(repo):
    """Return the one canonical identity of an absolute repository path."""
    try:
        path = os.fspath(repo)
    except TypeError:
        raise ValueError("repo must be an absolute path") from None
    if not isinstance(path, str) or not os.path.isabs(path):
        raise ValueError("repo must be an absolute path")
    canonical = os.path.realpath(path)
    if len(canonical) > MAX_REPO:
        raise ValueError("repo path is too long for the integration queue")
    return canonical


def enqueue(db, repo, request_id, request, *, experiment_id=None, project_id=None):
    """Queue one request, or return the saved request for this caller ID unchanged.

    A replay with the same ID, repository, request and saved scope is idempotent,
    even after a terminal stage. A different binding raises instead of replacing it.
    """
    obligation_sources.require_transaction(db)
    repo, request_id = canonical_repo(repo), _request_id(request_id)
    request = _object(request, "request", MAX_REQUEST)
    row = obligation_sources.latest_episode(db, SOURCE, request_id)
    if row is not None:
        saved = _view(row)
        if (
            saved["repo"] != repo
            or _canonical(saved["request"]) != _canonical(request)
            or saved["experiment_id"] != experiment_id
            or saved["project_id"] != project_id
        ):
            raise ValueError("this caller ID is already bound to another integration request")
        return saved
    if sum(view["stage"] == "queued" for view in _entries(db, repo)) >= MAX_QUEUED:
        raise ValueError("this repository's integration queue is full")
    owner, prefix = _RESPONSIBILITY["queued"]
    record = _record(repo, request, None, "queued", None)
    wait = f"{prefix}:{request_id}"
    return _view(_open(db, request_id, experiment_id, project_id, owner, wait, _canonical(record)))


def pending(db, repo):
    """Return this repository's queued requests in FIFO order."""
    obligation_sources.require_transaction(db)
    return [view for view in _entries(db, canonical_repo(repo)) if view["stage"] == "queued"]


def active(db, repo):
    """Return this repository's one active request, or ``None``."""
    obligation_sources.require_transaction(db)
    return _active(db, canonical_repo(repo))


def saved(db, request_id):
    """Return the latest saved episode of one caller ID, or ``None``."""
    obligation_sources.require_transaction(db)
    row = obligation_sources.latest_episode(db, SOURCE, _request_id(request_id))
    return None if row is None else _view(row)


def claim(db, repo):
    """Claim this repository's oldest queued request, or return ``None``.

    Nothing is claimed while another request for the same repository is active.
    """
    obligation_sources.require_transaction(db)
    repo = canonical_repo(repo)
    if _active(db, repo) is not None:
        return None
    queued = [view for view in _entries(db, repo) if view["stage"] == "queued"]
    return None if not queued else _move(db, queued[0], "claimed", None, None)


def checkpoint(db, request_id, stage, *, candidate=None, status=None):
    """Save one proven non-terminal stage transition for an active request.

    A replay of the saved stage is idempotent. ``conflict``, ``check_failed`` and
    ``awaiting_approval`` may only return to ``claimed``; approval may also become
    ``ready``. A different candidate needs a real stage change through ``claimed``.
    """
    obligation_sources.require_transaction(db)
    request_id = _request_id(request_id)
    if stage not in _ACTIVE:
        raise ValueError("checkpoint requires a claimed, waiting or ready integration stage")
    view = _latest(db, request_id)
    if view is None:
        raise ValueError(f"unknown integration request: {request_id}")
    if view["state"] != "open":
        raise ValueError("a terminal integration request cannot change stage")
    if stage != view["stage"] and stage not in _TRANSITIONS.get(view["stage"], ()):
        raise ValueError(f"integration request cannot move from {view['stage']} to {stage}")
    _only_active(db, view)
    re_prepare = stage != view["stage"] and "claimed" in (stage, view["stage"])
    return _move(db, view, stage, _candidate(candidate), _status(status), re_prepare=re_prepare)


def finish(db, request_id, stage):
    """Mark one request terminal, releasing its repository; terminal replay is exact.

    Only ``ready`` can be ``applied``. Any non-terminal stage can be ``abandoned``.
    """
    obligation_sources.require_transaction(db)
    request_id = _request_id(request_id)
    if stage not in _TERMINAL:
        raise ValueError("finish requires the applied or abandoned terminal stage")
    view = _latest(db, request_id)
    if view is None:
        raise ValueError(f"unknown integration request: {request_id}")
    if view["state"] != "open":
        if view["stage"] != stage:
            raise ValueError("a terminal integration request keeps its saved outcome")
        return view
    if stage == "applied" and view["stage"] != "ready":
        raise ValueError("conflict, check-failed and approval waits cannot be applied")
    _only_active(db, view)
    record = _record(view["repo"], view["request"], view["candidate"], stage, view["status"])
    scope = (db, request_id, view["experiment_id"], view["project_id"], view["owner"], view["wait"])
    row = _open(*scope, _canonical(record))
    operation = "satisfy" if stage == "applied" else "abandon"
    return _view(obligation_sources.terminalize(db, SOURCE, request_id, row, operation))


def _open(db, request_id, experiment_id, project_id, owner, wait, detail, *, source_transition=False):
    return obligation_sources.open_episode(
        db,
        SOURCE,
        request_id,
        source_transition=source_transition,
        experiment_id=experiment_id,
        project_id=project_id,
        kind="integration",
        target=request_id,
        owner=owner,
        wait=wait,
        detail=detail,
    )


def _move(db, view, stage, candidate, status, *, re_prepare=False):
    saved = view["candidate"]
    if candidate is None:
        candidate = saved
    elif saved is not None and _canonical(candidate) != _canonical(saved) and not re_prepare:
        raise ValueError("the saved integration candidate identity cannot change")
    owner, prefix = _RESPONSIBILITY[stage]
    source_transition = owner != view["owner"]
    episode = int(view["obligation_id"].rsplit(":", 1)[1])
    if source_transition and episode + 1 >= MAX_EPISODES:
        raise ValueError("this integration request exhausted its bounded wait history")
    status = view["status"] if status is None else status
    record = _record(view["repo"], view["request"], candidate, stage, status)
    scope = (db, view["request_id"], view["experiment_id"], view["project_id"], owner)
    row = _open(
        *scope,
        f"{prefix}:{view['request_id']}",
        _canonical(record),
        source_transition=source_transition,
    )
    if row["state"] != "open":
        raise ValueError("an active integration request cannot reopen terminal history")
    return _view(row)


def _active(db, repo):
    rows = [view for view in _entries(db, repo) if view["stage"] in _ACTIVE]
    if len(rows) > 1:
        raise ValueError("more than one integration candidate is active for this repository")
    return rows[0] if rows else None


def _only_active(db, view):
    if view["stage"] in _ACTIVE and any(
        row["stage"] in _ACTIVE and row["request_id"] != view["request_id"]
        for row in _entries(db, view["repo"])
    ):
        raise ValueError("another integration candidate is active for this repository")


def _entries(db, repo):
    """Return this repository's open queue episodes, oldest first.

    Rows with another ID shape are saved generic obligations and stay untouched.
    """
    rows = db.execute(
        "SELECT * FROM obligations WHERE kind='integration' AND state='open' ORDER BY created_at,id"
    ).fetchall()
    views = []
    for row in rows:
        row = dict(row)
        if row["id"].startswith(SOURCE + ":") and row["id"].rsplit(":", 1)[1].isdigit():
            view = _view(row)
            if view["repo"] == repo:
                views.append(view)
    return views


def _latest(db, request_id):
    row = obligation_sources.latest_episode(db, SOURCE, request_id)
    return None if row is None else _view(row)


def _view(row):
    record = _decode(row["detail"])
    return {
        **record,
        "obligation_id": row["id"],
        "request_id": row["id"][len(SOURCE) + 1 :].rsplit(":", 1)[0],
        "experiment_id": row["experiment_id"],
        "project_id": row["project_id"],
        "state": row["state"],
        "owner": row["owner"],
        "wait": row["wait"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _record(repo, request, candidate, stage, status):
    return dict(version=VERSION, repo=repo, request=request, candidate=candidate, stage=stage, status=status)


def _canonical(value):
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError):
        raise ValueError("integration queue data must be finite JSON") from None


def _decode(detail):
    try:
        record = json.loads(detail)
    except (TypeError, json.JSONDecodeError):
        raise ValueError("invalid integration queue detail") from None
    if not isinstance(record, dict) or set(record) != _FIELDS:
        raise ValueError("invalid integration queue detail fields")
    repo, request, candidate, status = (
        record["repo"],
        record["request"],
        record["candidate"],
        record["status"],
    )
    if (
        isinstance(record["version"], bool)
        or record["version"] != VERSION
        or not isinstance(repo, str)
        or not os.path.isabs(repo)
        or os.path.normpath(repo) != repo
        or len(repo) > MAX_REPO
        or not isinstance(request, dict)
        or len(_canonical(request)) > MAX_REQUEST
        or (candidate is not None and not isinstance(candidate, dict))
        or (candidate is not None and len(_canonical(candidate)) > MAX_CANDIDATE)
        or record["stage"] not in _STAGES
        or (status is not None and not isinstance(status, str))
        or (status is not None and len(status) > MAX_STATUS)
    ):
        raise ValueError("invalid integration queue detail")
    return record


def _object(value, name, limit):
    if not isinstance(value, dict) or len(_canonical(value)) > limit:
        raise ValueError(f"{name} must be a bounded JSON object")
    return value


def _candidate(value):
    return None if value is None else _object(value, "candidate", MAX_CANDIDATE)


def _request_id(value):
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 200
        or ":" in value
        or value != value.strip()
        or any(ord(char) < 32 for char in value)
    ):
        raise ValueError("request_id must be one bounded caller identity without ':'")
    return value


def _status(value):
    if value is not None and (not isinstance(value, str) or len(value) > MAX_STATUS):
        raise ValueError("status must be bounded text or None")
    return value
