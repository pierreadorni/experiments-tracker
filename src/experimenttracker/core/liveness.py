"""Exact C2 liveness: whether each open responsibility still has saved evidence.

Each family is checked against the exact saved source its writer recorded, never
against an age, a hold column or a guessed event: an exact agent turn, the owner
notice the writer already emits, the saved job or transfer row, the saved wakeup
identity and due time, the advancement lifetime's concrete dependency, or a
process-local record of real backend handler returns. Advancement alone reads the
saved experiment revision, because its dependency is the experiment's own progress.

Causes are short and machine-readable: ``source_missing``, ``source_not_live``,
``identity_mismatch``, ``notice_missing``, ``overdue``, ``unserviced``,
``missing_dependency``, ``missing_lifetime`` and ``unsupported_responsibility``. Nothing here writes, notifies,
logs, repairs or dispatches; the caller owns the transaction.
"""

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field

from . import obligation_sources, obligations

_TERMINAL_JOB_STATES = frozenset({"complete", "failed", "cancelled"})
_AGENT_TURN_STATES = frozenset({"dispatching", "running", "uncertain"})
# Every pending-request and plan-review writer emits this one owner notice kind.
_NOTICE_BY_KIND = {"owner_answer": "required_decision", "owner_approval": "required_decision"}
_NOTICE_KINDS = sorted(set(_NOTICE_BY_KIND.values()))
# A saved obligation ID starts with its source type, which names the source family.
_SOURCE_TYPES = frozenset(
    ("advancement", "agent-action", "cancellation", "job", "report-version", "request", "review")
    + ("transfer", "wakeup")
)
# The owner/kind pairs the source mappers save; another pair is reported, not raised.
_RESPONSIBILITIES = frozenset(
    {("agent", "agent_turn"), ("owner", "owner_answer"), ("owner", "owner_approval"), ("external", "job")}
    | {("backend", kind) for kind in ("agent_turn", "job", "transfer", "timer", "publication", "reconcile")}
)
_MISSING_LIFETIMES = (
    "SELECT e.id FROM experiments e WHERE e.deleted_at IS NULL "
    "AND e.phase NOT IN ('complete','failed','cancelled') "
    "AND json_extract(e.contract,'$.execution_contract') IS 3 "
    "AND NOT EXISTS (SELECT 1 FROM obligations o WHERE o.experiment_id=e.id AND o.state='open' "
    "AND substr(o.id,1,12)='advancement:') ORDER BY e.id"
)


@dataclass(frozen=True)
class ServiceEvidence:
    """One process's proof that a backend source family was actually handled.

    ``started_at`` is that process's start; ``serviced_at`` maps a source family
    (the obligation's source type, such as ``agent-action``) to the time a real
    handler returned for it. A family with no record keeps the restart grace of
    one watchdog interval; an obligation age is never service evidence.
    """

    started_at: float
    serviced_at: Mapping[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class LivenessVerdict:
    """One open obligation, or a saved lifetime with no open episode, and its cause."""

    experiment_id: str | None
    obligation_id: str | None
    family: str
    cause: str | None = None


@dataclass(frozen=True)
class LivenessReport:
    """One read pass; ``defects`` are the verdicts that cannot progress."""

    verdicts: tuple[LivenessVerdict, ...] = ()

    @property
    def defects(self):
        return tuple(verdict for verdict in self.verdicts if verdict.cause is not None)


def inspect(db, now, *, interval, service, owner_holds=()):
    """Classify every open obligation and every saved contract-3 lifetime.

    ``interval`` is the existing watchdog interval in seconds; ``service`` is the
    process-local record of real backend handler returns plus the process start;
    ``owner_holds`` are the saved holds whose next step belongs to the owner, so
    their experiment owes no other dependency than that answer.
    """
    obligation_sources.require_transaction(db)
    _check(now, interval, service)
    rows = obligations.open_for(db)
    watched = _Watch(db, rows, now, interval, service, owner_holds)
    verdicts = [LivenessVerdict(r["experiment_id"], r["id"], _family(r), watched.cause(r)) for r in rows]
    verdicts.extend(_missing_lifetimes(db))
    return LivenessReport(tuple(verdicts))


class _Watch:
    """The saved evidence one inspection pass reads once and shares."""

    def __init__(self, db, rows, now, interval, service, owner_holds):
        self.now = now
        self.interval = interval
        self.service = service
        self.owner_holds = owner_holds
        self.saved = _saved_sources(db, rows)
        self.experiments = _experiments(db, rows)
        self.noticed = _notices(db, rows)
        self.open_counts = Counter(row["experiment_id"] for row in rows)

    def cause(self, row):
        """The exact cause this open obligation cannot progress, or ``None`` if it can."""
        responsibility = (row["owner"], row["kind"])
        if responsibility not in _RESPONSIBILITIES:
            return "unsupported_responsibility"
        if row["owner"] == "agent":
            return self._agent_turn(row)
        if row["owner"] == "owner":
            notice = (row["experiment_id"], _NOTICE_BY_KIND[row["kind"]])
            return None if notice in self.noticed else "notice_missing"
        if row["owner"] == "external":
            return self._job(row)
        if row["kind"] == "timer":
            return self._timer(row)
        if row["kind"] == "transfer":
            return self._transfer(row)
        if _family(row) == "advancement":
            return self._advancement(row)
        return self._serviced(row)

    def _agent_turn(self, row):
        action = self.saved["agent_turn"].get(row["target"])
        if action is None:
            return "source_missing"
        if action["state"] not in _AGENT_TURN_STATES:
            return "source_not_live"
        if not action["thread_id"] or not action["turn_id"] or row["wait"] != f"turn:{action['turn_id']}":
            return "identity_mismatch"
        return None

    def _job(self, row):
        job = self.saved["job"].get(row["target"])
        if job is None:
            return "source_missing"
        if job["state"] in _TERMINAL_JOB_STATES:
            return "source_not_live"
        if job["job_id"] is None or row["wait"] != f"job:{job['job_id']}":
            return "identity_mismatch"
        return None

    def _transfer(self, row):
        transfer = self.saved["transfer"].get(row["target"])
        if transfer is None:
            return "source_missing"
        return None if transfer["state"] == "pending" else "source_not_live"

    def _timer(self, row):
        wakeup = self.saved["timer"].get(row["target"])
        if wakeup is None:
            return "source_missing"
        if wakeup["state"] != "pending":
            return "source_not_live"
        if row["due_at"] is None or wakeup["due_at"] != row["due_at"]:
            return "identity_mismatch"
        return "overdue" if self.now - row["due_at"] > self.interval else None

    def _advancement(self, row):
        # The dependency carries its own verdict: a defective source is reported
        # there, and advancement only needs one to delegate to.
        if self.open_counts[row["experiment_id"]] > 1:
            return None
        saved = self.experiments.get(row["experiment_id"])
        if saved is None:
            return "source_missing"
        if saved["hold_reason"] in self.owner_holds:
            return None
        # The saved revision is this experiment's concrete service evidence: an
        # advance pass that really moved it wrote `updated_at` with revision+1.
        return None if self.now - saved["updated_at"] <= self.interval else "missing_dependency"

    def _serviced(self, row):
        last = self.service.serviced_at.get(_family(row), self.service.started_at)
        return None if self.now - last <= self.interval else "unserviced"


def _saved_sources(db, rows):
    """Read the saved source row each open obligation of every kind points at."""
    return {
        "agent_turn": _by_id(db, "agent_actions", "id", rows, "agent_turn"),
        "job": _by_id(db, "jobs", "intent_id", rows, "job"),
        "transfer": _by_id(db, "execution_transfers", "id", rows, "transfer"),
        "timer": _by_id(db, "agent_wakeups", "id", rows, "timer"),
    }


def _by_id(db, table, key, rows, kind):
    ids = sorted({row["target"] for row in rows if row["kind"] == kind and row["target"]})
    if not ids:
        return {}
    sql = f"SELECT * FROM {table} WHERE {key} IN ({','.join('?' * len(ids))})"
    return {saved[key]: dict(saved) for saved in db.execute(sql, ids)}


def _experiments(db, rows):
    """The saved revision time and hold of every experiment an advancement covers."""
    ids = sorted({row["experiment_id"] for row in rows if _family(row) == "advancement"})
    if not ids:
        return {}
    sql = f"SELECT id,hold_reason,updated_at FROM experiments WHERE id IN ({','.join('?' * len(ids))})"
    return {saved["id"]: dict(saved) for saved in db.execute(sql, ids)}


def _notices(db, rows):
    """The existing owner notices of the experiments that owe an owner decision."""
    owed = {row["experiment_id"] for row in rows if row["owner"] == "owner" and row["experiment_id"]}
    if not owed:
        return set()
    sql = (
        "SELECT DISTINCT experiment_id,kind FROM inbox WHERE experiment_id IN "
        f"({','.join('?' * len(owed))}) AND kind IN ({','.join('?' * len(_NOTICE_KINDS))})"
    )
    return {(row["experiment_id"], row["kind"]) for row in db.execute(sql, (*sorted(owed), *_NOTICE_KINDS))}


def _missing_lifetimes(db):
    """Non-terminal contract-3 experiments whose advancement episode is not open."""
    return [
        LivenessVerdict(row["id"], None, "advancement", "missing_lifetime")
        for row in db.execute(_MISSING_LIFETIMES)
    ]


def _family(row):
    """The saved source family: the obligation ID's source type, else its kind."""
    prefix = row["id"].split(":", 1)[0]
    return prefix if prefix in _SOURCE_TYPES else row["kind"]


def _check(now, interval, service):
    numbers = (now, interval)
    bad = any(isinstance(value, bool) or not isinstance(value, (int, float)) for value in numbers)
    if not isinstance(service, ServiceEvidence) or bad or interval < 0:
        raise ValueError("inspect needs ServiceEvidence and a non-negative numeric now and interval")
