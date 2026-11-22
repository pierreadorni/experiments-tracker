"""Typed relay projection documents, built once for hosted publication and local reads.

Keys: ``experiment:<uuid>``, ``projects``, ``status`` and ``inbox``. An experiment
document keeps ``experiments.revision``; aggregate documents take a time-based
revision that the publisher raises above the last acknowledged one. Documents carry
no executor-local paths.
"""

import hashlib
import json
import math
import re
from decimal import Decimal
from uuid import UUID

from .. import clock
from ..store import public_projection
from .notifications import experiment_notification

# Executor-local locations that private rows or catalogs may hold.
LOCAL_FIELDS = frozenset({"path", "folder", "checkout", "state_dir", "job_output_root"})


def scrub(value):
    if isinstance(value, dict):
        return {k: scrub(v) for k, v in value.items() if k not in LOCAL_FIELDS}
    if isinstance(value, list):
        return [scrub(v) for v in value]
    return value


def digest(document):
    return hashlib.sha256(json.dumps(document, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


_INDEX = re.compile(r"(0|[1-9][0-9]{0,9})\Z")
_ABSOLUTE = re.compile(r"(?<![\w:/.@+-])/(?:[\w.@+-]+/)+[\w.@+-]*")
TOMBSTONE_DIGEST = hashlib.sha256(b"\x00tombstone").hexdigest()


def _number(x):
    if isinstance(x, int):
        return str(x)
    if not math.isfinite(x):
        raise ValueError("a published document cannot hold a non-finite number")
    if x == 0:
        return "0"
    # JavaScript's Number::toString from Python's shortest repr digits.
    t = Decimal(repr(abs(x))).as_tuple()
    n = len(t.digits) + t.exponent
    digits = "".join(map(str, t.digits)).rstrip("0")
    k = len(digits)
    if k <= n <= 21:
        text = digits + "0" * (n - k)
    elif 0 < n <= 21:
        text = digits[:n] + "." + digits[n:]
    elif -6 < n <= 0:
        text = "0." + "0" * -n + digits
    else:
        text = digits[0] + ("." + digits[1:] if k > 1 else "") + f"e{'+' if n > 0 else '-'}{abs(n - 1)}"
    return ("-" if x < 0 else "") + text


def wire(value):
    """Deterministic JSON that ``JSON.stringify(JSON.parse(wire))`` reproduces byte for byte.

    The Worker digests the parsed document, so key order must already be the order
    JavaScript uses: array-index keys ascending, then the others (here sorted).
    """
    if value is None or isinstance(value, bool):
        return json.dumps(value)
    if isinstance(value, (int, float)):
        return _number(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(wire(v) for v in value) + "]"
    if isinstance(value, dict):
        keys = sorted(
            value,
            key=lambda k: (not _INDEX.match(k) or int(k) >= 2**32 - 1, int(k) if _INDEX.match(k) else 0, k),
        )
        return "{" + ",".join(f"{json.dumps(k, ensure_ascii=False)}:{wire(value[k])}" for k in keys) + "}"
    raise TypeError(f"cannot publish {type(value).__name__}")


def wire_digest(document):
    """The digest the Worker stores: sha256 of ``JSON.stringify(document)``."""
    return hashlib.sha256(wire(document).encode()).hexdigest()


def scrub_local(value, roots=(), *, paths=False):
    """Replace executor-local roots, and with ``paths`` any absolute path, in every string."""
    roots = sorted({r for r in map(str, roots) if len(r) > 1}, key=len, reverse=True)
    if isinstance(value, dict):
        return {k: scrub_local(v, roots, paths=paths) for k, v in value.items()}
    if isinstance(value, list):
        return [scrub_local(v, roots, paths=paths) for v in value]
    if isinstance(value, str):
        for root in roots:
            value = value.replace(root, "<local>")
        return _ABSOLUTE.sub("<local>", value) if paths else value
    return value


def aggregate_revision(last_acked=0):
    return max(int(clock.now() * 1000), last_acked + 1)


def experiment_key(eid):
    return f"experiment:{eid}"


def parse_key(key):
    """Return the key's kind, or None when it is not a published key."""
    if key in ("projects", "status", "inbox"):
        return key
    kind, _, rest = key.partition(":")
    if kind == "experiment":
        try:
            return kind if str(UUID(rest)) == rest else None
        except ValueError:
            return None
    return None


class Projections:
    def __init__(self, store, *, status=None, catalog=None):
        self.store = store
        self.status = status or {}
        self.catalog = catalog or []

    def keys(self):
        live = self.store.rows("SELECT id FROM experiments WHERE deleted_at IS NULL ORDER BY created_at")
        return ["projects", "status", "inbox", *(experiment_key(r["id"]) for r in live)]

    def inbox(self):
        entries = self.store.rows(
            "SELECT i.* FROM inbox i JOIN experiments e ON e.id=i.experiment_id "
            "WHERE e.deleted_at IS NULL ORDER BY i.created_at DESC"
        ) + self.store.rows(
            "SELECT n.id,'' AS experiment_id,r.project_id,n.kind,n.text,n.created_at FROM autopilot_notices n "
            "JOIN autopilot_runs r ON r.id=n.run_id WHERE n.resolved_at IS NULL "
            "ORDER BY n.created_at DESC LIMIT 100"
        )
        return {"entries": entries}

    def experiment(self, eid):
        row = self.store.one("SELECT deleted_at,updated_at FROM experiments WHERE id=?", (eid,))
        if not row or row["deleted_at"]:
            return None
        view = public_projection(self.store.projection(eid))
        view["inbox"] = self.store.rows("SELECT * FROM inbox WHERE experiment_id=?", (eid,))
        return view, view["revision"], int(row["updated_at"] * 1000)

    def build(self, key, *, last_acked=0):
        """Return ``{key, revision, updated_at, document}``, or None for an absent or deleted key."""
        kind = parse_key(key)
        if kind == "experiment":
            built = self.experiment(key.partition(":")[2])
            if not built:
                return None
            document, revision, updated_at = built
        elif kind:
            document = {
                "projects": lambda: {"projects": self.catalog},
                "status": lambda: self.status,
                "inbox": self.inbox,
            }[kind]()
            revision = aggregate_revision(last_acked)
            updated_at = int(clock.now() * 1000)
        else:
            return None
        return {"key": key, "revision": revision, "updated_at": updated_at, "document": scrub(document)}

    def build_all(self):
        return [built for key in self.keys() if (built := self.build(key))]


def envelope(projection):
    """The publication envelope; the notification belongs to experiment documents."""
    result = {
        "schema_version": 1,
        "key": projection["key"],
        "revision": projection["revision"],
        "document": projection["document"],
    }
    if parse_key(projection["key"]) == "experiment":
        result["notify"] = experiment_notification(projection["document"])
    return result
