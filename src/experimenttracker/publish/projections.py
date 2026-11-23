"""Typed relay projection documents, built once for hosted publication and local reads.

Keys: ``experiment:<uuid>``, ``conversation:<uuid>:<seq>`` (one message each),
``projects``, ``status``, ``inbox``, ``knowledge:<pid>``, ``knowledge-report:<pid>:<v>``,
``autopilot:<pid>`` and ``maintenance``. An experiment document keeps
``experiments.revision``, knowledge its own revision, a conversation message its last
update, and a report version is immutable. Aggregate documents take a time-based
revision that the publisher raises above the last acknowledged one. Documents carry
no executor-local paths.
"""

import hashlib
import io
import json
import math
import re
import zipfile
from decimal import Decimal
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

from .. import clock
from ..knowledge import Knowledge
from ..store import public_projection
from .notifications import autopilot_notification, experiment_notification

BLOB_MAX = 10 * 1024 * 1024  # the Worker's relay blob cap
_FILE_SHA = {}  # (path, size, mtime_ns) -> sha256, so an unchanged paper is hashed once


def _blob_id(kind, *parts):
    return str(uuid5(NAMESPACE_URL, ":".join(("autopilot", kind, *map(str, parts)))))


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


def local_roots(config):
    """Executor-local directories that must not appear in published documents."""
    roots = [config.state_dir, config.job_output_root, config.backup_dir, config.web_dir]
    roots += [config.projects_root, config.api_token_file]
    return [r for r in (*roots, *(p.checkout for p in config.projects if p.checkout)) if r]


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


# Documents whose revision is the clock; the publisher sends them only when the digest changes.
AGGREGATES = ("projects", "status", "inbox", "maintenance", "autopilot")
_PROJECT = r"[A-Za-z0-9_-]{1,100}"
_KEYS = {
    "knowledge": re.compile(_PROJECT + r"\Z"),
    "autopilot": re.compile(_PROJECT + r"\Z"),
    "knowledge-report": re.compile(_PROJECT + r":\d+\Z"),
    "conversation": re.compile(r"[0-9a-f-]{36}:\d{10}\Z"),
}


def parse_key(key):
    """Return the key's kind, or None when it is not a published key."""
    if key in ("projects", "status", "inbox", "maintenance"):
        return key
    kind, _, rest = key.partition(":")
    if kind == "experiment" or kind == "conversation":
        try:
            if str(UUID(rest[:36])) != rest[:36]:
                return None
        except ValueError:
            return None
        if kind == "experiment":
            return kind if len(rest) == 36 else None
    return kind if kind in _KEYS and _KEYS[kind].match(rest) else None


def conversation_key(eid, seq):
    return f"conversation:{eid}:{seq:010d}"


class Projections:
    """``memory`` and ``autopilot`` are factories for the services the old routes read;
    ``maintenance`` returns the status dict, or None where the host has no updater."""

    def __init__(self, store, *, status=None, catalog=None, memory=None, autopilot=None, maintenance=None):
        self.store = store
        self.status = status or {}
        self.catalog = catalog or []
        self.memory = memory or (lambda: Knowledge(store))
        self.autopilot = autopilot
        self.maintenance = maintenance

    def keys(self, prefix=""):
        live = self.store.rows("SELECT id FROM experiments WHERE deleted_at IS NULL ORDER BY created_at")
        keys = ["projects", "status", "inbox", *(experiment_key(r["id"]) for r in live)]
        if self.maintenance and self.maintenance():
            keys.append("maintenance")
        keys += self.autopilot_keys()
        keys += self.revisions()
        return [k for k in keys if k.startswith(prefix)]

    def autopilot_keys(self):
        if not self.autopilot:
            return []
        runs = self.store.rows("SELECT DISTINCT project_id FROM autopilot_runs ORDER BY project_id")
        return [f"autopilot:{r['project_id']}" for r in runs if r["project_id"] in self.store.projects]

    def revisions(self):
        """``{key: revision}`` for conversation messages, knowledge and its report versions."""
        found = {}
        for r in self.store.rows(self._CONVERSATION + " ORDER BY d.experiment_id,d.seq"):
            found[conversation_key(r["experiment_id"], r["seq"])] = int(r["changed"] * 1000)
        state = {r["project_id"]: r["revision"] for r in self.store.rows("SELECT * FROM project_knowledge")}
        for pid in self.store.projects:
            found[f"knowledge:{pid}"] = state.get(pid, 0)
        for r in self.store.rows(
            "SELECT project_id,version FROM project_reports ORDER BY project_id,version"
        ):
            if r["project_id"] in self.store.projects:
                found[f"knowledge-report:{r['project_id']}:{r['version']}"] = 1
        return found

    # A message changes when its row or one of its activity lines does.
    _CONVERSATION = (
        "SELECT d.experiment_id,d.seq,MAX(d.updated_at,COALESCE((SELECT MAX(s.created_at) "
        "FROM stage_activity s WHERE s.action_id=d.action_id),0)) AS changed "
        "FROM discussions d JOIN experiments e ON e.id=d.experiment_id WHERE e.deleted_at IS NULL"
    )

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

    def conversation(self, key):
        eid, _, seq = key.partition(":")[2].partition(":")
        row = self.store.one(self._CONVERSATION + " AND d.experiment_id=? AND d.seq=?", (eid, int(seq)))
        if not row:
            return None
        # The old paged route returns the 20 messages below a seq; ours is the newest of them.
        message = self.store.conversation(eid, int(seq) + 1)[-1]
        return {"experiment_id": eid, **message}, int(row["changed"] * 1000)

    def knowledge(self, key):
        kind, _, rest = key.partition(":")
        pid, _, version = rest.partition(":")
        if pid not in self.store.projects:
            return None
        memory = self.memory()
        if kind == "knowledge":
            view = memory.view(pid)
            return view, view["revision"]
        report = memory.report(pid, int(version))
        return ({"schema_version": 1, "report": report}, 1) if report else None

    def autopilot_view(self, pid):
        if not self.autopilot or pid not in self.store.projects:
            return None
        if not self.store.one("SELECT 1 FROM autopilot_runs WHERE project_id=?", (pid,)):
            return None
        view = self.autopilot().view(pid)
        if view.get("run"):
            view = {**view, "downloads": [self._public_item(i) for i in self.autopilot_items(pid).values()]}
        return view

    @staticmethod
    def _public_item(item):
        return {k: v for k, v in item.items() if k != "read"}

    def autopilot_items(self, pid):
        """Downloadable bytes of the current run, by stable blob id: each latest working document,
        the newest compiled paper and the handoff archive. ``read`` returns the bytes."""
        run = self.store.one(
            "SELECT * FROM autopilot_runs WHERE project_id=? ORDER BY created_at DESC,rowid DESC LIMIT 1",
            (pid,),
        )
        if not run:
            return {}
        items = {}

        def add(blob_id, kind, name, size, sha, media_type, read, **extra):
            ok = sha is not None and size <= BLOB_MAX
            items[blob_id] = {
                "blob_id": blob_id,
                "kind": kind,
                "name": name,
                "size": size,
                "sha256": sha if ok else None,
                "media_type": media_type,
                "available": ok,
                **extra,
                "read": read,
            }

        documents = self.store.rows(
            "SELECT name,MAX(version) AS version FROM autopilot_documents WHERE run_id=? GROUP BY name ORDER BY name",
            (run["id"],),
        )
        contents = {}
        for doc in documents:
            row = self.store.one(
                "SELECT content FROM autopilot_documents WHERE run_id=? AND name=? AND version=?",
                (run["id"], doc["name"], doc["version"]),
            )
            data = row["content"].encode()
            contents[(doc["name"], doc["version"])] = data
            add(
                _blob_id("document", run["id"], doc["name"], doc["version"]),
                "document",
                doc["name"],
                len(data),
                hashlib.sha256(data).hexdigest(),
                "text/plain; charset=utf-8",
                lambda data=data: data,
                version=doc["version"],
            )
        paper = self.store.one(
            "SELECT * FROM autopilot_files WHERE run_id=? ORDER BY created_at DESC,rowid DESC LIMIT 1",
            (run["id"],),
        )
        if paper:
            path = Path(paper["path"])
            size = sha = None
            try:
                if path.is_file() and not path.is_symlink():
                    stat = path.stat()
                    size = stat.st_size
                    if size == paper["size"] and size <= BLOB_MAX:
                        mark = (str(path), size, stat.st_mtime_ns)
                        if mark not in _FILE_SHA:
                            _FILE_SHA[mark] = hashlib.sha256(path.read_bytes()).hexdigest()
                        sha = _FILE_SHA[mark]
            except OSError:
                sha = None
            add(
                _blob_id("file", paper["id"]),
                "file",
                "paper.pdf",
                size if size is not None else paper["size"],
                sha,
                "application/pdf",
                lambda path=path: path.read_bytes(),
            )
        handoff = json.loads(run["handoff"]) if run["handoff"] else {}
        included = handoff.get("documents") or documents
        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as bundle:

            def put(name, content):  # fixed timestamps: the same state gives the same bytes
                bundle.writestr(zipfile.ZipInfo(name, (1980, 1, 1, 0, 0, 0)), content, zipfile.ZIP_DEFLATED)

            put(
                "handoff.json",
                run["handoff"] or '{"incomplete":true,"summary":"Working draft; no handoff judgment yet"}',
            )
            for doc in included:
                data = contents.get((doc["name"], doc["version"]))
                if data is None:
                    row = self.store.one(
                        "SELECT content FROM autopilot_documents WHERE run_id=? AND name=? AND version=?",
                        (run["id"], doc["name"], doc["version"]),
                    )
                    data = row["content"].encode() if row else b""
                put(doc["name"], data)
            put(
                "BUILD.txt",
                "Working source draft. Compile paper.tex with pdflatex -no-shell-escape.\n"
                "Missing results remain TODOs; researcher review is required.\n",
            )
        data = archive.getvalue()
        sha = hashlib.sha256(data).hexdigest()
        add(
            _blob_id("handoff", run["id"], sha),
            "handoff",
            "autopilot-handoff.zip",
            len(data),
            sha,
            "application/zip",
            lambda data=data: data,
        )
        return items

    def autopilot_blob(self, blob_id):
        """The available download item with this id, or None; for the local ``/api/blobs`` alias."""
        for key in self.autopilot_keys():
            item = self.autopilot_items(key.partition(":")[2]).get(blob_id)
            if item:
                return item if item["available"] else None
        return None

    def build(self, key, *, last_acked=0):
        """Return ``{key, revision, updated_at, document}``, or None for an absent or deleted key."""
        kind = parse_key(key)
        now = int(clock.now() * 1000)
        if kind == "experiment":
            built = self.experiment(key.partition(":")[2])
            if not built:
                return None
            document, revision, updated_at = built
        elif kind in ("conversation", "knowledge", "knowledge-report"):
            built = self.conversation(key) if kind == "conversation" else self.knowledge(key)
            if not built:
                return None
            document, revision = built
            updated_at = revision if kind == "conversation" else now
        elif kind in AGGREGATES:
            if kind == "autopilot":
                document = self.autopilot_view(key.partition(":")[2])
            elif kind == "maintenance":
                document = self.maintenance() if self.maintenance else None
            else:
                document = {
                    "projects": lambda: {"projects": self.catalog},
                    "status": lambda: self.status,
                    "inbox": self.inbox,
                }[kind]()
            if document is None:
                return None
            revision, updated_at = aggregate_revision(last_acked), now
        else:
            return None
        return {"key": key, "revision": revision, "updated_at": updated_at, "document": scrub(document)}

    def build_all(self):
        return [built for key in self.keys() if (built := self.build(key))]


def envelope(projection):
    """The publication envelope; notifications belong to experiment and Autopilot documents."""
    result = {
        "schema_version": 1,
        "key": projection["key"],
        "revision": projection["revision"],
        "document": projection["document"],
    }
    kind = parse_key(projection["key"])
    notify = None
    if kind == "experiment":
        notify = experiment_notification(projection["document"])
    elif kind == "autopilot":
        notify = autopilot_notification(projection["key"].partition(":")[2], projection["document"])
    if notify:
        result["notify"] = notify
    return result
