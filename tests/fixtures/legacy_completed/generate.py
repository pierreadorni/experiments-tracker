"""Regenerate the frozen legacy completed fixture (temporary until M9-r).

Run from the worktree root with its locked environment, outside sandboxes whose
NSS isolation changes subprocess behaviour (AGENTS.md):

    uv run --locked python tests/fixtures/legacy_completed/generate.py

One ``toy_compare_v1`` experiment with the fixed identity below runs to
``complete`` in a throwaway directory under ``/tmp`` with the demo scheduler and
agent, and its saved contract loses the ``execution_contract`` marker before the
first tick, as ``tests/conftest.py`` does for historical suites. Writes ``root/``
(the tree, modes normalised) and ``rows.json``. Nothing in ``tests/`` imports it.
"""

import asyncio
import json
import os
import shutil
import tempfile
import time
from pathlib import Path
from uuid import uuid4

import experimenttracker
from experimenttracker.codex_client import DemoAgent
from experimenttracker.config import Config
from experimenttracker.slurm import DemoScheduler
from experimenttracker.store import Store, encode
from experimenttracker.workflow import Workflow

WORKTREE = Path(__file__).resolve().parents[3]
FIXTURE = Path(__file__).resolve().parent
EXPERIMENT_ID = "51a2d1f0-6f6f-4a1e-9d0b-7c5e2f4b8a61"
ROOT_TOKEN = "@ROOT@"
TABLES = ("experiments", "stages", "jobs", "agent_actions", "artifacts", "commands", "inbox")
PATH_COLUMNS = {"artifacts": "path", "jobs": "execution_root"}
# Rows that exist for this experiment but are deliberately not saved.
OMITTED = {
    "events": "the reader tests never consume the event log; revision stays verbatim",
    "outbox": "gateway delivery state belongs to the owner that wrote it",
}
# Knowledge.refresh() derives this table again from the saved experiments.
DERIVED = {"experiment_summaries"}
COUNTS = {"stages": 7, "jobs": 3, "agent_calls": 2, "artifacts": 13}
# Never allowed in saved rows or copied files, whatever the machine calls them.
PRIVATE = (str(WORKTREE), os.path.expanduser("~"))


def config(base):
    (base / "token").write_text("t" * 40)
    return Config(
        state_dir=base / "state",
        job_output_root=base / "outputs",
        backup_dir=base / "backups",
        recipe_dir=WORKTREE / "recipes/toy_compare",
        web_dir=base / "web",
        api_token_file=base / "token",
        scheduler_seconds=0.1,
    )


async def complete_toy_experiment(store, workflow):
    eid = store.command(
        uuid4(),
        "create",
        {
            "brief": "Compare linear and quadratic models on shared synthetic data.",
            "recipe_id": "toy_compare_v1",
        },
        experiment_id=EXPERIMENT_ID,
    )["experiment_id"]
    contract = store.experiment(eid)["contract"]
    if contract.pop("execution_contract", None) is not None:  # conftest parity for legacy suites
        with store.tx() as db:
            db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))
    deadline = time.monotonic() + 180
    while store.experiment(eid)["phase"] not in ("complete", "failed", "cancelled"):
        if time.monotonic() > deadline:
            raise SystemExit(f"{eid} did not settle: {store.experiment(eid)}")
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.01)
    return eid


def check_completed(store, scheduler, agent, eid):
    exp = store.experiment(eid)
    assert exp["phase"] == "complete" and exp["hold_reason"] is None and exp["error"] is None, exp
    assert exp["id"] == EXPERIMENT_ID and "execution_contract" not in exp["contract"], exp
    for table in ("stages", "jobs"):
        states = [
            row["state"] for row in store.rows(f"SELECT state FROM {table} WHERE experiment_id=?", (eid,))
        ]
        assert len(states) == COUNTS[table] and set(states) == {"complete"}, (table, states)
    actions = [
        row["state"] for row in store.rows("SELECT state FROM agent_actions WHERE experiment_id=?", (eid,))
    ]
    assert actions and set(actions) == {"completed"}, actions
    assert scheduler.submissions == COUNTS["jobs"], scheduler.submissions
    assert agent.calls == COUNTS["agent_calls"], agent.calls
    registered = store.rows("SELECT name FROM artifacts WHERE experiment_id=?", (eid,))
    assert len(registered) == COUNTS["artifacts"], len(registered)


def check_omitted_rows(store, eid):
    """Fail when dropping a table would silently lose this experiment's rows."""
    saved = set(TABLES) | set(OMITTED) | DERIVED | {"schema_version", "quota_snapshots"}
    for row in store.rows("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
        table = row["name"]
        columns = {column["name"] for column in store.rows(f"PRAGMA table_info({table})")}
        if table in saved or "experiment_id" not in columns:
            continue  # project-scoped knowledge tables are rebuilt by refresh() as well
        count = store.one(f"SELECT COUNT(*) AS n FROM {table} WHERE experiment_id=?", (eid,))["n"]
        if count:
            raise SystemExit(f"{table} holds {count} rows that the saved fixture would lose")


def saved_rows(store, eid):
    tables = {}
    for table in TABLES:
        column = "id" if table == "experiments" else "experiment_id"
        tables[table] = [
            dict(row) for row in store.rows(f"SELECT * FROM {table} WHERE {column}=? ORDER BY rowid", (eid,))
        ]
        if not tables[table]:
            raise SystemExit(f"no {table} rows saved for {eid}")
    return tables


def substitute_root(value, root):
    """Replace the temporary prefix in one saved path and keep it inside the root."""
    text = str(value)
    remainder = text.removeprefix(str(root))
    if remainder == text or (remainder and (not remainder.startswith("/") or ".." in remainder)):
        raise SystemExit(f"saved path is not inside the temporary root: {text}")
    return ROOT_TOKEN + remainder


def check_no_private_paths(tables, base, root):
    allowed = set(PATH_COLUMNS.items())
    forbidden = [str(base), str(root), *PRIVATE]
    for table, rows in tables.items():
        for row in rows:
            for column, value in row.items():
                if (table, column) not in allowed and ROOT_TOKEN in str(value):
                    raise SystemExit(f"{ROOT_TOKEN} outside a saved path column: {table}.{column}")
                for prefix in forbidden:
                    if prefix and prefix in str(value):
                        raise SystemExit(f"temporary path left in {table}.{column}: {value!r}")


def check_tree(root, base):
    """Reject symlinks, files git would drop, and private paths in the copied bytes."""
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise SystemExit(f"symlink in the experiment tree: {path}")
        if path.is_dir():
            continue  # only file bytes can leak a path
        if path.suffix in (".log", ".pyc") or ".sqlite" in path.name or path.parent.name == "__pycache__":
            raise SystemExit(f"file type .gitignore drops in the experiment tree: {path}")
        data = path.read_bytes()
        # The job wrappers keep the temporary prefix by design; nothing else may.
        for prefix in PRIVATE + (() if path.suffix == ".sh" else (str(base),)):
            if prefix.encode() in data:
                raise SystemExit(f"private path {prefix} in {path.relative_to(root)}")


def copy_tree(root, target, base):
    check_tree(root, base)
    if target.exists():
        shutil.rmtree(target)
    target.mkdir()
    for path in sorted(root.rglob("*")):
        copy = target / path.relative_to(root)
        if path.is_dir():
            copy.mkdir()
        else:
            copy.write_bytes(path.read_bytes())
            copy.chmod(0o644)
    for path in sorted(target.rglob("*"), reverse=True):
        path.chmod(0o755 if path.is_dir() else 0o644)


async def generate(settings):
    store = Store(settings.state_dir / "workflow.sqlite")
    scheduler = DemoScheduler(settings.state_dir / "scheduler")
    agent = DemoAgent()
    workflow = Workflow(settings, store, scheduler, agent)
    try:
        eid = await complete_toy_experiment(store, workflow)
    finally:
        await workflow.close()
    root = workflow.root(eid)
    assert root == root.resolve(), f"the temporary root must already be resolved: {root}"
    base = root.parent.parent
    try:
        check_completed(store, scheduler, agent, eid)
        check_omitted_rows(store, eid)
        tables = saved_rows(store, eid)
        if (root / "histories.json").exists():
            raise SystemExit("the fixture must not pre-register histories.json")
        for table, column in PATH_COLUMNS.items():
            for row in tables[table]:
                row[column] = substitute_root(row[column], root)
        check_no_private_paths(tables, base, root)
        copy_tree(root, FIXTURE / "root", base)
        version = store.one("SELECT MAX(version) AS version FROM schema_version")["version"]
    finally:
        store.close()
    rows = {"experiment_id": eid, "schema_version": version, "tables": tables}
    (FIXTURE / "rows.json").write_text(json.dumps(rows, indent=2, sort_keys=True) + "\n")
    return eid, root, tables


def main():
    module = Path(experimenttracker.__file__).resolve()
    if not module.is_relative_to(WORKTREE):
        raise SystemExit(f"run this generator with the worktree environment, not {module}")
    base = Path(tempfile.mkdtemp(prefix="legacy-completed-", dir="/tmp")).resolve()
    if not base.is_relative_to(Path(tempfile.gettempdir()).resolve()):
        raise SystemExit(f"the temporary root must live under /tmp: {base}")
    eid, root, tables = asyncio.run(generate(config(base)))
    print(f"{eid} generated from {module} in {base}, saved to {FIXTURE}")
    for table, rows in tables.items():
        print(f"  {table}: {len(rows)} rows")
    print("job scripts keeping the temporary prefix by design:", *sorted(root.rglob("*.sh")), sep="\n  ")
    print("update README.md when the generating commit, command or counts move")


if __name__ == "__main__":
    main()
