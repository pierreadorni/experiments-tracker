"""Load the frozen legacy completed run into a test store.

``tests/fixtures/legacy_completed`` holds one ``toy_compare_v1`` experiment that
no current contract produces any more. Reader tests load it instead of driving
the legacy execution path, so they keep proving what the saved data can prove
while nothing dispatches. ``generate.py`` in that directory wrote the files; it
is temporary and nothing here imports it.

The load is a copy, not a link, and one transaction: seven saved tables plus the
saved tree. No event, outbox, history, digest or manifest row is synthesized.
"""

import json
import shutil
from pathlib import Path

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "legacy_completed"
ROOT_TOKEN = "@ROOT@"
TABLES = ("experiments", "stages", "jobs", "agent_actions", "artifacts", "commands", "inbox")
# The only columns the generator rewrote; any other occurrence of the token fails.
PATH_COLUMNS = frozenset({("artifacts", "path"), ("jobs", "execution_root")})
# Parents before children: one transaction never breaks a foreign key.
INSERT_ORDER = ("experiments", "stages", "jobs", "agent_actions", "artifacts", "commands", "inbox")


def table_columns(store, table):
    return [row["name"] for row in store.rows(f"PRAGMA table_info({table})")]


def read_rows(source):
    saved = json.loads((source / "rows.json").read_text())
    tables = saved["tables"]
    if set(tables) != set(TABLES):
        raise ValueError(f"legacy fixture tables differ from the saved set: {sorted(tables)}")
    if (source / "root/histories.json").exists() or "histories" in tables:
        raise ValueError("legacy fixture must not pre-register histories")
    if any(row.get("name") == "histories.json" for row in tables["artifacts"]):
        raise ValueError("legacy fixture must not save histories.json as an artifact")
    return saved["experiment_id"], tables


def check_rows(store, eid, tables):
    """Every row belongs to the saved experiment and names only current columns."""
    checked = {}
    for table in TABLES:
        columns = table_columns(store, table)
        if not tables[table]:
            raise ValueError(f"legacy fixture {table} is empty")
        checked[table] = []
        for row in tables[table]:
            unknown = sorted(set(row) - set(columns))
            if unknown:
                raise ValueError(f"legacy fixture {table} has unknown columns {unknown}")
            identity = row.get("id") if table == "experiments" else row.get("experiment_id")
            if identity != eid:
                raise ValueError(f"legacy fixture {table} row does not belong to {eid}")
            checked[table].append(dict(row))
    return checked


def resolve_path(table, column, value, root):
    """Return the saved path under ``root``; only the saved prefix is rewritten."""
    text = str(value)
    remainder = text.removeprefix(ROOT_TOKEN)
    if remainder == text or ROOT_TOKEN in remainder:
        raise ValueError(f"legacy fixture {table}.{column} must carry one {ROOT_TOKEN}: {text}")
    if remainder and (not remainder.startswith("/") or ".." in remainder.split("/")):
        raise ValueError(f"legacy fixture {table}.{column} escapes the experiment root: {text}")
    # Production saves resolved evidence paths but the plain workflow root for jobs.
    return str(root.resolve() if table == "artifacts" else root) + remainder


def resolve_rows(tables, root):
    """Rewrite the two saved path columns, then confirm every saved artifact exists."""
    for table, rows in tables.items():
        for row in rows:
            for column, value in row.items():
                if (table, column) in PATH_COLUMNS:
                    row[column] = resolve_path(table, column, value, root)
                elif ROOT_TOKEN in str(value):
                    raise ValueError(f"legacy fixture {table}.{column} carries {ROOT_TOKEN} outside a path")
    for row in tables["artifacts"]:
        path = Path(row["path"])
        if path.is_symlink() or not path.is_relative_to(root.resolve()) or not path.is_file():
            raise ValueError(f"legacy fixture artifact is missing: {row['path']}")
    return tables


def copy_tree(tree, root):
    if root.exists():
        raise ValueError(f"legacy fixture target root already exists: {root}")
    for path in tree.rglob("*"):
        if path.is_symlink():
            raise ValueError(f"legacy fixture tree contains a symlink: {path}")
    shutil.copytree(tree, root)


def insert_rows(store, tables):
    with store.tx() as db:
        for table in INSERT_ORDER:
            for row in tables[table]:
                columns = list(row)
                db.execute(
                    f"INSERT INTO {table}({','.join(columns)}) VALUES({','.join('?' * len(columns))})",
                    [row[column] for column in columns],
                )
        if db.execute("PRAGMA foreign_key_check").fetchall():
            raise ValueError("legacy fixture rows break a foreign key")


def load_legacy_completed(store, workflow, *, directory=None):
    """Insert the frozen legacy run into ``store`` and return its fixed identity."""
    source = Path(directory) if directory is not None else FIXTURE_DIR
    eid, tables = read_rows(source)
    root = workflow.projects.base_root(eid)
    tables = check_rows(store, eid, tables)
    if json.loads(tables["experiments"][0].get("contract", "{}")).get("auto_repair"):
        raise ValueError("legacy fixture contract must not select a repair root")
    copy_tree(source / "root", root)
    insert_rows(store, resolve_rows(tables, root))
    if workflow.root(eid) != root:
        raise ValueError(f"legacy fixture root {root} differs from {workflow.root(eid)}")
    return eid
