# Frozen legacy completed run
One **synthetic** `toy_compare_v1` experiment saved from the legacy execution path, so reader
tests keep checking reports, audits and project memory without driving that path. Demonstration
data, not scientific research. Do not hand-edit: the audit checks `root/snapshot/recipe.py` and
every registered artifact against the saved rows.

- From commit `2f311f3462cdca4b9c67db6e982ba9e84f7857cf` by
  `uv run --locked python tests/fixtures/legacy_completed/generate.py`, outside NSS-isolating
  sandboxes (AGENTS.md); this card adds tests and one ruff exclude only.
- Fixed id `51a2d1f0-6f6f-4a1e-9d0b-7c5e2f4b8a61`, with no `execution_contract` marker in the
  saved contract, which is what makes this the legacy path. Counts: 3 submissions, 2 agent
  calls, 13 artifacts, 7 stages, 1 inbox notice, 1 `create` command, schema version 24.
- `root/` (18 files, about 66 KB) is the tree verbatim, modes normalised (0755/0644). `ruff`
  excludes it: formatting would break `verify_snapshot`. The three `*.sh` wrappers embed the
  generator's `/tmp` prefix by design, because `jobs.script_hash` covers their bytes; no other
  file or saved row keeps a temporary or private path.
- `rows.json` holds the seven tables in rowid order plus `schema_version` and the id. Only
  `artifacts.path` (13 rows) and `jobs.execution_root` (3 rows) carried the root; the generator
  rewrote them to `@ROOT@` and fails on it anywhere else.
- Not saved: `events`/`outbox` (no reader needs them; `revision` stays verbatim, so gateway
  projections skip the experiment until a new event), plus `report_versions`, `stage_activity`
  and knowledge tables, which the generator asserts hold no rows for this experiment (knowledge
  is derived again on `refresh()`). `histories.json` is never pre-registered.
- `tests/legacy_fixture.py` copies `root/` to `workflow.root(eid)` and inserts the rows in one
  transaction after checking columns and artifact paths. It writes no event, outbox, history or
  digest row and dispatches nothing; regenerating gives fresh job intent ids, so file names and
  script hashes move.
