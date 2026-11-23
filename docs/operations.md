# Operations

Use the same generated configuration for every command and restart, with uv,
Node 24, Slurm tools and the pinned runtime on the owner's noninteractive PATH. Run
one owner per state directory. Unrelated jobs and checkouts are outside this
application's authority.

## Status, stop and restart

```sh
uv run --locked experimenttracker --config .local/live.yaml doctor
uv run --locked experimenttracker --config .local/live.yaml status
uv run --locked experimenttracker --config .local/live.yaml inspect EXPERIMENT_UUID
```

These read saved state and dispatch nothing. Start the owner with `serve`; stop it
with Ctrl+C or SIGTERM. Shutdown saves state, takes an online backup and stops its
agent children; Slurm jobs keep running, and a restart with the same configuration
adopts their saved identities. Never use broad process kills or cancel scheduler
jobs as restart cleanup. Do not install a permanent supervisor until host policy and
local-disk retention are confirmed, and keep its secrets out of unit arguments and
logs.

In the UI, pausing the agent stops new turns and pausing compute stops new
submissions; running jobs continue. Turns also hold whenever the quota windows are
stale, unknown or below the reserve.

## Uncertain dispatch

Observe before retrying. These reconcile saved identities and never launch work:

```sh
uv run --locked experimenttracker --config .local/live.yaml reconcile --jobs
uv run --locked experimenttracker --config .local/live.yaml reconcile --action-id ACTION_UUID [--turn-id TURN_ID]
```

`--turn-id` recovers a lost response from an exact completed turn with matching
input. A job missing from the queue has not necessarily succeeded; allow for
accounting lag. Never clear an uncertain intent or invent a job ID to make progress.

## Backups and retention

```sh
uv run --locked experimenttracker --config .local/live.yaml backup
```

This takes a SQLite online backup into `backup_dir` while the owner runs. Never copy
the live database file alone. Backups contain private briefs and paths: protect
them, keep them on separate retained storage and check integrity and foreign keys
on a copy. They exclude scientific outputs, worktrees, Codex conversation storage,
configuration and credentials; retain those separately. Nothing deletes outputs or
worktrees automatically.

The running owner clears obsolete acknowledged outbox payloads in small batches; it
never removes events, receipts or scientific records. The file shrinks only after
offline compaction. Rehearse it on a completed backup, never the live database:

```sh
python -m experimenttracker.retention --backup /protected/backup/workflow.sqlite
python -m experimenttracker.retention --backup /protected/backup/workflow.sqlite \
  --output /local/disk/compacted.sqlite
```

The first form is a read-only audit. The second writes a verified compacted copy
(it needs three times the input's free space and refuses to overwrite). Installing
a compacted database is a separate owner operation: stop the owner, hold its
ownership lock through a final backup, compaction, comparison and an atomic
same-filesystem replacement, and keep a verified rollback snapshot until the
restarted owner is accepted.

Restores need a stopped writer and reconciliation with external jobs and turns; an
old backup cannot undo work already submitted.

## Application updates

Shared source lives on GitHub `main`. A push changes no installation: it does not
restart the executor, migrate state, submit work or publish the Worker.

To prepare a release, commit it and start a one-shot background preparation:

```sh
.venv/bin/python -m experimenttracker.maintenance prepare \
  --config /absolute/path/to/.local/live.yaml --commit FULL_COMMIT_SHA \
  --summary 'What changes' --detach
```

It extracts the commit into `state_dir/releases/COMMIT`, installs locked
dependencies there (never into the running owner) and runs `make check`, outside
NSS-isolating sandboxes. A failed check cannot produce a ready release, and
preparation never grants install authority.

The owner installs from **Executor & quota → Application updates → Install
update…**. The executor then holds new dispatch and waits up to 24 hours for a safe
boundary (no uncertain turns or submissions, unfinished transfers or open owner
requests), takes and verifies a backup, stops the owner through its loopback API,
starts the release once and checks that saved jobs, actions and pause controls
survived before dispatch resumes. Slurm jobs are never cancelled or resubmitted. A
failure before shutdown leaves the old owner running; a later failure holds new work
as **Update needs attention**. Nothing is retried blindly or restored
automatically. Evidence lives in `state_dir/maintenance/REQUEST_UUID/`,
`state_dir/release-preparation/` and `backup_dir/maintenance/`.

Before installing a release that changes the engine, check a fresh offline backup:

```sh
uv run --locked python -m experimenttracker.drain_inventory BACKUP.sqlite
```

Worker changes are published separately through the owner's registered Site, in
the order the release notes give (see [the Phase 5 rollout](refactor/notes/t5-rollout.md)).
D1 production migrations are append-only. A source rollback is not a database
rollback: prefer a compatible forward fix.

## Autopilot

Configure Autopilot in the project's Autopilot view: subject, scope, dates,
open-experiment limit, approval envelope and subscription reserves. Saving a draft
starts nothing; activating the run records the researcher's authorization and creates the
lead experiment. Pause holds the lead and member turns while jobs and publication
continue; Resume recomputes the calendar without catching up missed turns; Stop
ends automatic dispatch and keeps results. The lead's questions arrive in the Inbox
like any experiment's. Installing the software activates no project.

## Browser and phone

Use loopback directly or an SSH tunnel. Hosted access is private HTTPS with
separate browser and executor authentication. On a phone, open your private URL,
sign in, then Add to Home Screen. The PWA caches no research data.

## Development checks

`make setup` installs locked manifests. `make check` runs Ruff, the Python suite,
frontend checks, the gateway build and types, and local D1/R2 tests; it needs no
scheduler or credentials. Run it in an isolated checkout when an owner serves this
checkout's `web/dist`, and run Python subprocess tests outside sandboxes whose NSS
isolation breaks user or SSH resolution.

Browser checks run against a temporary loopback fixture with synthetic state:

```sh
export PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH=/path/to/chrome   # if Playwright's default is missing
node web/scripts/research-owner-acceptance.mjs   # starts the fixture and runs research-owner-smoke.mjs
```

`autopilot-smoke.mjs` and `cancellation-smoke.mjs` run against the same fixture:
start `uv run --locked python web/scripts/research-owner-fixture.py --root EMPTY_DIR
--port PORT`, then set `ET_UI_URL=http://127.0.0.1:PORT` and either
`ET_C4_FIXTURE_ROOT=EMPTY_DIR` (Autopilot) or `ET_TOKEN_FILE=EMPTY_DIR/api-token`
(cancellation, which uses Playwright's default browser). Stop the fixture afterwards.

Keep generated configuration, credentials and installation records (host
discovery, deployment identities, acceptance notes, the Site identity in
`.local/installation/site.json`) under ignored `.local/`. Ignore rules do not remove
files from history: inspect staged files before pushing.
