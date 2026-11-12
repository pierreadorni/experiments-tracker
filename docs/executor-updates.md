# Owner-authorized executor updates

This feature prepares tested software without restarting the owner. The gateway's
**Executor & quota → Application updates** shows the running release, preparation
and test progress, a pending commit and an **Install update…** confirmation. A
sidebar shortcut makes the update discoverable outside settings. Authorization
covers the backup, graceful handover and recovery checks; there is no second
confirmation after the backup.

## Bootstrap once

The previously running binary cannot process maintenance commands. Activate this
maintenance-enabled executor once using the existing authorized restart procedure
in `runbook.md`, with its protected online backup and identity comparisons. Schema
21 adds software release and maintenance records; saved scientific contracts are
unchanged. Publish the backward-compatible gateway/UI and append-only D1 migration
`0008_executor_maintenance.sql` to the exact private Site in your ignored `.local/installation/site.json` before
activating the executor. No new permanent service or deployment credential is
installed by this feature.

Start with the exact existing absolute configuration path using the normal CLI:

```sh
uv run --locked experimenttracker --config /absolute/path/to/.local/live.yaml serve
```

A development checkout is labeled **Development checkout** rather than claiming
it is the exact bytes of a tested commit. Once a tested release is installed, the
UI identifies its full saved commit (abbreviated visually). The coordinator starts
the release's own Python and source and serves its bundled local frontend. An
explicit `serve --release COMMIT` refuses to run from another source directory.

## Prepare future changes

Commit the intended changes. Ignored private configuration, credentials, state and
scientific outputs never enter the release archive. From a source checkout, run:

```sh
.venv/bin/python -m experimenttracker.maintenance prepare \
  --config /absolute/path/to/.local/live.yaml \
  --commit FULL_COMMIT_SHA \
  --summary 'Describe the resulting behavior' \
  --detach
```

The one-shot background process needs no model session. Use the existing Python
environment for the launcher; do not synchronize the running owner’s dependencies
in place. Dependency installation happens only inside the new release. It publishes preparation
and test states through the running owner, extracts committed source into
`state_dir/releases/COMMIT`, installs locked Python/Node dependencies and runs
`make check` there. Keep uv and Node 24 on its inherited PATH. This must run outside
the acacia NSS shell sandbox, just like other Python subprocess checks. A
per-commit OS lock prevents duplicate preparation. Interrupted candidates are
retained under an `.incomplete-*` software directory when explicitly prepared
again. A failed check cannot produce a ready release.

Successful preparation records the exact source/build software receipt and check
result in SQLite. The ready receipt cannot be overwritten. Before handover, the
coordinator compares the software files and saved receipt again. These are
software integrity checks; no project data, scientific outputs, checkpoints or
research directory inventories are involved. Preparation never grants install
authority. The agent can end its turn after starting preparation; the UI receives
the result without model polling.

Gateway source changes still require publishing their compatible gateway/UI build
through the existing registered private Site workflow during release preparation.
The local coordinator does not deploy Sites or D1. Do not advertise a release that
requires an unpublished gateway protocol or production migration. Frontend-only
hosted changes can use the existing deployment path without restarting execution.

## Install and observe

Select **Install update…**, review the summary, successful checks and last reported
submitted-job count, then select **Authorize installation**. The gateway persists
the request even if the owner is offline; SQLite accepts or refuses the exact
request transactionally before its delivery receipt is acknowledged. Double
clicks/redelivery cannot create a second local installation. Refusals also persist
locally before acknowledgement.

The executor holds new dispatch without rewriting any experiment pause controls.
It continues observing saved jobs. Existing turns can settle; new submissions
from those turns are held too. Uncertain turns/submissions, unresolved human
requests, unfinished transfers, pending restart steering, active admission and
filesystem tasks keep the restart waiting. A safe boundary is checked again before
shutdown. Pending restart steering/transfers may need operator resolution; the
coordinator does not force them through just to obtain a window. The wait expires
after 24 hours before stopping the owner.

Progress states are: waiting, backing up, verifying backup, stopping, starting,
checking recovery, complete. The coordinator is a separate one-shot process and
can publish outbound progress while the executor is offline. Gateway records
reject older progress timestamps; unavailable or old observations are labeled in
the UI. Missing gateway connectivity does not undo the local operation; the owner
republishes its authoritative status when delivery resumes.

The pre-migration online backup must pass SQLite integrity and foreign-key checks.
The coordinator requests graceful shutdown through the exact owner's authenticated
loopback API; it never sends broad process kills. It waits up to five minutes for
the existing ownership lock, then records saved job/action/project-turn identities
and experiment pause controls under that same lock. Configuration changes abort
the handover. The replacement starts exactly once from the registered release.
It must identify that release, finish a healthy executor cycle and preserve the
saved identities and pause controls before new dispatch resumes. Slurm jobs are
not cancelled, resubmitted or rerun.

## Failures and recovery

A failure before shutdown leaves the existing owner running and releases the
maintenance dispatch hold. A failure during shutdown/startup/verification holds
new work as **Update needs attention**. A missing/dead coordinator or uncertain
launch also holds for inspection. A database migration or launch is never blindly
retried and no database is automatically restored. `run --request UUID` only
accepts an exact waiting operation; it is not an automatic recovery command for
later phases.

Protected diagnostic files live in `state_dir/maintenance/REQUEST_UUID/`:
`coordinator.log`, `executor.log`, `launch.json` and `identities.json`. Preparation
logs live under `state_dir/release-preparation/`; online installation backups live
under `backup_dir/maintenance/REQUEST_UUID/workflow.sqlite`. These files are local
operator evidence and are never published to the browser. Backup/configuration
and source retention remain explicit owner responsibilities; no cleanup is added.

After a blocked handover, inspect the saved phase, coordinator process identity,
launch receipt and exact ownership/turn/job identities before choosing a compatible
forward fix. Resolve the saved maintenance record only with the writer stopped
under the ownership lock and explicit operator authorization. Restoring an older
database remains a separate authorized operation that can discard newer commands
and requires external-outcome reconciliation. The UI deliberately offers no blind
restart/retry button for a blocked handover.

## Validation boundary

`tests/test_maintenance.py` covers command idempotency, immutable software receipts,
backup permissions/integrity, authentication, dispatch holds, uncertain coordinator
launches, lost gateway acknowledgements and a real isolated demo process handover
with saved history. Gateway tests use actual local D1/R2. The browser smoke script
`web/scripts/maintenance-smoke.mjs` creates a synthetic demo experiment and mocks
the installation boundary to check desktop/mobile confirmation, progress, stale
status and blocked recovery. None establishes live Slurm, managed-account or
physical-phone acceptance.

