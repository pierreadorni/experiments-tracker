# Phase 5 rollout order

Phase 5 changes the executor, the Worker and its D1 schema. Installs and Worker
publishes take an exact commit, so each step below names the commit to use. Every
step is owner-authorized; publishing source authorizes nothing.

## Step 1 — executor with relay publication (E1/E2) and the Autopilot lead

Commit: `6a0c31a` (or any later commit before the T5.2g deletion lands).

Before installing:

1. Take a fresh offline SQLite backup and run
   `uv run --locked python -m experimenttracker.drain_inventory <backup.sqlite>`.
2. `local.autopilot_unsettled_actions` must be `[]`. If it is not, stay on the
   current release until those project turns settle, then repeat.
3. Review `local.autopilot_runs_awaiting_lead`. Each listed run gets a lead
   experiment at first startup and may start spending agent turns under its
   reserves. Pause or stop any run that should not continue.

Then install through Application updates. The executor publishes to both the old
routes and the relay; against the current Worker the relay routes answer 404 and
the executor backs off quietly.

## Step 2 — Worker with the relay and push by key (W1/W2)

Commit: the same `6a0c31a`. Applies D1 migrations `0010` (four `relay_*` tables) and
`0011` (copies push follows). Both are additive.

Publish only after step 1 is running and its relay manifest is backfilled: open
the hosted UI once the executor has had a few minutes; experiments, status and
inbox should appear. The hosted UI in this Worker reads relay projections only.
Follows made between applying `0011` and the publish can be lost; re-follow if so.

## Step 3 — executor without the old path (E3)

Commit: the T5.2g commit or later. Install only after step 2 is live: this
executor no longer publishes to or polls the old routes.

## Step 4 — Worker without the old routes (W3)

Commit: the T5.2h/T5.2i commits or later. Publish after step 3. Old D1 tables stay
in place, unused.

## Step 5 — simplified self-update (T5.5)

Commit: the T5.5a–d commit or later. Install it through the existing update flow:
the installed release's coordinator still compares identities and rehashes the
receipt, and this release keeps both shims for it. Then T5.5e can delete them.

## Rollback

Each step rolls back to the previous release. Rolling back the Worker past step 2
leaves the relay tables in place; the old routes still work with an executor from
step 1. Do not roll the executor back past step 1 while a lead experiment created
at cutover is running.
