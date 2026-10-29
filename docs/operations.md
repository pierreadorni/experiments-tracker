# Operations

Use the same generated configuration for every command and restart. Keep uv, Node 24, Slurm commands and the pinned runtime available on the owner's noninteractive PATH. Run only one owner per state directory; unrelated jobs and checkouts are outside this application's authority.

## Read status and stop safely

```sh
uv run --locked experimenttracker --config .local/live.yaml doctor
uv run --locked experimenttracker --config .local/live.yaml status
uv run --locked experimenttracker --config .local/live.yaml inspect EXPERIMENT_UUID
```

`doctor` and saved-state inspection do not dispatch work. Start the owner with `serve`; stop with Ctrl+C or SIGTERM. Shutdown persists workflow state, takes an online backup and stops owned agent children. Slurm jobs survive. Restarting the same configuration adopts saved identities. Do not use broad process kills or cancel scheduler jobs as routine restart cleanup.

The UI's agent pause stops new inference; compute pause stops new submissions. Existing jobs continue. Fresh explicit included-usage permission and five-hour/week quota windows gate agent dispatch; stale, unknown or denied windows hold work. Defaults reserve 25%/10%, with 30%/15% recovery thresholds. Compute observation and publication can continue while inference is held. No automatic API billing, purchased credits or credential extraction.

## Uncertain dispatch

Observe before retrying. These commands reconcile saved identities rather than launching jobs/turns:

```sh
uv run --locked experimenttracker --config .local/live.yaml reconcile --jobs
uv run --locked experimenttracker --config .local/live.yaml reconcile --action-id ACTION_UUID
```

Use CLI help and the saved record for targeted output/turn recovery. Queue disappearance does not prove success; account for accounting lag. Never clear an uncertain intent or invent a replacement job ID to make progress. Preserve thread/turn identities and require evidence of the saved outcome.

## Backups and retention

```sh
uv run --locked experimenttracker --config .local/live.yaml backup
```

The backup command uses SQLite online backup into the configured destination and can run while the owner is active. Do not copy only a live database file and omit its journal. Protect backups: they contain private briefs, notes and paths. Establish retention on a separate retained storage destination and verify SQLite integrity/foreign keys on a backup copy.

Database backups do not include scientific files, attempt folders, Codex conversation storage, configuration or credentials. Retain those through approved separate procedures. Contract 3 never deletes large outputs, scratch or working folders automatically. Restores need an authorized stopped writer and reconciliation with external jobs/actions; an old backup cannot erase work already submitted externally.

## Upgrades

Inspect running/paused work, pending commands, job/action/transfer intents and publication before selecting a restart boundary. Take a fresh online backup and retain the previous source/build identity. Build and run `make check` in an isolated checkout if the owner serves the current `web/dist`.

Migrations occur under ownership. SQLite schema 20 supports contract 3; old binaries cannot necessarily reopen upgraded state. Preserve historical saved contracts. Deploy backward-compatible gateway changes before activating incompatible new projections. D1 production migrations are append-only. A source checkout rollback is not a database rollback: prefer a compatible forward fix, or explicitly approve a stopped-writer restore and reconcile all external effects.

Do not install a permanent supervisor until host policy and local-disk retention are confirmed. Keep service secrets out of unit command arguments, logs and browser storage.

## Browser and phone

Use loopback directly or an SSH tunnel. Hosted access must be private HTTPS with independent browser and executor authentication. For phone installation, open your own private URL in Safari/Chrome, sign in, then Add to Home Screen / Install app. Test sign-in and actual device behavior separately; browser automation is not physical-phone acceptance. Private results remain network-only; the PWA does not cache authenticated research data.

## Development checks

`make setup` installs locked manifests. `make check` runs Python lint/format/tests, frontend checks, gateway build/types and real local Miniflare D1/R2 tests. Run Python subprocess tests outside restrictive agent sandboxes if NSS isolation prevents user/SSH resolution. These tests need no live scheduler or account credentials. Synthetic browser smoke scripts create demo experiments with mocked scheduler/model boundaries.
