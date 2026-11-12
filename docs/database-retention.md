# Transport payload retention

The owner visits at most 100 outbox identities every five seconds, clearing only
obsolete acknowledged payloads to `{}`. A batch targets at most 16 MiB of text
(one larger row may exceed this). Work runs off the event loop under the Store
transaction lock. Indexed event-sequence traversal resumes between batches and
starts another sweep at the end; acknowledgement changes are seen next sweep.
No migrations, events, receipt metadata, scientific records or files are removed.
The current revision, highest acknowledged revision, and every pending payload
remain intact. Invalid protected snapshots abort the batch. Log diagnostics give
visited cursor, cleared count and reclaimed characters, without payload contents.
A gateway outage can still accumulate pending full views. This conservative policy
does not bound that backlog. Clearing payloads frees pages for reuse; the existing
file and periodic backups shrink only after explicit offline compaction.

The October 31 artifact registration fix already suppresses unchanged registrations
and report churn. Predispatch prerequisite failures already have a persisted
60-second backoff; uncertain dispatches retain reconciliation holds. These facts
do not establish the historical root cause of every alternating event loop.
Progress observations retain fresh reported timestamps in SQLite, while timestamps
alone no longer generate full projections. Quota history retention is deferred;
workspace recovery baselines and scientific contracts are unchanged.

## Rehearse on a completed backup

Use the tested release Python. Do not pass a live database as `--backup`:

```sh
python -m experimenttracker.retention --backup /protected/completed/workflow.sqlite
python -m experimenttracker.retention --backup /protected/completed/workflow.sqlite \
  --output /verified/local/disk/compacted.sqlite
```

The default is a read-only audit. Output contains counts and character estimates,
not scientific payloads; redirect it to a mode-600 audit file. Apply creates a new
compacted derivative and leaves the input untouched. Use protected local disk for
the output. The tool requires free space of three times the input size, verifies
integrity, foreign keys and protected snapshot identities, and streams an exact
logical comparison of every table and receipt column. Only eligible payload
changes are allowed. SQLite internal page numbers may change on compaction.
The output is private and file/directory synchronized after verification.
The `.working` sibling remains after failure/interruption for investigation;
existing output/working paths refuse overwrite. An output left after interruption
must not be installed merely because it exists: rerun verification and comparison
or rehearse afresh using a new path. No automatic source replacement is provided.

## Production maintenance boundary

Prepare a committed release using `executor-updates.md`. Installation and a live
restart remain owner-authorized through the gateway. The normal update coordinator
does not compact or replace the database. On the actual owner host, discover paths
from the generated configuration, establish local disk capacity, and coordinate a
separate stopped-owner maintenance window. Verify the exact coordinator/owner is
stopped, take the existing `OwnershipLock(config.state_dir)`, and keep that lock
through the final snapshot, compaction, verification and installation. No backup
writer/coordinator may remain. Use SQLite backup, never bare-copy a live database.
Keep one verified post-shutdown pre-prune rollback snapshot through acceptance.

Compact that final snapshot on local disk with the tool above. Compare against the
post-shutdown authoritative database again before replacement. Close connections,
resolve journal state through SQLite, preserve ownership/mode 600 and atomically
replace via a verified sibling on the same local filesystem, syncing its directory.
Do not delete journals or replace the database with an NFS copy. This replacement
is an explicit owner operation, not an automated feature of this tool.

Resume only through the authorized owner path. Verify exact saved job/turn intents,
experiment revisions and pause/hold controls, publication/replay, artifact gating,
report visibility and fresh quota. Obtain a new completed periodic backup, verify
it and record its actual size. No Slurm/model dispatch is retried for maintenance.
Before resumed writes, recovery can restore the verified final rollback snapshot
under the same lock. After resumed writes, restoration loses newer state and
requires explicit owner reconciliation. Historical backups are retained; their
removal is a separate owner retention decision. Synthetic tests/rehearsal do not
establish live Slurm or physical-phone acceptance.
