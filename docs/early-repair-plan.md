# Early, selective repair: implementation plan

Status: implemented in source; owner activation and live Slurm acceptance remain
separate. Contract 3 starts diagnostic preparation after a confirmed failure while
unrelated tracked jobs continue. Saved historical/demo behavior retains its
terminal graph gate.

1. On the first confirmed compute or output failure, atomically record a repair
   incident tied to the failed producer and attempt. Hold dependent submissions,
   continue observing all jobs, and queue one durable diagnostic action per
   incident. Admission, pause, budget, managed authentication and exact-turn
   reconciliation remain unchanged. Uncertain submissions are not failures.
2. Start the diagnostic turn without waiting for unrelated active jobs. Supply a
   concise brief, failed stage/error, and stage/intent/status list. Do not embed
   all logs, manifests or output inventories. Allow scoped status/log reads on
   demand and point to existing declared sources and working files. Diagnosis must
   not overwrite files used by running jobs.
3. Add an experiment-scoped cancellation tool accepting an exact saved intent,
   request UUID and reason. Persist a unique cancellation decision and dispatch
   intent before scheduler calls. Verify the scheduler identity, observe terminal
   accounting, and adopt an already-finished job rather than relabel its result.
   Replay the same request idempotently; reconcile uncertain results after restart.
   Never infer that an absent queue entry means a terminal outcome.
4. Prepare fixes in an isolated candidate workspace. Record per-stage decisions
   to keep, cancel or retry and their rationale. Capture corrected declared
   sources only for replacement attempts. Preserve active jobs' attempt files and
   output bindings. A replacement is admitted only after its own predecessor is
   confirmed terminal, its dependencies are ready, and any output writer conflict
   is resolved. Unaffected jobs keep running and their valid results remain usable.
   Do not globally reset stages or force retries of successful branches.
5. Persist independent repair, action and cancellation states transactionally with
   events and projections; resume from the exact state after owner restart. Expose
   decisions and pending reconciliation. A completed agent-stage producer uses its
   saved action/turn identity, not a Slurm job ID. Add synthetic tests for racing
   completion, cancellation replay, uncertain submissions and turns, mixed agent
   and Slurm stages, output conflicts, selective retry, quota/pause, and restart.
   Live Slurm acceptance remains separate from synthetic tests.

Installation requires a tested committed executor release and owner gateway
authorization. Neither preparing the release nor this plan authorizes restart,
cancellation of live work, or changes to research checkouts.

Implementation notes (2026-11-02): diagnostic preparation uses the existing durable
prepare action and quota/turn reconciliation, with a new preparation revision.
`early_repair` records the incident and per-stage keep/retry decisions independently
of `job_cancellations`. The candidate contains only bounded declared work/project
sources; the agent session and writable roots point to those candidate folders.
A keep decision preserves the exact stage specification and requires explicit
reuse of successful producers. Retries retain old attempt/output identities;
replacement admission waits for every prior attempt of that stage to have exact
terminal accounting. Cancellation records a reason/request UUID before dispatch,
reconciles intent-to-job identity and scheduler state, and observes accounting
again after restart. An already-completed job retains its actual result. Unknown
queue/accounting outcomes remain pending; they never establish completion.

The cancellation tool handles tracked Slurm jobs only. Existing agent turns must
settle through their saved action/thread/turn reconciliation before retry; this
change does not add in-turn agent interruption. All stages from the approved graph
remain declared during selective repair; material objective changes retain review.
Declared outputs are isolated by attempt roots and existing path-overlap validation.
Kept active stages cannot change their output declarations, and superseded
completions cannot replace selected producers/readiness. Candidate folders and prior
attempts remain for owner-managed retention. These are operational isolation and
identity rules, not OS-level isolation against arbitrary shell access.

Synthetic acceptance covers early preparation beside a running sibling, candidate
source/session isolation, keep/retry readiness, preterminal local failure holds,
uncertain submissions/agent records, cancellation replay/lost responses/racing
success/accounting gaps, pause admission, restart and repeated targeted plot repair
with metric producer preservation. Actual scheduler cancellation, managed runtime
and live research acceptance remain separately required at owner activation.
