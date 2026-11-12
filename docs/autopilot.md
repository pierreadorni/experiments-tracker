# Research Autopilot operations

Autopilot is opt-in per project. Installing its software does not activate a
research project, refresh included-usage permission or authorize a live trial.
Existing/manual and synthetic-demo experiments retain their saved contracts.

Open **Project knowledge → Research Autopilot**, configure the subject, scope,
venue/guidelines, start/handoff/submission dates, weekly GPU cap and shared usage
reserves. Review the granted authority and save the draft. Saving starts no work.
The Autopilot view also provides direct five-hour and weekly reserve controls.
Saving creates a policy revision; the stricter owner reserve still applies, and
a held policy resumes only after the five-point recovery margin is met.
**Review activation → Activate Autopilot** records researcher authorization.
Plans for linked contract 3 experiments can then receive attributed Autopilot
approval within that envelope. Manual plans still require normal review.

The default calendar shares are 1/59/30/10. Times remain precise, displayed as
fractional weeks and dates. Foundation readiness is separate from calendar phase;
quota delays never silently complete initial review. The paper starts during
foundation. Guidelines, citations and reported numbers need actual sources;
missing values remain TODO. Handoff readiness is controller judgment for researcher
review, never scientific certification or automatic conference submission.

GPU weeks are fixed seven-day UTC intervals from the saved anchor, which is
immutable after activation and across subsequent runs in the same project. Each
individual job reserves total allocation GPUs × requested walltime. GPU count is
already for the entire allocation, including multi-node jobs. A whole experiment
can exceed one week's cap. Undispatched stages show **Waiting for autopilot limit
reset** and remain in their original graph/report. Resets and released reservations
re-admit them without inference. Outstanding queued/unknown exposure survives
resets. CPU-only jobs use no GPU allowance and retain normal admission limits.

Jobs exceeding the whole weekly cap need a different permitted allocation,
checkpointed stages, or a researcher cap revision. Lowering the cap never cancels
submitted work. Unexpected allocation/requeue behavior and missing terminal times
retain conservative charges and hold affected GPU admission for inspection.

## Live accounting prerequisite

Live Autopilot GPU dispatch is intentionally held until each execution context's
scheduler semantics have been verified. This is a prerequisite for live use, not
evidence supplied by software fixtures. Under the existing owner configuration,
record a protected `state_dir/autopilot-accounting.json` with entries like:

```json
{
  "acacia": {
    "verified_at": 1793610000,
    "no_requeue": true,
    "allocation_intervals": "single"
  }
}
```

The timestamp above is illustrative; use the actual verification time. Verify
job-level `sacct` Start/End/AllocTRES fields, duplicate allocation intervals,
and `scontrol show job` Restarts metadata, UTC timestamps, exact intent
comments, terminal accounting lag, and support for `sbatch --no-requeue`. Exclude
step rows; GPU allocation metadata can fall back conservatively to the validated
request. Determine preemption/requeue, suspended time and allocation-change
behavior with the cluster operator's policy and acceptance evidence. Only certify
`single` when repeated allocations are excluded or reliably reconciled. Do not
create this record from synthetic tests or to bypass an unknown scheduler policy.
Jean Zay requires its own `jean-zay` entry and verification. Unsupported contexts
remain `autopilot_accounting_prerequisite`. An anomaly never becomes a free retry.

Observe exact job IDs with ordinary read-only tools; inspect
`autopilot_allocations` and append-only `autopilot_accounting` through a read-only
SQLite connection if accounting needs operator investigation. Keep the owner as
the only writer. There is no force-refund or blind resubmit UI.

## Controls and recovery

Pause holds new project/child turns and unsubmitted compute while observation and
publication continue. User pause flags remain separate. Already submitted queued
jobs may start. Resume recomputes the current calendar without catch-up turn bursts.
Stop revokes automatic dispatch and keeps results/history. Explicit **Cancel…** on
a linked experiment uses existing exact cancellation and shows pending or
unconfirmed reconciliation. Deferral holds a linked experiment without clearing
researcher pauses or cancelling submitted work. Idle deferred investigations can
release an open-experiment slot; submitted/uncertain work cannot.

Project questions and guidance use a conversation with explicit Autopilot write
authority. Read-only knowledge chat retains its existing contract. Policy changes
are authenticated researcher commands. Late effects from a stale child/project
turn are rejected or continued from a known terminal result. Accepted tool results
are discoverable in project context. A lost turn-start response with no exact turn
identity is quarantined; inspect it rather than starting another turn.

Weekly/phase/result inputs coalesce against a saved cursor. Ordinary budget holds
remain quiet. Project input/handoff notices appear in the existing Inbox. Optional attention
pushes reuse devices following linked experiments, with one durable intent per
notice/device; unchanged budget holds remain quiet.
Scheduled read-only weekly synthesis is suppressed for an active Autopilot project;
manual knowledge use is retained. Shared subscription remaining-usage reserves
apply to project and automatic child turns with independent hysteresis.

The private project bridge explicitly authorizes its declared tools within the
activated project policy; other runtime tools keep their own approval behavior.
A terminal `request_guidance` decision always retains an Inbox question, even if
the turn could not invoke the question tool, and displays **Waiting for your reply**.
Answer the pending question to queue a durable continuation under current quota
and authority. Startup also recovers missing questions from the latest completed
guidance decision without repeating that turn or its accepted actions. After
installing the tool-authorization fix, an affected project can be continued by
answering its recovered question with “Continue within the saved project policy.”

Project documents are immutable SQLite-backed text versions with revision-checked
publication; staging files are not their authority. This deliberately avoids a
second crash window between published file bytes and version registration.
Authenticated routes serve only registered versions; gateway delivery places R2
objects before its monotonic ready view. Compile operations stage known source
versions under their saved request identity, disable shell escape and bound time,
stdout and PDF size. Missing TeX is a prerequisite; source drafts remain usable.
The current source package and any successfully compiled PDF are downloadable.
Registered handoffs retain their exact document/file version references; subsequent
working edits do not rewrite the earlier handoff.
Referenced figure assets remain published experiment artifacts/direct references.

At handoff time, the backend publishes the latest draft index and explicit missing
work without requiring model availability. New research/compute stops, including
old budget waiters. Already submitted jobs continue and are labeled late work.
Eligible analysis/packaging can version the handoff under fresh quota; a researcher
pause still holds it. An explicit deadline revision is required for more research.

## Release and acceptance

Schema 22 adds project authority/ledger/documents and child authority revisions;
gateway migration 0009 is append-only. Existing projects start disabled. Prepare
the exact committed release using [executor updates](executor-updates.md), publish
the compatible gateway to the Site in `docs/site.json`, and leave installation to
the owner's **Install update…** authorization. No permanent daemon is added.
Old binaries cannot use the migrated local schema; retain the installation backup
and choose compatible forward repair rather than checking out an older binary.

`tests/test_iteration_autopilot.py` uses fake time and explicitly synthetic runtime
and allocation metadata. `gateway/test/autopilot.mjs` uses real local D1/R2.
`web/scripts/autopilot-smoke.mjs` creates a labeled synthetic demo experiment and
checks desktop/mobile project controls using a mocked research boundary. None
establishes managed-account retrieval, real cluster accounting, scientific results
or physical-phone sign-in. Those remain separate owner-authorized acceptance.

Read-only compatibility discovery on 2026-11-02 found acacia Slurm 23.02.2:
AllocTRES and Suspended are available in `sacct --helpformat`, Restarts is not.
The adapter uses supported fields, `--duplicates` for Autopilot allocation rows
and bounded running-job `scontrol` metadata. This is field discovery, not live
GPU accounting acceptance or authorization to enable the profile. See the
[Slurm accounting reference](https://slurm.schedmd.com/sacct.html) and
[sbatch requeue options](https://slurm.schedmd.com/sbatch.html).
