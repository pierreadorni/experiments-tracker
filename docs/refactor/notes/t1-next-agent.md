# T1 next-agent handoff — 2026-11-17

The owner requested a fresh agent after this coordinator finished its current
card. **T1 is not complete.** Continue the whole Phase 2 objective; use Luna for
F/F+R implementation, coordinator review, and Astra sparingly for difficult design.
Do not activate any installation or live research. No worker/check remains active.

## Authoritative starting point

Main `6114ca7` includes the completed T1.1 legacy retirement and the first runtime
Workflow action-obligation card, **b2b**. A documentation commit containing this
handoff follows it. All committed changes are to be pushed before handoff.

- T1.1 is complete, including archival settlement, command/reply boundaries and
  retained audit/conversation/queued-report readers.
- T1.2 clock/harness and T1.3a schema/API are complete.
- Inert episode/action/job/admission helpers are committed. Runtime b2b is now
  committed. **b3 onward, backfill, derived holds/direct-write removal/watchdog,
  resolvers, runner, AgentPool and mechanical splits remain required.**
- `docs/refactor/todo.md` and the reviewed design notes remain the requirements.
  Do not substitute helper completion or green tests for the full T1 objective.

Reorient with status/stat/log15. Preserve dirty worktrees and unrelated untracked
`.impeccable/`, `.worktrees/` and `t1-concurrent-coordination.md`. The latter is
local duplicate-thread investigation context and was deliberately not published.
Never reset/clean these worktrees or copy a whole old Workflow over current main.

## Completed validation

b2b (`6114ca7`): source +139/-41; tests +259; **439 changed lines**. Runtime action
inserts, thread/turn saves, state changes, reconciliation and report correction
synchronize source + obligation + existing event in one transaction. Callbacks
check exact action ID/phase/evidence/thread/turn; unchanged completion replay is a
no-op. Ruff, **94 targeted tests and all665 Python tests passed** (488.63s).

Evidence in `.worktrees/t1-validation/check-results/`:
`t1-b2b-full.xml` and `t1-b2b-full.log`. No validation process remains active.
Final T1.1 also passed658 Python,29 web,45 gateway tests and Node24 format/build/
type checks. Those Node results precede b2b, which changes Python only.

A separately committed two-line test fix (`4f7c874`) gives the three-real-plot
conversation correction wait60 seconds. The original30-second wait timed out;
all continuation/result/identity/no-submission assertions remain unchanged.

Use isolated `.worktrees/t1-validation` and Node24:
`/share/home/adorni/.nvm/versions/node/v24.18.0/bin` on PATH. Locked dependencies
are installed there. Run Python subprocess tests **outside NSS-isolated sandboxes**
using approved escalation; worker sandbox stalls/failure markers without a final
traceback are inconclusive. Use durable XML/logs for full checks. Wait for every
returned shell session to finish before copying files or starting dependent work.

## Next: integrate these frozen Luna candidates in order

Each checkout has copied prerequisites on an older HEAD. Measure against the
named saved baselines, apply only the card's method hunks/files, and preserve
later main changes. Hard source+test budgets: S<150, M<500; docs excluded.

### b3 — Store and Conversation action lifecycle

Checkout `.worktrees/t1-action-store-conversation`.
Baselines: `b3-store.py.baseline`, `b3-conversation.py.baseline`.

Store continuation/session retry/title admission and Conversation creation,
correction, dispatch and restart readback hooks. Latest review added `expected`
snapshots to both `Conversation.advance` readback callbacks and a stale-turn
regression. Store is +7/-1, Conversation +28/-4; new Store test file
`tests/test_action_source_integration.py` (97 lines), plus Conversation test
changes (71 lines): about208 changed lines; remeasure before landing.

Three initial focused cases passed; the new stale-readback case passed separately.
Ruff passes. Run the full relevant tests outside NSS and the complete Python suite.
Preserve the committed60-second wait in `tests/test_conversation.py`; append the
new tests or apply its diff rather than copying the whole old-based test file.
Its Workflow is a copied b2b prerequisite: **do not apply that whole file**.

### b4 — Controller, cancellation and archived action settlement

Checkout `.worktrees/t1-controller-action-obligations`.
Baselines: `b4-{controller,store,cancellation,legacy_archive_actions}.py.baseline`.

Monitor action creation/supersession, same-transaction cancellation of pending
actions, and exact expected snapshots for readback callbacks. Supersession adds
an existing `agent_action_changed` event where the old writer had none.
Reported137 changed lines; focused tests passed, process-sensitive restart checks
were inconclusive in sandbox. Tests changed in `test_controller.py` and
`test_cancellation.py`. Preserve b3 Store methods when merging. No job/wakeup
obligation mapping is claimed by this card.

### b5b — four Workflow job source hooks

Checkout `.worktrees/t1-job-source-wiring`.
Baseline: `b5b-workflow.py.baseline`.

Intent insert before dispatch, accepted scheduler ID, missing-ID adoption, and
failed-output recovery terminal save. Reported232 additions: source22, output
recovery tests14, new `test_workflow_job_obligations.py`196. Four focused tests
passed. Preserve b2b Workflow changes and all C10 deletions; merge imports/hunks,
not the whole file. Ordinary observer settlement and other writers are still b6.

### b6 — unfinished review fixes; NOT ready to land

Checkout `.worktrees/t1-job-observation-wiring`.
Baselines: `b6-{workflow,cancellation,legacy_archive,steering}.py.baseline`.
Copied b5b Workflow hooks are prerequisites, not b6 changes.

Partial hooks cover normal observations/output failures, cancellation/legacy job
settlement and steering facts. Last report: source +153/-11, tests +52; remeasure.
29 mapper/legacy focused tests passed. Cancellation/steering/recovery integration
was inconclusive under NSS, including one failure marker with hung teardown.

**Required fixes were explicitly NOT applied when the worker stopped:**

1. Refresh the exact saved job snapshot after missing-ID adoption. Cancellation
   currently compares a post-adoption row to a stale `job_id=None`/submitting dict;
   normal observer b5b changes only `job['job_id']`, leaving stale state for b6
   guards. Avoid discarding valid same-cycle terminal observations.
2. `observe_jobs` catches `ValueError` as scientific output validation failure.
   Mapper scope/history conflicts must follow the existing generic backend error
   path, not fail scientific stages. Wrap only mapper errors appropriately and
   test rollback/no scientific failure; keep real validator errors unchanged.
3. Recheck parent cancellation inside the observation write transaction before
   stage/artifact readiness changes, preserving the existing cancellation policy.

Keep exact identities, known terminal accounting and legacy settlement semantics.
Do not infer scheduler absence, retry uncertain submissions, map pseudo-jobs as
external work, or introduce new schema/config/reasons.

## Remaining design constraints

Read `t1-phase2-source-reduction-design.md` before adding more abstractions. One
focused Astra review found only225–425 evidenced consolidation lines and no
funded forecast for the full remaining net-negative requirement. This is **not a
waiver or proof of impossibility**. Preserve the owner's Phase 2 net-negative
constraint, use measured replacement/delete-first cards, and do not omit features,
weaken safeguards, move implementation out of the accounting set or claim T1 done.
At620f91b production delta from42829f4 was +2030/-1837 =+193 across
`src gateway/src web/src scripts recipes`; b2b adds98 net source lines.

Read-only preparation notes, not implementation approval:

- `t1.3c-reason-matrix.md`: concrete reason/source families, notices, dynamic
  Autopilot values and bounded typed-mapping expansion. c1a currently supports
  only ten source-less admission pairs; terminal/cancellation/source-backed
  families and payload-version compatibility still need implementation.
- `t1.3b-transfer-publication-map.md`: b7–b9 writers/events/readiness. Transfers can
  replay a complete row's operation without marking it pending; nonterminal
  downloads allocate fresh UUIDs after uncertain failures. Decide responsibility,
  replay and cancellation semantics conservatively before wiring a naive mapper.
  Report readiness remains distinct from remote artifact delivery.

The old `apply-b2b-scoped.py` script and h7b staging patch have already been used.
Do not run either again. No scientific data, credentials, live operations or
installation records were changed or published.
