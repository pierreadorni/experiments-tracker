# Phase 2 source reduction: replacement design and feasibility gate

Read-only review, 2026-11-17. Committed baseline `620f91b`; the inspected working
tree additionally has T1.3b2b action-obligation wiring. No runtime, validation,
installation or live-state changes were made. This note does not approve live
activation, change the owner objective, or mark any T1 implementation complete.

## Decision

**Revise the implementation strategy to replace shared mechanisms, not layer a
new engine over each old role. Retain the Phase 2 net-negative requirement.**
There are concrete consolidation opportunities within C2/C4/C5. There is no
inspected deletion reserve sufficient to certify that all remaining T1 work will
finish net negative. Treat that as an unresolved feasibility constraint, not as
permission to waive the target, omit a feature, or stop independent authorized
work. A mechanical C11 split cannot pay for new behavior.

The accepted design notes specify behavior and safe intermediate states. Their
estimated card sizes do not establish a phase-wide source budget. Their staged
wrappers must have explicit retirement points; otherwise full C2, resolver,
runner and AgentPool implementation will accumulate alongside their predecessors.

## Measured starting point

`git diff --numstat 42829f4 620f91b -- src gateway/src web/src scripts recipes`
reports +2030/-1837, **+193 production-path lines**. Keep that baseline and path
set. Count new files, packages, moved implementation and deletions consistently;
report tests and documentation separately. In particular, do not obtain credit
by moving implementation to a directory outside the accounting set.

The coordinator's pending wiring estimates are b2b +98, b3 +28, b4 +25 and b5b
+22, totaling +173 before b6 and other remaining cards. They are estimates, not
committed measurements. If realized, the phase starts its remaining work at +366.

The already-landed deletion reserve has been spent: `workflow.py` is -669,
`artifacts.py` -174, the shipped recipe -167 and `remote_worker.py` -131 against
the baseline. The five new C2 modules total 827 lines. The demo and archive
settlement add necessary behavior. Neither their existence nor their tests is
an excuse to delete them. No second obsolete executable research engine was found
in the reviewed remaining production code.

## Real replacement seams

The ranges below are **local net saving estimates after a shared replacement and
its callers**, before the cost of new mandated behavior. They are not committed
credits, independent implementation promises, or permission to remove a guard.
Review actual diffs; avoid counting the same lifecycle statement twice.

| Seam and inspected source | Replacement and retained distinction | Estimated local net saving |
|---|---|---:|
| `Workflow.dispatch`, `dispatch_title`; `Conversation.dispatch` | One caller-transaction action-transition primitive for read/check/update/sync/event/returned saved row. Keep each caller's extra experiment/discussion writes in that same transaction; preserve exact expected state/identity and result comparison choices. | 35–70 |
| Readback branches in `Workflow.agent_stage`, `advance_title`, `Conversation.advance`, `Controller.advance`, `ResearchController.advance`, `ProjectSupervisor.advance` | One transport/readback outcome operation, with explicit source adapters and role-specific application. Remove old role readback branches once all their callers use it. Preserve terminal versus unknown failures, completed versus received, frozen settlement, throttles and exact identity. | 60–100 |
| Five dispatch bodies, seven admission sites, model selection and quota checks across the above classes | Shared C4 selection/binding quota and managed model/effort validation; one turn lifecycle only where policy is actually common. Preserve preparation, authority after awaits, prompt/schema/session/workspace differences and role-specific rejection effects. | 90–150 |
| `Workflow.start_agent_pool`, `agent_lane`, tick task drainage, quota interruption and close; research/supervisor close | Retire cloned-Workflow routing and duplicate drain/close loops when reservations and slot owners replace them. One slot loop, one connection lifecycle, existing threads drained separately. | 20–45 |
| `core/obligation_sources.py`, `job_obligations.py`, `action_obligations.py`, `advancement_decisions.py` | Share validated episode-history retrieval, terminalization and transaction precondition. Consider a small strict payload type for repeated shape checks, only with equivalent rejection semantics. | 20–40 |
| Existing no-ID job adoption in `Workflow.observe_jobs` and `legacy_archive.settle_job`; interim watchdog | C5 owns no-ID reconciliation/adoption once; observers delegate. Replace the heuristic watchdog, retaining notice formatting/deduplication. New resolver/liveness functionality costs more than these small deletions. | 0–20 |

Total **225–425 locally recoverable lines**, with uncertainty and no new-feature
cost included. Budget against the low end until measured. These estimates are
deliberately much smaller than the enclosing methods: the five dispatch methods
alone span roughly 630 lines in the inspected tree, but their role-specific
preparation, permissions, result handling and persisted events are not duplicate
code that can disappear.

Specific limits matter:

- `ResearchController.state` and `Knowledge.state` look similar but differ in
  replay behavior, allowed fields and event publication. Sharing SQL construction
  alone does not remove those contracts. Do not replace them with an unrestricted
  table/field writer or make project obligations part of experiment C2 by accident.
- `obligation_sources._history` checks canonical episode numbers, contiguous
  history and scope. `job_obligations._check_saved_identity` additionally checks
  every episode's detail, owner/kind and scheduler wait. Return validated history
  once and reuse it; do not delete the stronger job checks. Action identity checks
  are not interchangeable with job transition proof.
- A strict typed advancement payload must retain exact keys, version-1 saved
  compatibility, bool rejection for integers, nonempty phase, positive evidence
  revision, binding/generation checks and closed reason/wait validation. A
  coercing model or a permissive catch-all dictionary is not an equivalent saving.
- `Workflow.refresh_quota` repeats interruption on success and refresh failure;
  factor the common denied-policy branch without suppressing interruption after
  failed refresh. Fresh explicit admission remains immediately before dispatch.
- Scheduler terminal-state lists are not presently identical (`REVOKED` differs).
  Do not unify constants by silently changing semantics for a few lines of credit.
- Archive settlement and active result validation have different permitted side
  effects. Share identity observation/adoption; retain the frozen no-advance path.

## Implementation architecture

**C2:** Keep one source mapper per actual source family, reusing the episode API.
Make transaction-local action changes reusable while the b writers are still
being migrated, rather than duplicating before/after row retrieval at every site.
Its caller supplies the existing transaction and narrowly allowed mutation; the
caller still controls accompanying source writes and event identity. Do not use
SQL triggers, an all-purpose event bus, implicit commits or a second state machine.
Use the same source mapper for runtime writes and backfill. Finish the complete
source matrix, advancement decision, operational reader, compatibility reader,
cancellation lifetime and watchdog required by the accepted C2 notes.

The c rollout temporarily has two representations, as approved. List the exact
compatibility assignments for removal at c4b; delete them when switching both
readers. The old physical column and historical read fallback remain. C2 cannot
claim a large saving from retiring writes: most one-line decisions are replaced
by a more explicit transition, not eliminated.

**C5/C4:** Separate three concerns: role preparation/application, durable source
transition, and transport ownership. A source adapter names an existing action
table and exact identity, maps its established states/events and validates current
authority. A slot owns a CodexAgent/process generation and transport operations.
The pool selects prepared proposals and reserves typed owners and capacity. Do
not give every role its own new pool, resolver or generic Workflow facade.

Use the common transport readback for the operator reconciliation API and runtime
recovery where their requirements agree. Retain the operator's historical prompt
proof and explicit-turn restrictions; the automated C5 absence proof is additional
behavior and must not inherit looser assumptions. The project-role adapters do
not acquire C5 automatic retry/absence policy just because transport is shared.

After C4 activation, roles retain proposal generation and result application,
but lose direct slot selection/task ownership and duplicated quota admission.
Remove the cloned Workflow infrastructure only after callbacks, replies, quota,
readback and shutdown all use reservations. Received results retain owner locks;
unclaimed possibly-live turns retain capacity debt. Neither task completion nor
connection loss establishes a terminal external outcome.

**C11:** The runner and manual tick must call the same extracted steps. Do not
leave a second tick implementation for tests. Preserve the approved manual
ordering, maintenance gates, submission lock, primary lock until C4, health set,
single bridge and thread-safe shutdown. Subsequent module moves remain mechanical;
count them as zero expected reduction and allow import/export overhead. Do not
turn T1.7 into unreviewed behavioral consolidation to recover a deficit.

## Bounded sequence and gates

Continue the approved dependency order. The replacements below are refinements
within the named feature cards, not permission to activate C4 before C2/C5/runner.
An inert helper alone earns no deletion credit; its first caller replacement
belongs in the same bounded card where practical.

| Ordered refinement | Concrete review boundary | Budget / acceptance |
|---|---|---|
| R1, C2 source integration | Transaction-local action mutation plus two existing dispatch-transition callers; migrate remaining callers in separate cards | Each <499 source+test lines; rollback, stale callback and replay proofs. Existing event names/payloads and surrounding source writes retained. |
| R2, C2 helper consolidation | Validated history reuse and terminalization; payload validation is a separate optional card | Each <149 source+test lines if S. Reject malformed/gapped/conflicting history and all formerly invalid payloads. Drop a proposed extraction if its measured result grows without serving required C2 behavior. |
| R3, finish C2 b/c/d | Existing source/backfill and derived-hold cards, using the shared primitives | Existing bounded cards remain mandatory. Remove temporary dual writes at c4b; no incomplete activation. Recompute the remaining phase budget before c activation. |
| R4, C5 | Common evidence readback/adoption and bounded source adapters; new absence/backoff semantics in their own reviewed cards | <499 per M card; no-ID observer removal accompanies ownership handoff. Race/lag/duplicate/absence and frozen-boundary proofs precede runner activation. |
| R5, T1.5 | Approved shared-step extraction, race groundwork, runner activation | Keep the existing split; subdivide where required. A mechanical move counts both deletion and addition toward the card size. Delete the production outer tick loop at activation. |
| R6, T1.6a/b | Approved reservation/routing and proposal/quota/capacity splits, each paired with named predecessor retirement | <499 per M card. All-role routing must be coherent before removal of global guards. Separate common model/lifecycle extraction from activation if it would exceed budget. |
| R7, T1.6c and T1.7 | Complete recovery matrix, then remeasure and mechanically split each remaining concern | Full acceptance preserved. A 250-line move already approaches 500 changed lines before tests/imports; first split oversized methods in bounded behavior-preserving cards. |

Each card brief must identify the old blocks to remove, new shared cost, preserved
policy distinctions and tests. The coordinator records actual production and
source+test churn before committing. Never count a future removal twice, or
subtract tests, comments or documentation from the production deficit.

The implementation checks remain the repository's targeted/full Python and Ruff
checks, plus Node 24 `make check` for web/gateway changes in an isolated checkout.
This design review runs no tests and makes no synthetic or live acceptance claim.

## Feasibility arithmetic and stopping rule

Let `D` be measured phase production delta, `A` the remaining additions after
their ordinary replacements, and `S` independently identified further savings.
Completion requires **D + A - S < 0**. Keep an explicit source-family ledger;
do not substitute changed-line card ceilings for estimates of net additions.

At committed +193, even the optimistic 425-line local saving leaves only 231
lines for all remaining new behavior. With the coordinator's +173 wiring estimate,
that becomes 58 lines. At the conservative 225-line saving, that pending starting
point is already +141 before any other feature. These figures exclude the new
source families/backfill, source-backed and terminal decisions, exact liveness,
automatic resolvers, independent runner and process-generation/turn-debt pool.
The inspected code provides no support for assuming all of those fit the remaining
allowance. This is an unfunded plan, not a proof that every possible implementation
is mathematically impossible.

At each R3/R4/R6 boundary, record measured `D`, completed deletions, remaining
mandatory behavior and concrete unspent deletion blocks. If the low-risk estimate
cannot meet the inequality, stop claiming a funded completion forecast and
re-audit before adding another optional abstraction. Continue necessary aligned
cards and actual replacements; the owner being unavailable is not authorization
to relax the target or declare T1 done. Do not stop all useful work merely because
the forecast remains uncertain.

No owner amendment is needed for the replacements above. If complete behavior
and all justified in-scope consolidation ultimately still leave `D >= 0`, T1 is
not complete under the current instruction. The smallest possible *future*
objective amendment would explicitly waive the Phase 2 net-negative criterion
for the measured residual `D`; it is not granted or recommended by this note.
Alternatively the owner could approve a separately quantified consolidation
scope extension. Moving the target to a later phase, borrowing hypothetical C7
deletions, deleting historical readers or silently deferring C2/C4/C5 is not an
authorized substitute. Do not ask for that hypothetical decision now.
