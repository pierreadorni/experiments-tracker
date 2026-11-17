# T1 final acceptance — 2026-11-17

T1 is complete **under the owner's approved scope cut** (§8 of
[t1-owner-decisions.md](t1-owner-decisions.md)). This is source/test acceptance,
not installation or live-research acceptance. No Slurm, managed-account,
installation restart, private gateway deployment or phone access was authorized
or performed.

## Original reviewed tree

Original final implementation commits (T1.8 close-out follows below):

- `349a0c2` — T1.6a-4a: mechanical connection vocabulary and removal of
  `pool_owner` aliases; intermediate clones retained only until a-4b.
- `cefa44e` — T1.6a-4b: explicit `AgentSlot` connection/task state; removed
  per-connection `Workflow` construction and manually shared owner fields.

One `Workflow` now owns the store, gate, projects, controllers, resolver and
shared registries. Slots hold their agent transport, readiness, task and quota
refresh state. Dispatch and callbacks receive the assigned slot explicitly;
fixed primary compatibility properties read/write canonical slot-zero state.
No generic proxy, Workflow subclass or temporary mutation of the owner's agent
is used to dispatch concurrent work. The new regression checks shared ownership,
absence of Workflow clones, canonical primary state and agent replacement.

## Evidence

Coordinator ran `make check` on the final tree in the isolated
`.worktrees/t1-validation` checkout with Node 24.18.0 and locked dependencies:

- Ruff check and format: pass.
- Python: **866 passed**, one dependency deprecation warning, 469.35 seconds.
- Web format and tests: **29 passed**.
- Gateway build/type checks and local D1/R2 tests: **45 passed**.
- Durable local log: `check-results/t1-final-make-check.log` in that validation
  checkout (ignored, not published).

The retained validation checkout has the local fixture required by the real
synthetic-demo handover test. That test passed in the full check; fresh-worker
checkout failures from missing untracked `recipes/` were not treated as passes.

Source/test searches found no remaining `pool_owner`, `.lanes`,
`blocks_dispatch`, `spawn_agent`, `agent_assignments`, `STUCK_SQL` or
per-slot Workflow construction. Archived readers and explicit operator
reconciliation remain intentionally supported.

The scenario matrix covers mixed-role overlap and independent results, one-slot
and typed-owner serialization, same-owner title-to-implementation progress,
lost-start restart and 2→1 shrink with debt, readback at exhausted capacity,
production callback receipt against a stub transport, binding-denial release,
exact per-process quota interruptions and full-slot shutdown. Direct-SQL
accounting/routing fixtures are labeled; they are not dispatch integration or
live runtime evidence. Unsupported project lost-start adoption is not invented:
unknown outcomes retain their exact saved identities and block duplicate turns.

## Replacement/accounting

Owner §7's named retirements are satisfied: interim watchdog heuristic deleted;
outer API tick loop deleted; global role blockers replaced by typed owner and
conservative capacity admission; cloned Workflow lane scaffolding deleted.
The only deferred compatibility authority is the contract-3 hold/stage engine
retired by T4.8 under §8; T1.3c writer migration and T1.7 mechanical engine splits
are explicitly deferred rather than claimed implemented.

Per-commit final retirement counts:

| Card | Source +/− | Tests +/− | Changed lines |
|---|---:|---:|---:|
| a-4a | 136 / 137 | 62 / 63 | 398 |
| a-4b | 179 / 133 | 229 / 170 | 711 |

The coordinator approved two individually green mechanical cards rather than
one oversized card. Original acceptance production delta from `42829f4` across
`src gateway/src web/src scripts recipes`: **+5,175/−2,492 = +2,683**.
After T1.8 close-out it is **+5,203/−2,596 = +2,607** (net −76 since
`7046b07`), still above the previously reported +2,500 notification threshold.
The former net-negative target was waived by owner §7; the +2,500 notification
threshold was reported during b-2 and the final count is recorded here.

## T1.8 legacy-deletion close-out

Post-acceptance review at `7046b07` found dominated legacy branches and frozen
rechecks still present despite the T1.1d–h status. T1.8 closes that gap, without
altering archived readers, saved identity settlement or the deferred hold writers.
[The inventory](t1.8-site-inventory.md) proves saved execution-contract immutability,
records every baseline site and lists every survivor at its final-tree line.
Today's creator does not establish the shape of saved historical records: the
coordinator rejected that initial inventory inference and preserved reachable
terminal-title settlement, recovery/repair guards and historical observation.

| Card | Commit | Source +/− | Tests +/− |
|---|---|---:|---:|
| a: inventory / Store immutability regression | `7ff0a11` | 0 / 0 | 85 / 0 |
| b: 21 always-true predicate collapses, orphaned initial-report writer retirement | `e02aa91` | 78 / 114 | 7 / 5 |
| c: 13 dispatch/title recheck deletions, saved-fixture test replacement | `a57ab65` | 0 / 27 | 65 / 57 |
| d: 11 advancement recheck deletions and close-out docs | this close-out commit | 5 / 18 | 0 / 0 |

The docs/test-only a is production-neutral; all deletion cards are net-negative
and within their source/test budgets. No production behavior is added. Archived
queued reports retain their revision-filtered SQL arms; conversation readers,
legacy remote scheduler identity selection and six saved historical observation
branches remain. `Store.initial_report` lost its last production caller and was
deleted; the obligation/idempotence test uses the existing `working_report` API.
The two cross-await contract-flip test parameters construct an impossible state
and are replaced by stronger saved-legacy-state coverage of exact identity/raw
result, no advancement, no dispatch and replay safety (§7.3 of the inventory).
No saved-fixture settlement or cancellation assertion was dropped.

Coordinator acceptance on byte-identical source/tests/scripts in the isolated
`.worktrees/t1-8-validation` checkout, Node 24.18.0:

- Locked `make setup` passes; no manifest changes.
- Contract-3 synthetic demo passes before edits and again on the final combined
  tree (about 3 s); nonempty report and SVG prediction figure inspected, synthetic
  labeling present. Final log: `/tmp/t1-8-final-demo.log`.
- Per-card archive/fixture/conversation/audit/scenario targets pass.
- Combined Ruff check and format pass; **866 Python tests pass**, one dependency
  deprecation warning, **453.80 seconds**. Log: `/tmp/t1-8-combined-pytest.log`.
- The validation checkout retains the untracked `recipes/` directory required by
  the synthetic maintenance handover fixture; that test passes, unlike fresh
  worker checkouts missing the directory.
- Final searches: **17** `iteration.enabled` calls and **28** `execution_frozen`
  matches (27 calls + definition), all READER/SETTLE/ENTRY or the definition;
  the final site table matches the tree.
- Web/gateway unchanged; prior 29 web / 45 gateway results remain historical,
  not newly rerun. The card does not require `make check` for Python-only edits.

No live activation, scheduler or managed-runtime experiment, deployment or owner
restart follows. The phase remains +2,607 production lines above `42829f4`, not
net-negative; the owner waiver and prior +2,500 notification still apply.

## Continuing work

Phase 3 is the next planned phase, not part of this acceptance. Code publication
does not authorize activation on an installation. Managed runtime/JSONL process
behavior, real Slurm authorization/accounting, hosting and physical-device
acceptance retain their separate owner-authorized evidence requirements.
