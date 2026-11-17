# T1 final acceptance — 2026-11-17

T1 is complete **under the owner's approved scope cut** (§8 of
[t1-owner-decisions.md](t1-owner-decisions.md)). This is source/test acceptance,
not installation or live-research acceptance. No Slurm, managed-account,
installation restart, private gateway deployment or phone access was authorized
or performed.

## Final reviewed tree

Final implementation commits:

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
one oversized card. Final production delta from `42829f4` across
`src gateway/src web/src scripts recipes`: **+5,175/−2,492 = +2,683**.
The former net-negative target was waived by owner §7; the +2,500 notification
threshold was reported during b-2 and the final count is recorded here.

## Continuing work

Phase 3 is the next planned phase, not part of this acceptance. Code publication
does not authorize activation on an installation. Managed runtime/JSONL process
behavior, real Slurm authorization/accounting, hosting and physical-device
acceptance retain their separate owner-authorized evidence requirements.
