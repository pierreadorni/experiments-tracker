# T1 handoff (after M0b and M5, 2026-11-13)

Read `docs/refactor/README.md`, `todo.md`, `notes/t1-owner-decisions.md`, then the
current card's note. Root `AGENTS.md` applies. Source publication is not live activation.

## Owner instructions in force

- Continue T1 in order. Coordinator writes briefs, reviews, checks, commits and pushes.
  Implementers: `worker`, `model: deepseek/deepseek-flash`, own worktree under
  `.worktrees/<slug>`, branch `refactor/<slug>`; enable subagents first.
- Any design or sequencing question goes to a fresh read-only `reviewer`,
  `model: anthropic/claude-opus-5-5`, not the sleeping owner. S-card plans need its final
  decision. Ask the owner only for an untestable safety property or production behaviour
  beyond the README's approved boundaries; prefer bounded bug cards to losing coverage.
- No main `.local/` edits, live owner restarts, Slurm, managed-runtime calls, gateway
  deployment or unrelated checkouts. Demo servers use a worktree's own generated config.
- Every source/test card stays below its line budget, counting moves/deletions; generated
  fixture data is excluded. Phase 2 production source must end net negative overall.
- Never use `pkill -f` with a pattern present in the invoking shell command.

## Landed and pushed

Earlier T1: T1.1a `42829f4`; T1.2a `864ba91`; T1.3a `d644089`; T1.1c-1 `b6c1297`;
T1.2b `9b7bc8f`, `9bbef58`, `7e939ac`; T1.1c-2 `19ab387`.

Current migration:
- M0 `9426245`: shared helpers in `tests/helpers.py` (old imports re-exported).
- Browser correction `18b5b33`: reference picker matches the UI title and asserts saved
  reference identity. Empty and CLI-seeded synthetic smoke pass; full Node 24 checks pass.
- M0c `f3fca77`: temporary `current_contract` marker, conftest registration only.
- M1 `96e0812`: frozen legacy fixture, six reader migrations, five loader tests.
- P1 `b387af4`: discussion follow-ups always create `agent_experiment_v1`.
- M2 `09c4d9b`: all 17 discussion reader nodeids load saved evidence.
- M3 `566ea41`: 14 workflow nodeids on contract 3; lost-start/lost-completion agent-stage
  recovery preserves saved identities and never duplicates dispatch.
- M4 `d440fb8`: nine steering, fresh-implementation-thread and saved-title tests on contract 3.
- M5 `86b7e59`: branching DAG execution on contract 3; old reader-model constants retained.
- M0b `7254d2e`: final widened-probe addendum, Opus-approved with coordinator amendments.
- P-a `5fc3004`: ticking API create fixtures now explicitly keep contract 3.

Latest source validation: Ruff and all **571 Python tests pass**. M1's final isolated
`make check` also passed 29 web / 41 gateway tests. No live acceptance or activation.

## Current task and order

**M4b and M11 next; P-a has landed.** M4b's candidate is in its own worktree and needs
coordinator final-main checks/review under its approved §10 plan. M6's candidate is ready
but lands only after M4b/M11/P-a, with fresh review and rebased checks (§11).
Preserve old schema-2 DAG constants/invalid-graph unit tests for their remaining importers.

M11's declared-source allocation re-hash is deliberately legacy-only (Opus `8d70bb3e`):
no production bug card or new runtime guard. Keep that test on explicit static schema-2
state until h deletes the legacy worker body, and add a current-contract manifest-identity
failure test. Its scope includes one importer line in `tests/test_iteration.py`:
`work.mkdir(exist_ok=True)` because the new helper creates it. See the decision's §12.

The authoritative migration plan is disposition **§8–11**; §9 amends sequencing and
retirement. After M5, prepare M4b (compact reference), M11 (remote transport) and P-a
(ticking API producers), then M6, M7a/b, M8a–d and M8e. The three added cards may proceed
any time after M0, but all block M10/gate. Explicit static legacy fixture rewrites also
precede M10. Remeasure/split all retirement cards: r1, r2, r3a (local outputs), r3b
(resources), r4, r5 (M-sized temporary-output prompt/validation work, not live cleanup).
M9-r*, M10, the gate and T1.1b-1 still ship in **one push**. P-b precedes toy-create
removal in h. Do not treat classification as permission to remove live saved-state coverage.

## M0b facts and acceptance boundaries

- Pinned source `34f29ec`: transparent baseline 564 pass, 1126 records / 156 nodeids
  (132 entrypoint / 24 non-gate); `advance` past early returns accounts for 121.
- Original no-downgrade full run: 76 failed / 488 passed in 1797.49 s. It survived the
  worker's harness deadline and was not rerun. Its final probe has 568 records / 92 nodeids,
  including 58 toy-chain gate hits. Toy creates never had the downgraded contract flag.
- M1/M2/M3 have moved 37 of those 58 off execution; 21 remained at that mapping point.
  These are pinned measurements, not a current-main gate result.
- The final probe is **uncommitted** at `/tmp/m0b/m0b-probe-final.patch`; raw logs and
  JSONL are beside it. The tracked old partial patch is not valid gate evidence. Its
  mutation-only repair detector was insufficient; the worker's initial rewrite also
  introduced a non-transparent dependencies wrapper. Final attribution/corrections are §9.1.
- Before retirement, account for every baseline nodeid and rerun the widened probe on
  current main, rebased onto M0c conftest. Final gate zero is for §8 execution entrypoints,
  not retained reader helpers. Report cleanup, legacy observation/reconciliation and
  reader calls separately with **explicit static fixture origin** (G1–G4 in §9.6).
- Keep the three cleanup tests until h.5 deletes that live historical path. Keep legacy
  recovery refusal/rollback tests until the `1695-1703` branch is deleted: failed is
  terminal and not covered by the non-terminal archive sweep. **M8d adds separate
  contract-3 counterparts**, not an in-place conversion of their legacy fixture.
- Keep sealed restart `[True]` through M10/gate. T1.1b-1 explicitly decides whether it stays
  reachable (keep until f/h) or retires with a cited late-settlement replacement.
- Keep ten schema-1/2 model-validation tests for archived readers. Replace their old
  `test_generic.PLAN` import with static data when that module retires.

## Card facts to preserve

- `load_legacy_completed(store, workflow)` from `tests/legacy_fixture.py` copies one valid
  fixed-UUID fixture; seven saved tables, no events/outbox or pre-registered histories.
  Historical hashes/tree bytes are retained. Ruff excludes the frozen root. Generator
  removal belongs to the M9-r grouped push, after all required fixture generation.
- Mark converted functions individually with `current_contract`; prove contract 3 via
  `prepared()` or an explicit assertion after create. No manual contract restoration in
  CONVERT tests. M10 removes all marker uses/registration and the global downgrade.
- M4b's measured genuine schema-3 prompt is 12902 characters. It is test-only: retire legacy
  full contract/manifest and reference `verify_snapshot` assertions, not meaningful current
  context guards. Assert the existing pointer's full plan/analysis/brief/output rows, SHA,
  stable bytes and corruption refusal. Do not add audit history to agent context; keep 25 KB.
- M11 preserves all seven remote identity/transport/polling/admission guards; drop reused
  checkpoint byte re-checks outright. Contract 3 has no scientific inventories/hashes.
- P-a handles TestClient's real ticking loop **before the gate**. P-b cannot just replace
  recipe literals: stage/job foreign keys require a schema-3 graph or explicit saved history.
- M8e tests current declared-output/path safety only; no recursive child checks, new
  production restrictions, reasons or scientific hashing. See the exact §9.5 scope.
- M7a/M7b include the newly found prompt/catalog and five-GPU-stage budget tests. M7a may
  add an explicit current-contract writable-project discussion check if it fits; M2's old
  writable-project path had only incidental coverage, not an asserted write grant.
- P1 is the sole production change so far in this continuation (+1/−1). M8c/M8d may need
  separate production bug cards, each with fresh F+R review. Do not drop safety assertions.

## Checks and continuation process

1. One Flash writer per worktree; exact file boundary, budget, validation and “do not commit”.
2. Review, apply by file, run locked Ruff check/format, targeted tests and the full suite on
   the final rebased tree. Fresh Opus review for F+R/risky cards. `make check` for web/gateway
   changes, in an isolated Node 24 checkout. Python subprocess tests run outside NSS sandboxes.
3. Commit `<type>: <what> (<ID>)`, push main, then record source vs tests/docs numstat and
   evidence in a docs commit. Remove completed worktrees/branches; leave pre-existing
   `.worktrees/t1b-brief.md` alone.
4. Browser workaround: `/tmp/pw-browsers/chromium-1243` → installed `chromium-1248`, and
   matching headless-shell symlink; set `PLAYWRIGHT_BROWSERS_PATH=/tmp/pw-browsers`. Only
   synthetic worktree demo servers, never main state or live ports.

After T1.1: update root AGENTS future-contract wording, then T1.3b–d, T1.4, T1.5, T1.6,
T1.7. Existing S plans are in `notes/t1.*-design.md`; remeasure T1.7 after legacy deletion.
T1.3b source order: actions, jobs, transfers/publication, pending request, plan review;
`Store.update` needs a transaction variant. None of this authorizes live activation.
