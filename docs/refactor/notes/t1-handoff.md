# T1 handoff (T1.1m migration in progress, 2026-11-13)

A clean-context agent continues from here. Read, in order: `docs/refactor/README.md`
(rules, size budgets, commit style, checks), `docs/refactor/todo.md` (cards, log),
`notes/t1-owner-decisions.md` (owner approved decisions 1-6), then the card's note.
Root `AGENTS.md` applies (contract 3 only; no scientific hashing/inventories/freezes).

## Owner instructions in force
- Continue T1 (todo Phase 2) in order. Coordinator = you: write briefs, review diffs,
  run checks, commit, push, update the todo log. Implementers are `deepseek/deepseek-flash`
  subagents (agent `worker`, `model: deepseek/deepseek-flash`, own git worktree under
  `.worktrees/<slug>`, branch `refactor/<slug>`); call `subagents_enable` first.
- **Any design or sequencing question: ask a fresh `reviewer` subagent with
  `model: anthropic/claude-opus-5-5` (read-only; give file paths and the question)
  instead of asking the owner.** Escalate to the owner only if a safety property cannot
  get a contract-3 test, or production behaviour must change beyond the README rule
  "No new ceremony". S-sized cards: write your own plan, then get Opus's final decision.
- Browser checks: Node 24 + Playwright work with a workaround. Create
  `/tmp/pw-browsers` containing symlinks `chromium-1243 -> ~/.cache/ms-playwright/chromium-1248`
  and `chromium_headless_shell-1243 -> ~/.cache/ms-playwright/chromium_headless_shell-1248`,
  then set `PLAYWRIGHT_BROWSERS_PATH=/tmp/pw-browsers`. Run demo servers only in a
  worktree with its own `.local/` (`experimenttracker --config .local/demo.yaml init`,
  then add `port: 18765`). Never touch the main checkout's `.local/`, live state,
  Slurm or the gateway.
- Do not use `pkill -f` with a pattern that appears in your own shell command line (it
  kills the shell).
- Code publication does not authorize live activation. Phase 2 production source must
  end net negative overall (additive intermediate cards are approved).

## Landed on main (pushed)
T1.1a 42829f4 · T1.2a 864ba91 · T1.3a d644089 · T1.1c-1 b6c1297 · T1.2b (1a 9b7bc8f,
1b 9bbef58, 2 7e939ac) · T1.1c-2 19ab387 (browser smoke on contract 3; all smoke scripts
except iteration-smoke migrated) · T1.1m disposition note cbb7c16 · final T1.1m plan
f716898 · T1.1m-M0 9426245 (shared helpers moved into `tests/helpers.py`) · seeded-state browser smoke correction 18b5b33 (empty and CLI-seeded synthetic states, full Node 24 checks pass).

## Current task: T1.1m (fixture migration before archival)
Plan and Opus decisions: `notes/t1.1m-disposition.md` §7 (sequencing "retire at the
gate") and **§8 (final; supersedes §5 and §6)**. 122 tests drive the legacy branch of
`Workflow.advance`; the per-test disposition (CONVERT / READER / ANALOGUE / RETIRE-ONLY)
is in §1.

Order: M0 (done) · M0b investigation (running) · M0c (temporary marker prerequisite) · M1 · P1 · M2 · M3 · M4 · M5 · M6 · M7a/b · M8a-d · M9-r1..4 · M10 · gate probe · T1.1b-1. M9-r1..4, M10 and T1.1b-1 are pushed together so legacy code is never reachable without its tests.

Fresh Opus decision `09a27616` allows M0c/M1/P1/M2 and bounded M3–M8d conversions to land while M0b finishes. Keep its disposition note frozen until the addendum. Retirement, M10, archival and deletion remain blocked until the addendum is committed/Opus-approved, every baseline nodeid is mapped, and the widened probe is rerun on then-current main. Before M3+, check the provisional widened list for the card's files. M0c registers `current_contract` in conftest only, with no initial uses. Mark converted functions individually and prove contract 3; remove all marker uses and plumbing in M10.

**M0b (running, pinned `34f29ec`).** The partial probe was corrected (its dependencies wrapper must await the original, not replace it); the transparent widened baseline passes all 564 tests and records 153 nodeids (31 extras; advance itself records 121). A full downgrade-disabled suite is running in its own scratch worktree and can spend many minutes in historical test waits; elapsed time alone is not a hang. Supplemental cleanup probes and the per-nodeid classification remain pending. Append final findings as `## 9. M0b addendum` in `t1.1m-disposition.md`, with pinned ref and mappings for any meanwhile-landed conversions. Spec: §8. The old partial patch is investigation-only; never commit probe/env switches. Retirement must wait for completed and reviewed findings.

Card facts to keep (all in §8):
- P1 is a small PRODUCTION change: discussion follow-ups are created as
  `agent_experiment_v1` (`conversation.py:306`). M8c and M8d may need production cards.
  Production changes need F+R review.
- RETIRE-ONLY applies only to: toy snapshot-hash equality, the frozen `requirements.txt`
  check, local checkpoint hashing checks, and the validate stage / `strict_reproducibility`.
- ANALOGUE retirement must cite a contract-3 test and assertion. A weaker analogue is
  strengthened first (that part is a CONVERT).
- M1 builds a frozen legacy completed fixture under `tests/fixtures/legacy_completed/`,
  generated once from current code BEFORE any legacy code is removed. `@ROOT@` replaces
  the temporary root only in `artifacts.path` and `jobs.execution_root`. It adds no new
  hashes or inventory. The loader is `tests/legacy_fixture.py`.
- Gate: with M10 applied the full suite is green and the widened probe records zero
  legacy-mechanism nodeids.

## After T1.1m
- T1.1b-1: archival with hold `legacy_contract_archived`, an existing inbox kind, the
  inventory §15 / §17.4 guards, and tests on an explicit archived fixture inserted
  directly into saved state.
- T1.1d-h: legacy deletion; they retire the tests listed in the M0b addendum.
- Then T1.3b-d, T1.4a/b, T1.5, T1.6a-c, T1.7. S-card designs are Opus-approved in
  `notes/t1.*-design.md`. T1.1b, T1.3b-d, T1.4 and T1.7 have preflight maps only
  (inventory §15-17). T1.3b source order: agent_actions, jobs, transfers+publication,
  pending_request, plan_review. `Store.update` lacks `_db=`, so it needs a transaction
  variant. T1.7 areas were measured over budget; remeasure after the legacy deletion.
- When all of T1.1 is done, update the "Future brief-driven experiments" line in root
  `AGENTS.md` (contract 3 only; legacy archived; new demo; obligations/pool direction).

## Process that worked (keep doing this)
1. `git worktree add -b refactor/<x> .worktrees/<x> HEAD`; `uv sync --locked`; one Flash
   worker per worktree with the README brief template (edit boundary, size budget, stop
   conditions, exact validation, "do not commit").
2. Review the diff yourself (a fresh Opus code review for risky or F+R cards). Apply the
   worker's `git diff` to main with `git apply`. Run `uv run --locked ruff check` and
   `ruff format --check` on `src tests scripts`, the targeted tests, and the full
   `uv run --locked pytest -q` (564 tests at M0). Run `make check` when web or gateway
   code changed.
3. Commit one line `<type>: <what> (<ID>)` and push to origin main. Then a docs commit
   `docs: record <ID> (<task>)` updating todo status and the log with numstat (source vs
   tests/docs). Remove the worktree and branch.

## Pitfalls
- `tests/conftest.py` strips `execution_contract` from every experiment created in test
  files not named `test_iteration*`. Helper modules must not be named `test_*`.
- The 122-test probe only covers `advance`; other legacy-dependent tests exist (M0b).
- `.worktrees/` is untracked in main and holds a pre-existing `t1b-brief.md` (not mine);
  leave it. No worktrees of mine should remain.
- Do not edit `.local/`, run live restarts, Slurm or managed-model calls, or deploy the
  gateway.
