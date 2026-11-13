# T1 handoff (state at main after T1.1m-M0, 2026-11-13)

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
f716898 · T1.1m-M0 9426245 (shared helpers moved into `tests/helpers.py`).

## Current task: T1.1m (fixture migration before archival)
Plan and Opus decisions: `notes/t1.1m-disposition.md` §7 (sequencing "retire at the
gate") and **§8 (final; supersedes §5 and §6)**. 122 tests drive the legacy branch of
`Workflow.advance`; the per-test disposition (CONVERT / READER / ANALOGUE / RETIRE-ONLY)
is in §1.

Order: M0 (done) · **M0b next** · M1 · P1 · M2 · M3 · M4 · M5 · M6 · M7a/b · M8a-d ·
M9-r1..4 · M10 · gate probe · T1.1b-1. M9-r1..4, M10 and T1.1b-1 are pushed together so
legacy code is never reachable without its tests.

**M0b (not done).** A worker was stopped mid-run; nothing from it is committed except a
partial, unvalidated probe patch. M0b is a read-only investigation: widen the probe, dry
run the full suite with the downgrade disabled, classify every extra legacy-dependent
test, and append the result as `## 9. M0b addendum` in `t1.1m-disposition.md`. The spec
is in §8 ("Amended card order" and the gate-probe paragraph). The partial probe is
`notes/t1.1m-m0b-partial-probe.patch`: a `tests/conftest.py` autouse fixture that logs
nodeids when a legacy mechanism runs on a non-contract-3 experiment, plus an
`M0B_NO_DOWNGRADE=1` switch for the dry run. Apply it in a scratch worktree only and
never commit it. Re-launch one Flash worker in a fresh worktree (or do it yourself). The
dry run takes minutes because it runs the full suite; this is not a hang.

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
