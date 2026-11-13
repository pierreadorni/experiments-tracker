# T1 handoff (state at main b6c1297)

Read first: `README.md` (rules), `todo.md` (cards, log), `notes/t1-owner-decisions.md`
(owner approved decisions 1-6 on 2026-11-13), then the card's `notes/t1*-design.md`.
Goal given by the owner: complete the T1 stage (todo Phase 2). Use
`deepseek/deepseek-flash` subagents for F and F+R cards (no versioned "Flash 4.1" id
exists in the registry); for S cards write your own plan, then get a fresh-context
`anthropic/claude-opus-5-5` reviewer's final decision. After all of T1.1 is up, update
the "Future brief-driven experiments" line in root `AGENTS.md` to reflect the refactor
(current contract-3 only; legacy archived; new demo; obligations/pool direction).

## Landed on main (all pushed)
T1.1a inventory 42829f4 · T1.2a clock 864ba91 · T1.3a obligations (schema 24) d644089 ·
T1.1c-1 contract-3 demo b6c1297. Owner-approved rollout docs 006a39f.

## S-card designs, Opus-approved (no re-review needed unless scope changes)
T1.2b (split 1a/1b/2), T1.3a (done), T1.5 (a/b/c), T1.6a (a-1..3), T1.6b (b-1..3),
T1.6c (c-1..3), T1.1c. T1.1b/T1.1m/T1.3b-d/T1.4/T1.7 have preflight maps only
(inventory §15-17 for archival; T1.3b order: agent_actions, jobs, transfers+publication,
pending_request, plan_review; `Store.update` lacks `_db=` so needs a tx variant; T1.7
areas measured over budget, remeasure after legacy deletion and split). They need your
own plan + Opus decision (S) or a Flash brief (F/F+R).

## In flight / next
- **T1.2b-1a candidate**: local branch `refactor/t1-scenario-primitives`, worktree
  `.worktrees/t1-scenario-primitives` (untracked dir; commit d767a35 is local only).
  488 test lines, 7 tests pass, UNREVIEWED. Review against design note, then land.
  Then T1.2b-1b (driver/fixture/lost-submission port), then 1b-2 ports.
- **T1.1c-2** browser half: `web/src/main.tsx:1654` still sends `toy_compare_v1` in demo
  mode; smoke scripts (`smoke.mjs`, `progress-smoke.mjs`, `maintenance-smoke.mjs`,
  `autopilot-smoke.mjs`, `cancellation-smoke.mjs`, `scripts/gateway_acceptance.py` done)
  must approve the plan, use final report version, `evidence/<id>/<name>` ZIP paths, drop
  frozen-recipe reproduction check. Needs Node 24 `make check` + browser smoke.
- Then T1.1m fixture migration (preflight: C0 marker foundation -> C1..C5; retire
  `tests/conftest.py` historical downgrade), then T1.1b archival, T1.1d-h deletion,
  T1.3b-d, T1.4, T1.5, T1.6, T1.7. Do not activate archival before T1.1c-2 and T1.1m.

## How cards were run (keep doing this)
1. `git worktree add -b refactor/<x> .worktrees/<x> HEAD`; one Flash worker per worktree,
   brief per README template (edit boundary, size budget, stop conditions).
2. Review the diff yourself; for risky cards get a fresh Opus code review (it caught real
   bugs every time). AST-compare mechanical cards.
3. Verify in an isolated copy (not the live checkout): `make setup`, Ruff
   check/format, targeted + full `uv run --locked pytest -q`, `make demo`, Node 24.
4. Apply the diff to main, commit one line `<type>: <what> (<ID>)`, push, update todo
   log with numstat (source vs tests/docs). Phase 2 production source must end net
   negative overall; additive intermediate cards are owner-approved.

## Pitfalls
- Root `docs/refactor/README.md` has uncommitted owner edits: stage only your own hunks.
- `tests/conftest.py` strips `execution_contract` from every test file not named
  `test_iteration*`: new contract-3 tests need that name prefix.
- Never touch `.local/`, live state, Slurm, gateway deploys. Code publication does not
  authorize live activation.
