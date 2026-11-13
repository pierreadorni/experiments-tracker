# T1 owner decisions

Approved by the owner on 2026-11-13: “ok for proposals 1-6. you can proceed”.
The six decisions below authorize the bounded source/test refactor, not live
activation. T1 remains incomplete until its implementation and acceptance pass.

## Approved decision package

1. **Net source accounting:** enforce net-negative source lines across Phase 2 as
   a whole, while allowing additive intermediate cards. Keep per-commit source and
   tests/docs numstat reporting. Do not delete safety coverage to meet a number.
2. **Safe sequence and bounded cards:** ship the current-contract demo before
   activating archival. Split demo Python/browser work and migrate legacy execution
   fixtures in bounded cards before archival; retain archive-reader tests. Accept
   the reviewed concern-based splits for harness, runner and AgentPool, measuring
   each card against its budget. Remeasure T1.7 after preceding deletions and split
   oversized methods before delegation; do not accept an over-budget monolith.
3. **Toy API removal:** change demo/browser consumers first; defer rejecting new
   `toy_compare_v1` create requests until T1.1h. Preserve archived projection and
   reader compatibility even after the create enum changes.
4. **Browser acceptance:** replace frozen-recipe exact reproduction assertions
   for the new demo with contract-3 direct-output/export assertions, including
   offline report and figure loading. Keep historical archive-reader coverage;
   never label current-contract exports as frozen reproducibility bundles.
5. **Possibly-live turn capacity:** accept Opus's conservative T1.6b rule: both a
   free connection and free turn capacity are required; unclaimed durable
   dispatching/running/uncertain actions from all three tables count once against
   `codex.max_sessions`. Reconciliation/readback/interruption remain ungated.
   This is stricter after multi-session restarts and can stop new turns until an
   unreadable result is reconciled. The owner accepted this conservative rule;
   any future connection-only interpretation requires a new decision.
6. **Archival of in-flight work:** proposed conservative boundary: reconcile and
   settle exact saved job/action identities, retaining outcomes and accounting,
   but do not resume legacy stages, submit new work, continue a turn or fail an
   archived experiment from that observation. Preserve its archive phase/hold
   until cancellation. Finish already-queued report rendering from saved evidence
   without inference; use existing report failure handling if evidence is missing.
   Never infer external absence from an archive transition or process shutdown.

Items 1–5 resolve the design/process reviews; item 6 resolves the T1.1a inventory's
in-flight boundary explicitly. These approved choices supersede any pending-owner
wording in the earlier review notes, without waiving their size, identity, test or
scope requirements.

## Evidence and resume boundary

- T1.1a is complete and published as `42829f4`; independent T1.2a is complete as
  `864ba91`, under existing rules (497 source/test changed lines, production source
  −2, tests +23). Other T1 implementation cards remain open. No accounting or
  sequencing exception was used; this does not authorize blocked archival work.
- Baseline isolated Node 24 locked setup/check passed: 535 Python, 29 web and 41
  gateway tests. The old synthetic demo completed with its report/figure inspected.
  These are baseline checks, not acceptance of the unimplemented refactor.
- Coordinator-authored S-card plans and their Opus decisions are in the adjacent
  `t1.*design.md` notes. Design approval never marks implementation complete.
- The registry provides `deepseek/deepseek-flash`, not a separately versioned
  Flash 4.1 identifier; that is the Flash route used. Opus reviews use the exact
  `anthropic/claude-opus-5-5` entry. No claim of a separately verified Flash 4.1
  backend version is made.
- The clock seam is the only accepted runtime refactor so far: AST-equivalent
  clock/import substitutions, 90 targeted and all 537 Python tests, Ruff and a
  labelled synthetic demo/report/figure pass on an exact isolated copy. No live
  state, Slurm job, hosting deployment or AGENTS future-contract text has changed.
  Update AGENTS only after all of T1.1 is actually up.

Update the task sequence/splits without changing the reviewed semantics, then
continue bounded Flash implementation for the approved demo/fixture work.
The coordinator retains diff review, targeted/full checks, commit and publication
responsibility. F+R cards receive stronger review; S designs retain Opus decisions.
