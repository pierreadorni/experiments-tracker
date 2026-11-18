# T4 fresh-context handoff

Start from current GitHub `main`. `.worktrees/` and `.impeccable/` are local, untracked directories, not unfinished source to stage or delete. Read [working rules](../README.md), [T4 task cards](../todo.md#phase-4--research-loop-execution-contract-4-c7), [C7](../design.md#c7-research-loop-execution-contract-4), and [pilot checklist](t4.7-pilot.md). Use Luna workers if available; isolate concurrent writers and review exact saved identity boundaries before merging.

## Verified baseline

Contract 3 remains the default; contract 4 is per-project opt-in. Contract 4 now has a local manual approval and submission path, pinned sources, exact job recovery, debounced owner/job wakeups, steering, explicit timers, result/report publication, and terminal completion. `tests/test_research_scenario.py` exercises approval → lost submit reply → exact adoption (one submit) → debounced result turn → ready report → `complete`, with scripted turns and scheduler. Timer tests additionally prove restart, rollback and supersession. The local research detail, private checkpoint reuse and opt-in completion integration passed an isolated Node 24 `make check`: **1,385 Python, 36 web, 48 gateway tests**. The isolated contract-3 demo passed separately; neither is live acceptance.

## Next work

1. **T4.3/T4.4**: local submission remains CPU-only. Finish remote observation/transfer and the Autopilot approval envelope. The research loop now rotates an exact settled thread at 80% of its reported context window when a private checkpoint can seed the next turn.
2. **T4.5/T4.6**: C4 report status and evidence exports use working-report semantics. The local Worker now accepts exact durable research questions and approvals, pause/resume and bounded next-turn steering. Scripted scenarios cover quota denial/restart/resume, independent owner questions, and figure failure without losing metrics. The watchdog detects orphaned C4 runs. The remaining conflict scenario must drive the C6 resolver from two C4 integration requests; existing C4 queue and C6 resolution tests do not prove that lifecycle. Hosted deployment/device acceptance is separate.
3. **T4.7 live pilot**: complete with assisted acceptance. The owner-approved exact objective led to one local CPU/no-GPU Slurm job, exact terminal accounting, registered metrics, and a ready report. A separate metadata-only result copy was necessary; the original output was preserved. See the [acceptance summary](t4.7-pilot.md); exact receipts remain private.
4. **T4.8**: see [switch, drain, and retirement gates](t4.8-rollout.md). The default switch and engine removal are separate. Preserve archive reads and exact settlement of historical identities. Pilot permission is not rollout or retirement permission.
