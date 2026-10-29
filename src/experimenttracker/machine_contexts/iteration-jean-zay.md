# Jean Zay iteration execution

Acacia owns SQLite and managed ChatGPT inference. Jean Zay receives bounded
declared sources and runs tracked Slurm attempts. No Codex credential or resident
service belongs on Jean Zay. Use fresh capabilities and authorized account, QoS,
partition, hardware, runtime and task resources. Busy nodes may queue; down or
drained nodes are not usable. Nested scheduler submission is prohibited.

Set the project remote base (absolute or $WORK/, $SCRATCH/, $HOME/) using
set_project_execution before submission. For execution contract 3, the backend
stages only declared small sources in a private folder for each exact attempt;
whole-project rsync templates are not run. Do not transfer environments, datasets,
checkpoints, secrets or unrelated trees. Declare package helpers explicitly.
Scripts initialize their module/runtime; acacia virtual environments are not
portable. Jean Zay rejects --mem, --mem-per-cpu and --mem-per-gpu. Structured
memory is an estimate; the cluster assigns memory from CPU/GPU allocation.

The .sbatch body runs in EXPERIMENT_PROJECT (private declared source copies),
with scripts in EXPERIMENT_SOURCE and this attempt's outputs in EXPERIMENT_ROOT.
Read EXPERIMENT_INPUTS for direct original producing-attempt paths and opaque
output IDs with host/context. Do not follow repair-root reuse manifests or assume
inputs share your output directory. Foreign-host inputs require explicit transfer.
Datasets may remain in existing authorized locations, including $DSDIR where
appropriate; inspect the actual environment rather than guessing dataset names.

Declare large checkpoints/directories in local_outputs. The allocation checks
paths without reading their scientific bytes or enumerating directory membership.
Only bounded declared small outputs, owned logs and selected TensorBoard previews
return automatically. Result transport retries never resubmit compute. Remote
observation stays at the configured ten-minute cadence. No polling loop or
permanent service. No automatic cleanup after publication.

Evaluate authoritative metrics once with cheap shape/unit/lambda/split checks;
separate requested figures and additional analysis into resumable tasks. Preserve
explicit scientific statistics/repetitions. Do not add data hashing, independent
metric recomputation, raw prediction retention or publication-grade controls by
default. Save predictions when requested or useful for later analysis.
