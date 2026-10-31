# Acacia iteration execution

The acacia owner retains SQLite, managed ChatGPT inference and ownership. Choose
stage resources using fresh cluster capabilities and Slurm authorization; optional
owner limits also apply. Never submit outside the backend or change unrelated jobs.

Choose executor:agent with resources:null for bounded model checks, quick inference
and light preparation. Its instruction guides an interactive shell/tool loop that
fixes and retests errors within the same turn. Declared outputs gate dependent
work. Sustained compute and training use executor:slurm with resources. Local GPU
use still requires actual runtime/host authorization. Working-source corrections
are captured by dependent attempts at dispatch; submitted source copies stay fixed.

Implement in the selected isolated checkout and stable experiment workspace.
Declare the small executable source files and project files actually needed,
including package helpers and __init__.py. The backend copies only these files
before each attempt. Jobs use EXPERIMENT_SOURCE and EXPERIMENT_PROJECT copies;
never import editable working sources or install into a shared environment.

EXPERIMENT_ROOT is this attempt's output directory. EXPERIMENT_INPUTS is JSON
containing selected original output IDs, producer attempts, execution context,
host and absolute paths. Read those paths directly instead of repair manifests or
assuming predecessor outputs are in your output directory. A missing path needs
an explicit replacement or a targeted producer rerun. Cross-host paths require
an explicit authorized transfer; never interpret a Jean Zay path on acacia.

Large outputs/directories are ordinary local_outputs references. No recursive
hashing, inventories, environment freezes, dataset/checkpoint audits or universal
final validation. Evaluation writes bounded structured metrics once; plotting
and additional analyses are separate tasks. Keep cheap shapes, units, requested
lambda and split checks and honor requested repetitions/statistics. Published
small results/figures are retained independently of mutable working files.
No automatic checkpoint, scratch or working-folder deletion on completion.
