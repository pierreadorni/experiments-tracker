---
name: research-report
description: Present completed ExperimentTracker evidence as a readable research report, selecting and interleaving narrative, tables and figures without running new experiments.
---

Explain the researcher's question through the actual saved evidence. Lead with the result, give enough method to interpret it, and place each figure/table alongside the argument it supports. Choose the structure and visual encodings for the question; avoid a fixed paper outline when it adds empty sections.

Use `Analysis.presentation` to supply an ordered document. Paragraphs/figures cite artifact IDs. Tables use metric references (name and variant) instead of copied numbers. Select a small set of informative columns and summary rows; individual repetitions and exhaustive measurements remain in Evidence. Tables have a caption and human-readable labels/units. The renderer handles numeric precision, booktabs styling and numbering.

For comparisons, `metric_plot` can show referenced measurements, with optional error references and individual repetitions. Define the x/y labels, meaningful ordering, metric direction and exactly what the error bars measure. An SD across initialization seeds on one split is not a confidence interval or uncertainty across datasets. Use saved repetitions only; do not manufacture uncertainty. If appropriate, show individual observations as well as aggregates. Do not claim statistical superiority from the lowest observed mean alone.

For JSON records, `curve_plot` selects a registered artifact, record array and numeric x/y fields. Select/label series and optionally average repeated y values at a common x. Use x-order for sampled functions; preserve source order only when order itself is meaningful (e.g. a trajectory). Keep an observed target as points. Split overlapping comparisons into panels when that helps. The block caption describes averaging, units and the data population. Existing image artifacts can also be selected as `figure` blocks; inspect them and avoid citing an unreadable plot as primary support.

Keep prose plain and claims proportionate to evidence. Separate measurements, explanations and limitations. Retain methodological details that affect the conclusion; place full configurations, checksums, runnable provenance, training logs and implementation detail in Protocol/Evidence. Do not repeat validation plumbing as a finding. Use concise informative captions and alt text, consistent precision and readable axis labels. The saved brief is an instruction, not a future idea.

The supplied JSON schema describes the available blocks. Fill only fields relevant to each block, leaving other optional fields null. Planning a report adds no scheduler job or model-family restriction. References resolve against this experiment's immutable registered evidence; presentation cannot modify measurements, protocol, source snapshots or old reports. A later presentation is a new report version. Examine rendered output when available, particularly label overlap, crowded legends, mobile scaling and whether the main comparison answers the question.

For execution_contract 3, this is an incremental working document. Use available
registered metrics and figures while requested tasks remain visibly unfinished;
update_report queues a version independently of compute completion. Output IDs
refer directly to original producing attempts and paths with host context.
Structural ingestion checks do not certify science or reproducibility. Do not
request dataset/checkpoint hashes, independent metric recomputation, retained
predictions or extra statistics merely to compose a report. Honor explicit
scientific requests. A failed figure can be repaired without evaluating again.
Small published objects remain stable; working source and large outputs may
change. Convenience downloads do not promise a reconstructable experiment.
