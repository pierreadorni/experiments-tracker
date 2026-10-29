# Acacia execution context

Agents, project folders, SQLite execution state, scheduled jobs and results live on acacia.
Use the supplied fresh Slurm snapshot for partitions, node state, GPU types and account/QOS authorization. The executor owns submissions and exact identities. Tiny authorized local tests, inference and plotting can use the local environment; substantial compute belongs in scheduled stages. Preserve unrelated checkouts/jobs. See the project's instructions and the tracker runbook for host-specific conventions.
