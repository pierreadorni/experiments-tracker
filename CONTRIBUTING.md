# Contributing

Start with README.md, AGENTS.md and docs/architecture.md. Use a separate checkout from running deployments and research code. Install with `make setup`, use Node 24 and Python 3.11+, and run `make check`. Dependencies must use the committed lockfiles. No live account, gateway secrets or Slurm submissions are needed for source checks.

Keep changes reviewable. Describe the concrete behavior, validation and remaining limitations. Portability contributions should state the tested cluster policy and preserve saved execution contracts and exact dispatch reconciliation. Include tests for changed recovery, transaction, authorization or scheduler behavior; keep purely editorial changes lightweight.

Never include research data, credentials, local configuration, database backups or private deployment identities. Report sensitive findings privately through the maintainer's GitHub profile rather than opening a public issue with secrets. This early project has no dedicated security response service.
