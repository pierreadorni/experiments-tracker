# Working in this repository

Read [install](docs/install.md), [operations](docs/operations.md) and [architecture](docs/architecture.md)
first. Refactoring follows [docs/refactor/](docs/refactor/README.md).

## Owner safety

- Ask the owner before the first live experiment. Code publication does not authorize live activation:
  installs, restarts, Worker publishes and Slurm or model calls each need the owner.
- Run the isolated synthetic demo before live setup; a synthetic demo is not research.
- Synthetic tests, fixtures and browser smoke scripts are not live acceptance. Real Slurm, the managed
  runtime, private hosting and a physical phone each need their own evidence.
- Never extract, read or copy credentials. Keep them in ignored protected files or runtime secrets, never
  in source, argv, URLs, logs or browser storage. An executor credential cannot authorize a browser user.
- API-billed inference runs only when the owner configures it explicitly (owner waiver 2026-11-22), never
  as an automatic fallback. Every billed backend runs under an owner spend budget, every subscription
  backend under fresh quota windows. Admission holds when either is stale or unknown.
- Live resources come from fresh cluster capabilities within Slurm authorization and optional owner
  limits. Keep the short CPU-only synthetic demo separate from live authorization.
- Do not touch unrelated Slurm jobs or research checkouts. Do not install a permanent service until host
  policy and local-disk retention are established.

## Execution rules

- SQLite on retained local disk owns execution; D1/R2 hold commands and published views. Keep user
  commands, events, outbox updates and readiness transitions transactional and idempotent.
- Persist job, action and transfer intents before dispatch. Never retry an uncertain submission or turn
  before establishing its saved outcome; reconcile exact identities.
- Use generated local configuration and one ownership lock per state directory.
- New experiments use contract 4; archived contracts and their evidence stay readable. Do not add
  scientific data hashing, recursive inventories, dependency freezes, a final validation barrier or
  completion cleanup.
- SQLite migrations are additive; D1 production migrations are append-only. Hosting is owner-private:
  register your own Site and never reuse another installation's identity.
- Read the [portability map](docs/install.md#portability-map) before adapting the saved `acacia` or
  `jean-zay` contexts or the remote scheduler policy. Preserve historical records.

## Code and checks

- GitHub `main` is the single shared history. Commit and push reusable improvements here, with no
  snapshot-export workflow; never force-push or publish production identities, research data,
  databases, backups or secrets.
- Installation records live in ignored `.local/installation/`; read its `AGENTS.md` when present. Keep
  adaptations reviewable and do not publish local operations records.
- Use Node 24 and `make setup` from the locked manifests. Run `make check` in an isolated checkout when an
  owner serves this checkout's `web/dist`. Run Python subprocess tests outside sandboxes whose NSS
  isolation changes subprocess behaviour.
