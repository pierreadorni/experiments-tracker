# Development and local installations

The GitHub `main` branch is the shared source of truth and the latest available
software. Develop normal commits on this history and push reviewed improvements
back to the same repository. Tags and release packages are optional. Do not
export a second source repository or publish installation history.

Plan refactors with the [refactor workspace](refactor/README.md) task cards.

## Files that belong in Git

Track application source, tests, migrations, locked dependencies, generic agent
instructions, configuration examples and reusable documentation. Run `make check`
with Node 24 and locked dependencies in an isolated checkout when a live owner
serves the working checkout. Keep synthetic and live acceptance evidence distinct.

## Files that stay local

Keep generated configuration and credentials in ignored protected files, normally
under `.local/`. Store installation instructions, host discovery, operating notes,
deployment identities and acceptance records under `.local/installation/`.
`.local/installation/site.json` records the installation's existing private Site
identity; the registered Site checkout retains its own ignored hosting metadata.
Never publish production identities, research data, databases, backups or secrets.
Git ignore rules do not remove a file from existing history: inspect staged files
and diffs before pushing. Do not force-push private historical branches to GitHub.

Local agent instructions can be supplied in the task or read from
`.local/installation/AGENTS.md`; they must not replace reusable tracked instructions.
Preserve runtime environments, ignored builds and configuration when updating code.

## Code publication and installation

A push updates shared source. It does not restart an executor, migrate live state,
submit scientific work or deploy a hosted gateway. Use the existing owner-authorized
update procedure in [executor updates](executor-updates.md). Keep any hosting source
checkout only as the hosting provider's publication artifact; implement changes in
this repository first. See [operations](operations.md) and [Autopilot](autopilot.md).
