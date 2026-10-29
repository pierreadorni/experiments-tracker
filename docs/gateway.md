# Optional private gateway

Start with the local UI and SSH tunnel. Hosting is a separate installation task, not required for the executor or demo. The source includes a Sites-compatible Worker with D1 (`DB`) and R2 (`BUCKET`) bindings; a generic Cloudflare production deployment is not preconfigured or accepted.

## Local development

```sh
make setup
npm run build --prefix gateway
npm run typecheck --prefix gateway
npm test --prefix gateway
```

Tests use real local D1/R2 emulation with synthetic identities; they do not deploy or establish hosted authentication. The development server `node gateway/scripts/dev.mjs` additionally requires protected `.local/api-token` and `.local/executor-token` files. It listens on loopback port 8788, defaults to executor ID `acacia`, and stores emulated state under `/tmp/experimenttracker-gateway-local` (override with `ET_GATEWAY_STATE`). It is not a production proxy or an HTTPS gateway endpoint accepted by the live configuration.

## Private Sites deployment with an agent

Use an agent with Sites access and the Sites installation/deployment workflow. Create/register your own owner-private Site and retain its exact identity in protected local deployment records. The public repository supplies binding metadata in `gateway/.openai/hosting.json`, not someone else's project identity. Keep the registered deployment checkout separate from the research checkout. `scripts/sync_site.py` can refresh source into `.local/phone-site` only after that directory has its own registered manifest.

Build `gateway` with the nested/shared frontend, provision `DB` and `BUCKET`, and apply the existing `gateway/drizzle` migration sequence in order through the hosting workflow. The Worker build includes migrations and shared static assets. Never rewrite applied production migrations or use local database initialization as a production migration strategy.

Configure runtime values through protected hosting settings:

| Binding | Purpose |
| --- | --- |
| `AUTH_MODE=sites` | Production platform browser identity; protect the Site with owner-only access. |
| `EXECUTOR_ID` | Exact match with the local generated configuration. |
| `EXECUTOR_TOKEN` | Strong independent application machine secret; not a browser login token. |
| `DB`, `BUCKET` | D1 command/projection database and R2 published-object storage. |
| `VAPID_PUBLIC_KEY`, `VAPID_PRIVATE_KEY`, `VAPID_SUBJECT` | Optional Web Push, configured separately; private key stays secret. |

The `token` auth mode and `USER_TOKEN` are available for controlled testing; production setup in this project uses trusted Sites identity. Another host needs an authentication design and verification, not a forged platform identity header. This application has one trusted owner and no general multi-tenant authorization layer.

For executor access, establish both platform machine access and the app executor secret. Save secrets in protected ignored files using a hidden-input workflow such as `scripts/save_secret.py` (inspect its help). Never use secret values in argv, URLs, Git, chat or browser storage. Configure only paths:

```yaml
gateway:
  url: https://YOUR-PRIVATE-SITE-ORIGIN
  credential_file: /ABSOLUTE/PROTECTED/executor-token
  sites_credential_file: /ABSOLUTE/PROTECTED/sites-token
  poll_seconds: 5
```

The platform credential uses `OAI-Sites-Authorization`; the application machine credential uses `Authorization`. Neither authorizes a browser user. A gateway URL must be an HTTPS origin without credentials, path, query or fragment.

## Acceptance

Verify that signed-out/non-owner browser requests cannot read private results; a valid executor credential cannot access browser-only routes; and the executor can fetch/commit/acknowledge a command and publish a small artifact. Check D1 projection revisions and R2 availability through the application. Restart/replay must deduplicate commands and retain failed outbox publication. Stop the executor temporarily at an authorized boundary and verify published views remain readable. Finally test actual phone sign-in and installation. Keep these results separate from local emulation and mocked browser tests.
