# T6: selectable agent backends (Codex and Claude Agent SDK)

Status: plan, 2026-11-22. Nothing here is implemented, and none of it authorizes
live use. Section 1 lists the owner decisions; decision 1 is made, and decisions
2 and 3 must be made before T6.5.

## Goal

The owner can run any agent role (research turn, integrator, Autopilot
orchestrator, discussion, knowledge synthesis, title) on either:

- **Codex** (current): `codex app-server` over JSON-RPC, with a managed ChatGPT
  login and five-hour/weekly quota windows; or
- **Claude**: the Claude Agent SDK (`claude-agent-sdk`, Python), which runs the
  Claude Code harness as a library.

The C4 contract, the MCP tool bridge, the obligations, the intent ledger and the
UI stay the same whichever backend runs. Each saved session belongs to exactly
one backend for its lifetime.

## Why after T4.8

`CodexAgent` is used today by the C3 stage engine (`workflow.dispatch`,
`agent_stage`, `iteration_prompt`), contract 4 research dispatch, the integration
resolver, Autopilot, discussion, knowledge and titles. T4.8 deletes the first
group. Building the abstraction after T4.8 means the stage engine never has to be
ported. T6.1 and T6.2 can start once the engine deletion has landed. T6 is
independent of Phase 5, except that T5.1 (Autopilot on C4) and T6.3 both touch
Autopilot's session options: whichever lands second rebases.

## 1. Owner decisions (block T6.5)

1. **Authentication and billing. Decided 2026-11-22.** The owner waived the
   "managed ChatGPT only" rule for all backends; AGENTS.md now allows explicitly
   configured API billing under an owner spend budget. Anthropic's Agent SDK
   documentation says third-party products may not offer claude.ai login or
   subscription rate limits without Anthropic's approval. The Claude backend
   therefore uses an **Anthropic API key**, kept in a protected credential file.
   The owner has **$100 of monthly API credits**, so the initial monthly budget
   is $100. The daily and per-turn caps are set in T6.1 from measured turn costs.
   Claude is never an automatic fallback for Codex quota exhaustion. Selection is
   explicit per project or role. The Codex guard in `CodexAgent.start` that
   refuses non-ChatGPT accounts stays: Codex API-key mode is out of scope unless
   a later card adds it under the same spend ledger.
2. **Data handling.** Prompts, checkpoints and repository content would go to a
   second provider. The owner confirms which projects may use it (per-project
   opt-in).
3. **Default model and effort.** Proposed default: `claude-opus-5-5` with
   `effort="high"` for research and integration, and `"low"` for titles. Model
   IDs come from configuration, never from code.

## 2. Target shape (design component C14)

### 2.1 `AgentBackend` protocol

Extract the surface that call sites already use from `CodexAgent`:

| Member | Codex today | Claude |
|---|---|---|
| `start()` / `close()` / `available` / `start_count` | app-server process, account check, model catalog | SDK and CLI version check, credential check, configured catalog |
| `session(thread_id, workspace, *, writable, model, runtime_config, approval_policy)` → `(id, model)` | `thread/start` or `thread/resume` | Reserve or validate a session ID (see 2.3); no process is kept between turns |
| `begin(thread_id, prompt, schema, *, workspace, effort, project_workspace, additional_writable_roots)` → `turn_id` | `turn/start` | Starts `ClaudeSDKClient` with `resume=`, the scoped options and the executor-minted turn ID; returns that ID once the SDK accepted the query |
| `finish(thread_id, turn_id)` → `dict` | waits for `turn/completed` and parses the last agent message | Drains `receive_response()` up to `ResultMessage` and returns `structured_output` |
| `read_thread` / `read_turn(..., expected_prompt_hash)` | `thread/read` | Transcript readback (2.3) |
| `interrupt(thread_id, turn_id)` | `turn/interrupt` | `client.interrupt()`, then drain the interrupted messages |
| `reply(pending)` | answers a JSON-RPC approval request | Resolves the pending `can_use_tool` future |
| `quota()` plus `quota_handler` / `activity_handler` / `request_handler` | rate-limit RPC and notifications | Admission source (2.4), normalized activity (2.6) |
| `models`, `resolved_model`, `is_demo` | catalog from `model/list` | Catalog from configuration |

The error types (`RpcError`, `RpcRejected`, `NeedsAttention`, `SessionUnavailable`,
`TerminalTurnError`, `TransportError`) move to a neutral `agent_errors.py`, and
`codex_client` re-exports them for one card. About 20 modules import them from
`codex_client` today. `DemoAgent` conforms to the protocol and stays the
no-network test double. The tests add a scripted `FakeClaudeAgent` that replays
recorded SDK message sequences.

### 2.2 Binding and saved identity

- **Configuration.** `CodexConfig` stays as it is. A new optional `ClaudeConfig`
  holds: the pinned SDK and CLI versions, `credential_file` (a protected file,
  never put on argv), `provider`, `model`, `effort`, the per-turn budget and the
  `max_sessions`. An absent `claude:` section means the backend is unavailable.
- **Selection.** There is a project-level `agent_backend` default (`codex` | `claude`),
  which a role can override. The create path records
  `contract.agent = {"backend": ..., "model": ...}` in the experiment contract.
  Saved contracts without it mean Codex, so no rewrite is needed. Project roles
  (integrator, Autopilot) save the backend on their action input.
- **Saved IDs.** `experiments.thread_id` and `agent_actions.thread_id/turn_id`
  keep their columns. For Claude, `thread_id` holds the SDK `session_id`, and
  `turn_id` holds an executor-minted UUID that is saved before dispatch. The
  backend comes from the bound contract or action input, never from the shape of
  an ID. Resuming a session on a different backend raises `SessionUnavailable`.
  Moving to another backend is only possible at an existing checkpoint rotation,
  as a new session seeded from the checkpoint.
- **Schema.** No new table. If an action-level backend column turns out to be
  needed (for example for drain inventory queries), it is one additive column,
  named by T6.3. Ask the owner first, per the README rules.

### 2.3 Exact readback (the hard part)

Codex keeps turns on its server, so `read_turn` can prove whether a turn completed,
was interrupted or failed. The Agent SDK writes the conversation to JSONL
transcripts under `$CLAUDE_CONFIG_DIR/projects/<encoded cwd>/<session>.jsonl`. It
can read them with `get_session_messages()` and resume by `session_id`, but it
has no server-side turn object. The design:

- Set `CLAUDE_CONFIG_DIR` to a directory under the executor's retained state
  (never the owner's `~/.claude`), and set `setting_sources=[]` so that user or
  project settings, skills and hooks are not loaded implicitly.
- Each prompt starts with a fixed marker line carrying the minted `turn_id`. The
  saved `prompt_sha256` covers the full prompt, marker included.
- `read_turn` finds the marker's user message, then the assistant messages that
  follow it, up to the next marker:
  - a recorded result with structured output for that turn → **completed**;
  - an interrupt or error record → **failed** or **interrupted**;
  - a marker with no terminal record → **uncertain** (`NeedsAttention`), exactly
    like an unreadable Codex thread;
  - no marker → absence is proven **only** if the transcript is readable and the
    saved intent shows that `begin` never returned. Otherwise the turn is
    uncertain.
- T6.1 has to verify that the persisted transcript contains enough to make
  these distinctions (whether the result and structured output are written, and
  what an interrupted turn looks like). If they are not, T6.6 records the
  `ResultMessage` itself: in the same transaction as the turn settlement, and
  also in a write-ahead file under the action's private state, written before
  `finish` returns. A crash between the SDK result and that write then yields
  "uncertain", never a retry.
- Never retry an uncertain turn. The existing resolver and owner reconciliation
  apply unchanged.

### 2.4 Admission and spend

`quota.py` becomes one `AdmissionSource` per backend:

- **Codex:** the current five-hour/weekly windows, thresholds and staleness rules.
- **Claude:** an owner spend budget plus provider rate limits.
  - Each settled turn's `total_cost_usd`, `usage` and `model_usage` are saved on
    the action result.
  - A ledger sums them per day and per month. An uncertain or unsettled turn is
    charged its full per-turn cap, mirroring the decision that unresolved remote
    turns consume capacity.
  - `max_budget_usd` is passed on every turn as a hard per-turn ceiling. Hitting
    it (`error_max_budget_usd`) is a terminal turn failure.
  - A `RateLimitEvent` with status `rejected` holds dispatch until `resets_at`;
    `allowed_warning` raises the reserve. A stale or unknown budget state holds
    work, as unknown quota windows do today.
- **Pool.** `AgentPool` keeps its slot and ownership logic, but slots and capacity
  are counted per backend, so Claude turns never consume Codex quota and the
  reverse. The pool's typed owner keys do not change.

### 2.5 Tools, permissions and sandbox

- **MCP.** `controller.session_config()` currently returns a Codex config key
  (`mcp_servers.experimenttracker`). It will return a neutral server spec instead
  (command, args, timeouts). Each backend translates it: Codex keeps its config
  key, and Claude passes `mcp_servers={"experimenttracker": {...stdio...}}`. The
  Autopilot project server (`autopilot_controller.py`) follows the same pattern.
- **Built-in tools.** `allowed_tools` covers Read, Glob, Grep, Edit, Write and
  Bash, plus the scoped MCP tools. `disallowed_tools` covers WebSearch, WebFetch,
  `AskUserQuestion` (owner questions go only through the `ask_owner` MCP tool)
  and Task/subagents unless enabled later.
- **Write boundary.** Codex enforces `writableRoots` with an OS sandbox. For
  Claude, set `cwd` to the worktree, pass `add_dirs` for the project workspace
  and the extra writable roots, and enable SDK `sandbox` settings for Bash. A
  `can_use_tool` callback denies any Write/Edit outside the roots and anything
  in `.git` outside the worktree's own administrative paths. T6.1 must confirm that the Bash
  sandbox works on the executor host (it needs Linux namespaces). If it does
  not, Claude stays read-only plus MCP and Edit/Write until the owner accepts a
  weaker boundary.
- **Approvals.** `permission_mode="default"`, with `can_use_tool` routed to the
  existing `request_handler`. Manual review and the Autopilot envelope apply
  unchanged. Never use `bypassPermissions`.
- **Structured output.** Pass `output_format={"type": "json_schema", "schema":
  strict_output_schema(TurnEnd...)}`. Use `ResultMessage.structured_output`. The
  subtype `error_max_structured_output_retries` maps to the existing
  "agent output validation" `TerminalTurnError`.

### 2.6 Activity, timeouts and the UI

- **Timeouts.** Map SDK `AssistantMessage`, tool-use and tool-result messages to
  the same progress signal that drives `turn_timeout_seconds` and
  `turn_max_seconds`. Inactivity calls `interrupt()` and then follows the
  existing confirm-or-uncertain path.
- **Activity events.** The `activity_handler` payload becomes a neutral record
  (`message`, `reasoning_summary`, `tool_started`, `tool_completed`, `usage`).
  `Conversation.tsx`, `Progress.tsx` and `progress.ts` currently parse Codex item
  shapes, so they switch to the neutral record. Saved historical Codex events
  keep a reader.
- **Display.** The UI and Worker show backend and model per experiment and per
  role, the Claude spend against budget next to the Codex windows, and backend
  choice in project settings and creation. The Worker relays these fields; it
  gains no authority.

## 3. Tests (C12)

Every card uses the scenario harness with scripted backends and no network:

- dispatch, completion, interrupt and timeout for each backend;
- restart in each turn state (before `begin`, after `begin`, after the result but
  before settlement), with readback proving completed, absent or uncertain;
- budget exhaustion, a rejected rate limit and a stale budget state holding
  dispatch;
- a backend mismatch on resume;
- MCP tool calls through both translations;
- path-escape denial by `can_use_tool`;
- mixed Codex and Claude experiments sharing the pool without consuming each
  other's capacity.

A recorded-transcript fixture from T6.1 anchors the readback parser.

## 4. Risks

- **Transcript format drift.** The JSONL layout belongs to the CLI. Pin the SDK
  and its bundled CLI version (as `codex.expected_version` already does), and
  have `doctor` check it. The readback parser fails closed (uncertain).
- **Cost.** Long research turns at high effort can be expensive. The per-turn
  `max_budget_usd`, the daily and monthly ledger, and conservative charging of
  uncertain turns bound it.
- **Two harnesses with different tool semantics.** Prompts (`research/prompt.py`)
  should rely only on MCP tools and plain file and shell work, never on
  Codex-specific tool names. T6.5 audits this.
