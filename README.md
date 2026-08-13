# codex-deepseek-subagent-proxy

Local forwarding proxy that makes the **OpenCode Go** (Console Go) gateway usable
from the Codex CLI/desktop app (`wire_api = "responses"`), fixing:

1. **MultiAgentV2 subagent task delivery** for DeepSeek and other non-OpenAI
   Responses providers — no core rebuild required.
2. **OpenCode Go gateway quirks** that break normal Codex sessions with
   `deepseek-v4-pro` / `deepseek-v4-flash` (see [Related issues](#related-issues)).

## Problems

### 1. Subagent task delivery

When Codex spawns a subagent (`spawn_agent` / `followup_task` / `send_message`)
on a non-OpenAI provider, the task text is carried in a private `agent_message`
response item whose payload lives in `encrypted_content`. OpenAI endpoints
decrypt that server-side; third-party Responses providers such as DeepSeek do
not understand these items. The child agent therefore sees only an empty
envelope and replies with a generic "Ready to help..." — it never executes the
delegated task.

### 2. OpenCode Go (Console Go) gateway quirks

- The gateway rejects any tool that has no `name` (e.g. Codex's `tool_search`
  and `web_search`) with `tools[N].function: missing field 'name'`.
- Its SSE stream for `/responses` is non-standard: `response.output_text.delta`
  events omit `item_id` / `output_index` / `content_index`, there are no
  `output_item.added` / `content_part.added` scaffolding events, function-call
  events lack `item_id` and closing `done` events, and `response.completed`
  carries an empty `output` array. The official Codex SSE parser cannot render
  such a stream, so replies show up empty in the UI.

## Fix

The proxy sits between Codex and the upstream (`https://opencode.ai/zen/go/v1`
by default, configurable via the `DEEPSEEK_UPSTREAM` env var). Point
`[model_providers.deepseek].base_url` at `http://127.0.0.1:8787/`; on
`POST /responses` the proxy:

1. Rewrites `agent_message` items into a standard `message` (`role: user`)
   with plain `input_text` content.
2. Drops tools without a `name` (`tool_search`, `web_search`).
3. Normalizes the upstream SSE stream into the standard OpenAI Responses
   format (synthesizes `output_item.added` / `content_part.added`, injects
   `item_id` / `output_index`, closes items with `done` events, and patches
   `response.completed` with the full `output` array).
4. Converts assistant-message `content` arrays into plain strings — the
   gateway rejects array `output_text` for `deepseek-v4-pro` with
   `Invalid assistant message: content or tool_calls must be set`.
5. Auto-retries once with `reasoning.effort = "none"` when the gateway
   intermittently fails (400/500) because it drops `reasoning_content` in
   thinking mode (`The reasoning_content in the thinking mode must be passed
   back to the API`), so a flaky continuation turns into a successful call
   instead of breaking the whole session.

Requests that need no rewrite are forwarded byte-for-byte; headers and the
streaming body pass through. The proxy forwards the `Authorization` header
from Codex unchanged and stores no keys. It binds to `127.0.0.1` only.

`GET /healthz` returns `{"status":"ok","upstream":...}`.

## Install & use (Windows)

```powershell
# 1. Start the proxy (hidden window; logs to logs\proxy*.log)
powershell -ExecutionPolicy Bypass -File "$env:USERPROFILE\.codex\deepseek-proxy\start.ps1"

# 2. Point Codex at the proxy in ~/.codex/config.toml
#    [model_providers.deepseek]
#    base_url = "http://127.0.0.1:8787/"

# 3. Fully restart Codex, then send a message / spawn a subagent
```

The upstream is fixed by the watchdog (`DEEPSEEK_UPSTREAM=https://opencode.ai/zen/go/v1`).
To override, set the env var before starting the proxy.

## Stability & management

The proxy is supervised by `watchdog.ps1`: if the node process exits for any
reason it is restarted automatically within ~2 seconds. If the port is still
held by a previous instance, the watchdog waits until it frees and then takes
over. A scheduled task `codex-deepseek-proxy` starts the watchdog at logon.

```powershell
# start (also used by the scheduled task)
powershell -ExecutionPolicy Bypass -File "$env:USERPROFILE\.codex\deepseek-proxy\start.ps1"

# stop (stops watchdog + proxy)
powershell -ExecutionPolicy Bypass -File "$env:USERPROFILE\.codex\deepseek-proxy\stop.ps1"

# remove auto-start if you do not want it
Unregister-ScheduledTask -TaskName 'codex-deepseek-proxy' -Confirm:$false
```

Runtime logs: `logs\proxy.log`, `logs\proxy-error.log`,
`logs\proxy-watchdog.log`; watchdog PID in `.cache\watchdog.pid`.

## Verification

```powershell
# health check (should return {"status":"ok","upstream":...})
Invoke-RestMethod http://127.0.0.1:8787/healthz
```

End-to-end: spawn a subagent with `fork_turns="none"` and the message
`Reply with exactly the single word PONG` — the child should return `PONG`.

## Related issues

- [anomalyco/opencode#42090](https://github.com/anomalyco/opencode/issues/42090) —
  `deepseek-v4-pro` over OpenCode Go Responses API: `tools[N].function: missing
  field 'name'` (web_search rejected); flash and the official API work
- [anomalyco/opencode#42135](https://github.com/anomalyco/opencode/issues/42135) —
  `deepseek-v4-pro` fails on any multi-turn `/responses` request
  (`Invalid assistant message: content or tool_calls must be set`)
- [anomalyco/opencode#42091](https://github.com/anomalyco/opencode/issues/42091) —
  `deepseek-v4-pro` returns `Empty input messages` for string content
- [anomalyco/opencode#41061](https://github.com/anomalyco/opencode/issues/41061),
  [anomalyco/opencode#24714](https://github.com/anomalyco/opencode/issues/24714),
  [anomalyco/opencode#29690](https://github.com/anomalyco/opencode/issues/29690) —
  gateway drops `reasoning_content` in thinking mode on follow-up / tool-call
  messages: `The reasoning_content in the thinking mode must be passed back to
  the API` (official DeepSeek API is unaffected)
- [anomalyco/opencode#42134](https://github.com/anomalyco/opencode/issues/42134),
  [anomalyco/opencode#42228](https://github.com/anomalyco/opencode/issues/42228) —
  `deepseek-v4-pro`/`deepseek-v4-flash` gateway issues (China-hosted model,
  identity probe)
- [openai/codex#36586](https://github.com/openai/codex/issues/36586) — DeepSeek
  subagents cannot consume encrypted V2 task payloads
- [openai/codex#36321](https://github.com/openai/codex/issues/36321) — child
  agents receive empty task payload (multi-agent v2)
- [openai/codex#34833](https://github.com/openai/codex/issues/34833) —
  cross-provider subagent cannot consume encrypted task assignment
- [deepseek-ai/awesome-deepseek-agent#349](https://github.com/deepseek-ai/awesome-deepseek-agent/issues/349)
  — root-cause analysis and the rewrite approach this proxy implements
- Alternative approach (requires rebuilding codex-core):
  [CCanxue/codex-deepseek-subagent-fix](https://github.com/CCanxue/codex-deepseek-subagent-fix)

## License

MIT
