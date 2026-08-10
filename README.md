# codex-deepseek-subagent-proxy

Local forwarding proxy that fixes **Codex MultiAgentV2 subagent task delivery for
DeepSeek and other non-OpenAI Responses providers** — no core rebuild required.

## Problem

When Codex spawns a subagent (`spawn_agent` / `followup_task` / `send_message`)
on a non-OpenAI provider such as DeepSeek, the task text is carried in a private
`agent_message` response item whose payload lives in `encrypted_content`.
OpenAI endpoints decrypt that server-side; third-party Responses providers such
as DeepSeek do not understand these items. The child agent therefore sees only
an empty envelope and replies with a generic "Ready to help. What would you
like me to work on?" — it never executes the delegated task.

## Fix

The proxy sits between Codex and DeepSeek. Point
`[model_providers.deepseek].base_url` at `http://127.0.0.1:8787/`; on
`POST /responses` the proxy rewrites `agent_message` items into a standard
`message` (`role: user`) with plain `input_text` content, then forwards
everything else (headers, streaming responses) unchanged.

Only requests that contain `agent_message` are rewritten; regular parent-session
traffic and OpenAI endpoints are unaffected.

## Install & use (Windows)

```powershell
# 1. Start the proxy (hidden window; logs to proxy.log / proxy-error.log)
powershell -ExecutionPolicy Bypass -File "$env:USERPROFILE\.codex\deepseek-proxy\start-proxy.ps1"

# 2. Point Codex at the proxy in ~/.codex/config.toml
#    [model_providers.deepseek]
#    base_url = "http://127.0.0.1:8787/"

# 3. Fully restart Codex, then spawn a subagent
```

The proxy forwards the `Authorization` header from Codex unchanged and stores
no keys. It binds to `127.0.0.1` only.

## Verification

```powershell
node "$env:USERPROFILE\.codex\deepseek-proxy\proxy.mjs" --selftest
node "$env:USERPROFILE\.codex\deepseek-proxy\proxy.mjs" --mock-test
```

End-to-end: spawn a subagent with `fork_turns="none"` and the message
`Reply with exactly the single word PONG` — the child should return `PONG`
(verified against `deepseek-v4-flash`, `wire_api = "responses"`).

## Related issues

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
