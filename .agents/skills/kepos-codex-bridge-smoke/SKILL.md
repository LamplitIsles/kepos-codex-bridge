---
name: kepos-codex-bridge-smoke
description: Use when running or debugging a Pi session through this repository's transparent managed-OAuth Responses relay, including Pi compat Lite, Ogul Remote Compaction V2, or cache telemetry. Establish the explicit local model endpoint before treating Pi traffic as bridge traffic.
---

# Kepos Codex Bridge Smoke

## Guardrails

Run an approved live probe with a fresh, mode-`0700` test root, test-owned
Pi profile/session directories, and a separate loopback bridge process and
port. Preserve existing bridge processes, live profiles, and sessions.

When the user explicitly approves a live-auth smoke, pass the existing
managed-auth file to the isolated bridge only as an opaque `--auth-file`
reference. Do not open, parse, copy, log, hash, retain, or put its contents in
a Pi profile; do not run bridge login or otherwise alter the file yourself.
The bridge may perform its normal upstream-401 recovery. If authentication
still fails, stop and report rather than intervene with the credential.
Without explicit approval, require a test-owned managed-auth file.

Never print or retain OAuth data, placeholders, prompts, request bodies,
opaque checkpoints, or session contents. Retain only route confirmation,
success/failure, and numeric input/cache-read/cache-write telemetry. On
cleanup, remove only the root and bridge process created for this probe.

## Route invariant

`kepos-codex-bridge` is an application-level Responses endpoint. Pi uses it
only when its `openai-codex` model explicitly sets:

```text
http://127.0.0.1:<bridge-port>/codex/responses
```

A system `HTTP_PROXY` or Clash/Mihomo route controls egress only. It does not
replace a Pi model base URL:

```text
bridge test: Pi -> 127.0.0.1:<port>/codex/responses -> managed Codex upstream
ordinary Pi: Pi -> https://chatgpt.com/backend-api -> normal network route
```

The relay is model-independent: select a model in the client, not with a
bridge serve option. It forwards HTTP/SSE and WebSocket Responses traffic
without parsing Lite, compaction, cache, or continuation fields. It replaces
only peer identity with its managed OAuth identity.

## Isolated profile

1. Create a fresh private root and start a separate test-owned bridge on an
   unused loopback port. Record `$PORT` and the selected client `$MODEL`.
2. Write `$ROOT/pi-agent/models.json` with the explicit relay base URL and a
   syntactically JWT-shaped, non-secret placeholder. Its base64url payload must
   contain a synthetic `chatgpt_account_id` under
   `https://api.openai.com/auth`; Pi and Ogul inspect it locally. The relay
   ignores the placeholder and must never receive a real OAuth credential.

   ```json
   {
     "providers": {
       "openai-codex": {
         "baseUrl": "http://127.0.0.1:<bridge-port>/codex/responses",
         "apiKey": "<synthetic JWT-shaped placeholder>"
       }
     }
   }
   ```

3. Write `$ROOT/pi-agent/settings.json` with `defaultProvider` set to
   `openai-codex` and `defaultModel` set to `$MODEL`. Create a separate
   `$ROOT/sessions` directory.
4. Do not invoke Pi OAuth login in this profile. Do not load a second
   compaction implementation into an Ogul session.

## Verify before spending tokens

Before the first request in either row:

- read the test profile's `models.json` and confirm the exact loopback
  `/codex/responses` base URL;
- confirm the temporary bridge listener is bound on `$PORT`;
- confirm Pi's selected model is `$MODEL`;
- confirm the bridge process, Pi profile, and session paths belong to the new
  test root; with explicitly approved live auth, confirm only the opaque
  `--auth-file` reference is external.

Only after all four checks may the client call the model.

## Stock Pi + Ogul row (Lite off)

Launch exactly one isolated Pi process with Ogul:

```bash
PI_CODING_AGENT_DIR="$ROOT/pi-agent" \
NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost \
pi --session-dir "$ROOT/sessions" \
  --provider openai-codex --model "$MODEL" \
  --extension "$HOME/.pi/agent/npm/node_modules/@ogulcancelik/pi-codex-compaction/index.ts"
```

This is the Lite-off row: do not load `pi-openai-codex-compat` or another
conversion provider, and do not set a Lite environment override. Stock Pi owns
ordinary normal Responses; Ogul owns Remote Compaction V2 and its opaque
checkpoint.

Complete one normal turn, run manual `/compact`, then complete one normal
follow-up. Record only whether the configured route was used, each step
succeeded or failed, and numeric usage/cache telemetry. For an agent-driven
manual sequence, use the Herdr driver below.

## Pi compat Lite row (Lite on)

Use a separate test root/session and the same explicit relay profile. Load the
test-owned `pi-openai-codex-compat` extension and enable Lite explicitly:

```bash
PI_CODING_AGENT_DIR="$ROOT/pi-agent" \
PI_OPENAI_CODEX_COMPAT_RESPONSES_LITE=on \
NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost \
pi --session-dir "$ROOT/sessions" \
  --provider openai-codex --model "$MODEL" \
  --extension '<test-owned pi-openai-codex-compat extension path>'
```

Complete the same normal → manual `/compact` → normal follow-up sequence.
The package owns Lite construction, cache lineage, continuation, and Remote V2
state; do not inspect their values. Record only the allowed result fields. The
Lite-off comparison row is Stock Pi + Ogul above, not this package with Lite
disabled.

## Manual compaction driver (Herdr)

For an agent-driven manual `/compact` smoke, discover Herdr before starting
Pi: call `tools.search({ query: "herdr" })`, then `tools.describe` the
returned layout, agent, and pane tools. Use their discovered references and
schemas; do not substitute a raw shell PTY driver.

1. Create a fresh Herdr workspace/tab or shell pane with the discovered layout
   tool.
2. Start exactly one `kind: "pi"` agent in that new pane through the
   discovered agent tool. Pass only the isolated profile, session, model, and
   exactly one compaction extension through `agentArgs`; do not put the task
   in `agentArgs`.
3. Send the normal task with `herdr_agent.prompt` and wait for `idle` or
   `done`. For a long cache prefix, keep the corpus in a test-owned file and
   send only a short task that refers to it.
4. Send `/compact` through the same agent tool after the normal task settles,
   wait for it to settle, then send one normal follow-up through that same
   agent.
5. Use lifecycle state plus structural/numeric session telemetry for the
   verdict. Do not read the terminal transcript or request body. Close only
   the created pane with the discovered pane tool.

## Nanocodex and scope

The pinned Nanocodex client is a hermetic acceptance seam, not a third paid
live probe. Its public overrides are `.api_base_url("<relay>/codex")` and
`.websocket_url("<relay-ws>/codex/responses")`, using a non-secret local API-key
placeholder.

This smoke does not validate DSH or Pi compat's optional image/web companion
endpoints. The bridge has no session persistence, cache-owner role, or
cache-hit guarantee.
