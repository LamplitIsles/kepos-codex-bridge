# kepos-codex-bridge

A small Linux `x86_64` service that keeps one Codex `auth.json` on the bridge
host and exposes the native Codex Responses stream to permitted Kepos peers.
It is a relay, not an agent runtime: Pi or DSH owns history policy, tool
execution, and cancellation decisions.

## Public contract

The bridge publishes two sibling routes through the same HTTP service:

- `POST /codex/responses` accepts a full streaming native Responses request (it
  rejects `previous_response_id`) and returns `text/event-stream`; events are
  `data: {native JSON}\n\n` followed by `data: [DONE]\n\n`. WebSocket upgrade on
  the same route accepts text `response.create` frames and returns native
  response event frames. `response.cancel` cancels the active operation. This
  route retains its existing 4 MiB request limit.
- `POST /codex/images` accepts JSON `{ "prompt": string, "images"?: string[] }`.
  A nonblank prompt with no images performs generation; one through five
  `data:image/...` inputs performs editing. The route returns exactly
  `{ "image_url": "data:image/png;base64,..." }` and has a 32 MiB encoded
  request limit. `api_key` may be supplied only as ignored compatibility data;
  all other unknown fields are rejected.

The image route is a fixed Codex capability boundary, not a general OpenAI
Images API. It does not accept multipart bodies, remote URLs, file paths,
provider options, or `/v1` aliases. The bridge selects `gpt-image-2` and the
fixed Nanocodex-compatible `background`, `quality`, and `size` values. Future
clients own image loading, history selection, tool schemas, artifacts, and
rendering.

Incoming API keys are compatibility data only. They are ignored and never
become upstream credentials. The bridge loads only its local managed ChatGPT
OAuth identity. Kepos Noise peer identity and the publisher allowlist are the
access-control boundary; the bridge adds no application bearer token.

`nanocodex-oai-api` owns managed refresh, WebSocket retry, and the bounded
WebSocket-to-HTTP/SSE fallback. The bridge does not add a retry coordinator,
tool executor, deduplicator, database, queue, or durable continuation store.
Cancellation and disconnect drop the connection-scoped session operation.

## Build

The release artifact is one Linux `x86_64` binary. Build it on the target Linux
host or in the approved Linux build environment:

```bash
cargo build --release --target x86_64-unknown-linux-gnu
# target/x86_64-unknown-linux-gnu/release/kepos-codex-bridge
```

This repository deliberately adds no Docker image, container manifest, Helm
chart, non-Linux target, or other platform package.

## Operator setup

Select a private writable credential path. Do not use a client application's
credential directory as a test fixture or copy this file to peers.

```bash
export KEPOS_CODEX_AUTH_FILE=/var/lib/kepos-codex-bridge/auth.json
kepos-codex-bridge login --auth-file "$KEPOS_CODEX_AUTH_FILE"
```

`login` runs the upstream browser PKCE flow and writes the Codex-compatible
file atomically. A deployment that receives a private secret mount must copy
it into this owner-only writable path before starting the service. Serving
checks the file before binding and refuses group/other permissions or invalid
managed credentials.

Start the loopback listener:

```bash
kepos-codex-bridge serve \
  --auth-file "$KEPOS_CODEX_AUTH_FILE" \
  --port 8787 \
  --model gpt-5.6-luna
```

The listener is `127.0.0.1:8787` by default. It does not terminate TLS and it
does not expose login, token inspection, logout, metrics, or admin routes.

## Kepos publication

Publish the bridge as a **named Kepos HTTP service** whose target is the local
loopback port `8787` (or the selected `--port`). Use the normal Kepos HTTP /
WebSocket-over-Noise publisher path, not a raw TCP service and not a separate
WSS listener. Set the service allowlist to the peer public keys allowed to use
this subscription; inheriting the publisher allowlist is acceptable when it is
already narrow.

The client-side endpoints are the named HTTP service URL produced by Kepos
with `/codex/responses` or `/codex/images` appended. The publisher target
remains plain local HTTP; the peer leg is protected by Kepos Noise. Exact
service naming and allowlist syntax belongs to the Kepos deployment
configuration, for example:

```text
service name: codex-bridge
publisher target: http://127.0.0.1:8787
paths: /codex/responses and /codex/images
transport: standard Kepos HTTP service (HTTP + WebSocket upgrade for Responses)
allowlist: the approved Pi/DSH peer public keys
```

Do not put `auth.json`, an OAuth refresh token, or a bridge bearer token in the
Kepos service definition or in a client configuration.

## Pi and DSH configuration

Configure the existing native `openai-codex-responses` provider to use the
Kepos service URL at `/codex/responses`, with the fixed model
`gpt-5.6-luna`. No client-side OAuth login is configured.

Pi 0.84.1 needs a syntactically JWT-shaped, non-secret placeholder because its
native Codex adapter locally reads a `chatgpt_account_id` claim before it
connects. The bridge ignores that value; it is never forwarded upstream. An
arbitrary nonempty string such as `bridge-placeholder` is not sufficient for
Pi. Use a test-owned Pi config directory containing:

`models.json`:

```json
{
  "providers": {
    "openai-codex": {
      "baseUrl": "<Kepos HTTP service URL>/codex/responses",
      "apiKey": "eyJhbGciOiJub25lIn0.eyJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOnsiY2hhdGdwdF9hY2NvdW50X2lkIjoidGVzdC1hY2NvdW50In19.dummy"
    }
  }
}
```

`settings.json`:

```json
{
  "defaultProvider": "openai-codex",
  "defaultModel": "gpt-5.6-luna",
  "transport": "websocket"
}
```

Set `PI_CODING_AGENT_DIR` to that directory. Pi attempts WebSocket first and
may fall back to SSE before its first event. Other clients should use whatever
non-secret placeholder shape their own Codex adapter requires.

Pi and DSH continue to send native function calls, receive them, execute tools
locally, and send the native function-call output continuation. The bridge
never executes or deduplicates those calls.

## Verification

Hermetic adapter tests use a deterministic Tower service and no credentials,
live configuration, or production service:

```bash
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
```

They cover matching HTTP/SSE and WebSocket framing, the missing `/v1` alias,
placeholder-key redaction, Luna image typing, typed function-call output image
continuation, the image generation/edit contract and boundaries, generic image
failures, and cancellation/disconnect propagation. Upstream Nanocodex transport
and Kepos ACL matrices are intentionally not duplicated.

The two non-hermetic acceptance probes are operator/deployment checks, not CI
or agent tests. Run one combined Pi probe and one combined DSH probe only after
installing the corresponding client and configuring a test-owned client
profile. Each probe must cover WebSocket text, a Luna image, one function-call
round, and cancellation; a missing client or test profile is a failure, not a
skip. Operator publication verification must use the same named, allowlisted
Kepos HTTP service for `/codex/images` and a test-owned rejected request to
confirm that no intermediary imposes a lower-than-32-MiB bound; it must not
trigger image generation or mutate OAuth state. The minimal live OAuth request
is an explicitly approved deployment validation and must use a dedicated bridge
credential file; it is never run by CI or this repository's automated tests.

## Scope and security boundary

This is a single-node, in-memory bridge. It keeps no cross-connection
continuation state and drops a live operation on completion, cancellation,
disconnect, or failed transport. Transport faults therefore retain Nanocodex's
bounded at-least-once behavior; the bridge makes no exactly-once or external
tool-effect guarantee.
