# kepos-codex-bridge

A small Linux `x86_64` loopback service that supplies one bridge-host ChatGPT
OAuth identity to permitted Kepos peers. It is a transparent Codex Responses
relay, not an agent runtime: the connected client owns its request shape,
history, Lite rendering, cache lineage, continuations, tools, and compaction.

## Public contract

The bridge publishes four fixed sibling routes:

- `POST /codex/responses` forwards an HTTP/SSE Responses request. It retains a
  4 MiB encoded request limit and streams the final upstream status, safe
  end-to-end headers, and bytes unchanged.
- `GET /codex/responses` accepts a WebSocket upgrade. It retains the client
  query and application headers, forwards Text and Binary frames unchanged,
  propagates Close, and lets the endpoint libraries handle Ping/Pong.
- `POST /codex/images` retains the existing fixed image capability: JSON
  `{ "model": string, "prompt": string, "images"?: string[] }`, where
  `model` is required, nonblank, and forwarded exactly to the managed
  upstream. It retains a 32 MiB encoded limit and returns exactly
  `{ "image_url": "data:image/png;base64,..." }`. A prompt alone generates;
  one through five `data:image/...` inputs edit.
- `POST /codex/buffered/responses` accepts one caller-supplied, non-streaming
  Responses request, including caller-supplied tool definitions. It retains the
  4 MiB encoded request limit,
  replaces peer identity with managed OAuth, forces the Codex upstream request
  to stream, and returns one buffered `application/json` Responses object. It
  removes `max_output_tokens`; when it does, the response includes
  `x-kepos-ignored-parameters: max_output_tokens`; that limit is not enforced
  before or after generation. For the exact
  `gpt-5.3-codex-spark` model, it also removes `reasoning.summary` (and an
  empty `reasoning` object); other models, including Luna, retain that field.
  Requests with `previous_response_id` or `stream: true` are rejected. The
  adapter forwards tool definitions and preserves returned function calls, but
  the caller remains responsible for executing tools and supplying subsequent
  tool output.
- `POST /codex/web-search` is a stateless text-search adapter. It accepts only
  `{ "commands": { ... } }` with one or more of `search_query` (one to three
  queries), `weather`, `sports` (exactly one operation), `finance`, and `time`.
  A search query is `{ "q": string, "recency"?: non-negative integer,
  "domains"?: string[] }`; weather is `{ "location": string, "start"?:
  "YYYY-MM-DD", "duration"?: positive integer }`; sports is one `{ "fn":
  "schedule" | "standings", "league": "nba" | "wnba" | "nfl" | "nhl" |
  "mlb" | "epl" | "ncaamb" | "ncaawb" | "ipl", ... }` (the bridge adds the
  upstream-only `tool: "sports"` field); finance is `{ "ticker":
  string, "type": "equity" | "fund" | "crypto" | "index", "market"?: string
  }`; and time is `{ "utc_offset": "+HH:MM" | "-HH:MM" }`. Weather,
  finance, and time arrays are capped at 16 entries. All user strings must be
  non-blank, and dates/offsets must use their stated syntax. The bridge fixes
  the upstream model, short response length, settings, and token budget,
  supplies its managed OAuth identity, and rejects caller model, input, request
  ID, response length, continuation, navigation, image-search, and other
  stateful fields. The upstream `results` array is ordinary plaintext JSON and
  is preserved as opaque values; encrypted `encrypted_output` continuation state
  and all other upstream fields are removed. Requests and successful responses
  are bounded at 64 KiB and 1 MiB, respectively, and failures are generic JSON
  errors.

Responses is JSON-opaque. The relay forwards client-controlled model,
instructions, Lite/beta/cache/session/thread/request headers, continuation
state, opaque checkpoints, and compressed bodies without decoding or
reserializing them. It removes peer `Authorization`, `Proxy-Authorization`,
`X-Api-Key`, `Cookie`, account, and FedRAMP identity headers; it then injects
the bridge's managed bearer/account identity. A pre-stream or pre-upgrade
upstream 401 receives one managed-OAuth refresh retry. The relay never exposes
upstream cookies or manufactures SSE events, Responses IDs, or protocol state.

`/codex/buffered/responses` is intentionally the exception to the transparent
relay's JSON-opaque contract. It preserves every compatible request field while
performing only the documented transport and Spark normalizations. For a
successful upstream SSE response, it buffers at most 4 MiB, rebuilds ordered
`output` from `response.output_item.done` events, and combines it with the first
terminal `response.completed`, `response.incomplete`, or `response.failed`
object. It does not stream downstream, and it returns a generic 502 without
partial data when the successful SSE stream is oversized, malformed, or lacks a
terminal response. Non-successful upstream HTTP statuses and safe headers/bodies
remain recognizable. Downstream streaming and provider-managed continuation
workflows continue to use `/codex/responses`.

There is no `/v1` alias, `/compact` route, model alias, client-facing mode
switch, or fixed `--model`/`--instructions` serve option. In particular,
`/codex/buffered/responses` is not a generic OpenAI Responses compatibility API.
Model and instructions belong to the caller. `/codex/images` remains a fixed
transport capability, not a generic Images API: the bridge does not choose,
allowlist, or default image models. The paired DSH imagegen consumer owns its
generation/edit model policy and must send the required field, so update both
sides together before deployment.

## Build

Build the single Linux `x86_64` release artifact on the target Linux host or
approved Linux build environment:

```bash
cargo build --release --target x86_64-unknown-linux-gnu
# target/x86_64-unknown-linux-gnu/release/kepos-codex-bridge
```

The GitHub Actions workflow publishes the Linux `amd64` image on every push to
`main`:

```text
ghcr.io/lamplitisles/kepos-codex-bridge:latest
ghcr.io/lamplitisles/kepos-codex-bridge:sha-<commit>
```

The image runs as UID `10001`, includes CA certificates for the managed OAuth
upstream, and has no container manifest, Helm chart, or non-Linux/amd64 build.

## Bridge-host setup and Kepos publication

Keep the managed OAuth file private and writable only by its owner. It belongs
on the bridge host; never copy it to a peer or put it in a Pi profile.

```bash
export KEPOS_CODEX_AUTH_FILE=/var/lib/kepos-codex-bridge/auth.json
kepos-codex-bridge login --auth-file "$KEPOS_CODEX_AUTH_FILE"
kepos-codex-bridge serve --auth-file "$KEPOS_CODEX_AUTH_FILE" --port 8787
```

The listener is `127.0.0.1:8787` by default. It does not terminate TLS or
publish login, token inspection, logout, metrics, or admin endpoints.

Publish it as a **named Kepos HTTP service** targeting that loopback port, using
the normal HTTP/WebSocket-over-Noise publisher path. Allowlist only the peer
public keys authorized to use the bridge subscription:

```text
service name: codex-bridge
publisher target: http://127.0.0.1:8787
paths: /codex/responses, /codex/buffered/responses, /codex/images, and /codex/web-search
transport: standard Kepos HTTP service (HTTP + WebSocket upgrade for Responses)
allowlist: approved peer public keys
```

Kepos peer identity and its publisher allowlist are the authorization boundary.
An allowed peer can use the bridge account, so do not publish this service to
untrusted peers. The bridge adds no bearer authentication, account
multiplexing, token inspection, request-body logging, conversation persistence,
or prompt-cache registry.

## Test-owned client setup

Use a fresh, private test root for each probe. The examples use a synthetic,
non-secret JWT-shaped placeholder because Pi validates the local
`chatgpt_account_id` claim before connecting. It is ignored by the relay; never
substitute a real Codex OAuth token.

```bash
umask 077
ROOT=$(mktemp -d)
MODEL='<client-selected Codex model>'
RELAY='http://127.0.0.1:<bridge-port>/codex/responses'
PLACEHOLDER='eyJhbGciOiJub25lIn0.eyJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOnsiY2hhdGdwdF9hY2NvdW50X2lkIjoidGVzdC1hY2NvdW50In19.dummy'
mkdir -p "$ROOT/pi-agent" "$ROOT/sessions"
cat >"$ROOT/pi-agent/models.json" <<EOF
{"providers":{"openai-codex":{"baseUrl":"$RELAY","apiKey":"$PLACEHOLDER"}}}
EOF
cat >"$ROOT/pi-agent/settings.json" <<EOF
{"defaultProvider":"openai-codex","defaultModel":"$MODEL","transport":"websocket"}
EOF
```

Before any paid request, read that test `models.json`, confirm the selected
model, and confirm a listener is bound at the loopback port. A system
`HTTP_PROXY` or Clash/Mihomo setting affects egress only; it does not configure
the model endpoint. The explicit route must be:

```text
Pi -> 127.0.0.1:<bridge-port>/codex/responses -> managed Codex upstream
```

### Stock Pi with Ogul Remote Compaction V2

Do not run Pi OAuth login in this profile. Start Pi with the isolated profile,
session directory, and exactly the Ogul compaction extension:

```bash
PI_CODING_AGENT_DIR="$ROOT/pi-agent" \
NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost \
pi --session-dir "$ROOT/sessions" \
  --provider openai-codex --model "$MODEL" \
  --extension "$HOME/.pi/agent/npm/node_modules/@ogulcancelik/pi-codex-compaction/index.ts"
```

Run one normal turn, invoke `/compact` manually, then run one normal follow-up.
Ogul owns the Remote Compaction V2 request and opaque checkpoint; the relay
neither interprets nor stores either. Retain only route confirmation,
success/failure, and numeric input/cache-read/cache-write usage. Do not retain
prompts, request bodies, checkpoints, OAuth data, or test session files.

### Pi `pi-openai-codex-compat` with Responses Lite

Use the same isolated Pi profile and placeholder, install/load the package in
that test-owned profile, and explicitly enable Lite. An environment override
keeps the setting out of a live profile:

```bash
PI_CODING_AGENT_DIR="$ROOT/pi-agent" \
PI_OPENAI_CODEX_COMPAT_RESPONSES_LITE=on \
NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost \
pi --session-dir "$ROOT/sessions" \
  --provider openai-codex --model "$MODEL" \
  --extension '<test-owned pi-openai-codex-compat extension path>'
```

Again run normal → manual `/compact` → normal follow-up. Lite request fields,
`additional_tools`, client metadata, cache keys, and continuation state are
client-owned and pass through untouched except for managed identity.

### Pinned Nanocodex endpoint overrides

Nanocodex needs no bridge-specific session adapter. Configure its public
endpoints and a non-secret local API-key placeholder; choose its normal
transport as needed:

```rust
let openai = OpenAi::builder("nonsecret-local-placeholder")
    .model(Model::Luna)
    .api_base_url("http://127.0.0.1:<bridge-port>/codex")
    .websocket_url("ws://127.0.0.1:<bridge-port>/codex/responses")
    .build()?;
```

Its own `Session::turn().create()` and `turn().compact()` then use the selected
HTTP or WebSocket transport. The pinned client is verified against a hermetic
recording origin; it does not require a separate paid live run.

## Verification

Hermetic relay and image checks require no live credentials or services:

```bash
cargo fmt --check
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
cargo build --release --target x86_64-unknown-linux-gnu
```

### Git hooks

Install [Lefthook](https://lefthook.dev/) once, then activate the repository hooks:

```bash
lefthook install
```

`pre-commit` verifies formatting. `pre-push` runs the hermetic test suite and
Clippy with warnings denied. GitHub Actions repeats those checks and the Linux
release build for every pull request.

For the explicitly approved live acceptance matrix, start a separate temporary
loopback bridge using a dedicated, test-owned managed-auth file and port. Do
not reuse, stop, or refresh an existing bridge process or its auth file. Verify
the test profile route, listener, and model before each row; run the Stock
Pi+Ogul and Pi compat Lite normal → compact → follow-up rows; then securely
remove only the test root and the temporary bridge it started. A cache hit is
an observed provider result, not a guarantee or CI assertion.

## Scope exclusions

The bridge does not validate DSH. It does not validate or proxy Pi compat's
optional image or web companion endpoints. It is not a bridge-side Lite
renderer, Responses schema adapter, session/cache owner, compaction encoder,
durable store, generic reverse proxy, or cache-hit guarantee. Pi, Ogul,
Nanocodex, and `pi-openai-codex-compat` remain unmodified clients that own
reconnection, retry, continuation, history, and all client protocol state.
