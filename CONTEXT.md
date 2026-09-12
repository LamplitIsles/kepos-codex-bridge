# Kepos Codex Bridge context

## Language

**Transparent Responses relay** — The `/codex/responses` endpoint that replaces
peer credentials with managed OAuth and otherwise relays HTTP/SSE or WebSocket
Responses traffic without interpreting its protocol.

**Buffered Responses adapter** — The consumer-neutral `/codex/buffered/responses`
endpoint for one non-streaming Responses request: it makes the
upstream Codex request stream, collects that SSE response, and returns one
standard Responses JSON object. It removes `max_output_tokens`. The caller
selects the model; requests with `previous_response_id` or `stream: true` are
rejected.

**Caller request** — The Responses request supplied by an integration. The
adapter preserves its compatible fields except for documented upstream
normalizations.

**Image model responsibility split** — `POST /codex/images` requires a
nonblank caller-supplied `model` and forwards that identifier unchanged to the
managed image upstream. The bridge owns managed identity, generation/edit
routing, input limits, fixed image options, and the `image_url` response; the
calling image tool owns model policy and defaults. The bridge has no image
model allowlist, default, or compatibility fallback, so paired callers must
send the field before this route is deployed with them.
