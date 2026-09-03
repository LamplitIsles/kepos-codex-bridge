# 1. Make the buffered Responses adapter consumer-neutral

Date: 2026-09-03

## Status

Accepted

## Context

Some integrations need a normal, non-streaming Responses result, whereas the
managed Codex upstream returns SSE. The existing buffered adapter is
mechanically useful for multiple callers and models, but its route and
documentation bind that transport conversion to one consumer.

## Decision

Replace the former consumer-specific route with `POST /codex/buffered/responses`.

The endpoint accepts one JSON-object request without tools,
`previous_response_id`, or `stream: true`, makes the upstream request stream,
and returns the collected terminal response as one standard Responses JSON
object. The caller supplies the model, so the same contract can serve Luna and
GPT-5.3-Codex-Spark. It removes `max_output_tokens` for the Codex upstream and,
for the exact Spark model only, removes `reasoning.summary` and any resulting
empty `reasoning` object. The transparent `/codex/responses` relay remains the
route for streaming, tools, and provider continuation state.

The first version is limited to one non-streaming, no-tools request. It is a
transport adapter, not a second provider SDK: it does not invent a generic
request schema or emulate provider state.

## Consequences

The old consumer-specific route is removed. Buffered callers share one
explicitly bounded contract, while the known Spark incompatibility remains a
documented model-specific normalization rather than a consumer-specific
adapter.
