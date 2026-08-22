Status: draft

## Problem Statement

The current image transport is intentionally minimal, but a client cannot request a transparent PNG, select a valid GPT Image 2 working canvas, choose draft versus final rendering quality, or provide a mask for a local edit. That makes it unsuitable for controlled small UI-asset work: an edit can return an arbitrary auto-sized canvas and callers cannot distinguish the requested operation and settings from verified output facts.

## Current Blocker

One explicitly approved live probe was made through the managed ChatGPT OAuth bridge with a PNG mask, `background: "transparent"`, `size: "848x976"`, `quality: "low"`, fixed `gpt-image-2`, and explicit PNG output. The request succeeded, but its returned PNG was `1170x1345` RGB with no alpha channel. The subscription `/images/edits` backend therefore did not honor at least the requested size and transparent background. The trial source change and PATH helper options were removed rather than exposing a misleading contract.

A follow-up native `/responses` hosted-image baseline was accepted with a valid test-owned input and an explicit `1024x1024` tool size, but returned a `1254x1254` RGB PNG. The alternate subscription-native surface therefore also lacks demonstrated size control.

This spec cannot proceed unless the product boundary changes to an explicitly chosen documented provider/credential path. There is no fallback to the uncontrolled image route.

## Solution

Extend the existing JSON `POST /codex/images` contract with only controlled image-editing inputs: optional source images, one optional PNG mask, `background`, `size`, and `quality`. Keep the upstream model fixed to `gpt-image-2`, keep PNG output fixed, and infer generation versus editing from whether source images exist.

Update the current-machine `kepos-image` helper to expose those controls for direct generation and source-image editing. It remains a transport client: it does not promise pixel-exact preservation, resizing, cropping, or compositing.

## User Stories

1. As a client implementer, I can request a transparent PNG, a valid GPT Image 2 size, and a rendering quality, so that I can create an appropriately sized working asset without relying on prompt wording or `auto` settings.
2. As a client implementer, I can submit one to five source images and one PNG mask, so that the image backend receives an edit request whose masked region is explicitly identified.
3. As a client implementer, I receive the image data URL plus honest request-derived metadata, so that I can record what was requested without mistaking it for unverified provider output metadata.
4. As an operator, I can use the local `kepos-image` helper with source images, mask, background, size, and quality options without exposing the bridge OAuth credential.
5. As an operator, I can run one explicitly approved live OAuth compatibility probe through the bridge and learn whether the ChatGPT subscription endpoint accepts the required JSON edit options; a rejection must not silently fall back to the old uncontrolled request.

## Delivery Boundary

This spec is implemented and reviewed as one PR. It extends the existing bridge image route and updates the current-machine helper. It does not turn the bridge into an image-processing or UI-asset pipeline.

## Implementation Decisions

- Keep one JSON `POST /codex/images` route, one fixed `gpt-image-2` model, fixed PNG output, and the existing local managed ChatGPT OAuth path. Do not add model selection, a second generation/edit route, or an `action` field; missing or empty `images` means generation and one through five images means edit.
- Accept only these optional request controls: `mask` as one PNG data URL, `background` as `auto`, `opaque`, or `transparent`, `size` as `auto` or a valid `WIDTHxHEIGHT`, and `quality` as `auto`, `low`, `medium`, or `high`. A mask requires at least one source image. Reject unknown or invalid values and invalid sizes before an upstream request.
- Validate non-auto sizes against the documented GPT Image 2 bounds: dimensions are multiples of 16, each edge is at most 3840, long-to-short ratio is at most 3:1, and total pixels are 655,360 through 8,294,400. Do not decode, resize, crop, inspect alpha, or compare source/mask dimensions in the bridge.
- Forward the normalized JSON edit shape with image and optional mask objects containing `image_url`, the requested background/size/quality, fixed `model: gpt-image-2`, and explicit `output_format: png`. The public OpenAI Image API documents this JSON shape, but the ChatGPT OAuth endpoint must be proven by the live compatibility probe.
- Return the existing `image_url` plus only honest metadata: derived operation, `requested_model`, `requested_background`, `requested_size`, `requested_quality`, and fixed output format. Do not claim actual model, output dimensions, alpha state, revised prompt, provider timestamps, or mask adherence.
- Update the current-machine Python helper with `--mask`, `--background`, `--size`, and `--quality`; it encodes local source/mask files as data URLs and sends the selected controls. Keep its simple path output contract. Do not add a generic client SDK, multipart support, remote URLs, or local image processing.
- A resulting small UI asset is client-owned post-processing. For example, `173x199` is not a valid GPT Image 2 output size; `848x976` is a compatible near-ratio working canvas, after which a caller may explicitly choose deterministic resizing or composition outside this bridge and helper.

## Testing Decisions

- Extend the existing test-owned local upstream integration seam to prove the normalized generation and edit JSON bodies, option forwarding, fixed model/PNG choice, metadata, and stable failure behavior.
- Add representative validation tests for mask-without-image, non-PNG masks, invalid option values, a valid custom size such as `848x976`, and invalid `173x199`. Do not create exhaustive image-format or size matrices.
- Exercise the updated local helper against a test-owned local bridge for both generation and masked edit payloads; do not use credentials or a production service in that test.
- After hermetic checks pass, run one explicitly approved live OAuth probe through the loopback bridge with a test-owned mask and a valid controlled edit request. Check only HTTP success, PNG signature, returned metadata, and requested working dimensions when deterministically readable. Never print or inspect credentials or raw authorization headers.
- Run the targeted tests, full Rust suite, formatting, clippy, and Linux x86_64 release build. The live probe is an operator verification, not CI.

## Out of Scope

- Pixel-exact masked edits, generated text correctness, UI layout preservation, image resizing/cropping/compositing, or final `173x199` asset production.
- Model selection or discovery, multiple output images, streaming, partial images, moderation controls, JPEG/WebP/compression, remote URLs, file IDs, multipart uploads, or a general OpenAI Images API.
- Any new bridge persistence, queues, retries, TLS, application auth, Docker packaging, Pi/DSH extension, or DSH integration test.

## Further Notes

- Masks are prompt-guidance rather than hard pixel constraints. The caller must retain the original canvas and perform deterministic post-processing when exact geometry or untouched regions matter.
- The pinned Nanocodex source proves JSON image inputs but does not exercise mask or custom option fields against ChatGPT OAuth. The compatibility probe is therefore an explicit acceptance check, not a fallback trigger.
- Research evidence and source citations are recorded in `.scratch/codex-image-transport/research.md`.

### Code-size estimate

Estimated hand-written change: 250–400 lines excluding generated files and lockfiles. This includes controlled request parsing/validation, upstream JSON forwarding, focused integration coverage, route documentation, and the current-machine Python helper update; it excludes a compositor or image library.
