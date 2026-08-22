# Findings: smallest expansion for controlled `gpt-image-2` editing

## Scope and conclusion

This began as source and documentation research. It was followed by one
explicitly approved live probe through the local bridge; no credential material
was inspected or printed.

### Live ChatGPT OAuth compatibility probe — blocker

The probe sent two supplied PNG references, a test-owned `173x199` RGBA PNG
mask, `background: "transparent"`, `size: "848x976"`, `quality: "low"`, and
the bridge's fixed `model: "gpt-image-2"` plus `output_format: "png"`. The
bridge's hermetic upstream fixture already proves those fields are emitted
(`tests/bridge.rs` in the discarded trial), and the live request returned HTTP
success, so the ChatGPT OAuth backend did not reject the JSON shape.

The returned PNG was instead `1170x1345`, RGB (`color_type=2`) with no alpha
channel. It therefore did **not** honor the requested working size or
transparent background. The result alone cannot establish whether it honored
`quality` or the mask, but it disproves the controlled-output contract needed
for a final UI-asset workflow.

A follow-up native `/responses` hosted-image baseline was also accepted with a
valid test-owned input, but requested `1024x1024` output came back as a
`1254x1254` RGB PNG. Thus the alternate subscription-native surface likewise
does not establish size control.

**Current conclusion:** do not ship an option facade on either observed
subscription image surface. The trial bridge extension and PATH-helper options
were removed and the proven thin bridge was restored. Achieving a documented
control contract requires an explicitly chosen public API credential boundary;
do not add a silent fallback or continue final-asset generation on the current
image route.

## 1. Request shape, media type, and upstream evidence

### What the pinned Nanocodex source actually sends

The bridge pins Nanocodex revision `7068272602508100b166715b4dfa68c3c36cdf22`
(`Cargo.toml:11`). The local upstream checkout is
`/home/neil/code/references/github.com/gakonst/nanocodex`.

* `ImageGenerationHandler` constructs `/images/generations` and
  `/images/edits` by appending those paths to the configured API base
  (`crates/nanocodex-tools/src/image_generation/mod.rs:35-52`). Its request
  structs are JSON-serializable: generation has `prompt`, `background`,
  `model`, `quality`, and `size` (`:227-233`); edit has those same fields plus
  `images: Vec<ImageUrl>` (`:236-248`).
* The HTTP call uses `request.json(body)` (`:158-180`), so this upstream path is
  an `application/json` request, not multipart. The local unit test captures
  the generation body and asserts the exact JSON shape, including
  `model: "gpt-image-2"`, `background: "auto"`, `quality: "auto"`, and
  `size: "auto"` (`crates/nanocodex-tools/src/image_generation/tests.rs:76-90`).
* `request_for_args` selects generation when there are no references and edit
  otherwise; it wraps each image as `{ "image_url": data_url }`
  (`mod.rs:268-334`). It limits references to five (`:21-23, 268-277`).
* The OAuth integration test proves two generation requests followed by an
  edit request at `/v1/images/generations`, `/v1/images/generations`, and
  `/v1/images/edits`, and proves bearer/account/FedRAMP header behavior; it
  does not test a mask (`tests/it/oauth/image_generation.rs:25-87,
  99-133`).

The pinned Nanocodex code therefore proves this compatibility shape:

```json
// generation
{
  "prompt": "...",
  "background": "auto",
  "model": "gpt-image-2",
  "quality": "auto",
  "size": "auto"
}

// edit
{
  "images": [{"image_url": "data:image/png;base64,..."}],
  "prompt": "...",
  "background": "auto",
  "model": "gpt-image-2",
  "quality": "auto",
  "size": "auto"
}
```

The bridge uses the auth-mode base URL, not necessarily the public OpenAI
origin: the upstream auth source defines API-key mode as
`https://api.openai.com/v1` and ChatGPT mode as
`https://chatgpt.com/backend-api/codex`
(`crates/nanocodex-oai-api/src/auth/mod.rs:35-41`). Thus, public OpenAI API
documentation is evidence for a public surface, not conclusive evidence that
the ChatGPT OAuth backend accepts every public field.

### Official public Image API evidence

Official references consulted:

* [Image generation guide](https://developers.openai.com/api/docs/guides/image-generation),
  especially **Generate Images**, **Edit Images**, and **Customize Image
  Output**.
* [Create image](https://developers.openai.com/api/reference/resources/images/methods/generate)
  and [Create image edit](https://developers.openai.com/api/reference/resources/images/methods/edit).

The official generation example explicitly uses `Content-Type: application/json`
and `POST https://api.openai.com/v1/images/generations`. The generation
reference lists these known fields: `prompt`, `background`, `model`, `moderation`,
`n`, `output_compression`, `output_format`, `partial_images`, `quality`,
`response_format`, `size`, `stream`, and `user`. Some are model-specific:
`response_format` is not supported for GPT Image models; `style` is DALL-E-only;
`output_compression` applies to JPEG/WebP. The guide says GPT Image outputs are
base64 and that PNG is the default.

The official edit surface has two documented presentations that must not be
silently conflated:

1. The guide's Image API edit example uses `POST /v1/images/edits` with `-F`
   file parts, i.e. `multipart/form-data`, for uploaded images.
2. The current edit reference exposes a JSON body schema with
   `images: [{file_id, image_url}]` and `mask: {file_id, image_url}`. An image
   URL may be a fully qualified URL or a base64 data URL; exactly one of
   `file_id` or `image_url` is provided for each reference. Its known fields
   are `images`, `prompt`, `background`, `input_fidelity`, `mask`, `model`,
   `moderation`, `n`, `output_compression`, `output_format`,
   `partial_images`, `quality`, `size`, `stream`, and `user`.

For this bridge, the smallest no-upload representation suggested by the JSON
reference is:

```json
{
  "images": [{"image_url": "data:image/png;base64,..."}],
  "mask": {"image_url": "data:image/png;base64,..."}
}
```

That is **official-public-API evidence**, not Nanocodex OAuth evidence:
Nanocodex's edit struct has no mask field (`mod.rs:236-243`), and its OAuth
integration test never sends one (`tests/it/oauth/image_generation.rs:38-87`).
The guide additionally requires the image and mask to have the same format and
size, requires an alpha channel, applies a mask to the first image when several
images are supplied, and warns that masking is prompt-based rather than pixel-
exact. The guide's `input_fidelity` section says to omit that parameter for
`gpt-image-2`; the model processes image inputs at high fidelity automatically.

## 2. Current bridge versus caller control

### Already implemented

* Public route: `POST /codex/images`, with a separate 32 MiB body limit, while
  `/codex/responses` remains at 4 MiB (`src/lib.rs:48-55, 112-124`).
* Public input is JSON with required `prompt`, optional `images`, and an ignored
  compatibility `api_key`; unknown fields are rejected by `deny_unknown_fields`
  (`src/lib.rs:427-434`). Content type must be `application/json`, prompt must
  be nonblank, there may be at most five images, and every image must begin as
  a nonempty `data:image/...` URL (`src/lib.rs:451-482`). This is only a shallow
  data-URL check; it does not decode the bytes or validate their image format.
* Empty `images` selects `/images/generations`; nonempty `images` selects
  `/images/edits`. The bridge sends the Nanocodex-compatible fixed body with
  `gpt-image-2`, `background: "auto"`, `quality: "auto"`, and `size: "auto"`
  (`src/lib.rs:536-564`). Edit inputs are wrapped as `{image_url: ...}`.
* It sends managed auth headers and retries once through Nanocodex's managed
  ChatGPT recovery path after a 401 (`src/lib.rs:565-575, 594-612`). A peer
  `api_key` never becomes an upstream credential; the existing integration test
  asserts this (`tests/bridge.rs:325-381`).
* It takes the first upstream `data[].b64_json` and returns
  `{ "image_url": "data:image/png;base64,..." }`; non-success, malformed, or
  empty results become a generic 502 (`src/lib.rs:484-496, 518-521, 584-591`).

### Callers cannot control

Callers currently cannot select a mask, background, size, quality, output
format, number of outputs, streaming, moderation, or a model. They also cannot
request an explicit generation/edit operation independent of images; that is
currently inferred from images. The bridge drops upstream `created`, output
metadata, and any revised prompt because `ImageResponse` only retains `data`
and `b64_json` (`src/lib.rs:442-449, 584-591`). It also unconditionally labels
the result PNG without an explicit `output_format` in its outgoing body. That
label is consistent with the documented GPT Image default, but the current
bridge code alone does not establish the bytes' format.

## 3. Minimal public contract recommendation

### Request

Keep one endpoint and this small JSON object:

```json
{
  "prompt": "string, nonblank",
  "images": ["data:image/... URL", "..."],
  "mask": "data:image/png URL",
  "background": "auto | opaque | transparent",
  "size": "auto | WIDTHxHEIGHT",
  "quality": "auto | low | medium | high"
}
```

All fields other than `prompt` remain optional. Recommended semantics:

* **Model fixed:** use `gpt-image-2` internally; do not expose `model`. This
  preserves the bridge's capability boundary and avoids a model compatibility
  matrix.
* **Selection by inputs:** omitted/empty `images` means generation; one to five
  images means edit. Do not add `action`; it would duplicate this state. A
  `mask` without at least one image is a 400. A mask is one PNG data URL and is
  forwarded as `{ "image_url": mask }`; with multiple images it applies to the
  first image per the official guidance.
* **Background:** default `auto`; permit `opaque` and `transparent` only if the
  OAuth compatibility seam accepts them. Official docs currently describe
  transparent GPT Image backgrounds as preview functionality. `transparent`
  requires PNG (already fixed here).
* **Size:** default `auto`; permit a validated `WIDTHxHEIGHT` string rather
  than a separate target-width/target-height pair. Validate current
  `gpt-image-2` constraints at the bridge boundary: both edges multiples of 16,
  aspect ratio no wider than 3:1, total pixels between 655,360 and 8,294,400,
  and maximum edge no greater than 3,840. These constraints come from the
  official guide; they are not present in the current Nanocodex structs.
* **Quality:** default `auto`; permit `low`, `medium`, and `high`. `auto` is
  the existing behavior and `low` is the sensible default for quick UI-asset
  iteration if a caller explicitly chooses a quality.
* **PNG:** do not expose `output_format` or compression. The bridge should
  request `output_format: "png"` internally if the OAuth endpoint accepts the
  documented field, and only promise PNG after that compatibility is verified.
  If the subscription endpoint rejects the field, the alternative is to rely
  on its proven default only after a test-owned compatibility observation; the
  current `data:image/png` prefix is not proof by itself.

The public route should remain JSON and data-URL-only. It should not accept
multipart merely because the public file-upload example uses it. The bridge's
known working OAuth transport is JSON (`mod.rs:158-180`; `src/lib.rs:594-612`).

### Response

The smallest compatibility-preserving response remains:

```json
{"image_url":"data:image/png;base64,..."}
```

If metadata is required for new callers, add only invariant/request metadata,
not guessed provider metadata:

```json
{
  "image_url": "data:image/png;base64,...",
  "requested_model": "gpt-image-2",
  "operation": "generation | edit",
  "format": "png"
}
```

`operation` is derived from `images` and is response metadata, not a second
request action. Do not claim actual width/height, quality, background, revised
prompt, or `created` unless the bridge either receives and validates those
values or decodes the returned image. Name all request echoes honestly: `requested_model`, `requested_size`,
`requested_quality`, and `requested_background`; do not present them as actual
output metadata. The current upstream response
parser gives no such facts (`src/lib.rs:442-449`).

## 4. The 173x199 UI asset

`173x199` cannot be sent as a `gpt-image-2` output size: neither dimension is
a multiple of 16 and its area is only 34,427 pixels, below the documented
655,360-pixel minimum. The model also cannot be asked for arbitrary small output
by using the current `size: "auto"` contract.

A practical valid canvas that stays very close to the asset's aspect ratio is
`848x976`:

* both dimensions are multiples of 16;
* area is 827,648 pixels, within the current minimum/maximum;
* the aspect ratio is approximately 1.151, close to `199/173` approximately
  1.150; and
* it is well below the edge and 3:1 limits.

That particular size is a calculation from the published constraints, not an
OpenAI-prescribed size. A caller can request it, then deterministically decode
the returned PNG and downsample to exactly `173x199`. If preserving a source
composition matters, the caller—not the bridge—should also own any letterbox,
crop, alpha-preserving resize, or final compositing. For a masked edit, the
caller must prepare the source and mask in matching dimensions/format and keep
the alpha channel; the official guide warns that the model's mask adherence is
not exact, so deterministic post-composition is the only way to guarantee the
final UI geometry.

The bridge should not add an image-processing dependency or silently resize
inputs. It is a transport/auth boundary; the current implementation has no
filesystem, artifact, or image-decoding responsibility (`src/lib.rs:536-612`).

## 5. Verification seams

1. **Pure request validation:** content type, unknown fields, blank prompt,
   empty versus nonempty `images`, five-image boundary, data-URL validation,
   mask-without-image rejection, PNG mask media type, option enums, and size
   constraint boundaries. Include `848x976` as an accepted example and
   `173x199` as a rejected output size.
2. **Local upstream HTTP fixture:** assert `application/json`, exact generation
   body, exact edit body, wrapped `images`, optional `mask` object, fixed model,
   PNG choice, option forwarding, and endpoint selection. Assert no peer key or
   source payload leaks into auth or error responses. The existing fixture and
   tests already establish this pattern (`tests/bridge.rs:59-114, 325-501`).
3. **Auth seam:** retain the existing managed bearer/account/FedRAMP header
   assertions and one-401 recovery behavior; do not read or alter live OAuth
   state. Nanocodex's OAuth test is the primary local precedent
   (`tests/it/oauth/image_generation.rs:25-87, 128-133`).
4. **Response seam:** fixture a valid base64 response and verify the data URL,
   empty/malformed data failure, and the promised metadata. If `format: "png"`
   is promised, test the explicit upstream PNG option or validate the returned
   PNG signature; otherwise omit the claim.
5. **Compatibility probe (operator-owned, not this research run):** a dedicated
   test-owned or explicitly approved environment must establish whether the
   ChatGPT OAuth `/images/edits` backend accepts JSON `mask`, custom `size`,
   `background`, and `output_format`. Public API documentation cannot substitute
   for this proof, and no live image call should be added to CI.
6. **Client seam:** test resize/crop/alpha/composition in the eventual Pi/DSH
   client, not in this bridge. The bridge test should only assert that the
   requested model canvas and data URL cross the transport.

## Explicitly out of scope

* model selection, model discovery, a second public action field, separate
  generation/edit routes, or a general OpenAI Images API compatibility layer;
* multipart uploads, File API IDs, remote URLs, URL fetching, local paths,
  conversation-history selection, artifact storage, rendering, or client tool
  schemas;
* `n > 1`, streaming/partial images, moderation/user controls, JPEG/WebP,
  compression, revised prompts, response IDs, and `input_fidelity` for
  `gpt-image-2`;
* bridge-side resizing, cropping, masking/compositing, image caches, durable
  jobs, queues, billing, quotas, or retries beyond managed OAuth recovery; and
* live image API calls, credential inspection, or product implementation in
  this research task.

## Source index

* Bridge: `src/lib.rs:427-482, 498-612`; integration evidence:
  `tests/bridge.rs:325-501`.
* Pinned upstream dependency: `Cargo.toml:11`.
* Local Nanocodex image transport:
  `/home/neil/code/references/github.com/gakonst/nanocodex/crates/nanocodex-tools/src/image_generation/mod.rs:21-24,35-180,218-360`.
* Local Nanocodex image tests:
  `/home/neil/code/references/github.com/gakonst/nanocodex/crates/nanocodex-tools/src/image_generation/tests.rs:1-90` and
  `/home/neil/code/references/github.com/gakonst/nanocodex/crates/nanocodex-tools/tests/it/oauth/image_generation.rs:25-133`.
* Official docs: [image generation guide](https://developers.openai.com/api/docs/guides/image-generation),
  [generation reference](https://developers.openai.com/api/reference/resources/images/methods/generate),
  and [edit reference](https://developers.openai.com/api/reference/resources/images/methods/edit).
