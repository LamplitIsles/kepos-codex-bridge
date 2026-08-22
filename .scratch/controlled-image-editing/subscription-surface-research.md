# Controlled image editing: subscription-surface research

**Scope.** This began as source research, then used two explicitly approved live probes with the managed ChatGPT OAuth identity. No credential contents or authorization headers were inspected or printed. The conclusion distinguishes source-level evidence from observed behavior on this account.

## Executive finding

Two subscription-native surfaces have now been measured, and neither supplies the required output-control guarantee:

1. **Standalone `/images/edits`.** A request with a valid `173x199` source, same-sized alpha mask, `background: "transparent"`, `size: "848x976"`, `quality: "low"`, fixed `gpt-image-2`, and PNG output succeeded but returned a `1170x1345` RGB PNG with no alpha. It accepted the JSON shape but did not honor at least size or transparency.
2. **Native Responses `image_generation` tool.** A valid test-owned `256x256` RGBA image was sent to `/backend-api/codex/responses` using the exact observed Codex-style shape: main model `gpt-5.6-sol`, hosted `image_generation` tool with `gpt-image-2`, `size: "1024x1024"`, `quality: "low"`, PNG output, opaque background, forced tool choice, streaming, and no store. It succeeded, proving this account accepts the native hosted tool baseline. Its returned PNG was nevertheless `1254x1254` RGB, not the requested `1024x1024`.

The installed OpenClaw OAuth provider independently uses this same native Responses approach, but it also normalizes to preset sizes and reroutes a transparent default `gpt-image-2` request to `gpt-image-1.5`; that is implementation evidence, not a guarantee for this account.

**Decision:** There is no evidenced subscription-native route that guarantees requested dimensions, transparent output, and mask semantics. Do not spend further live requests adding mask/transparency to the native tool: the baseline size mismatch already fails the core controlled-asset requirement. The public OpenAI API remains the only documented control contract, subject to output-byte validation. Adding its credential would violate this product's managed-ChatGPT-only credential boundary unless that boundary is deliberately changed.

## 1. What is proven

### 1.1 Repository and pinned Nanocodex revision

The repository pins:

- `Cargo.toml`: `nanocodex-oai-api` from `https://github.com/gakonst/nanocodex`, revision `7068272602508100b166715b4dfa68c3c36cdf22`.
- `Cargo.lock`: the same immutable git source and revision.
- The local checkout identifies that revision as commit `7068272602508100b166715b4dfa68c3c36cdf22`.

At that revision:

- `crates/nanocodex-oai-api/src/responses/item.rs` has a typed `ResponseItem::ImageGenerationCall` with `id`, `status`, optional `revised_prompt`, and required `result`.
- `crates/nanocodex-oai-api/src/responses/content.rs` has `ContentItem::InputImage { image_url, detail }`; `image_url` is documented as a data URL or remote URL. Function output can also contain an `InputImage`.
- The same `item.rs` has no image-generation tool definition. `ToolDefinition` in `responses/tool.rs` contains function, namespace, custom, and tool-search variants, but not `image_generation`.
- `responses/request.rs` serializes a native `ResponseCreate` with `input`, tool choice, reasoning, etc.; there is no typed `image_generation` tool option in that request structure. The response item variant is therefore more complete than the request-tool surface.

This proves that the pinned library can retain/deserialize the result shape, not that its native request builder can ask the backend to execute the public image-generation tool.

### 1.2 The pinned Nanocodex standalone image path

The same checkout's `crates/nanocodex-tools/src/image_generation/mod.rs` is a separate tool implementation:

- It constructs `{api_base_url}/images/generations` for generation and `{api_base_url}/images/edits` for editing.
- `ImageGenerationRequest` and `ImageEditRequest` contain `prompt`, `background`, `model`, `quality`, and `size`; the edit request additionally contains `images: [{"image_url": ...}]`.
- The tool hardcodes `model: "gpt-image-2"`, `background: "auto"`, `quality: "auto"`, and `size: "auto"` for both generation and editing.
- It accepts up to five referenced images or recent conversation images, then returns the first `data[].b64_json` result as a `data:image/png;base64,...` image.
- There is no mask member in `ImageEditRequest` or in the model-facing `ImagegenArgs`.
- `send_authorized` attaches a bearer token from the active `OpenAiAuth` and, where present, `ChatGPT-Account-ID`; ChatGPT auth has an unauthorized-refresh path.

This is strong source evidence for an official-style subscription-authenticated image client, but it is not evidence that the ChatGPT endpoint honors non-`auto` controls. It also does not provide mask control.

### 1.3 Official OpenAI Responses image-generation contract

The official guide states that the Responses request includes an `image_generation` tool and that the model may generate or edit using prompt and image inputs. It documents these tool options:

- `action`: `generate`, `edit`, or `auto`;
- `background`: `transparent`, `opaque`, or `auto`;
- `size`, including flexible `WIDTHxHEIGHT` values for `gpt-image-2` subject to divisibility, aspect-ratio, pixel, and edge limits;
- `quality`;
- `output_format` (`png`, `webp`, or `jpeg`);
- `output_compression`;
- `input_fidelity`;
- `input_image_mask`;
- `partial_images`.

The official Python schema is the precise machine-readable representation:

```json
{
  "type": "image_generation",
  "model": "gpt-image-2",
  "action": "edit",
  "background": "transparent",
  "quality": "high",
  "size": "848x976",
  "output_format": "png",
  "input_image_mask": {
    "image_url": "data:image/png;base64,..."
  }
}
```

The tool fields are optional except `type` in the generated schema; the example above shows the relevant controlled fields, not a claim that every model supports every option. The current guide says `gpt-image-2` flexible resolutions require width and height divisible by 16 and an aspect ratio between 1:3 and 3:1; `848x976` meets those arithmetic constraints. The guide describes transparent output for `gpt-image-2` as preview functionality and requires a transparency-capable format such as PNG or WebP. There is a first-party documentation/schema mismatch worth preserving as uncertainty: the current generated `openai-python` tool schema also contains a model-specific note saying `gpt-image-2` does not support transparent backgrounds, while the current guide says transparency is available in preview. This makes model/version validation and output-byte validation necessary even on the public API path.

Image input is a normal Responses input content part, not a tool argument:

```json
{
  "type": "input_image",
  "image_url": "data:image/png;base64,...",
  "detail": "auto"
}
```

The official input-image schema also permits `file_id` instead of `image_url`; `image_url` may be a fully qualified URL or a base64 data URL. The mask is different: it is nested under the `image_generation` tool as `input_image_mask`, with `image_url` or `file_id`.

The result is an output item containing base64 image data:

```json
{
  "id": "ig_123",
  "type": "image_generation_call",
  "status": "completed",
  "revised_prompt": "...",
  "result": "<base64 image bytes>"
}
```

The official guide also documents streaming image-generation events, including partial-image events, and says that the completed call's `result` is base64-encoded. The official Python response model describes statuses including `in_progress`, `generating`, `completed`, and `failed`.

### 1.4 First-party OpenAI Codex source

At first-party Codex revision `315195492c80fdade38e917c18f9584efd599304`:

- `codex-rs/ext/image-generation/src/backend.rs` resolves the active model provider and active auth, constructs an `ImagesClient`, and exposes separate `generate` and `edit` calls.
- `codex-rs/codex-api/src/endpoint/images.rs` posts JSON to `images/generations` and `images/edits`.
- `codex-rs/codex-api/src/images.rs` defines generation/edit request bodies with `prompt`, `background`, `model`, `n`, `quality`, and `size`; edit inputs are `images: [{"image_url": ...}]`. It defines the response as `created` plus `data[].b64_json` and optional echoed metadata.
- `codex-rs/ext/image-generation/src/tool.rs` fixes the extension to `gpt-image-2`, `background: auto`, `quality: auto`, and `size: auto`; it selects up to five edit images and returns the decoded result to the model/client.
- `codex-rs/core/src/tools/spec_plan.rs` gates the model-visible image-generation capability on provider authorization/capability and image input modality. Its current source contains an `image_gen` namespace/tool path, while the separate image backend calls the Images routes.

This proves that first-party Codex source has a subscription-native image workflow and that Codex treats image generation as a provider capability. It does **not** prove that a standalone ChatGPT OAuth request with explicit `848x976`, `transparent`, or a mask is accepted or honored. The checked-in tool does not send those controls.

### 1.5 Pi source and documentation

Installed Pi is version `0.84.1` (`@earendil-works/pi-ai/package.json`). Its documentation says:

- ChatGPT Plus/Pro is a subscription provider for the `openai-codex` Responses API.
- Pi's image-generation surface is separate from chat/stream APIs and, at this version, is available through only OpenRouter.
- Pi's image-generation context accepts text and image content, but image-generation models do not participate in tool calling.

The source matches that boundary:

- `dist/providers/images/register-builtins.js` registers only `openrouter-images`.
- `dist/api/openai-responses-shared.js::convertResponsesTools` converts Pi tools to `function` or `custom`; it has no `image_generation` conversion.
- The same parser creates output slots for reasoning, message, function calls, and custom tool calls. An `image_generation_call` has no Pi output slot and would not become an image content block.
- `dist/api/openai-codex-responses.d.ts` exposes ordinary Codex stream options and only string tool choices (`auto`, `none`, `required`), not the specific public image-generation tool-choice object.

Therefore Pi can send ordinary `input_image` content through its Codex Responses adapter, but it currently has no subscription-native image-generation API implementation or complete `image_generation_call` result handling.

### 1.6 The current bridge boundary

The bridge's current `src/lib.rs` is stricter than the public Responses schema:

- `WireTool::lower` accepts only `type: "function"` and rejects all other tool types.
- The current bridge has a separate `/codex/images` contract with fixed provider options; it is not a general Images API and does not expose `size`, `background`, `mask`, multipart bodies, or arbitrary provider options.

Thus the current native bridge cannot carry the public `image_generation` tool as-is. This is source inspection only; no product code was changed.

## 2. What is unknown

1. **ChatGPT OAuth compatibility of the public Responses tool.** Public OpenAI documentation describes an API-key Responses contract. First-party Codex source shows authorization-gated image capability, but no inspected source establishes that the ChatGPT OAuth bearer accepted by `/backend-api/codex/responses` is authorized for the same hosted tool schema.
2. **Whether ChatGPT OAuth honors the options.** The already-proven `/backend-api/codex/images/edits` result—1170x1345, RGB, despite requested `848x976` and `transparent`—is evidence against relying on that standalone route. It does not prove what the Responses hosted tool does.
3. **Mask behavior on the subscription backend.** The public schema has `input_image_mask`, but neither the pinned Nanocodex standalone client nor first-party Codex's shipped standalone request uses it. Acceptance, alpha semantics, and edit behavior under ChatGPT OAuth remain unproven.
4. **Which model/transport pairing is enabled for this account.** The public guide's supported mainline models and the repository's Codex model names are not themselves proof that the same model identifier can invoke the hosted image tool through the ChatGPT backend.
5. **Output enforcement.** Even on a documented public path, the consumer must decode and validate dimensions, PNG color type/alpha, and the edit result. A requested field is not a substitute for checking the returned bytes.

## 3. Smallest safe next probe, if warranted

Do not repeat the already-known standalone `/images/edits` probe. If product approval exists for one live compatibility check, use a test-owned tiny PNG and tiny PNG mask, the existing managed ChatGPT OAuth credential, and one direct native Responses request—not a public API key and not a production bridge deployment—with:

- one text-capable Codex model known to accept the native Responses request;
- one `input_image` data URL;
- `tools: [{"type":"image_generation", ...}]` with `action: "edit"`, `size: "848x976"`, `background: "transparent"`, `output_format: "png"`, and `input_image_mask.image_url`;
- `tool_choice: {"type":"image_generation"}` to avoid an inconclusive “model chose not to call it” result;
- no conversation continuation, persistence, or real user image.

Record only HTTP status, non-secret response shape, decoded dimensions, and PNG color type/alpha. A successful call that returns the expected dimensions and alpha establishes compatibility for that exact account/model/transport at that time; a rejection or wrong output should be treated as “not a supported guarantee,” not as a reason to add fallback credential plumbing. This probe was **not run** for this document.

## 4. Is a direct public-API credential the only currently evidenced way to guarantee controls?

**Yes, in the evidence reviewed.** The public OpenAI Responses documentation and first-party SDK schema are the only sources that explicitly define the requested `image_generation` options and result contract together. The first-party Codex subscription path is real at source level, but its standalone tool deliberately sends `auto` values and omits masks; the ChatGPT OAuth behavior of the public hosted tool is unproven.

This means “public API credential” is the only currently evidenced way to obtain a documented control contract—not a guarantee that every model will honor every option. For a real guarantee, use a model/option combination documented as supported and validate the returned bytes.

## 5. Would adding a public-API credential violate existing product constraints?

**Yes.** The repository's `README.md` explicitly says:

- the bridge loads only its local managed ChatGPT OAuth identity;
- incoming API keys are compatibility data only, ignored, and never become upstream credentials;
- `/codex/images` is a fixed Codex capability boundary, not a general OpenAI Images API;
- the bridge does not add another application bearer credential.

Adding, accepting, or forwarding a direct public OpenAI API credential would contradict that stated security/product boundary and the requested “without adding a public API key” constraint. It should not be added unless the product constraints are deliberately changed in a separate decision.

## Primary sources

### Repository and pinned dependency

- `Cargo.toml` and `Cargo.lock` — pinned Nanocodex git revision.
- `/home/neil/.cargo/git/checkouts/nanocodex-bb564505e4b5e1ea/7068272/crates/nanocodex-oai-api/src/responses/item.rs`
- `/home/neil/.cargo/git/checkouts/nanocodex-bb564505e4b5e1ea/7068272/crates/nanocodex-oai-api/src/responses/content.rs`
- `/home/neil/.cargo/git/checkouts/nanocodex-bb564505e4b5e1ea/7068272/crates/nanocodex-oai-api/src/responses/request.rs`
- `/home/neil/.cargo/git/checkouts/nanocodex-bb564505e4b5e1ea/7068272/crates/nanocodex-oai-api/src/responses/tool.rs`
- `/home/neil/.cargo/git/checkouts/nanocodex-bb564505e4b5e1ea/7068272/crates/nanocodex-tools/src/image_generation/mod.rs`
- Pinned source repository: <https://github.com/gakonst/nanocodex/tree/7068272602508100b166715b4dfa68c3c36cdf22/crates/nanocodex-oai-api>

### Pi

- `/home/neil/.local/share/npm-global/lib/node_modules/@earendil-works/pi-coding-agent/docs/providers.md`
- `/home/neil/.local/share/npm-global/lib/node_modules/@earendil-works/pi-coding-agent/node_modules/@earendil-works/pi-ai/README.md`
- `/home/neil/.local/share/npm-global/lib/node_modules/@earendil-works/pi-coding-agent/node_modules/@earendil-works/pi-ai/dist/api/openai-responses-shared.js`
- `/home/neil/.local/share/npm-global/lib/node_modules/@earendil-works/pi-coding-agent/node_modules/@earendil-works/pi-ai/dist/api/openai-codex-responses.js`
- `/home/neil/.local/share/npm-global/lib/node_modules/@earendil-works/pi-coding-agent/node_modules/@earendil-works/pi-ai/dist/providers/images/register-builtins.js`

### OpenAI public API

- Image-generation tool guide: <https://developers.openai.com/api/docs/guides/tools-image-generation>
- Official Python tool schema: <https://github.com/openai/openai-python/blob/main/src/openai/types/responses/tool_param.py>
- Official Python result schema: <https://github.com/openai/openai-python/blob/main/src/openai/types/responses/response_item.py>
- Official Python input-image schema: <https://github.com/openai/openai-python/blob/main/src/openai/types/responses/response_input_image_param.py>

### First-party OpenAI Codex

All links below pin Codex revision `315195492c80fdade38e917c18f9584efd599304`:

- Image request/response types: <https://github.com/openai/codex/blob/315195492c80fdade38e917c18f9584efd599304/codex-rs/codex-api/src/images.rs>
- Images endpoints: <https://github.com/openai/codex/blob/315195492c80fdade38e917c18f9584efd599304/codex-rs/codex-api/src/endpoint/images.rs>
- OAuth/provider-backed image backend: <https://github.com/openai/codex/blob/315195492c80fdade38e917c18f9584efd599304/codex-rs/ext/image-generation/src/backend.rs>
- Model-facing image tool: <https://github.com/openai/codex/blob/315195492c80fdade38e917c18f9584efd599304/codex-rs/ext/image-generation/src/tool.rs>
- Image capability/auth gate: <https://github.com/openai/codex/blob/315195492c80fdade38e917c18f9584efd599304/codex-rs/core/src/tools/spec_plan.rs>
