# OpenAI Codex HEAD: subscription image generation vs public API controls

## Scope and revision

Research was local-only. No source files in the Codex checkout were modified, no credentials were read, and no live API was called.

- Checkout: `/home/neil/code/references/github.com/openai/codex`
- Checked-out HEAD: `343074d4207d572809bd8cea15f4be1d09d98e0b`
- HEAD subject: `Report runtime MCP connection status (#40068)`
- The checkout was clean apart from its branch metadata when inspected.

The conclusions below are source/history conclusions. They do not establish what an OpenAI server currently accepts or honors unless the source explicitly encodes that behavior.

## Executive conclusion

The latest source establishes a real subscription-native Codex image path, but its **model-facing contract is deliberately narrow**:

1. Codex gates the built-in image tool away from Free accounts and requires an OpenAI/Codex-authorized provider, an image-capable model, namespace support, and the image-generation provider capability (`codex-rs/core/src/tools/spec_plan.rs:611-650`). This is strong code evidence for subscription/Codex entitlement gating, but it does not name Plus/Pro quotas or prove server-side entitlement details.
2. For that tool, Codex fixes `gpt-image-2`, `background: auto`, `quality: auto`, `size: auto`, and omits `n` (`codex-rs/ext/image-generation/src/tool.rs:58-59,400-468`). The model can choose only a prompt and image references/recent-image count (`:89-95`), not public API controls.
3. The standalone backend resolves the active provider and auth at request time and posts JSON to `images/generations` or `images/edits` (`codex-rs/ext/image-generation/src/backend.rs:47-117`; `codex-rs/codex-api/src/endpoint/images.rs:16-72`). With ChatGPT-style auth and no configured base URL, the provider default is `https://chatgpt.com/backend-api/codex` (`codex-rs/model-provider-info/src/lib.rs:40,292-316`), so the resulting private routes are `/images/generations` and `/images/edits`.
4. The typed internal request structs expose some image fields—`background`, `model`, `n`, `quality`, and `size`—but **not** `mask`, `output_format`, `output_compression`, `moderation`, or `input_fidelity` (`codex-rs/codex-api/src/images.rs:5-30`). Edit inputs are JSON `images: [{"image_url": ...}]`, not the public CLI's multipart image/mask shape.
5. The repository explicitly separates the built-in tool from its API/CLI fallback: the checked-in reference says the two surfaces do not expose the same controls and says not to treat `quality`, `input_fidelity`, masks, background, and output format as built-in `image_gen` arguments (`codex-rs/skills/src/assets/samples/imagegen/references/image-api.md:1-9,44-90`).

**Assessment:** This supports “private Codex image generation has a deliberately fixed/auto limited client contract” much more strongly than “the private interface is merely behind or accidentally unupgraded.” It is nevertheless true that the shipped private request surface is behind the checked-in public API/CLI control surface: mask and output-format controls are absent, and explicit size/quality/background are never sent by the built-in tool. Source alone cannot tell whether the private server would accept or honor those omitted fields.

## 1. Subscription/Codex capability and routing

### 1.1 Eligibility gate

`image_generation_available` requires all of the following:

- the `image_generation` feature is enabled;
- the cached account plan is not `PlanType::Free`;
- the active provider reports both `image_generation` and `namespace_tools` capability;
- the model advertises `InputModality::Image`; and
- either the provider has OpenAI actor authorization, or it requires OpenAI auth and the current auth uses the Codex backend (`codex-rs/core/src/tools/spec_plan.rs:611-650`).

The extension itself marks its provider configuration available only for the OpenAI provider, providers requiring OpenAI auth, or providers carrying OpenAI actor authorization (`codex-rs/ext/image-generation/src/extension.rs:31-44`). App-server installs the extension (`codex-rs/app-server/src/extensions.rs:107-114`), and the tool registry drops it when the eligibility gate fails (`codex-rs/core/src/tools/spec_plan.rs:1258-1282`).

This is code proof of a **non-Free, Codex-authorized capability gate**. It is not proof that every paid plan is enabled, of the numerical quota, of model availability, or of server behavior. The server remains the authority for actual authorization and limits.

The client also recognizes a server-side usage-limit response whose `limit_id` is exactly `image_gen`, preserving a reset time when available (`codex-rs/ext/image-generation/src/tool.rs:234-259`). That establishes the expected quota/error metadata shape, not the quota values.

### 1.2 Provider and auth selection

The image backend does not hardcode an API key or a separate image host. It calls the active `SharedModelProvider`'s `api_provider()` and `api_auth()`, then constructs `ImagesClient<ReqwestTransport>` (`codex-rs/ext/image-generation/src/backend.rs:47-76`). It adds `x-codex-image-turn-id` and, when present, the originator header (`:83-117`).

The provider adapter chooses a default base URL from auth mode:

- ChatGPT, ChatGPT auth tokens, headers, agent identity, and personal access token auth -> `CHATGPT_CODEX_BASE_URL`, defined as `https://chatgpt.com/backend-api/codex`;
- other auth modes -> `https://api.openai.com/v1`;
- an explicitly configured `base_url` overrides either default (`codex-rs/model-provider-info/src/lib.rs:40,292-316`).

Auth resolution prefers a provider-configured API key or experimental bearer token; otherwise it returns an unauthenticated provider for providers that do not require OpenAI auth, or converts the active `CodexAuth` into bearer/header auth (`codex-rs/model-provider/src/auth.rs:172-220,289-320`). Thus the same typed image client can be provider-generic, but the built-in subscription route is selected by the active provider/auth configuration rather than by an image-specific credential.

`ModelProvider::api_provider` and `api_auth` are the abstraction boundary used by the backend (`codex-rs/model-provider/src/provider.rs:204-239`). The source does not show a server-side account lookup or a plan-to-model mapping in the image backend.

## 2. Current image backend and wire request types

### 2.1 Routes and serialization

`ImagesClient::generate` posts to `images/generations`; `ImagesClient::edit` posts to `images/edits`. Both call one generic JSON POST helper and deserialize the response into `ImageResponse` (`codex-rs/codex-api/src/endpoint/images.rs:16-72`). `EndpointSession` builds the provider base URL plus the relative path, merges extra headers, applies auth, and executes the request (`codex-rs/codex-api/src/endpoint/session.rs:19-110`; URL joining is `codex-rs/codex-api/src/provider.rs:43-88`).

For a default ChatGPT-auth provider, the source therefore constructs:

- `POST https://chatgpt.com/backend-api/codex/images/generations`
- `POST https://chatgpt.com/backend-api/codex/images/edits`

That URL derivation is source proof. It is not proof that the server treats the routes identically to public `/v1/images/*` routes.

### 2.2 Request fields

`ImageGenerationRequest` contains:

```text
prompt: String
background: Option<ImageBackground>
model: String
n: Option<u64>
quality: Option<ImageQuality>
size: Option<String>
```

`ImageEditRequest` contains the same options plus `images: Vec<ImageUrl>`, where `ImageUrl` serializes as `{ "image_url": ... }` (`codex-rs/codex-api/src/images.rs:5-38`). `background` is `transparent|opaque|auto`, and `quality` is `low|medium|high|auto` (`:40-54`). Optional fields are omitted from JSON when `None`.

There is no request member for:

- `mask` / `input_image_mask`;
- `output_format`;
- `output_compression`;
- `moderation`; or
- `input_fidelity`.

The client response requires `created`, `data`, and `data[].b64_json`, while it optionally records echoed `background`, `quality`, and `size` (`codex-rs/codex-api/src/images.rs:56-70`). A client test includes response `output_format` and token-usage fields but the expected typed response ignores them (`codex-rs/codex-api/src/endpoint/images.rs:148-185`). This proves the current Rust client does not consume those response fields; it does not prove that the server cannot return them.

## 3. Model-facing private Codex contract

The extension's model-facing schema is only:

```text
prompt: String
referenced_image_paths: Option<Vec<AbsolutePathBuf>>   // max 5
num_last_images_to_include: Option<usize>               // 1..=5
```

It rejects unknown fields (`codex-rs/ext/image-generation/src/tool.rs:84-95`). Generation occurs when neither image selector is supplied. Referenced paths are read, converted into data URLs, and used for editing; alternatively, a bounded recent-image window can be selected (`:400-467,480-513`).

Both generation and edit requests are constructed with exactly:

```text
model      = "gpt-image-2"
background = "auto"
quality    = "auto"
size       = "auto"
n          = None
```

The generation construction is at `codex-rs/ext/image-generation/src/tool.rs:413-420`; the edit construction is at `:460-467`. The current tests lock this behavior in as “generate with fixed defaults” (`codex-rs/ext/image-generation/src/tests.rs:48-75`).

The result is treated as base64 PNG: generated conversation/tool output is formed as `data:image/png;base64,...` (`codex-rs/ext/image-generation/src/tool.rs:620-635`), and the artifact path is always `<call-id>.png` (`codex-rs/ext/image-generation/src/artifact.rs:5-29`). The tool does preserve returned transparency metadata: `ImageResponse.background` maps to `true`, `false`, or `null` (`codex-rs/ext/image-generation/src/tool.rs:174-198`). With `auto`, `null` is explicitly the expected result when the response reports automatic or unavailable background.

The built-in description tells the model to ask for transparent output in natural-language instructions, but it does not add a wire-level background argument (`codex-rs/ext/image-generation/imagegen_description.md:1-14`). Therefore, the source proves a prompt-level request plus an `auto` backend parameter; it does not prove that the private server will turn a prompt into native transparency.

## 4. Public API/CLI control surface in the same checkout

The repository's fallback reference explicitly labels itself CLI/API-only and warns that built-in `image_gen` and the fallback CLI do not expose the same controls (`codex-rs/skills/src/assets/samples/imagegen/references/image-api.md:1-9`). It documents:

- models: `gpt-image-2`, `gpt-image-1.5`, `gpt-image-1`, `gpt-image-1-mini`;
- quality: `low|medium|high|auto`;
- size: `auto` or model-specific dimensions;
- background: `transparent|opaque|auto`;
- output format: `png|jpeg|webp`;
- output compression;
- moderation;
- edit image inputs, up to the documented model/API limit;
- edit mask; and
- input fidelity for models that support it (`image-api.md:8-18,20-42,44-67`).

The fallback CLI documentation makes the same boundary explicit and lists `--quality`, `--input-fidelity`, and edit-only `--mask` as CLI fallback controls (`codex-rs/skills/src/assets/samples/imagegen/references/cli.md:13,64-84,146-175`). It documents per-job `size`, `quality`, `background`, `output_format`, `output_compression`, `moderation`, `n`, and `model` overrides (`:227-234`).

The checked-in script is executable evidence of those public/API calls:

- defaults to `gpt-image-2`, size `auto`, quality `medium`, and output format `png` (`codex-rs/skills/src/assets/samples/imagegen/scripts/image_gen.py:25-38`);
- validates flexible `gpt-image-2` dimensions, including max edge, 16-pixel divisibility, 3:1 ratio, and pixel bounds (`:121-144`);
- rejects `background=transparent` and `input_fidelity` for `gpt-image-2` in its fallback validation (`:184-201`);
- includes `output_format` in generation payloads (`:710-752`); and
- includes `output_format` and `input_fidelity`, attaches a separate `mask`, and calls `client.images.edit(**request)` for edits (`:767-831`).

This establishes a materially broader **fallback/public API client contract** than the built-in subscription tool. It does not establish that all those fields are accepted or honored by the private ChatGPT/Codex routes.

## 5. Relevant local history

The history strongly favors intentional contract design over an accidental stale implementation:

- `423488480` (2026-05-22), **Add typed Images client to codex-api**: introduced the typed `images/generations` and `images/edits` client, request/response types, and tests. The commit message says the client was added for the Codex image proxy routes and that model slugs remain open-ended.
- `ecb41fcb6` (2026-05-28), **Add feature-gated standalone image generation extension**: introduced the standalone extension. Its commit message explicitly says: “The initial extension contract intentionally fixes the image model to `gpt-image-2` and uses automatic image parameters.” It also says the hosted tool remains fallback when the standalone executor is unavailable. Relevant paths: `codex-rs/ext/image-generation/src/backend.rs`, `src/tool.rs`, and `src/extension.rs`.
- `10b039903` (2026-05-29), **Route extension image generation through the native image completion pipeline**: changed completion/persistence/UI integration, not the image option contract.
- `123cf62a4` (2026-06-08), **Route image edits through referenced file paths**: replaced a semantic `action` argument/history heuristics with `referenced_image_paths`; it retained fixed `gpt-image-2`/auto request construction. Relevant path: `codex-rs/ext/image-generation/src/tool.rs`.
- `a7c72aee8` (2026-07-09), **Use the image generation extension by default**: made the extension the default path where eligible, reducing the likelihood that the current built-in behavior is merely an unused prototype.
- `928bda82c` (2026-08-05), **Preserve image transparency metadata in app-server items**: added response-derived `transparentBackground`; it did not add an explicit transparency request option to the model-facing schema.
- `8cabf5a6c` (2026-08-10), **Use native transparency in the imagegen skill**: updated skill guidance to request transparent output in built-in `image_gen`, while keeping explicit CLI fallback controls separate. The current extension source still sends `background: auto`, so the guidance is not proof of an explicit private wire control.
- `edcec1337` (2026-08-11), **Expose image generation usage-limit failures**: added `image_gen` usage-limit metadata handling.
- `682f57254` (2026-08-17), **Persist generated images through turn executors**: improved sandboxed persistence and path hints, not request controls.

Local history searches at this HEAD find only the initial extension commit for the `IMAGE_MODEL` assignment and only the typed-client introduction for `output_format` in the Rust image client/extension paths. The later relevant commits add routing, persistence, metadata, and quota handling rather than expanding the built-in option schema.

## 6. Hypothesis assessment

### Hypothesis A: “The private Codex image interface is behind/unupgraded.”

**Partially supported only in the narrow surface-area sense.** The shipped private/client path lacks public-style mask and output-format controls and never sends explicit size, quality, or background from the built-in tool. If “behind” means “does not expose the public API's full control contract,” that is directly established.

**Not supported as the best explanation for the design.** The initial extension commit explicitly calls the fixed `gpt-image-2` plus automatic parameters intentional, and subsequent history keeps that contract while adding production integration. The source looks like a deliberate private model-facing abstraction, not an unfinished public API wrapper.

### Hypothesis B: “The private Codex image interface deliberately uses an auto/fixed limited contract.”

**Strongly supported by code and history.** The fixed model and auto values are visible in the request builder, the model-facing schema excludes option fields, the tests assert fixed defaults, and the original extension commit documents the intent.

### What source cannot decide

The source cannot establish any of the following server-side facts:

- whether private `/backend-api/codex/images/*` accepts public `output_format`, `mask`, or other fields if manually added;
- whether private `gpt-image-2` honors `size`, `quality`, `background`, or `auto` differently from public API `gpt-image-2`;
- whether a natural-language request for transparency causes the server to resolve `background: auto` to transparent;
- the precise Plus/Pro/Business/Enterprise quota or reset policy; or
- whether the private route is implemented by the same backend/version as public `/v1/images/*`.

The strongest defensible statement is therefore: **Codex source proves a subscription-gated, provider-authenticated image capability whose shipped built-in contract intentionally delegates options to automatic server selection and returns base64 image data. The same checkout documents a broader public/API fallback control surface. It does not prove private-server parity or non-parity for fields the client never sends.**

## Primary local sources

All paths below are under `/home/neil/code/references/github.com/openai/codex` and refer to HEAD `343074d4207d572809bd8cea15f4be1d09d98e0b`:

- `codex-rs/core/src/tools/spec_plan.rs:611-650,1258-1282`
- `codex-rs/ext/image-generation/src/extension.rs:31-44,83-103`
- `codex-rs/ext/image-generation/src/backend.rs:47-117`
- `codex-rs/ext/image-generation/src/tool.rs:58-95,174-198,234-259,400-513,620-635`
- `codex-rs/ext/image-generation/src/artifact.rs:5-29`
- `codex-rs/ext/image-generation/src/tests.rs:48-75`
- `codex-rs/codex-api/src/images.rs:5-70`
- `codex-rs/codex-api/src/endpoint/images.rs:16-72,148-185`
- `codex-rs/codex-api/src/endpoint/session.rs:19-110`
- `codex-rs/codex-api/src/provider.rs:43-88`
- `codex-rs/model-provider/src/provider.rs:204-239`
- `codex-rs/model-provider/src/auth.rs:172-220,289-320`
- `codex-rs/model-provider-info/src/lib.rs:40,292-316,467-478`
- `codex-rs/skills/src/assets/samples/imagegen/references/image-api.md:1-90`
- `codex-rs/skills/src/assets/samples/imagegen/references/cli.md:13-15,64-84,146-175,227-234`
- `codex-rs/skills/src/assets/samples/imagegen/scripts/image_gen.py:25-38,121-201,710-831`
- `codex-rs/ext/image-generation/imagegen_description.md:1-14`

Relevant history commits: `423488480`, `ecb41fcb6`, `10b039903`, `123cf62a4`, `a7c72aee8`, `928bda82c`, `8cabf5a6c`, `edcec1337`, `682f57254`.
