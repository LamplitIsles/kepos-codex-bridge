//! Thin native Codex relay for a loopback Kepos service.
//!
//! The bridge exposes native Responses and a client-independent image transport.
//! Authentication is local managed ChatGPT OAuth; peer authentication belongs
//! to Kepos.

use std::{
    convert::Infallible,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use tower::Service;

use axum::{
    Router,
    body::{Body, Bytes},
    extract::{
        DefaultBodyLimit, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, Response as HttpResponse, StatusCode, header},
    response::{IntoResponse, Response},
    routing::post,
};
use futures_util::StreamExt;
use nanocodex_oai_api::auth::{OpenAiAuth, OpenAiAuthMode, OpenAiAuthSnapshot};
use nanocodex_oai_api::{
    Model, OpenAi, ResponseEvent,
    responses::{
        ContentItem, FunctionOutputBody, FunctionOutputContent, JsonSchema, MessageRole,
        ResponseItem,
    },
    session::{ResponseInput, Session},
    tools::ToolDefinition,
    tower::ResponsesServiceFactory,
};
use reqwest::header::{AUTHORIZATION, USER_AGENT};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;

pub const ENDPOINT: &str = "/codex/responses";
pub const IMAGE_ENDPOINT: &str = "/codex/images";
const MAX_REQUEST_BYTES: usize = 4 * 1024 * 1024;
const MAX_IMAGE_REQUEST_BYTES: usize = 32 * 1024 * 1024;
const MAX_EDIT_IMAGES: usize = 5;
const CHANNEL_CAPACITY: usize = 32;
const IMAGE_MODEL: &str = "gpt-image-2";
const NANOCODEX_USER_AGENT: &str = "nanocodex/0.5.0";

/// A configured bridge endpoint.
#[derive(Clone)]
pub struct Bridge<F> {
    openai: OpenAi<F>,
    image_auth: OpenAiAuth,
    image_client: reqwest::Client,
    image_api_base_url: Arc<str>,
    model: Model,
    instructions: Arc<str>,
}

impl<F> Bridge<F>
where
    F: ResponsesServiceFactory + Clone + Send + Sync + 'static,
    F::Service: Service<
            nanocodex_oai_api::tower::ResponsesAttempt,
            Response = nanocodex_oai_api::tower::ResponsesServiceResponse,
        > + Send
        + 'static,
    <F::Service as Service<nanocodex_oai_api::tower::ResponsesAttempt>>::Error:
        Into<nanocodex_oai_api::ResponseError> + Send,
    <F::Service as Service<nanocodex_oai_api::tower::ResponsesAttempt>>::Future: Send,
{
    /// Creates a bridge using one fixed supported model and a fallback
    /// developer instruction used when a client omits `instructions`.
    pub fn new(
        openai: OpenAi<F>,
        image_auth: OpenAiAuth,
        model: Model,
        instructions: impl Into<Arc<str>>,
    ) -> Result<Self, BridgeConfigError> {
        let instructions = instructions.into();
        if instructions.trim().is_empty() {
            return Err(BridgeConfigError::EmptyInstructions);
        }
        Ok(Self {
            openai,
            image_api_base_url: Arc::from(image_auth.mode().default_api_base_url()),
            image_auth,
            image_client: reqwest::Client::new(),
            model,
            instructions,
        })
    }

    /// Overrides the upstream image API base URL for an isolated deployment or
    /// test-owned local listener. The public bridge contract remains fixed.
    #[must_use]
    pub fn with_image_api_base_url(mut self, base_url: impl Into<Arc<str>>) -> Self {
        self.image_api_base_url = base_url.into();
        self
    }

    /// Returns the endpoint router. The router is loopback-agnostic; callers
    /// choose the listener address and Kepos owns peer access control.
    pub fn router(self) -> Router {
        let state = Arc::new(self);
        Router::new()
            .route(
                ENDPOINT,
                post(http_responses::<F>)
                    .get(websocket_responses::<F>)
                    .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES)),
            )
            .route(
                IMAGE_ENDPOINT,
                post(http_images::<F>).layer(DefaultBodyLimit::max(MAX_IMAGE_REQUEST_BYTES)),
            )
            .with_state(state)
    }

    /// Serves the bridge on an already-bound listener.
    pub async fn serve(self, listener: tokio::net::TcpListener) -> Result<(), std::io::Error> {
        axum::serve(listener, self.router())
            .with_graceful_shutdown(shutdown_signal())
            .await
    }

    fn new_session(
        &self,
        instructions: Option<&str>,
        tools: Vec<ToolDefinition>,
        prompt_cache_key: Option<&str>,
    ) -> Result<Session<F::Service>, BridgeError> {
        let mut builder = self
            .openai
            .instructions(instructions.unwrap_or(&self.instructions))
            .tool_definitions(tools);
        if let Some(prompt_cache_key) = prompt_cache_key {
            if prompt_cache_key.trim().is_empty() {
                return Err(BridgeError::InvalidRequest(
                    "prompt_cache_key must not be empty",
                ));
            }
            builder = builder.prompt_cache_key(prompt_cache_key);
        }
        builder
            .build()
            .map_err(|_| BridgeError::InvalidRequest("invalid session configuration"))
    }
}

/// Configuration error for the public bridge constructor.
#[derive(Debug, thiserror::Error)]
pub enum BridgeConfigError {
    /// A session needs a nonempty developer instruction.
    #[error("bridge instructions must not be empty")]
    EmptyInstructions,
}

#[derive(Debug, thiserror::Error)]
enum BridgeError {
    #[error("invalid request: {0}")]
    InvalidRequest(&'static str),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRequest {
    #[serde(rename = "type", default)]
    kind: Option<String>,
    model: String,
    #[serde(default)]
    input: Option<WireInput>,
    #[serde(default)]
    instructions: Option<String>,
    /// Compatibility data accepted but never used for authorization.
    #[serde(rename = "api_key", default)]
    _api_key: Option<String>,
    #[serde(default)]
    tools: Vec<WireTool>,
    #[serde(default)]
    store: Option<bool>,
    #[serde(default)]
    stream: Option<bool>,
    #[serde(default)]
    #[serde(rename = "previous_response_id")]
    previous_response_id: Option<String>,
    #[serde(default)]
    tool_choice: Option<Value>,
    #[serde(rename = "parallel_tool_calls", default)]
    _parallel_tool_calls: Option<bool>,
    #[serde(default)]
    reasoning: Option<Value>,
    #[serde(default)]
    text: Option<Value>,
    #[serde(default)]
    service_tier: Option<String>,
    #[serde(default)]
    prompt_cache_key: Option<String>,
    #[serde(default)]
    include: Option<Vec<String>>,
    #[serde(rename = "max_output_tokens", default)]
    _max_output_tokens: Option<u64>,
    #[serde(rename = "temperature", default)]
    _temperature: Option<f64>,
    #[serde(rename = "top_p", default)]
    _top_p: Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum WireInput {
    Text(String),
    Items(Vec<Value>),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireTool {
    #[serde(rename = "type")]
    kind: String,
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    strict: bool,
    parameters: Value,
}

struct ParsedRequest {
    items: Vec<ResponseItem>,
    instructions: Option<String>,
    tools: Vec<ToolDefinition>,
    prompt_cache_key: Option<String>,
}

impl WireRequest {
    fn parse(raw: &[u8], websocket: bool, expected_model: Model) -> Result<Self, BridgeError> {
        let request: Self = serde_json::from_slice(raw)
            .map_err(|_| BridgeError::InvalidRequest("malformed or unsupported request"))?;
        if websocket && request.kind.as_deref() != Some("response.create") {
            return Err(BridgeError::InvalidRequest(
                "WebSocket frames must be response.create",
            ));
        }
        if !websocket && request.kind.is_some() {
            return Err(BridgeError::InvalidRequest(
                "HTTP requests must not contain a frame type",
            ));
        }
        if !websocket && request.previous_response_id.is_some() {
            return Err(BridgeError::InvalidRequest(
                "previous_response_id is only supported on WebSocket",
            ));
        }
        if request.model.parse::<Model>().ok() != Some(expected_model) {
            return Err(BridgeError::InvalidRequest(
                "unsupported model for this bridge",
            ));
        }
        if request.store == Some(true) {
            return Err(BridgeError::InvalidRequest("store: true is unsupported"));
        }
        if request.stream == Some(false) {
            return Err(BridgeError::InvalidRequest(
                "the bridge only supports streaming",
            ));
        }
        if let Some(choice) = request.tool_choice.as_ref()
            && choice.as_str().is_some_and(|choice| choice != "auto")
        {
            return Err(BridgeError::InvalidRequest(
                "only tool_choice: auto is supported",
            ));
        }
        if let Some(include) = request.include.as_ref()
            && include
                .iter()
                .any(|value| value != "reasoning.encrypted_content")
        {
            return Err(BridgeError::InvalidRequest(
                "include value is outside the supported subset",
            ));
        }
        if let Some(value) = request.service_tier.as_deref()
            && !matches!(value, "default" | "priority")
        {
            return Err(BridgeError::InvalidRequest("unsupported service_tier"));
        }
        if let Some(value) = request.text.as_ref()
            && value
                .get("verbosity")
                .and_then(Value::as_str)
                .is_some_and(|value| !matches!(value, "low" | "medium" | "high"))
        {
            return Err(BridgeError::InvalidRequest("unsupported text verbosity"));
        }
        if let Some(value) = request.reasoning.as_ref()
            && value
                .get("effort")
                .and_then(Value::as_str)
                .is_some_and(|value| {
                    !matches!(
                        value,
                        "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max"
                    )
                })
        {
            return Err(BridgeError::InvalidRequest("unsupported reasoning effort"));
        }
        Ok(request)
    }

    fn lower(self) -> Result<ParsedRequest, BridgeError> {
        let items = match self.input.unwrap_or(WireInput::Items(Vec::new())) {
            WireInput::Text(text) => vec![ResponseItem::message(
                MessageRole::User,
                [ContentItem::input_text(text)],
            )],
            WireInput::Items(items) => items
                .into_iter()
                .map(lower_input_item)
                .collect::<Result<_, _>>()?,
        };
        if items.is_empty() {
            return Err(BridgeError::InvalidRequest("input must not be empty"));
        }
        for item in &items {
            if !is_supported_input_item(item) {
                return Err(BridgeError::InvalidRequest(
                    "input item is outside the supported subset",
                ));
            }
        }
        let tools = self
            .tools
            .into_iter()
            .map(WireTool::lower)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ParsedRequest {
            items,
            instructions: self.instructions,
            tools,
            prompt_cache_key: self.prompt_cache_key,
        })
    }
}

fn lower_input_item(mut value: Value) -> Result<ResponseItem, BridgeError> {
    if let Value::Object(item) = &mut value
        && !item.contains_key("type")
        && matches!(
            item.get("role").and_then(Value::as_str),
            Some("user" | "assistant")
        )
        && item.contains_key("content")
    {
        item.insert("type".to_owned(), Value::String("message".to_owned()));
    }
    serde_json::from_value(value)
        .map_err(|_| BridgeError::InvalidRequest("malformed or unsupported request"))
}

fn is_supported_input_item(item: &ResponseItem) -> bool {
    match item {
        ResponseItem::Message {
            role: MessageRole::User,
            content,
            ..
        } => content.iter().all(|content| {
            matches!(
                content,
                ContentItem::InputText { .. } | ContentItem::InputImage { .. }
            )
        }),
        ResponseItem::Message {
            role: MessageRole::Assistant,
            content,
            ..
        } => content
            .iter()
            .all(|content| matches!(content, ContentItem::OutputText { .. })),
        ResponseItem::Reasoning { .. } | ResponseItem::FunctionCall { .. } => true,
        ResponseItem::FunctionCallOutput { output, .. } => match output {
            FunctionOutputBody::Text(_) => true,
            FunctionOutputBody::Content(content) => content.iter().all(|content| {
                matches!(
                    content,
                    FunctionOutputContent::InputText { .. }
                        | FunctionOutputContent::InputImage { .. }
                )
            }),
        },
        _ => false,
    }
}

impl WireTool {
    fn lower(self) -> Result<ToolDefinition, BridgeError> {
        if self.kind != "function" || self.name.trim().is_empty() {
            return Err(BridgeError::InvalidRequest(
                "only named function tools are supported",
            ));
        }
        Ok(ToolDefinition::Function {
            name: self.name.into_boxed_str(),
            description: self.description.into_boxed_str(),
            strict: self.strict,
            defer_loading: None,
            parameters: JsonSchema::from(self.parameters),
            output_schema: None,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ImageWireRequest {
    prompt: String,
    #[serde(default)]
    images: Vec<String>,
    /// Compatibility data accepted but never used for authorization.
    #[serde(rename = "api_key", default)]
    _api_key: Option<String>,
}

struct ImageOperation {
    prompt: String,
    images: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ImageResponse {
    data: Vec<ImageData>,
}

#[derive(Debug, Deserialize)]
struct ImageData {
    b64_json: String,
}

fn parse_image_request(headers: &HeaderMap, raw: &[u8]) -> Result<ImageOperation, &'static str> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim);
    if content_type != Some("application/json") {
        return Err("content type must be application/json");
    }
    let request: ImageWireRequest =
        serde_json::from_slice(raw).map_err(|_| "malformed or unsupported image request")?;
    if request.prompt.trim().is_empty() {
        return Err("prompt must not be blank");
    }
    if request.images.len() > MAX_EDIT_IMAGES {
        return Err("images must contain at most five inputs");
    }
    if request.images.iter().any(|image| !is_data_image_url(image)) {
        return Err("images must be data:image URLs");
    }
    Ok(ImageOperation {
        prompt: request.prompt,
        images: request.images,
    })
}

fn is_data_image_url(value: &str) -> bool {
    value
        .strip_prefix("data:image/")
        .and_then(|value| value.split_once(','))
        .is_some_and(|(media_type, data)| !media_type.is_empty() && !data.is_empty())
}

fn image_error(status: StatusCode, message: &'static str) -> Response {
    (
        status,
        axum::Json(json!({
            "error": { "type": if status == StatusCode::BAD_REQUEST {
                "invalid_request_error"
            } else {
                "server_error"
            }, "message": message }
        })),
    )
        .into_response()
}

async fn http_images<F>(
    State(bridge): State<Arc<Bridge<F>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response
where
    F: ResponsesServiceFactory + Clone + Send + Sync + 'static,
    F::Service: Service<
            nanocodex_oai_api::tower::ResponsesAttempt,
            Response = nanocodex_oai_api::tower::ResponsesServiceResponse,
        > + Send
        + 'static,
    <F::Service as Service<nanocodex_oai_api::tower::ResponsesAttempt>>::Error:
        Into<nanocodex_oai_api::ResponseError> + Send,
    <F::Service as Service<nanocodex_oai_api::tower::ResponsesAttempt>>::Future: Send,
{
    let operation = match parse_image_request(&headers, &body) {
        Ok(operation) => operation,
        Err(message) => return image_error(StatusCode::BAD_REQUEST, message),
    };
    match bridge.perform_image(operation).await {
        Ok(image_url) => axum::Json(json!({ "image_url": image_url })).into_response(),
        Err(()) => image_error(StatusCode::BAD_GATEWAY, "image operation failed"),
    }
}

impl<F> Bridge<F>
where
    F: ResponsesServiceFactory + Clone + Send + Sync + 'static,
    F::Service: Service<
            nanocodex_oai_api::tower::ResponsesAttempt,
            Response = nanocodex_oai_api::tower::ResponsesServiceResponse,
        > + Send
        + 'static,
    <F::Service as Service<nanocodex_oai_api::tower::ResponsesAttempt>>::Error:
        Into<nanocodex_oai_api::ResponseError> + Send,
    <F::Service as Service<nanocodex_oai_api::tower::ResponsesAttempt>>::Future: Send,
{
    async fn perform_image(&self, operation: ImageOperation) -> Result<String, ()> {
        let auth = self.image_auth.snapshot().await.map_err(|_| ())?;
        let endpoint_kind = if operation.images.is_empty() {
            "generations"
        } else {
            "edits"
        };
        let endpoint = format!(
            "{}/images/{endpoint_kind}",
            self.image_api_base_url.trim_end_matches('/')
        );
        let body = if operation.images.is_empty() {
            json!({
                "prompt": operation.prompt,
                "background": "auto",
                "model": IMAGE_MODEL,
                "quality": "auto",
                "size": "auto"
            })
        } else {
            json!({
                "images": operation.images.iter().map(|image| json!({ "image_url": image })).collect::<Vec<_>>(),
                "prompt": operation.prompt,
                "background": "auto",
                "model": IMAGE_MODEL,
                "quality": "auto",
                "size": "auto"
            })
        };
        let response = self.send_image_request(&endpoint, &body, &auth).await?;
        let response = if response.status() == reqwest::StatusCode::UNAUTHORIZED
            && auth.mode() == OpenAiAuthMode::ChatGpt
        {
            self.image_auth
                .recover_unauthorized(&auth)
                .await
                .map_err(|_| ())?;
            let refreshed = self.image_auth.snapshot().await.map_err(|_| ())?;
            self.send_image_request(&endpoint, &body, &refreshed)
                .await?
        } else {
            response
        };
        if !response.status().is_success() {
            return Err(());
        }
        let decoded = response
            .bytes()
            .await
            .map_err(|_| ())
            .and_then(|body| serde_json::from_slice::<ImageResponse>(&body).map_err(|_| ()))?;
        let image = decoded.data.into_iter().next().ok_or(())?;
        if image.b64_json.trim().is_empty() {
            return Err(());
        }
        Ok(format!("data:image/png;base64,{}", image.b64_json))
    }

    async fn send_image_request(
        &self,
        endpoint: &str,
        body: &Value,
        auth: &OpenAiAuthSnapshot,
    ) -> Result<reqwest::Response, ()> {
        let mut request = self
            .image_client
            .post(endpoint)
            .header(USER_AGENT, NANOCODEX_USER_AGENT)
            .header(AUTHORIZATION, format!("Bearer {}", auth.bearer()));
        if let Some(account_id) = auth.account_id() {
            request = request.header("ChatGPT-Account-ID", account_id);
        }
        if auth.is_fedramp() {
            request = request.header("X-OpenAI-Fedramp", "true");
        }
        request.json(body).send().await.map_err(|_| ())
    }
}

async fn http_responses<F>(State(bridge): State<Arc<Bridge<F>>>, body: Bytes) -> impl IntoResponse
where
    F: ResponsesServiceFactory + Clone + Send + Sync + 'static,
    F::Service: Service<
            nanocodex_oai_api::tower::ResponsesAttempt,
            Response = nanocodex_oai_api::tower::ResponsesServiceResponse,
        > + Send
        + 'static,
    <F::Service as Service<nanocodex_oai_api::tower::ResponsesAttempt>>::Error:
        Into<nanocodex_oai_api::ResponseError> + Send,
    <F::Service as Service<nanocodex_oai_api::tower::ResponsesAttempt>>::Future: Send,
{
    let parsed = WireRequest::parse(&body, false, bridge.model).and_then(WireRequest::lower);
    let (status, receiver) = match parsed {
        Ok(request) => match bridge.new_session(
            request.instructions.as_deref(),
            request.tools,
            request.prompt_cache_key.as_deref(),
        ) {
            Ok(session) => {
                let state = Arc::new(ConnectionState {
                    session: Mutex::new(session),
                    last_input: Mutex::new(Vec::new()),
                    last_output: Mutex::new(Vec::new()),
                    failed: AtomicBool::new(false),
                });
                (StatusCode::OK, spawn_operation(state, request.items))
            }
            Err(error) => (StatusCode::BAD_REQUEST, error_receiver(error)),
        },
        Err(error) => (StatusCode::BAD_REQUEST, error_receiver(error)),
    };
    sse_response(status, receiver)
}

async fn websocket_responses<F>(
    State(bridge): State<Arc<Bridge<F>>>,
    upgrade: WebSocketUpgrade,
) -> impl IntoResponse
where
    F: ResponsesServiceFactory + Clone + Send + Sync + 'static,
    F::Service: Service<
            nanocodex_oai_api::tower::ResponsesAttempt,
            Response = nanocodex_oai_api::tower::ResponsesServiceResponse,
        > + Send
        + 'static,
    <F::Service as Service<nanocodex_oai_api::tower::ResponsesAttempt>>::Error:
        Into<nanocodex_oai_api::ResponseError> + Send,
    <F::Service as Service<nanocodex_oai_api::tower::ResponsesAttempt>>::Future: Send,
{
    upgrade.on_upgrade(move |socket| websocket_loop(bridge, socket))
}

fn sse_response(status: StatusCode, receiver: mpsc::Receiver<Value>) -> HttpResponse<Body> {
    let stream = ReceiverStream::new(receiver).map(|value| {
        let data = serde_json::to_string(&value).unwrap_or_else(|_| {
            r#"{"type":"error","error":{"type":"server_error","message":"serialization failed"}}"#.to_owned()
        });
        Ok::<Bytes, Infallible>(Bytes::from(format!("data: {data}\n\n")))
    });
    let stream = stream.chain(futures_util::stream::once(async {
        Ok::<Bytes, Infallible>(Bytes::from_static(b"data: [DONE]\n\n"))
    }));
    HttpResponse::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from_stream(stream))
        .unwrap_or_else(|_| HttpResponse::new(Body::empty()))
}

fn error_receiver(error: BridgeError) -> mpsc::Receiver<Value> {
    let (sender, receiver) = mpsc::channel(1);
    let BridgeError::InvalidRequest(message) = error;
    let _ = sender.try_send(error_value(message));
    receiver
}

fn error_value(message: &'static str) -> Value {
    json!({
        "type": "error",
        "error": { "type": "invalid_request_error", "message": message }
    })
}

fn upstream_error_value(message: &'static str) -> Value {
    let _ = message;
    json!({
        "type": "error",
        "error": { "type": "server_error", "message": "upstream transport failed" }
    })
}

struct ConnectionState<S> {
    session: Mutex<Session<S>>,
    last_input: Mutex<Vec<ResponseItem>>,
    last_output: Mutex<Vec<ResponseItem>>,
    failed: AtomicBool,
}

struct Operation {
    receiver: mpsc::Receiver<Value>,
    task: tokio::task::JoinHandle<()>,
}

fn spawn_operation<S>(
    state: Arc<ConnectionState<S>>,
    items: Vec<ResponseItem>,
) -> mpsc::Receiver<Value>
where
    S: tower::Service<
            nanocodex_oai_api::tower::ResponsesAttempt,
            Response = nanocodex_oai_api::tower::ResponsesServiceResponse,
        > + Send
        + 'static,
    S::Error: Into<nanocodex_oai_api::ResponseError> + Send,
    S::Future: Send,
{
    spawn_operation_with_handle(state, items).receiver
}

fn spawn_operation_with_handle<S>(
    state: Arc<ConnectionState<S>>,
    items: Vec<ResponseItem>,
) -> Operation
where
    S: tower::Service<
            nanocodex_oai_api::tower::ResponsesAttempt,
            Response = nanocodex_oai_api::tower::ResponsesServiceResponse,
        > + Send
        + 'static,
    S::Error: Into<nanocodex_oai_api::ResponseError> + Send,
    S::Future: Send,
{
    let (sender, receiver) = mpsc::channel(CHANNEL_CAPACITY);
    let task = tokio::spawn(async move {
        let failed_state = Arc::clone(&state);
        if let Err(error) = drive_operation(state, items, sender.clone()).await {
            failed_state.failed.store(true, Ordering::Release);
            let _ = sender.send(upstream_error_value(error)).await;
        }
    });
    Operation { receiver, task }
}

async fn drive_operation<S>(
    state: Arc<ConnectionState<S>>,
    items: Vec<ResponseItem>,
    sender: mpsc::Sender<Value>,
) -> Result<(), &'static str>
where
    S: tower::Service<
            nanocodex_oai_api::tower::ResponsesAttempt,
            Response = nanocodex_oai_api::tower::ResponsesServiceResponse,
        > + Send
        + 'static,
    S::Error: Into<nanocodex_oai_api::ResponseError> + Send,
    S::Future: Send,
{
    let request_items = items;
    let previous_input = state.last_input.lock().await.clone();
    let previous_output = state.last_output.lock().await.clone();
    let items = input_delta(request_items.clone(), &previous_input, &previous_output);
    let mut session = state.session.lock().await;
    let mut turn = session.turn();
    let mut response = turn.create(ResponseInput::items(items));
    let response_id = format!("resp_bridge_{}", uuid::Uuid::new_v4().simple());
    let mut output_items = Vec::new();
    loop {
        let event = tokio::select! {
            _ = sender.closed() => return Err("client disconnected"),
            event = response.next() => event,
        };
        let Some(event) = event else {
            break;
        };
        let event = event.map_err(|_| "upstream operation failed")?;
        if let Some(value) = native_event(event, &response_id, &mut output_items) {
            sender
                .send(value)
                .await
                .map_err(|_| "client disconnected")?;
        }
    }
    let completed = response.await.map_err(|_| "upstream operation failed")?;
    if output_items.is_empty() {
        output_items.extend(completed.output().iter().cloned());
    }
    *state.last_output.lock().await = completed.output().to_vec();
    *state.last_input.lock().await = request_items;
    let terminal = json!({
        "type": "response.completed",
        "response": {
            "id": response_id,
            "object": "response",
            "status": "completed",
            "model": "bridge",
            "output": output_items,
            "usage": completed.usage(),
            "end_turn": completed.end_turn()
        }
    });
    // The SDK normally emits this event itself. The guard keeps custom
    // services that return only an aggregate native at the public boundary.
    if terminal_needs_sending(&terminal) {
        sender
            .send(terminal)
            .await
            .map_err(|_| "client disconnected")?;
    }
    Ok(())
}

fn terminal_needs_sending(_terminal: &Value) -> bool {
    // ResponseEvent::Completed is emitted by the managed stream in the normal
    // case; sending a second completion is not native behavior. Aggregate-only
    // services are handled by the stream's synthesized Completed event.
    false
}

fn input_delta(
    items: Vec<ResponseItem>,
    previous_input: &[ResponseItem],
    previous_output: &[ResponseItem],
) -> Vec<ResponseItem> {
    let baseline = previous_input
        .iter()
        .chain(previous_output.iter())
        .collect::<Vec<_>>();
    if items.len() >= baseline.len()
        && items.iter().zip(baseline).all(|(item, known)| {
            serde_json::to_value(item).ok() == serde_json::to_value(known).ok()
        })
    {
        items
            .into_iter()
            .skip(previous_input.len() + previous_output.len())
            .collect()
    } else {
        items
    }
}

fn response_items_match(left: &ResponseItem, right: &ResponseItem) -> bool {
    match (left.id(), right.id()) {
        (Some(left), Some(right)) => left == right,
        _ => serde_json::to_value(left).ok() == serde_json::to_value(right).ok(),
    }
}

fn native_event(
    event: ResponseEvent,
    response_id: &str,
    output: &mut Vec<ResponseItem>,
) -> Option<Value> {
    match event {
        ResponseEvent::Created => Some(json!({
            "type": "response.created",
            "response": { "id": response_id, "object": "response", "status": "in_progress" }
        })),
        ResponseEvent::OutputItemAdded(item) => {
            let index = output.len();
            output.push(item.clone());
            Some(
                json!({ "type": "response.output_item.added", "output_index": index, "item": item }),
            )
        }
        ResponseEvent::OutputTextDelta(delta) => Some(json!({
            "type": "response.output_text.delta",
            "output_index": output.len().saturating_sub(1),
            "delta": delta
        })),
        ResponseEvent::ToolCallInputDelta {
            item_id,
            call_id,
            delta,
        } => {
            let index = output
                .iter()
                .position(|item| item.id().is_some_and(|id| id.as_str() == item_id.as_str()))
                .unwrap_or_else(|| output.len().saturating_sub(1));
            Some(json!({
                "type": "response.custom_tool_call_input.delta",
                "output_index": index,
                "item_id": item_id,
                "call_id": call_id,
                "delta": delta
            }))
        }
        ResponseEvent::ReasoningSummaryDelta {
            delta,
            summary_index,
        } => Some(json!({
            "type": "response.reasoning_summary_text.delta",
            "summary_index": summary_index,
            "delta": delta
        })),
        ResponseEvent::ReasoningSummaryDone {
            item_id,
            text,
            summary_index,
        } => Some(json!({
            "type": "response.reasoning_summary_text.done",
            "item_id": item_id,
            "summary_index": summary_index,
            "text": text
        })),
        ResponseEvent::ReasoningContentDelta {
            delta,
            content_index,
        } => Some(json!({
            "type": "response.reasoning_text.delta",
            "content_index": content_index,
            "delta": delta
        })),
        ResponseEvent::ReasoningSummaryPartAdded { summary_index } => Some(json!({
            "type": "response.reasoning_summary_part.added",
            "summary_index": summary_index
        })),
        ResponseEvent::OutputItemDone(item) => {
            if !output
                .iter()
                .any(|known| response_items_match(known, &item))
            {
                output.push(item.clone());
            }
            let index = output
                .iter()
                .position(|known| response_items_match(known, &item))
                .unwrap_or(0);
            Some(
                json!({ "type": "response.output_item.done", "output_index": index, "item": item }),
            )
        }
        ResponseEvent::Completed { usage, end_turn } => Some(json!({
            "type": "response.completed",
            "response": {
                "id": response_id,
                "object": "response",
                "status": "completed",
                "output": output,
                "usage": usage,
                "end_turn": end_turn
            }
        })),
        _ => None,
    }
}

async fn websocket_loop<F>(bridge: Arc<Bridge<F>>, mut socket: WebSocket)
where
    F: ResponsesServiceFactory + Clone + Send + Sync + 'static,
    F::Service: tower::Service<
            nanocodex_oai_api::tower::ResponsesAttempt,
            Response = nanocodex_oai_api::tower::ResponsesServiceResponse,
        > + Send
        + 'static,
    <F::Service as Service<nanocodex_oai_api::tower::ResponsesAttempt>>::Error:
        Into<nanocodex_oai_api::ResponseError> + Send,
    <F::Service as Service<nanocodex_oai_api::tower::ResponsesAttempt>>::Future: Send,
{
    let mut session: Option<Arc<ConnectionState<F::Service>>> = None;
    let mut active: Option<Operation> = None;
    loop {
        if let Some(operation) = &mut active {
            tokio::select! {
                frame = operation.receiver.recv() => {
                    match frame {
                        Some(frame) => if socket.send(Message::Text(frame.to_string().into())).await.is_err() { operation.task.abort(); return; },
                        None => {
                            let finished = active.take();
                            if let Some(operation) = finished { let _ = operation.task.await; }
                            if session.as_ref().is_some_and(|state| state.failed.load(Ordering::Acquire)) {
                                session = None;
                            }
                        }
                    }
                }
                incoming = socket.next() => {
                    match incoming {
                        Some(Ok(Message::Text(text))) => {
                            if let Ok(value) = serde_json::from_str::<Value>(&text) && value.get("type").and_then(Value::as_str) == Some("response.cancel") {
                                if let Some(operation) = active.take() { operation.task.abort(); }
                                session = None;
                            } else if socket.send(Message::Text(error_value("one response may be active at a time").to_string().into())).await.is_err() { return; }
                        }
                        Some(Ok(Message::Close(_))) | None => { operation.task.abort(); return; }
                        Some(Ok(_)) => { let _ = socket.send(Message::Text(error_value("only text frames are supported").to_string().into())).await; }
                        Some(Err(_)) => { operation.task.abort(); return; }
                    }
                }
            }
            continue;
        }
        match socket.next().await {
            Some(Ok(Message::Text(text))) => {
                let parsed = WireRequest::parse(text.as_bytes(), true, bridge.model)
                    .and_then(WireRequest::lower);
                match parsed {
                    Ok(request) => {
                        let state = match &session {
                            Some(state) => Arc::clone(state),
                            None => match bridge.new_session(
                                request.instructions.as_deref(),
                                request.tools,
                                request.prompt_cache_key.as_deref(),
                            ) {
                                Ok(new_session) => {
                                    let state = Arc::new(ConnectionState {
                                        session: Mutex::new(new_session),
                                        last_input: Mutex::new(Vec::new()),
                                        last_output: Mutex::new(Vec::new()),
                                        failed: AtomicBool::new(false),
                                    });
                                    session = Some(Arc::clone(&state));
                                    state
                                }
                                Err(_error) => {
                                    let _ = socket
                                        .send(Message::Text(
                                            error_value("invalid session configuration")
                                                .to_string()
                                                .into(),
                                        ))
                                        .await;
                                    continue;
                                }
                            },
                        };
                        active = Some(spawn_operation_with_handle(state, request.items));
                    }
                    Err(error) => {
                        if socket
                            .send(Message::Text(
                                error_value(match error {
                                    BridgeError::InvalidRequest(message) => message,
                                })
                                .to_string()
                                .into(),
                            ))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                }
            }
            Some(Ok(Message::Close(_))) | None => return,
            Some(Ok(_)) => {
                if socket
                    .send(Message::Text(
                        error_value("only text frames are supported")
                            .to_string()
                            .into(),
                    ))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            Some(Err(_)) => return,
        }
    }
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

/// Binds a loopback listener for the operator-facing service.
pub async fn bind_loopback(
    port: u16,
) -> Result<(tokio::net::TcpListener, SocketAddr), std::io::Error> {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    let address = listener.local_addr()?;
    Ok((listener, address))
}

/// Validates the private state file before the listener is bound.
#[cfg(unix)]
pub fn validate_private_auth_file(path: &std::path::Path) -> Result<(), AuthFileError> {
    use std::os::unix::fs::PermissionsExt;
    let metadata = std::fs::metadata(path).map_err(|_| AuthFileError::Unavailable)?;
    if !metadata.is_file()
        || metadata.permissions().mode() & 0o077 != 0
        || metadata.permissions().mode() & 0o200 == 0
    {
        return Err(AuthFileError::Permissions);
    }
    Ok(())
}

/// Validates the private state file before the listener is bound.
#[cfg(not(unix))]
pub fn validate_private_auth_file(_path: &std::path::Path) -> Result<(), AuthFileError> {
    Err(AuthFileError::UnsupportedPlatform)
}

/// Credential file validation failure.
#[derive(Debug, thiserror::Error)]
pub enum AuthFileError {
    #[error("credential file is unavailable")]
    Unavailable,
    #[error("credential file must be owner-readable and owner-writable only")]
    Permissions,
    #[error("credential file permissions cannot be checked on this platform")]
    UnsupportedPlatform,
}

#[cfg(test)]
mod tests {
    use super::*;
    use nanocodex_oai_api::{Model, responses::ContentItem};

    #[test]
    fn unsupported_request_values_are_rejected_without_echoing_payload() {
        let raw = br#"{"model":"gpt-5.6-luna","input":"hello","store":true}"#;
        let error = WireRequest::parse(raw, false, Model::Luna).unwrap_err();
        assert_eq!(
            error.to_string(),
            "invalid request: store: true is unsupported"
        );
        assert!(!error.to_string().contains("hello"));
    }

    #[test]
    fn image_input_is_typed_and_not_rewritten() {
        let raw = br#"{"model":"gpt-5.6-luna","input":[{"type":"message","role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,AAAA"}]}]}"#;
        let request = WireRequest::parse(raw, false, Model::Luna)
            .unwrap()
            .lower()
            .unwrap();
        assert!(
            matches!(&request.items[0], ResponseItem::Message { content, .. } if matches!(&content[0], ContentItem::InputImage { image_url, .. } if image_url.starts_with("data:image/png")))
        );
    }
}
