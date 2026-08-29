//! Transparent managed-OAuth Codex relay for a loopback Kepos service.
//!
//! The bridge owns only the ChatGPT OAuth credential. Responses request and
//! response protocol state belongs entirely to the connected client.

use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::ws::{CloseFrame as AxumCloseFrame, Message as AxumMessage, WebSocket},
    extract::{DefaultBodyLimit, State, WebSocketUpgrade},
    http::{HeaderMap, HeaderName, HeaderValue, Response as HttpResponse, StatusCode, Uri, header},
    response::{IntoResponse, Response},
    routing::post,
};
use futures_util::{SinkExt, StreamExt};
use nanocodex_oai_api::auth::{OpenAiAuth, OpenAiAuthMode, OpenAiAuthSnapshot};
use reqwest::header::{AUTHORIZATION, USER_AGENT};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{
        Error as WebSocketError, Message as TungsteniteMessage, client::IntoClientRequest,
        protocol::CloseFrame as TungsteniteCloseFrame,
    },
};

pub const ENDPOINT: &str = "/codex/responses";
pub const HINDSIGHT_RESPONSES_ENDPOINT: &str = "/hindsight/responses";
pub const IMAGE_ENDPOINT: &str = "/codex/images";
const MAX_REQUEST_BYTES: usize = 4 * 1024 * 1024;
const MAX_HINDSIGHT_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const MAX_IMAGE_REQUEST_BYTES: usize = 32 * 1024 * 1024;
const MAX_EDIT_IMAGES: usize = 5;
const IMAGE_MODEL: &str = "gpt-image-2";
const NANOCODEX_USER_AGENT: &str = "nanocodex/0.5.0";

/// A configured bridge endpoint.
#[derive(Clone)]
pub struct Bridge {
    auth: OpenAiAuth,
    responses_client: reqwest::Client,
    image_client: reqwest::Client,
    image_api_base_url: Arc<str>,
    responses_api_base_url: Arc<str>,
}

impl Bridge {
    /// Creates a bridge which substitutes this host's managed OAuth identity.
    #[must_use]
    pub fn new(auth: OpenAiAuth) -> Self {
        let base_url: Arc<str> = Arc::from(auth.mode().default_api_base_url());
        Self {
            auth,
            responses_client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("Responses client configuration is valid"),
            image_client: reqwest::Client::new(),
            image_api_base_url: base_url.clone(),
            responses_api_base_url: base_url,
        }
    }

    /// Overrides the upstream image API base URL for a test-owned listener.
    #[must_use]
    pub fn with_image_api_base_url(mut self, base_url: impl Into<Arc<str>>) -> Self {
        self.image_api_base_url = base_url.into();
        self
    }

    /// Overrides the upstream Responses API base URL for a test-owned listener.
    #[must_use]
    pub fn with_responses_api_base_url(mut self, base_url: impl Into<Arc<str>>) -> Self {
        self.responses_api_base_url = base_url.into();
        self
    }

    /// Returns the endpoint router. Kepos owns peer access control.
    pub fn router(self) -> Router {
        let state = Arc::new(self);
        Router::new()
            .route(
                ENDPOINT,
                post(http_responses)
                    .get(websocket_responses)
                    .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES)),
            )
            .route(
                HINDSIGHT_RESPONSES_ENDPOINT,
                post(hindsight_responses).layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES)),
            )
            .route(
                IMAGE_ENDPOINT,
                post(http_images).layer(DefaultBodyLimit::max(MAX_IMAGE_REQUEST_BYTES)),
            )
            .with_state(state)
    }

    /// Serves the bridge on an already-bound listener.
    pub async fn serve(self, listener: tokio::net::TcpListener) -> Result<(), std::io::Error> {
        axum::serve(listener, self.router())
            .with_graceful_shutdown(shutdown_signal())
            .await
    }

    async fn forward_response(
        &self,
        uri: &Uri,
        headers: &HeaderMap,
        body: Bytes,
    ) -> Result<reqwest::Response, ()> {
        let endpoint = response_endpoint(&self.responses_api_base_url, uri);
        let auth = self.auth.snapshot().await.map_err(|_| ())?;
        let response = self
            .send_response_request(&endpoint, headers, body.clone(), &auth)
            .await?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED
            && auth.mode() == OpenAiAuthMode::ChatGpt
        {
            self.auth
                .recover_unauthorized(&auth)
                .await
                .map_err(|_| ())?;
            let refreshed = self.auth.snapshot().await.map_err(|_| ())?;
            self.send_response_request(&endpoint, headers, body, &refreshed)
                .await
        } else {
            Ok(response)
        }
    }

    async fn send_response_request(
        &self,
        endpoint: &str,
        headers: &HeaderMap,
        body: Bytes,
        auth: &OpenAiAuthSnapshot,
    ) -> Result<reqwest::Response, ()> {
        let mut request = self
            .responses_client
            .post(endpoint)
            .headers(forward_request_headers(headers))
            .header(AUTHORIZATION, format!("Bearer {}", auth.bearer()))
            .body(body);
        if let Some(account_id) = auth.account_id() {
            request = request.header("ChatGPT-Account-ID", account_id);
        }
        if auth.is_fedramp() {
            request = request.header("X-OpenAI-Fedramp", "true");
        }
        request.send().await.map_err(|_| ())
    }

    async fn connect_websocket(
        &self,
        uri: &Uri,
        headers: &HeaderMap,
    ) -> Result<(UpstreamWebSocket, HeaderMap, Option<HeaderValue>), ()> {
        let endpoint = response_websocket_endpoint(&self.responses_api_base_url, uri);
        let auth = self.auth.snapshot().await.map_err(|_| ())?;
        match self
            .connect_websocket_request(&endpoint, headers, &auth)
            .await
        {
            Ok(connection) => Ok(connection),
            Err(error)
                if is_unauthorized_websocket_handshake(&error)
                    && auth.mode() == OpenAiAuthMode::ChatGpt =>
            {
                self.auth
                    .recover_unauthorized(&auth)
                    .await
                    .map_err(|_| ())?;
                let refreshed = self.auth.snapshot().await.map_err(|_| ())?;
                self.connect_websocket_request(&endpoint, headers, &refreshed)
                    .await
                    .map_err(|_| ())
            }
            Err(_) => Err(()),
        }
    }

    async fn connect_websocket_request(
        &self,
        endpoint: &str,
        headers: &HeaderMap,
        auth: &OpenAiAuthSnapshot,
    ) -> Result<(UpstreamWebSocket, HeaderMap, Option<HeaderValue>), WebSocketError> {
        let mut request = endpoint.into_client_request()?;
        request
            .headers_mut()
            .extend(forward_websocket_headers(headers));
        request.headers_mut().insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", auth.bearer()))
                .expect("managed bearer must be a valid HTTP header"),
        );
        if let Some(account_id) = auth.account_id() {
            request.headers_mut().insert(
                "ChatGPT-Account-ID",
                HeaderValue::from_str(account_id)
                    .expect("managed account ID must be a valid HTTP header"),
            );
        }
        if auth.is_fedramp() {
            request
                .headers_mut()
                .insert("X-OpenAI-Fedramp", HeaderValue::from_static("true"));
        }
        let (socket, response) = connect_async(request).await?;
        let selected_protocol = response
            .headers()
            .get(header::SEC_WEBSOCKET_PROTOCOL)
            .cloned();
        Ok((
            socket,
            safe_websocket_response_headers(response.headers()),
            selected_protocol,
        ))
    }

    async fn perform_image(&self, operation: ImageOperation) -> Result<String, ()> {
        let auth = self.auth.snapshot().await.map_err(|_| ())?;
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
            self.auth
                .recover_unauthorized(&auth)
                .await
                .map_err(|_| ())?;
            let refreshed = self.auth.snapshot().await.map_err(|_| ())?;
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

async fn http_responses(
    State(bridge): State<Arc<Bridge>>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    match bridge.forward_response(&uri, &headers, body).await {
        Ok(upstream) => relay_response(upstream),
        Err(()) => (StatusCode::BAD_GATEWAY, "upstream request failed").into_response(),
    }
}

async fn hindsight_responses(
    State(bridge): State<Arc<Bridge>>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (body, ignored_max_output_tokens) = match adapt_hindsight_request(&body) {
        Ok(request) => request,
        Err(()) => {
            return (
                StatusCode::BAD_REQUEST,
                "invalid Hindsight Responses request",
            )
                .into_response();
        }
    };
    match bridge.forward_response(&uri, &headers, body).await {
        Ok(upstream) if !upstream.status().is_success() => relay_response(upstream),
        Ok(upstream) => adapt_hindsight_response(upstream, ignored_max_output_tokens).await,
        Err(()) => (StatusCode::BAD_GATEWAY, "upstream request failed").into_response(),
    }
}

fn adapt_hindsight_request(raw: &[u8]) -> Result<(Bytes, bool), ()> {
    let mut request: Value = serde_json::from_slice(raw).map_err(|_| ())?;
    let request = request.as_object_mut().ok_or(())?;
    let ignored_max_output_tokens = request.remove("max_output_tokens").is_some();
    request.insert("stream".to_owned(), Value::Bool(true));
    serde_json::to_vec(&request)
        .map(Bytes::from)
        .map(|body| (body, ignored_max_output_tokens))
        .map_err(|_| ())
}

async fn adapt_hindsight_response(
    upstream: reqwest::Response,
    ignored_max_output_tokens: bool,
) -> Response {
    let status = upstream.status();
    let mut headers = safe_response_headers(upstream.headers());
    let response = read_hindsight_response_stream(upstream).await;
    let response = match response {
        Ok(response) => response,
        Err(()) => {
            return (
                StatusCode::BAD_GATEWAY,
                "upstream response adaptation failed",
            )
                .into_response();
        }
    };
    headers.remove(header::CONTENT_TYPE);
    headers.remove(header::CONTENT_LENGTH);
    headers.remove(header::CONTENT_ENCODING);
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    if ignored_max_output_tokens {
        headers.insert(
            "x-kepos-ignored-parameters",
            HeaderValue::from_static("max_output_tokens"),
        );
    }
    let mut downstream = HttpResponse::builder().status(status);
    downstream
        .headers_mut()
        .expect("response builder headers")
        .extend(headers);
    downstream
        .body(Body::from(response.to_string()))
        .unwrap_or_else(|_| HttpResponse::new(Body::empty()))
}

async fn read_hindsight_response_stream(upstream: reqwest::Response) -> Result<Value, ()> {
    let mut body = Vec::new();
    let mut stream = upstream.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| ())?;
        let length = body.len().checked_add(chunk.len()).ok_or(())?;
        if length > MAX_HINDSIGHT_RESPONSE_BYTES {
            return Err(());
        }
        body.extend_from_slice(&chunk);
    }
    aggregate_hindsight_sse(&body)
}

fn aggregate_hindsight_sse(body: &[u8]) -> Result<Value, ()> {
    let text = std::str::from_utf8(body).map_err(|_| ())?;
    let normalized = text.replace("\r\n", "\n");
    let mut output_items = BTreeMap::new();
    let mut terminal = None;

    for record in normalized.split("\n\n") {
        let mut event_name = None;
        let mut data = Vec::new();
        for line in record.lines() {
            if line.starts_with(':') {
                continue;
            }
            let (field, value) = line.split_once(':').unwrap_or((line, ""));
            let value = value.strip_prefix(' ').unwrap_or(value);
            match field {
                "event" => event_name = Some(value),
                "data" => data.push(value),
                _ => {}
            }
        }
        if data.is_empty() {
            continue;
        }
        let data = data.join("\n");
        let named_type = event_name.filter(|name| is_hindsight_response_event(name));
        if data.trim() == "[DONE]" {
            if named_type.is_some() {
                return Err(());
            }
            continue;
        }
        let parsed = serde_json::from_str::<Value>(&data);
        let value = match parsed {
            Ok(value) => value,
            Err(_) if named_type.is_some() || looks_like_hindsight_response_payload(&data) => {
                return Err(());
            }
            Err(_) => continue,
        };
        let data_type = value.get("type").and_then(Value::as_str);
        let event_type = data_type
            .filter(|name| is_hindsight_response_event(name))
            .or(named_type);
        let Some(event_type) = event_type else {
            continue;
        };
        if named_type.is_some() && data_type != Some(event_type) {
            return Err(());
        }
        match event_type {
            "response.output_item.done" => {
                let output_index = value
                    .get("output_index")
                    .and_then(Value::as_u64)
                    .ok_or(())?;
                let item = value
                    .get("item")
                    .filter(|item| item.is_object())
                    .cloned()
                    .ok_or(())?;
                output_items.insert(output_index, item);
            }
            "response.completed" | "response.incomplete" | "response.failed" => {
                let response = value
                    .get("response")
                    .filter(|response| response.is_object())
                    .cloned()
                    .ok_or(())?;
                terminal.get_or_insert(response);
            }
            _ => return Err(()),
        }
    }

    let mut terminal = terminal.ok_or(())?;
    if !output_items.is_empty() {
        terminal["output"] = Value::Array(output_items.into_values().collect());
    }
    Ok(terminal)
}

fn is_hindsight_response_event(name: &str) -> bool {
    matches!(
        name,
        "response.output_item.done"
            | "response.completed"
            | "response.incomplete"
            | "response.failed"
    )
}

fn looks_like_hindsight_response_payload(data: &str) -> bool {
    let data = data.trim_start();
    if !data.starts_with('{') || !data.contains("\"type\"") {
        return false;
    }
    [
        "response.output_item.done",
        "response.completed",
        "response.incomplete",
        "response.failed",
    ]
    .iter()
    .any(|event| data.contains(event))
}

async fn websocket_responses(
    State(bridge): State<Arc<Bridge>>,
    ws: WebSocketUpgrade,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let (upstream, semantic_headers, selected_protocol) =
        match bridge.connect_websocket(&uri, &headers).await {
            Ok(connection) => connection,
            Err(()) => return (StatusCode::BAD_GATEWAY, "upstream request failed").into_response(),
        };
    let mut ws = ws;
    if let Some(protocol) = selected_protocol {
        ws.set_selected_protocol(protocol);
    }
    let mut response = ws.on_upgrade(move |downstream| relay_websockets(downstream, upstream));
    response.headers_mut().extend(semantic_headers);
    response
}

fn response_endpoint(base_url: &str, uri: &Uri) -> String {
    let query = uri
        .query()
        .map_or(String::new(), |query| format!("?{query}"));
    format!("{}/responses{query}", base_url.trim_end_matches('/'))
}

fn response_websocket_endpoint(base_url: &str, uri: &Uri) -> String {
    let websocket_base = base_url.strip_prefix("https://").map_or_else(
        || {
            base_url
                .strip_prefix("http://")
                .map_or_else(|| base_url.to_owned(), |base| format!("ws://{base}"))
        },
        |base| format!("wss://{base}"),
    );
    response_endpoint(&websocket_base, uri)
}

fn forward_request_headers(headers: &HeaderMap) -> HeaderMap {
    forward_headers(headers, HeaderDirection::Request)
}

fn safe_response_headers(headers: &HeaderMap) -> HeaderMap {
    forward_headers(headers, HeaderDirection::Response)
}

fn forward_websocket_headers(headers: &HeaderMap) -> HeaderMap {
    let mut forwarded = forward_request_headers(headers);
    remove_websocket_framing_headers(&mut forwarded);
    forwarded
}

fn safe_websocket_response_headers(headers: &HeaderMap) -> HeaderMap {
    let mut forwarded = safe_response_headers(headers);
    remove_websocket_framing_headers(&mut forwarded);
    forwarded.remove(header::SEC_WEBSOCKET_PROTOCOL);
    forwarded
}

fn remove_websocket_framing_headers(headers: &mut HeaderMap) {
    for name in [
        "sec-websocket-accept",
        "sec-websocket-extensions",
        "sec-websocket-key",
        "sec-websocket-version",
    ] {
        headers.remove(name);
    }
}

enum HeaderDirection {
    Request,
    Response,
}

fn forward_headers(headers: &HeaderMap, direction: HeaderDirection) -> HeaderMap {
    let connection_headers = connection_header_names(headers);
    let mut forwarded = HeaderMap::new();
    for (name, value) in headers {
        if is_hop_by_hop(name, &connection_headers) {
            continue;
        }
        match direction {
            HeaderDirection::Request
                if name == header::CONTENT_LENGTH || is_peer_identity_header(name) =>
            {
                continue;
            }
            HeaderDirection::Response if name == header::SET_COOKIE => continue,
            _ => {}
        }
        forwarded.append(name.clone(), value.clone());
    }
    forwarded
}

fn connection_header_names(headers: &HeaderMap) -> Vec<HeaderName> {
    headers
        .get_all(header::CONNECTION)
        .iter()
        .flat_map(|value| value.to_str().unwrap_or_default().split(','))
        .filter_map(|name| HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect()
}

fn is_hop_by_hop(name: &HeaderName, connection_headers: &[HeaderName]) -> bool {
    connection_headers.iter().any(|item| item == name)
        || matches!(
            name.as_str(),
            "connection"
                | "keep-alive"
                | "proxy-authenticate"
                | "proxy-authorization"
                | "te"
                | "trailer"
                | "transfer-encoding"
                | "upgrade"
                | "host"
        )
}

fn is_peer_identity_header(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "authorization"
            | "proxy-authorization"
            | "x-api-key"
            | "cookie"
            | "chatgpt-account-id"
            | "x-openai-fedramp"
    )
}

fn relay_response(upstream: reqwest::Response) -> Response {
    let status = upstream.status();
    let headers = safe_response_headers(upstream.headers());
    let mut response = HttpResponse::builder().status(status);
    let response_headers = response.headers_mut().expect("response builder headers");
    response_headers.extend(headers);
    response
        .body(Body::from_stream(upstream.bytes_stream()))
        .unwrap_or_else(|_| HttpResponse::new(Body::empty()))
}

type UpstreamWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

fn is_unauthorized_websocket_handshake(error: &WebSocketError) -> bool {
    matches!(error, WebSocketError::Http(response) if response.status() == StatusCode::UNAUTHORIZED)
}

async fn relay_websockets(downstream: WebSocket, upstream: UpstreamWebSocket) {
    let (mut downstream_sink, mut downstream_stream) = downstream.split();
    let (mut upstream_sink, mut upstream_stream) = upstream.split();
    loop {
        tokio::select! {
            message = downstream_stream.next() => match message {
                Some(Ok(message)) => {
                    let close = matches!(message, AxumMessage::Close(_));
                    if upstream_sink.send(to_upstream_message(message)).await.is_err() || close {
                        break;
                    }
                }
                Some(Err(_)) | None => break,
            },
            message = upstream_stream.next() => match message {
                Some(Ok(message)) => {
                    let close = matches!(message, TungsteniteMessage::Close(_));
                    if let Some(message) = to_downstream_message(message)
                        && (downstream_sink.send(message).await.is_err() || close)
                    {
                        break;
                    }
                }
                Some(Err(_)) | None => {
                    let _ = downstream_sink.send(AxumMessage::Close(None)).await;
                    break;
                }
            },
        }
    }
}

fn to_upstream_message(message: AxumMessage) -> TungsteniteMessage {
    match message {
        AxumMessage::Text(text) => TungsteniteMessage::Text(text.to_string().into()),
        AxumMessage::Binary(bytes) => TungsteniteMessage::Binary(bytes),
        AxumMessage::Ping(bytes) => TungsteniteMessage::Ping(bytes),
        AxumMessage::Pong(bytes) => TungsteniteMessage::Pong(bytes),
        AxumMessage::Close(frame) => {
            TungsteniteMessage::Close(frame.map(|frame| TungsteniteCloseFrame {
                code: frame.code.into(),
                reason: frame.reason.to_string().into(),
            }))
        }
    }
}

fn to_downstream_message(message: TungsteniteMessage) -> Option<AxumMessage> {
    match message {
        TungsteniteMessage::Text(text) => Some(AxumMessage::Text(text.to_string().into())),
        TungsteniteMessage::Binary(bytes) => Some(AxumMessage::Binary(bytes)),
        TungsteniteMessage::Ping(_)
        | TungsteniteMessage::Pong(_)
        | TungsteniteMessage::Frame(_) => None,
        TungsteniteMessage::Close(frame) => {
            Some(AxumMessage::Close(frame.map(|frame| AxumCloseFrame {
                code: frame.code.into(),
                reason: frame.reason.to_string().into(),
            })))
        }
    }
}

#[derive(Debug, Deserialize)]
struct ImageWireRequest {
    prompt: String,
    #[serde(default)]
    images: Vec<String>,
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

async fn http_images(
    State(bridge): State<Arc<Bridge>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let operation = match parse_image_request(&headers, &body) {
        Ok(operation) => operation,
        Err(message) => return image_error(StatusCode::BAD_REQUEST, message),
    };
    match bridge.perform_image(operation).await {
        Ok(image_url) => axum::Json(json!({ "image_url": image_url })).into_response(),
        Err(()) => image_error(StatusCode::BAD_GATEWAY, "image operation failed"),
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
