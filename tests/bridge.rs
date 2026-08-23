use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::post,
};
use futures_util::{SinkExt, Stream, StreamExt};
use kepos_codex_bridge::{Bridge, ENDPOINT, IMAGE_ENDPOINT};
use nanocodex_oai_api::{
    Model, OpenAi,
    auth::{
        OpenAiAuth, OpenAiAuthError, OpenAiAuthFuture, OpenAiAuthMode, OpenAiAuthSnapshot,
        OpenAiAuthSource,
    },
    transport::ResponsesTransport,
};
use reqwest::Client;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{Mutex, oneshot},
    time::timeout,
};
use tokio_tungstenite::{
    accept_hdr_async, connect_async,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        handshake::server::{Request as WebSocketRequest, Response as WebSocketResponse},
        protocol::CloseFrame,
    },
};

struct TestManagedAuth {
    recovered: AtomicBool,
}

impl TestManagedAuth {
    fn new() -> Self {
        Self {
            recovered: AtomicBool::new(false),
        }
    }
}

impl OpenAiAuthSource for TestManagedAuth {
    fn validate(&self) -> Result<(), OpenAiAuthError> {
        Ok(())
    }

    fn snapshot(&self) -> OpenAiAuthFuture<'_, Result<OpenAiAuthSnapshot, OpenAiAuthError>> {
        let bearer = if self.recovered.load(Ordering::SeqCst) {
            "managed-fresh"
        } else {
            "managed-stale"
        };
        Box::pin(async move {
            Ok(OpenAiAuthSnapshot::new(
                OpenAiAuthMode::ChatGpt,
                bearer,
                Some("managed-account"),
                true,
                0,
            ))
        })
    }

    fn recover_unauthorized(
        &self,
        _rejected: &OpenAiAuthSnapshot,
    ) -> OpenAiAuthFuture<'_, Result<(), OpenAiAuthError>> {
        self.recovered.store(true, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

fn managed_auth() -> OpenAiAuth {
    OpenAiAuth::managed_chatgpt(Arc::new(TestManagedAuth::new()))
}

#[derive(Clone, Debug)]
struct RecordedRequest {
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
}

#[derive(Clone)]
struct RecordingOrigin {
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    unauthorized_stale: bool,
    response_status: StatusCode,
}

async fn recording_responses(
    State(state): State<RecordingOrigin>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    state.requests.lock().await.push(RecordedRequest {
        uri,
        headers: headers.clone(),
        body: body.clone(),
    });
    if state.unauthorized_stale
        && headers
            .get("authorization")
            .is_some_and(|value| value == "Bearer managed-stale")
    {
        return Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .header("set-cookie", "upstream-secret=never-forward")
            .body(Body::from("unauthorized"))
            .expect("401 response");
    }
    let compaction = String::from_utf8_lossy(&body).contains("compaction_trigger");
    let completed = if compaction {
        json!({"type":"response.completed","response":{"id":"compact-response","status":"completed","output":[{"type":"compaction","id":"cmp-upstream","encrypted_content":"opaque-output"}],"usage":null}})
    } else {
        json!({"type":"response.completed","response":{"id":"normal-response","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"relay answer"}]}],"usage":null}})
    };
    let body = format!("event: opaque\ndata: {}\n\ndata: [DONE]\n\n", completed);
    Response::builder()
        .status(state.response_status)
        .header("content-type", "text/event-stream")
        .header("x-codex-turn-state", "client-owned-turn")
        .header("x-reasoning-included", "true")
        .header("set-cookie", "upstream-secret=never-forward")
        .body(Body::from(body))
        .expect("SSE response")
}

async fn start_recording_origin(
    unauthorized_stale: bool,
    response_status: StatusCode,
) -> (
    String,
    Arc<Mutex<Vec<RecordedRequest>>>,
    tokio::task::JoinHandle<()>,
) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let state = RecordingOrigin {
        requests: requests.clone(),
        unauthorized_stale,
        response_status,
    };
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("origin listener");
    let address = listener.local_addr().expect("origin address");
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new()
                .route("/responses", post(recording_responses))
                .with_state(state),
        )
        .await
        .expect("recording origin");
    });
    (format!("http://{address}"), requests, task)
}

async fn start_bridge(
    auth: OpenAiAuth,
    responses_base: String,
    image_base: Option<String>,
) -> (String, tokio::task::JoinHandle<()>) {
    let bridge = Bridge::new(auth).with_responses_api_base_url(responses_base);
    let bridge = if let Some(image_base) = image_base {
        bridge.with_image_api_base_url(image_base)
    } else {
        bridge
    };
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bridge listener");
    let address = listener.local_addr().expect("bridge address");
    let task = tokio::spawn(async move {
        axum::serve(listener, bridge.router())
            .await
            .expect("bridge server");
    });
    (format!("http://{address}{ENDPOINT}"), task)
}

#[tokio::test]
async fn relays_zstd_bytes_client_protocol_and_sse_without_parsing() {
    let (origin, requests, origin_server) =
        start_recording_origin(false, StatusCode::CREATED).await;
    let (url, bridge_server) = start_bridge(managed_auth(), origin, None).await;
    let opaque = br#"{"model":"arbitrary-client-model","input":[{"role":"developer","content":"keep all fields"}],"prompt_cache_key":"cache-client","previous_response_id":"response-client","additional_tools":[{"type":"computer"}],"client_metadata":{"opaque":"value"}}"#;
    let compressed = zstd::stream::encode_all(&opaque[..], 3).expect("compress request");
    let response = Client::new()
        .post(format!("{url}?client-query=opaque"))
        .header("content-encoding", "zstd")
        .header("content-type", "application/json")
        .header("x-openai-internal-codex-responses-lite", "true")
        .header("x-codex-beta-features", "remote_compaction_v2")
        .header("session-id", "session-client")
        .header("thread-id", "thread-client")
        .header("x-client-request-id", "request-client")
        .header("x-codex-turn-state", "turn-client")
        .header("authorization", "Bearer peer-secret")
        .header("x-api-key", "peer-api-key")
        .header("cookie", "peer-cookie=secret")
        .header("chatgpt-account-id", "peer-account")
        .header("x-openai-fedramp", "false")
        .body(compressed.clone())
        .send()
        .await
        .expect("relay response");
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(
        response.headers()["x-codex-turn-state"],
        "client-owned-turn"
    );
    assert!(!response.headers().contains_key("set-cookie"));
    let sse = response.bytes().await.expect("SSE bytes");
    assert!(sse.starts_with(b"event: opaque\ndata: "));
    assert!(sse.ends_with(b"data: [DONE]\n\n"));

    let recorded = requests.lock().await.pop().expect("origin request");
    assert_eq!(recorded.uri.query(), Some("client-query=opaque"));
    assert_eq!(recorded.body, compressed);
    for (name, value) in [
        ("content-encoding", "zstd"),
        ("x-openai-internal-codex-responses-lite", "true"),
        ("x-codex-beta-features", "remote_compaction_v2"),
        ("session-id", "session-client"),
        ("thread-id", "thread-client"),
        ("x-client-request-id", "request-client"),
        ("x-codex-turn-state", "turn-client"),
    ] {
        assert_eq!(recorded.headers[name], value);
    }
    assert_eq!(recorded.headers["authorization"], "Bearer managed-stale");
    assert_eq!(recorded.headers["chatgpt-account-id"], "managed-account");
    assert_eq!(recorded.headers["x-openai-fedramp"], "true");
    for name in ["x-api-key", "cookie"] {
        assert!(!recorded.headers.contains_key(name));
    }
    bridge_server.abort();
    origin_server.abort();
}

#[tokio::test]
async fn retries_a_401_once_with_refreshed_managed_identity() {
    let (origin, requests, origin_server) = start_recording_origin(true, StatusCode::OK).await;
    let (url, bridge_server) = start_bridge(managed_auth(), origin, None).await;
    let response = Client::new()
        .post(&url)
        .header("authorization", "Bearer peer-secret")
        .header("cookie", "peer-cookie=secret")
        .body("complete Lite request with remote_compaction_v2")
        .send()
        .await
        .expect("relay response");
    assert_eq!(response.status(), StatusCode::OK);
    assert!(!response.headers().contains_key("set-cookie"));
    let sse =
        String::from_utf8(response.bytes().await.expect("SSE bytes").to_vec()).expect("SSE text");
    assert!(!sse.contains("peer-secret"));
    assert!(!sse.contains("managed-"));
    let calls = requests.lock().await;
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].headers["authorization"], "Bearer managed-stale");
    assert_eq!(calls[1].headers["authorization"], "Bearer managed-fresh");
    for call in calls.iter() {
        assert!(!call.headers.contains_key("cookie"));
    }
    bridge_server.abort();
    origin_server.abort();
}

struct PendingStream(Option<oneshot::Sender<()>>);
impl Stream for PendingStream {
    type Item = Result<Bytes, Infallible>;
    fn poll_next(self: std::pin::Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}
impl Drop for PendingStream {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

async fn pending_response(
    State(sender): State<Arc<Mutex<Option<oneshot::Sender<()>>>>>,
) -> Response {
    let sender = sender.lock().await.take().expect("one request");
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .body(Body::from_stream(PendingStream(Some(sender))))
        .expect("pending response")
}

#[tokio::test]
async fn downstream_disconnect_drops_the_upstream_stream() {
    let (sender, receiver) = oneshot::channel();
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("origin listener");
    let address = listener.local_addr().expect("origin address");
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new()
                .route("/responses", post(pending_response))
                .with_state(Arc::new(Mutex::new(Some(sender)))),
        )
        .await
        .expect("origin server");
    });
    let (url, bridge_server) =
        start_bridge(managed_auth(), format!("http://{address}"), None).await;
    let response = Client::new()
        .post(url)
        .body("opaque request")
        .send()
        .await
        .expect("response headers");
    drop(response);
    tokio::time::timeout(Duration::from_secs(2), receiver)
        .await
        .expect("upstream stream release")
        .expect("drop signal");
    bridge_server.abort();
    task.abort();
}

#[tokio::test]
async fn pinned_nanocodex_http_client_creates_then_compacts_through_relay() {
    let (origin, requests, origin_server) = start_recording_origin(false, StatusCode::OK).await;
    let (url, bridge_server) = start_bridge(managed_auth(), origin, None).await;
    let base = url.trim_end_matches(ENDPOINT).to_owned() + "/codex";
    let openai = OpenAi::builder("nonsecret-test-key")
        .model(Model::Luna)
        .transport(ResponsesTransport::Https)
        .api_base_url(base)
        .build()
        .expect("Nanocodex HTTP client");
    let mut session = openai
        .instructions("test instruction")
        .build()
        .expect("Nanocodex session");
    let mut turn = session.turn();
    assert_eq!(
        turn.create("first request")
            .await
            .expect("create response")
            .output_text(),
        "relay answer"
    );
    turn.compact().await.expect("compact response");
    let calls = requests.lock().await;
    assert_eq!(calls.len(), 2);
    assert!(String::from_utf8_lossy(&calls[1].body).contains("compaction_trigger"));
    bridge_server.abort();
    origin_server.abort();
}

async fn read_http_head(stream: &mut tokio::net::TcpStream) -> String {
    let mut bytes = Vec::new();
    loop {
        if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
            return String::from_utf8(bytes).expect("HTTP header text");
        }
        assert_ne!(
            stream.read_buf(&mut bytes).await.expect("read HTTP header"),
            0
        );
    }
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn websocket_retries_managed_auth_and_relays_semantic_metadata_and_payloads() {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("origin listener");
    let origin_address = listener.local_addr().expect("origin address");
    let origin_server = tokio::spawn(async move {
        let (mut rejected, _) = listener.accept().await.expect("stale connection");
        let stale_headers = read_http_head(&mut rejected).await.to_ascii_lowercase();
        assert!(stale_headers.contains("authorization: bearer managed-stale"));
        assert!(!stale_headers.contains("peer-secret"));
        assert!(!stale_headers.contains("peer-cookie"));
        rejected
            .write_all(
                b"HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
            )
            .await
            .expect("401 response");

        let (stream, _) = listener.accept().await.expect("fresh connection");
        let mut socket = accept_hdr_async(
            stream,
            |request: &WebSocketRequest, mut response: WebSocketResponse| {
                assert_eq!(request.uri().query(), Some("client-query=opaque"));
                for (name, value) in [
                    ("x-openai-internal-codex-responses-lite", "true"),
                    ("openai-beta", "responses_websockets=2026-02-06"),
                    ("prompt-cache-key", "cache-client"),
                    ("session-id", "session-client"),
                    ("thread-id", "thread-client"),
                    ("x-client-request-id", "request-client"),
                    ("x-codex-turn-state", "turn-client"),
                ] {
                    assert_eq!(request.headers()[name], value);
                }
                assert_eq!(request.headers()["authorization"], "Bearer managed-fresh");
                assert_eq!(request.headers()["sec-websocket-protocol"], "codex-test");
                assert_eq!(request.headers()["chatgpt-account-id"], "managed-account");
                assert_eq!(request.headers()["x-openai-fedramp"], "true");
                for name in ["x-api-key", "cookie"] {
                    assert!(!request.headers().contains_key(name));
                }
                response.headers_mut().insert(
                    "sec-websocket-protocol",
                    "codex-test".parse().expect("protocol header"),
                );
                response.headers_mut().insert(
                    "x-codex-turn-state",
                    "upstream-turn".parse().expect("turn header"),
                );
                response.headers_mut().insert(
                    "openai-model",
                    "upstream-model".parse().expect("model header"),
                );
                response.headers_mut().insert(
                    "x-reasoning-included",
                    "true".parse().expect("reasoning header"),
                );
                response.headers_mut().insert(
                    "set-cookie",
                    "upstream-secret=never-forward"
                        .parse()
                        .expect("cookie header"),
                );
                Ok(response)
            },
        )
        .await
        .expect("upstream WebSocket handshake");
        assert_eq!(
            socket
                .next()
                .await
                .expect("text frame")
                .expect("text message"),
            Message::Text(r#"{"type":"response.create","previous_response_id":"response-client","prompt_cache_key":"cache-client","client_metadata":{"opaque":"value"}}"#.into())
        );
        assert_eq!(
            socket
                .next()
                .await
                .expect("binary frame")
                .expect("binary message"),
            Message::Binary(vec![0, 255, 42].into())
        );
        socket
            .send(Message::Text("upstream text payload".into()))
            .await
            .expect("upstream text");
        socket
            .send(Message::Binary(vec![9, 8, 7].into()))
            .await
            .expect("upstream binary");
        socket
            .send(Message::Ping(vec![1, 2, 3].into()))
            .await
            .expect("upstream ping");
        let pong = timeout(Duration::from_secs(2), socket.next())
            .await
            .expect("pong deadline")
            .expect("pong frame")
            .expect("pong message");
        assert_eq!(pong, Message::Pong(vec![1, 2, 3].into()));
        let close = timeout(Duration::from_secs(2), socket.next())
            .await
            .expect("close deadline")
            .expect("close frame")
            .expect("close message");
        assert!(
            matches!(close, Message::Close(Some(frame)) if u16::from(frame.code) == 4001 && frame.reason == "client close")
        );
    });

    let (url, bridge_server) =
        start_bridge(managed_auth(), format!("http://{origin_address}"), None).await;
    let websocket_url = format!("{}?client-query=opaque", url.replacen("http", "ws", 1));
    let mut request = websocket_url.into_client_request().expect("client request");
    for (name, value) in [
        ("x-openai-internal-codex-responses-lite", "true"),
        ("openai-beta", "responses_websockets=2026-02-06"),
        ("prompt-cache-key", "cache-client"),
        ("session-id", "session-client"),
        ("thread-id", "thread-client"),
        ("x-client-request-id", "request-client"),
        ("x-codex-turn-state", "turn-client"),
        ("authorization", "Bearer peer-secret"),
        ("x-api-key", "peer-api-key"),
        ("cookie", "peer-cookie=secret"),
        ("sec-websocket-protocol", "codex-test"),
    ] {
        request
            .headers_mut()
            .insert(name, value.parse().expect("client header"));
    }
    let (mut socket, response) = connect_async(request)
        .await
        .expect("bridge WebSocket handshake");
    assert_eq!(response.headers()["sec-websocket-protocol"], "codex-test");
    assert_eq!(response.headers()["x-codex-turn-state"], "upstream-turn");
    assert_eq!(response.headers()["openai-model"], "upstream-model");
    assert_eq!(response.headers()["x-reasoning-included"], "true");
    assert!(!response.headers().contains_key("set-cookie"));
    socket
        .send(Message::Text(r#"{"type":"response.create","previous_response_id":"response-client","prompt_cache_key":"cache-client","client_metadata":{"opaque":"value"}}"#.into()))
        .await
        .expect("client text");
    socket
        .send(Message::Binary(vec![0, 255, 42].into()))
        .await
        .expect("client binary");
    assert_eq!(
        socket
            .next()
            .await
            .expect("upstream text")
            .expect("text message"),
        Message::Text("upstream text payload".into())
    );
    assert_eq!(
        socket
            .next()
            .await
            .expect("upstream binary")
            .expect("binary message"),
        Message::Binary(vec![9, 8, 7].into())
    );
    socket
        .send(Message::Close(Some(CloseFrame {
            code: 4001.into(),
            reason: "client close".into(),
        })))
        .await
        .expect("client close");
    timeout(Duration::from_secs(5), origin_server)
        .await
        .expect("origin completion")
        .expect("origin result");
    bridge_server.abort();
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn pinned_nanocodex_websocket_client_creates_then_compacts_through_relay() {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("origin listener");
    let origin_address = listener.local_addr().expect("origin address");
    let origin_server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("WebSocket connection");
        let mut socket = accept_hdr_async(
            stream,
            |request: &WebSocketRequest, response: WebSocketResponse| {
                assert_eq!(request.headers()["authorization"], "Bearer managed-stale");
                assert_eq!(request.headers()["chatgpt-account-id"], "managed-account");
                assert_eq!(request.headers()["x-openai-fedramp"], "true");
                assert!(!request.headers().contains_key("x-api-key"));
                Ok(response)
            },
        )
        .await
        .expect("upstream WebSocket handshake");
        let create = socket
            .next()
            .await
            .expect("create frame")
            .expect("create message")
            .into_text()
            .expect("create text");
        assert!(create.contains("first request"));
        socket.send(Message::Text(json!({"type":"response.completed","response":{"id":"create-response","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"relay answer"}]}],"usage":null}}).to_string().into())).await.expect("create response");
        let compact = socket
            .next()
            .await
            .expect("compact frame")
            .expect("compact message")
            .into_text()
            .expect("compact text");
        assert!(compact.contains("compaction_trigger"));
        socket.send(Message::Text(json!({"type":"response.output_item.done","item":{"id":"cmp-upstream","type":"compaction","encrypted_content":"opaque-output"}}).to_string().into())).await.expect("compaction item");
        socket.send(Message::Text(json!({"type":"response.completed","response":{"id":"compact-response","status":"completed","output":[],"usage":null}}).to_string().into())).await.expect("compact response");
    });

    let (url, bridge_server) =
        start_bridge(managed_auth(), format!("http://{origin_address}"), None).await;
    let openai = OpenAi::builder("nonsecret-test-key")
        .model(Model::Luna)
        .transport(ResponsesTransport::WebSocket)
        .websocket_warmup(false)
        .websocket_url(url.replacen("http", "ws", 1))
        .build()
        .expect("Nanocodex WebSocket client");
    let mut session = openai
        .instructions("test instruction")
        .build()
        .expect("Nanocodex session");
    let mut turn = session.turn();
    assert_eq!(
        turn.create("first request")
            .await
            .expect("create response")
            .output_text(),
        "relay answer"
    );
    turn.compact().await.expect("compact response");
    timeout(Duration::from_secs(5), origin_server)
        .await
        .expect("origin completion")
        .expect("origin result");
    bridge_server.abort();
}

#[derive(Clone)]
struct ImageOrigin {
    calls: Arc<Mutex<Vec<(String, HeaderMap, Value)>>>,
    fail: bool,
}

async fn image_response(
    State(origin): State<ImageOrigin>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body = serde_json::from_slice(&body).expect("image JSON");
    origin
        .calls
        .lock()
        .await
        .push((uri.path().to_owned(), headers, body));
    if origin.fail {
        StatusCode::INTERNAL_SERVER_ERROR.into_response()
    } else {
        axum::Json(json!({"data":[{"b64_json":"AAAA"}]})).into_response()
    }
}

async fn start_image_origin(
    fail: bool,
) -> (
    String,
    Arc<Mutex<Vec<(String, HeaderMap, Value)>>>,
    tokio::task::JoinHandle<()>,
) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("image listener");
    let address = listener.local_addr().expect("image address");
    let origin = ImageOrigin {
        calls: calls.clone(),
        fail,
    };
    let origin_server = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new()
                .route("/images/generations", post(image_response))
                .route("/images/edits", post(image_response))
                .with_state(origin),
        )
        .await
        .expect("image origin");
    });
    (format!("http://{address}"), calls, origin_server)
}

#[tokio::test]
async fn fixed_image_endpoint_remains_unchanged() {
    let (image_base, calls, origin_server) = start_image_origin(false).await;
    let (url, bridge_server) = start_bridge(
        managed_auth(),
        "http://127.0.0.1:1".to_owned(),
        Some(image_base),
    )
    .await;
    let response = Client::new()
        .post(url.replace(ENDPOINT, IMAGE_ENDPOINT))
        .json(&json!({"prompt":"draw","api_key":"peer-secret"}))
        .send()
        .await
        .expect("image relay");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.json::<Value>().await.expect("image JSON"),
        json!({"image_url":"data:image/png;base64,AAAA"})
    );
    let calls = calls.lock().await;
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, "/images/generations");
    assert!(calls[0].2.get("api_key").is_none());
    assert_eq!(calls[0].1["authorization"], "Bearer managed-stale");
    assert_eq!(calls[0].1["chatgpt-account-id"], "managed-account");
    bridge_server.abort();
    origin_server.abort();
}

#[tokio::test]
async fn fixed_image_edit_limit_and_generic_errors_remain_unchanged() {
    let (image_base, calls, origin_server) = start_image_origin(false).await;
    let (url, bridge_server) = start_bridge(
        managed_auth(),
        "http://127.0.0.1:1".to_owned(),
        Some(image_base),
    )
    .await;
    let image_url = url.replace(ENDPOINT, IMAGE_ENDPOINT);
    let images = (0..5)
        .map(|index| format!("data:image/png;base64,IMG{index}"))
        .collect::<Vec<_>>();
    let response = Client::new()
        .post(&image_url)
        .json(&json!({"prompt":"edit","images":images}))
        .send()
        .await
        .expect("image edit");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(calls.lock().await[0].0, "/images/edits");
    for invalid in [
        json!({"prompt":"six","images":["data:image/png;base64,A","data:image/png;base64,B","data:image/png;base64,C","data:image/png;base64,D","data:image/png;base64,E","data:image/png;base64,F"]}),
        json!({"prompt":"remote","images":["https://example.test/image.png"]}),
    ] {
        let response = Client::new()
            .post(&image_url)
            .json(&invalid)
            .send()
            .await
            .expect("invalid image");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(
            response
                .text()
                .await
                .expect("invalid image body")
                .contains("invalid_request_error")
        );
    }
    bridge_server.abort();
    origin_server.abort();

    let (image_base, _, failing_origin) = start_image_origin(true).await;
    let (url, bridge_server) = start_bridge(
        managed_auth(),
        "http://127.0.0.1:1".to_owned(),
        Some(image_base),
    )
    .await;
    let response = Client::new()
        .post(url.replace(ENDPOINT, IMAGE_ENDPOINT))
        .header("content-type", "application/json")
        .body(r#"{"prompt":"do-not-echo-this"}"#)
        .send()
        .await
        .expect("image upstream failure");
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let body = response.text().await.expect("generic error");
    assert!(body.contains("image operation failed"));
    assert!(!body.contains("do-not-echo-this"));
    bridge_server.abort();
    failing_origin.abort();
}
