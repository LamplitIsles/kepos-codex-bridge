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
use kepos_codex_bridge::{
    BUFFERED_RESPONSES_ENDPOINT, Bridge, ENDPOINT, IMAGE_ENDPOINT, WEB_SEARCH_ENDPOINT,
};
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
    let mut response = Response::builder().status(state.response_status);
    if state.response_status.is_redirection() {
        response = response.header("location", "/redirected");
    }
    response
        .header("content-type", "text/event-stream")
        .header("content-length", body.len().to_string())
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

#[derive(Clone)]
struct BufferedOrigin {
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    response_status: StatusCode,
    response_chunks: Vec<Bytes>,
}

async fn buffered_origin_responses(
    State(state): State<BufferedOrigin>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let content_length = state
        .response_chunks
        .iter()
        .map(Bytes::len)
        .sum::<usize>()
        .to_string();
    state
        .requests
        .lock()
        .await
        .push(RecordedRequest { uri, headers, body });
    Response::builder()
        .status(state.response_status)
        .header("content-type", "text/event-stream")
        .header("content-length", content_length)
        .header("x-codex-turn-state", "upstream-turn")
        .header("set-cookie", "upstream-secret=never-forward")
        .body(Body::from_stream(futures_util::stream::iter(
            state.response_chunks.into_iter().map(Ok::<_, Infallible>),
        )))
        .expect("buffered origin response")
}

async fn start_buffered_origin(
    response_status: StatusCode,
    response_chunks: Vec<Bytes>,
) -> (
    String,
    Arc<Mutex<Vec<RecordedRequest>>>,
    tokio::task::JoinHandle<()>,
) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("buffered origin listener");
    let address = listener.local_addr().expect("buffered origin address");
    let state = BufferedOrigin {
        requests: requests.clone(),
        response_status,
        response_chunks,
    };
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new()
                .route("/responses", post(buffered_origin_responses))
                .with_state(state),
        )
        .await
        .expect("buffered origin server");
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

#[derive(Clone)]
struct SearchOrigin {
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    response_status: StatusCode,
    response_body: Bytes,
    unauthorized_stale: bool,
}

async fn search_origin_response(
    State(state): State<SearchOrigin>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    state.requests.lock().await.push(RecordedRequest {
        uri,
        headers: headers.clone(),
        body,
    });
    if state.unauthorized_stale
        && headers
            .get("authorization")
            .is_some_and(|value| value == "Bearer managed-stale")
    {
        return Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .header("content-type", "text/plain")
            .body(Body::from("secret unauthorized detail"))
            .expect("search 401 response");
    }
    Response::builder()
        .status(state.response_status)
        .header("content-type", "application/json")
        .body(Body::from(state.response_body))
        .expect("search response")
}

async fn start_search_origin(
    response_status: StatusCode,
    response_body: Bytes,
    unauthorized_stale: bool,
) -> (
    String,
    Arc<Mutex<Vec<RecordedRequest>>>,
    tokio::task::JoinHandle<()>,
) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let state = SearchOrigin {
        requests: requests.clone(),
        response_status,
        response_body,
        unauthorized_stale,
    };
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("search origin listener");
    let address = listener.local_addr().expect("search origin address");
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new()
                .route("/alpha/search", post(search_origin_response))
                .with_state(state),
        )
        .await
        .expect("search origin");
    });
    (format!("http://{address}"), requests, task)
}

#[derive(Clone)]
struct SequencedSearchOrigin {
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    responses: Arc<Mutex<Vec<(StatusCode, Bytes)>>>,
}

async fn sequenced_search_origin_response(
    State(state): State<SequencedSearchOrigin>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    state
        .requests
        .lock()
        .await
        .push(RecordedRequest { uri, headers, body });
    let (status, response_body) = {
        let mut responses = state.responses.lock().await;
        if responses.len() > 1 {
            responses.remove(0)
        } else {
            responses.pop().expect("sequenced search response fixture")
        }
    };
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(response_body))
        .expect("sequenced search response")
}

async fn start_sequenced_search_origin(
    responses: Vec<(StatusCode, Bytes)>,
) -> (
    String,
    Arc<Mutex<Vec<RecordedRequest>>>,
    tokio::task::JoinHandle<()>,
) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let state = SequencedSearchOrigin {
        requests: requests.clone(),
        responses: Arc::new(Mutex::new(responses)),
    };
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("sequenced search origin listener");
    let address = listener
        .local_addr()
        .expect("sequenced search origin address");
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new()
                .route("/alpha/search", post(sequenced_search_origin_response))
                .with_state(state),
        )
        .await
        .expect("sequenced search origin");
    });
    (format!("http://{address}"), requests, task)
}

#[tokio::test]
async fn stateless_web_search_uses_fixed_envelope_and_preserves_plaintext_results() {
    let upstream_body = json!({
        "output": "The answer",
        "results": [{
            "type": "text_result",
            "nested": {"unknown": [1, true, null]},
            "future_field": "preserve me"
        }],
        "encrypted_output": "do-not-forward",
        "other_upstream_state": {"secret": true}
    });
    let (origin, requests, origin_server) = start_search_origin(
        StatusCode::OK,
        Bytes::from(serde_json::to_vec(&upstream_body).expect("search fixture")),
        false,
    )
    .await;
    let (url, bridge_server) = start_bridge(managed_auth(), origin.clone(), None).await;
    let request_body = json!({
        "commands": {
            "search_query": [
                {"q": "  rust async  ", "recency": 0, "domains": ["example.com"]}
            ],
            "weather": [{"location": "Taipei", "start": "2026-09-01", "duration": 1}],
            "sports": [{"fn": "schedule", "league": "nba", "team": "GSW"}],
            "finance": [{"ticker": "ACME", "type": "equity", "market": "USA"}],
            "time": [{"utc_offset": "+08:00"}]
        }
    });
    let response = Client::new()
        .post(url.replace(ENDPOINT, WEB_SEARCH_ENDPOINT))
        .header("authorization", "Bearer peer-secret")
        .header("cookie", "peer-cookie=secret")
        .header("chatgpt-account-id", "peer-account")
        .header("x-openai-fedramp", "false")
        .json(&request_body)
        .send()
        .await
        .expect("web search response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.json::<Value>().await.expect("search JSON"),
        upstream_body["output"]
            .as_str()
            .map(|output| {
                json!({
                    "output": output,
                    "results": upstream_body["results"].clone()
                })
            })
            .expect("output fixture")
    );

    let request = requests.lock().await.pop().expect("search origin request");
    assert_eq!(request.uri.path(), "/alpha/search");
    assert_eq!(request.headers["authorization"], "Bearer managed-stale");
    assert_eq!(request.headers["chatgpt-account-id"], "managed-account");
    assert_eq!(request.headers["x-openai-fedramp"], "true");
    assert_eq!(request.headers["user-agent"], "nanocodex/0.5.0");
    assert_eq!(request.headers["content-type"], "application/json");
    let encoded: Value = serde_json::from_slice(&request.body).expect("upstream request JSON");
    assert!(encoded["id"].as_str().is_some_and(|id| !id.is_empty()));
    assert_eq!(encoded["model"], "gpt-5.6-sol");
    assert_eq!(encoded["commands"]["response_length"], "short");
    assert_eq!(encoded["commands"]["sports"][0]["tool"], "sports");
    assert_eq!(
        encoded["settings"],
        json!({
            "allowed_callers": ["direct"],
            "external_web_access": true
        })
    );
    assert_eq!(encoded["max_output_tokens"], 10_000);
    assert!(encoded.get("input").is_none());
    assert!(
        request
            .body
            .windows(b"peer-secret".len())
            .all(|window| window != b"peer-secret")
    );
    bridge_server.abort();
    origin_server.abort();
}

#[tokio::test]
async fn stateless_web_search_rejects_invalid_requests_without_upstream_contact() {
    let (origin, requests, origin_server) = start_search_origin(
        StatusCode::OK,
        Bytes::from_static(br#"{"output":"unused"}"#),
        false,
    )
    .await;
    let (url, bridge_server) = start_bridge(managed_auth(), origin, None).await;
    let invalid = [
        json!({"commands": {}}),
        json!({"commands": {"search_query": [{"q": ""}]}}),
        json!({"commands": {"search_query": [{"q": "one"}, {"q": "two"}, {"q": "three"}, {"q": "four"}]}}),
        json!({"commands": {"sports": [{"fn": "schedule", "league": "nba"}, {"fn": "standings", "league": "nfl"}]}}),
        json!({"commands": {"weather": [{"location": "Taipei", "start": "2026-2-30"}]}}),
        json!({"commands": {"weather": [{"location": "Taipei", "duration": null}]}}),
        json!({"commands": {"time": [{"utc_offset": "+8:00"}]}}),
        json!({"commands": {"open": [{"ref_id": "turn0search0"}]}}),
        json!({"commands": {"find": [{"ref_id": "turn0search0", "pattern": "secret"}]}}),
        json!({"commands": {"click": [{"ref_id": "turn0search0", "id": 1}]}}),
        json!({"commands": {"image_query": [{"q": "cats"}]}}),
        json!({"commands": {"search_query": [{"q": "one"}], "response_length": "long"}}),
        json!({"commands": {"search_query": [{"q": "one"}]}, "model": "caller-model"}),
        json!({"commands": {"search_query": [{"q": "one"}]}, "id": "caller-id"}),
        json!({"commands": {"search_query": [{"q": "one"}]}, "input": "caller-input"}),
        json!({"commands": {"search_query": [{"q": "one", "nested": "rejected"}]}}),
    ];
    for body in invalid {
        let response = Client::new()
            .post(url.replace(ENDPOINT, WEB_SEARCH_ENDPOINT))
            .json(&body)
            .send()
            .await
            .expect("validation response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let error = response.json::<Value>().await.expect("validation JSON");
        assert_eq!(error["error"]["type"], "invalid_request_error");
    }
    assert!(requests.lock().await.is_empty());
    bridge_server.abort();
    origin_server.abort();
}

#[tokio::test]
async fn stateless_web_search_recovers_managed_401_once() {
    let (origin, requests, origin_server) = start_search_origin(
        StatusCode::OK,
        Bytes::from_static(br#"{"output":"fresh"}"#),
        true,
    )
    .await;
    let (url, bridge_server) = start_bridge(managed_auth(), origin, None).await;
    let response = Client::new()
        .post(url.replace(ENDPOINT, WEB_SEARCH_ENDPOINT))
        .json(&json!({"commands": {"search_query": [{"q": "recover"}]}}))
        .send()
        .await
        .expect("recovered search response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.json::<Value>().await.expect("search JSON"),
        json!({"output": "fresh"})
    );
    let requests = requests.lock().await;
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].headers["authorization"], "Bearer managed-stale");
    assert_eq!(requests[1].headers["authorization"], "Bearer managed-fresh");
    assert_eq!(
        serde_json::from_slice::<Value>(&requests[0].body).expect("first request JSON")["id"],
        serde_json::from_slice::<Value>(&requests[1].body).expect("second request JSON")["id"]
    );
    bridge_server.abort();
    origin_server.abort();
}

#[tokio::test]
async fn stateless_web_search_returns_generic_error_for_unsafe_upstream_data() {
    let oversized = Bytes::from(vec![b'x'; 1024 * 1024 + 1]);
    let (origin, requests, origin_server) =
        start_search_origin(StatusCode::OK, oversized, false).await;
    let (url, bridge_server) = start_bridge(managed_auth(), origin, None).await;
    let response = Client::new()
        .post(url.replace(ENDPOINT, WEB_SEARCH_ENDPOINT))
        .json(&json!({"commands": {"time": [{"utc_offset": "+00:00"}]}}))
        .send()
        .await
        .expect("oversized search response");
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let body = response.text().await.expect("error body");
    assert!(body.contains("web search operation failed"));
    assert!(!body.contains('x'));
    assert_eq!(requests.lock().await.len(), 1);
    bridge_server.abort();
    origin_server.abort();
}

#[tokio::test]
async fn stateless_web_search_hides_malformed_or_missing_output_fixtures() {
    let fixtures = [
        (
            "malformed-output-secret",
            Bytes::from_static(br#"{"output":"malformed-output-secret""#),
        ),
        (
            "missing-output-secret",
            Bytes::from_static(br#"{"results":[{"secret":"missing-output-secret"}]}"#),
        ),
    ];
    for (secret, response_body) in fixtures {
        let (origin, requests, origin_server) =
            start_search_origin(StatusCode::OK, response_body, false).await;
        let (url, bridge_server) = start_bridge(managed_auth(), origin, None).await;
        let response = Client::new()
            .post(url.replace(ENDPOINT, WEB_SEARCH_ENDPOINT))
            .json(&json!({"commands": {"search_query": [{"q": "malformed"}]}}))
            .send()
            .await
            .expect("malformed search response");
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let body = response
            .json::<Value>()
            .await
            .expect("malformed error JSON");
        assert_eq!(body["error"]["type"], "server_error");
        assert_eq!(body["error"]["message"], "web search operation failed");
        assert!(!body.to_string().contains(secret));
        assert_eq!(requests.lock().await.len(), 1);
        bridge_server.abort();
        origin_server.abort();
    }
}

#[tokio::test]
async fn stateless_web_search_retries_one_5xx_and_hides_error_body() {
    let secret = "upstream-5xx-secret";
    let fixture = Bytes::from(format!(r#"{{"error":"{secret}"}}"#));
    let (origin, requests, origin_server) = start_sequenced_search_origin(vec![
        (StatusCode::INTERNAL_SERVER_ERROR, fixture.clone()),
        (StatusCode::INTERNAL_SERVER_ERROR, fixture),
    ])
    .await;
    let (url, bridge_server) = start_bridge(managed_auth(), origin, None).await;
    let response = Client::new()
        .post(url.replace(ENDPOINT, WEB_SEARCH_ENDPOINT))
        .json(&json!({"commands": {"search_query": [{"q": "retry"}]}}))
        .send()
        .await
        .expect("5xx search response");
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let body = response.json::<Value>().await.expect("5xx error JSON");
    assert_eq!(body["error"]["type"], "server_error");
    assert_eq!(body["error"]["message"], "web search operation failed");
    assert!(!body.to_string().contains(secret));
    assert_eq!(requests.lock().await.len(), 2);
    bridge_server.abort();
    origin_server.abort();
}

#[tokio::test]
async fn stateless_web_search_rejects_oversized_requests_before_upstream_contact() {
    let (origin, requests, origin_server) = start_search_origin(
        StatusCode::OK,
        Bytes::from_static(br#"{"output":"unused"}"#),
        false,
    )
    .await;
    let (url, bridge_server) = start_bridge(managed_auth(), origin, None).await;
    let response = Client::new()
        .post(url.replace(ENDPOINT, WEB_SEARCH_ENDPOINT))
        .header("content-type", "application/json")
        .body(vec![b' '; 64 * 1024 + 1])
        .send()
        .await
        .expect("oversized request response");
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        response.json::<Value>().await.expect("oversized JSON")["error"]["type"],
        "invalid_request_error"
    );
    assert!(requests.lock().await.is_empty());
    bridge_server.abort();
    origin_server.abort();
}

#[tokio::test]
async fn relays_zstd_bytes_client_protocol_and_sse_without_parsing() {
    let (origin, requests, origin_server) =
        start_recording_origin(false, StatusCode::CREATED).await;
    let (url, bridge_server) = start_bridge(managed_auth(), origin, None).await;
    let opaque = br#"{"model":"arbitrary-client-model","input":[{"role":"developer","content":"keep all fields"},{"type":"compaction_trigger"}],"prompt_cache_key":"cache-client","previous_response_id":"response-client","additional_tools":[{"type":"computer"}],"client_metadata":{"opaque":"value"}}"#;
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
    let content_length = response.headers()["content-length"]
        .to_str()
        .expect("content length")
        .to_owned();
    let sse = response.bytes().await.expect("SSE bytes");
    assert_eq!(content_length, sse.len().to_string());
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
async fn adapts_luna_buffered_request_and_returns_a_buffered_response() {
    let (origin, requests, origin_server) = start_recording_origin(false, StatusCode::OK).await;
    let (url, bridge_server) = start_bridge(managed_auth(), origin, None).await;
    let request = json!({
        "model": "gpt-5.6-luna",
        "input": [
            {"role": "system", "content": "follow instructions"},
            {"role": "user", "content": "hello"}
        ],
        "reasoning": {"effort": "medium", "summary": "auto"},
        "text": {"format": {"type": "json_schema", "name": "result", "schema": {"type": "object"}}},
        "max_output_tokens": 37,
        "stream": false
    });
    let response = Client::new()
        .post(url.replace(ENDPOINT, BUFFERED_RESPONSES_ENDPOINT))
        .header("authorization", "Bearer peer-secret")
        .header("cookie", "peer-cookie=secret")
        .json(&request)
        .send()
        .await
        .expect("adapted response");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "application/json");
    assert_eq!(
        response.headers()["x-kepos-ignored-parameters"],
        "max_output_tokens"
    );
    assert_eq!(
        response.json::<Value>().await.expect("adapted JSON"),
        json!({
            "id": "normal-response",
            "status": "completed",
            "output": [{
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "relay answer"}]
            }],
            "usage": null
        })
    );

    let recorded = requests.lock().await.pop().expect("origin request");
    assert_eq!(
        serde_json::from_slice::<Value>(&recorded.body).expect("rewritten JSON"),
        json!({
            "model": "gpt-5.6-luna",
            "input": [
                {"role": "system", "content": "follow instructions"},
                {"role": "user", "content": "hello"}
            ],
            "reasoning": {"effort": "medium", "summary": "auto"},
            "text": {"format": {"type": "json_schema", "name": "result", "schema": {"type": "object"}}},
            "stream": true
        })
    );
    assert_eq!(recorded.headers["authorization"], "Bearer managed-stale");
    assert!(!recorded.headers.contains_key("cookie"));
    bridge_server.abort();
    origin_server.abort();
}

#[tokio::test]
async fn normalizes_spark_reasoning_without_leaving_empty_reasoning() {
    let (origin, requests, origin_server) = start_recording_origin(false, StatusCode::OK).await;
    let (url, bridge_server) = start_bridge(managed_auth(), origin, None).await;
    let client = Client::new();

    for request in [
        json!({
            "model": "gpt-5.3-codex-spark",
            "input": "summary only",
            "reasoning": {"summary": "auto"},
            "stream": false
        }),
        json!({
            "model": "gpt-5.3-codex-spark",
            "input": "effort and summary",
            "reasoning": {"effort": "medium", "summary": "auto"}
        }),
    ] {
        let response = client
            .post(url.replace(ENDPOINT, BUFFERED_RESPONSES_ENDPOINT))
            .json(&request)
            .send()
            .await
            .expect("Spark buffered response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], "application/json");
        assert!(
            !response
                .headers()
                .contains_key("x-kepos-ignored-parameters")
        );
    }

    let calls = requests.lock().await;
    assert_eq!(calls.len(), 2);
    let first: Value = serde_json::from_slice(&calls[0].body).expect("first Spark request JSON");
    assert_eq!(first["model"], "gpt-5.3-codex-spark");
    assert_eq!(first["input"], "summary only");
    assert_eq!(first["stream"], true);
    assert!(first.get("reasoning").is_none());
    let second: Value = serde_json::from_slice(&calls[1].body).expect("second Spark request JSON");
    assert_eq!(second["reasoning"], json!({"effort": "medium"}));
    assert_eq!(second["stream"], true);
    bridge_server.abort();
    origin_server.abort();
}

#[tokio::test]
async fn rejects_tools_continuation_and_caller_streaming_before_forwarding() {
    let (origin, requests, origin_server) = start_recording_origin(false, StatusCode::OK).await;
    let (url, bridge_server) = start_bridge(managed_auth(), origin, None).await;
    let client = Client::new();
    for request in [
        json!({
            "model": "gpt-5.6-sol",
            "input": "tool request",
            "tools": [{"type": "function", "name": "lookup"}]
        }),
        json!({
            "model": "gpt-5.6-sol",
            "input": "continuation request",
            "previous_response_id": "resp-1"
        }),
        json!({
            "model": "gpt-5.6-sol",
            "input": "streaming request",
            "stream": true
        }),
    ] {
        let response = client
            .post(url.replace(ENDPOINT, BUFFERED_RESPONSES_ENDPOINT))
            .json(&request)
            .send()
            .await
            .expect("invalid buffered response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response.text().await.expect("invalid request body"),
            "invalid buffered Responses request"
        );
    }
    assert!(requests.lock().await.is_empty());
    bridge_server.abort();
    origin_server.abort();
}

#[tokio::test]
async fn aggregates_split_buffered_sse_output_items_in_order() {
    let sse = concat!(
        "event: response.created\r\n",
        "data: {\"type\":\"response.created\"}\r\n\r\n",
        "event: opaque\r\n",
        "data: response.completed\r\n\r\n",
        "event: response.output_item.done\r\n",
        "data: {\"type\":\"response.output_item.done\",\"output_index\":2,\"item\":{\"type\":\"function_call\",\"call_id\":\"call-1\",\"name\":\"lookup\",\"arguments\":\"{\\\"city\\\":\\\"Taipei\\\"}\"}}\r\n\r\n",
        "event: response.output_item.done\r\n",
        "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg-text\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"hello\"}]}}\r\n\r\n",
        "event: response.output_item.done\r\n",
        "data: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":{\"type\":\"message\",\"id\":\"msg-json\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"{\\\"answer\\\":42}\"}]}}\r\n\r\n",
        "event: response.completed\r\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"response-1\",\"object\":\"response\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":12,\"output_tokens\":5},\"tools\":[{\"type\":\"function\",\"name\":\"lookup\"}]}}\r\n\r\n",
        "data: [DONE]\r\n\r\n"
    );
    let upstream_content_length = sse.len().to_string();
    let chunks = sse
        .as_bytes()
        .chunks(17)
        .map(Bytes::copy_from_slice)
        .collect();
    let (origin, _, origin_server) = start_buffered_origin(StatusCode::OK, chunks).await;
    let (url, bridge_server) = start_bridge(managed_auth(), origin, None).await;
    let response = Client::new()
        .post(url.replace(ENDPOINT, BUFFERED_RESPONSES_ENDPOINT))
        .json(&json!({"model": "gpt-5.4", "input": "hello", "stream": false}))
        .send()
        .await
        .expect("aggregated response");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "application/json");
    assert_eq!(response.headers()["x-codex-turn-state"], "upstream-turn");
    assert_ne!(
        response.headers()["content-length"],
        upstream_content_length
    );
    assert!(!response.headers().contains_key("set-cookie"));
    assert!(
        !response
            .headers()
            .contains_key("x-kepos-ignored-parameters")
    );
    assert_eq!(
        response.json::<Value>().await.expect("aggregated JSON"),
        json!({
            "id": "response-1",
            "object": "response",
            "status": "completed",
            "output": [
                {"type": "message", "id": "msg-text", "role": "assistant", "content": [{"type": "output_text", "text": "hello"}]},
                {"type": "message", "id": "msg-json", "role": "assistant", "content": [{"type": "output_text", "text": "{\"answer\":42}"}]},
                {"type": "function_call", "call_id": "call-1", "name": "lookup", "arguments": "{\"city\":\"Taipei\"}"}
            ],
            "usage": {"input_tokens": 12, "output_tokens": 5},
            "tools": [{"type": "function", "name": "lookup"}]
        })
    );
    bridge_server.abort();
    origin_server.abort();
}

#[tokio::test]
async fn preserves_incomplete_and_failed_terminal_responses() {
    for (event, response) in [
        (
            "response.incomplete",
            json!({
                "id": "incomplete-1",
                "status": "incomplete",
                "incomplete_details": {"reason": "max_output_tokens"},
                "output": [{"type": "message", "id": "partial"}],
                "usage": {"input_tokens": 2, "output_tokens": 1}
            }),
        ),
        (
            "response.failed",
            json!({
                "id": "failed-1",
                "status": "failed",
                "error": {"code": "server_error", "message": "upstream failed"},
                "output": [],
                "usage": null
            }),
        ),
    ] {
        let body =
            format!("event: {event}\ndata: {{\"type\":\"{event}\",\"response\":{response}}}\n\n");
        let (origin, _, origin_server) =
            start_buffered_origin(StatusCode::OK, vec![Bytes::from(body)]).await;
        let (url, bridge_server) = start_bridge(managed_auth(), origin, None).await;
        let received = Client::new()
            .post(url.replace(ENDPOINT, BUFFERED_RESPONSES_ENDPOINT))
            .json(&json!({"model": "gpt-5.4", "input": "hello"}))
            .send()
            .await
            .expect("terminal response");
        assert_eq!(received.status(), StatusCode::OK);
        assert_eq!(
            received.json::<Value>().await.expect("terminal JSON"),
            response
        );
        bridge_server.abort();
        origin_server.abort();
    }
}

#[tokio::test]
async fn preserves_safe_non_successful_buffered_upstream_responses() {
    let (origin, _, origin_server) = start_buffered_origin(
        StatusCode::UNPROCESSABLE_ENTITY,
        vec![Bytes::from_static(b"upstream validation error")],
    )
    .await;
    let (url, bridge_server) = start_bridge(managed_auth(), origin, None).await;
    let response = Client::new()
        .post(url.replace(ENDPOINT, BUFFERED_RESPONSES_ENDPOINT))
        .json(&json!({
            "model": "gpt-5.4",
            "input": "hello",
            "max_output_tokens": 37
        }))
        .send()
        .await
        .expect("upstream failure response");

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(response.headers()["x-codex-turn-state"], "upstream-turn");
    assert_eq!(
        response.headers()["x-kepos-ignored-parameters"],
        "max_output_tokens"
    );
    assert!(!response.headers().contains_key("set-cookie"));
    assert_eq!(
        response.text().await.expect("upstream failure body"),
        "upstream validation error"
    );
    bridge_server.abort();
    origin_server.abort();
}

#[tokio::test]
async fn retries_managed_auth_once_for_buffered_requests() {
    let (origin, requests, origin_server) = start_recording_origin(true, StatusCode::OK).await;
    let (url, bridge_server) = start_bridge(managed_auth(), origin, None).await;
    let response = Client::new()
        .post(url.replace(ENDPOINT, BUFFERED_RESPONSES_ENDPOINT))
        .header("authorization", "Bearer peer-secret")
        .json(&json!({"model": "gpt-5.4", "input": "hello"}))
        .send()
        .await
        .expect("retried adapted response");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.json::<Value>().await.expect("adapted JSON")["status"],
        "completed"
    );
    let calls = requests.lock().await;
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].headers["authorization"], "Bearer managed-stale");
    assert_eq!(calls[1].headers["authorization"], "Bearer managed-fresh");
    bridge_server.abort();
    origin_server.abort();
}

#[tokio::test]
async fn keeps_the_existing_request_limit_for_buffered_requests() {
    let (origin, requests, origin_server) = start_recording_origin(false, StatusCode::OK).await;
    let (url, bridge_server) = start_bridge(managed_auth(), origin, None).await;
    let response = Client::new()
        .post(url.replace(ENDPOINT, BUFFERED_RESPONSES_ENDPOINT))
        .json(&json!({"model": "gpt-5.4", "input": "x".repeat(4 * 1024 * 1024)}))
        .send()
        .await
        .expect("limited response");

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert!(requests.lock().await.is_empty());
    bridge_server.abort();
    origin_server.abort();
}

#[tokio::test]
async fn rejects_malformed_oversized_and_unterminated_buffered_streams() {
    let failures = [
        vec![Bytes::from_static(
            b"event: response.completed\ndata: {\"type\":\"response.completed\"\n\n",
        )],
        vec![Bytes::from_static(
            b"event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\"\n\n",
        )],
        vec![Bytes::from_static(
            b"event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\"}}\n\n",
        )],
        vec![Bytes::from(vec![b'x'; 4 * 1024 * 1024 + 1])],
    ];
    for chunks in failures {
        let (origin, _, origin_server) = start_buffered_origin(StatusCode::OK, chunks).await;
        let (url, bridge_server) = start_bridge(managed_auth(), origin, None).await;
        let response = Client::new()
            .post(url.replace(ENDPOINT, BUFFERED_RESPONSES_ENDPOINT))
            .json(&json!({"model": "gpt-5.4", "input": "do not expose this"}))
            .send()
            .await
            .expect("adaptation failure response");
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let body = response.text().await.expect("generic failure body");
        assert!(body.contains("upstream response adaptation failed"));
        assert!(!body.contains("do not expose this"));
        bridge_server.abort();
        origin_server.abort();
    }
}

#[tokio::test]
async fn relays_redirect_response_without_following_it() {
    let (origin, requests, origin_server) = start_recording_origin(false, StatusCode::FOUND).await;
    let (url, bridge_server) = start_bridge(managed_auth(), origin, None).await;
    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("non-redirecting client");
    let response = client
        .post(&url)
        .body("opaque request")
        .send()
        .await
        .expect("relay response");
    assert_eq!(response.status(), StatusCode::FOUND);
    assert_eq!(response.headers()["location"], "/redirected");
    assert!(!response.bytes().await.expect("redirect bytes").is_empty());
    assert_eq!(requests.lock().await.len(), 1);
    bridge_server.abort();
    origin_server.abort();
}

#[tokio::test]
async fn unsupported_route_remains_absent() {
    let (origin, requests, origin_server) = start_recording_origin(false, StatusCode::OK).await;
    let (url, bridge_server) = start_bridge(managed_auth(), origin, None).await;
    let client = Client::new();
    for path in ["/codex/unsupported", "/hindsight/responses"] {
        let response = client
            .post(url.replace(ENDPOINT, path))
            .send()
            .await
            .expect("unsupported response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
    assert!(requests.lock().await.is_empty());
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
