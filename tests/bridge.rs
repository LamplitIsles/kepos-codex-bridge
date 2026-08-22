use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use kepos_codex_bridge::{Bridge, ENDPOINT, IMAGE_ENDPOINT};
use nanocodex_oai_api::{
    Model, OpenAi, ResponseError, ResponseEvent,
    auth::{
        OpenAiAuth, OpenAiAuthError, OpenAiAuthFuture, OpenAiAuthMode, OpenAiAuthSnapshot,
        OpenAiAuthSource,
    },
    responses::{ContentItem, MessageRole, ResponseItem},
    tower::{GenerationOutput, ResponsePipelineStats, ResponsesOutput, ResponsesServiceResponse},
};
use reqwest::Client;
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::Mutex};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tower::service_fn;

struct TestManagedAuth;

impl OpenAiAuthSource for TestManagedAuth {
    fn validate(&self) -> Result<(), OpenAiAuthError> {
        Ok(())
    }

    fn snapshot(&self) -> OpenAiAuthFuture<'_, Result<OpenAiAuthSnapshot, OpenAiAuthError>> {
        Box::pin(async {
            Ok(OpenAiAuthSnapshot::new(
                OpenAiAuthMode::ChatGpt,
                "managed-bearer",
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
        Box::pin(async { Err(OpenAiAuthError::LoginRequired("test auth".into())) })
    }
}

#[derive(Clone)]
struct ImageUpstreamState {
    calls: Arc<Mutex<Vec<(String, HeaderMap, Value)>>>,
    fail: bool,
}

async fn image_upstream(
    State(state): State<ImageUpstreamState>,
    uri: axum::http::Uri,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    let value = serde_json::from_slice(&body).expect("image upstream JSON");
    state
        .calls
        .lock()
        .await
        .push((uri.path().to_owned(), headers, value));
    if state.fail {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    axum::Json(json!({
        "created": 1,
        "data": [{"b64_json": "AAAA"}]
    }))
    .into_response()
}

async fn start_image_upstream(
    fail: bool,
) -> (
    String,
    Arc<Mutex<Vec<(String, HeaderMap, Value)>>>,
    tokio::task::JoinHandle<()>,
) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let state = ImageUpstreamState {
        calls: calls.clone(),
        fail,
    };
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("image upstream listener");
    let address = listener.local_addr().expect("image upstream address");
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new()
                .route("/images/generations", axum::routing::post(image_upstream))
                .route("/images/edits", axum::routing::post(image_upstream))
                .with_state(state),
        )
        .await
        .expect("image upstream server");
    });
    (format!("http://{address}"), calls, task)
}

async fn start_bridge(
    calls: Arc<AtomicUsize>,
    cancelled: Arc<AtomicBool>,
    wait: bool,
) -> (String, tokio::task::JoinHandle<()>) {
    start_bridge_mode(calls, cancelled, wait, false, false, None, None, None).await
}

#[allow(clippy::too_many_arguments)]
async fn start_bridge_mode(
    calls: Arc<AtomicUsize>,
    cancelled: Arc<AtomicBool>,
    wait: bool,
    function_round: bool,
    multi_output: bool,
    image_seen: Option<Arc<AtomicBool>>,
    output_seen: Option<Arc<AtomicBool>>,
    output_image_seen: Option<Arc<AtomicBool>>,
) -> (String, tokio::task::JoinHandle<()>) {
    start_bridge_mode_with_image(
        calls,
        cancelled,
        wait,
        function_round,
        multi_output,
        image_seen,
        output_seen,
        output_image_seen,
        None,
        None,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn start_bridge_mode_with_image(
    calls: Arc<AtomicUsize>,
    cancelled: Arc<AtomicBool>,
    wait: bool,
    function_round: bool,
    multi_output: bool,
    image_seen: Option<Arc<AtomicBool>>,
    output_seen: Option<Arc<AtomicBool>>,
    output_image_seen: Option<Arc<AtomicBool>>,
    image_auth: Option<OpenAiAuth>,
    image_api_base_url: Option<String>,
    request_inputs: Option<Arc<Mutex<Vec<Vec<ResponseItem>>>>>,
) -> (String, tokio::task::JoinHandle<()>) {
    let image_auth = image_auth.unwrap_or_else(|| OpenAiAuth::api_key("dummy-client-key"));
    let openai = OpenAi::builder(image_auth.clone())
        .model(Model::Luna)
        .service(move || {
            let calls = Arc::clone(&calls);
            let cancelled = Arc::clone(&cancelled);
            let image_seen = image_seen.clone();
            let output_seen = output_seen.clone();
            let output_image_seen = output_image_seen.clone();
            let request_inputs = request_inputs.clone();
            service_fn(move |attempt: nanocodex_oai_api::tower::ResponsesAttempt| {
                let calls = Arc::clone(&calls);
                let cancelled = Arc::clone(&cancelled);
                let image_seen = image_seen.clone();
                let output_seen = output_seen.clone();
                let output_image_seen = output_image_seen.clone();
                let request_inputs = request_inputs.clone();
                async move {
                    let call_number = calls.fetch_add(1, Ordering::SeqCst);
                    let inputs = attempt.input_items().cloned().collect::<Vec<_>>();
                    if let Some(request_inputs) = request_inputs {
                        request_inputs.lock().await.push(inputs.clone());
                    }
                    let encoded_inputs = serde_json::to_string(&inputs).expect("test input JSON");
                    if encoded_inputs.contains("data:image/png") {
                        if let Some(seen) = image_seen.as_ref() {
                            seen.store(true, Ordering::SeqCst);
                        }
                    }
                    if encoded_inputs.contains("function_call_output") {
                        if let Some(seen) = output_seen.as_ref() {
                            seen.store(true, Ordering::SeqCst);
                        }
                        if encoded_inputs.contains("data:image/png")
                            && let Some(seen) = output_image_seen.as_ref()
                        {
                            seen.store(true, Ordering::SeqCst);
                        }
                    }
                    attempt.emit(ResponseEvent::Created).await;
                    if wait {
                        struct CancelGuard(Arc<AtomicBool>);
                        impl Drop for CancelGuard {
                            fn drop(&mut self) {
                                self.0.store(true, Ordering::SeqCst);
                            }
                        }
                        let _guard = CancelGuard(cancelled);
                        tokio::time::sleep(Duration::from_secs(30)).await;
                    }
                    let items = if multi_output {
                        vec![
                            (
                                ResponseItem::message(
                                    MessageRole::Assistant,
                                    [ContentItem::output_text("first output")],
                                ),
                                "first output",
                            ),
                            (
                                ResponseItem::message(
                                    MessageRole::Assistant,
                                    [ContentItem::output_text("second output")],
                                ),
                                "second output",
                            ),
                        ]
                    } else {
                        vec![(
                            if function_round && call_number == 0 {
                                ResponseItem::FunctionCall {
                                    id: Some("fc-test".into()),
                                    name: "lookup".into(),
                                    namespace: None,
                                    arguments: r#"{"city":"Paris"}"#.into(),
                                    encrypted_function_args: None,
                                    call_id: "call-test".into(),
                                    caller: None,
                                    status: None,
                                    created_by: None,
                                    internal_chat_message_metadata_passthrough: None,
                                }
                            } else {
                                ResponseItem::message(
                                    MessageRole::Assistant,
                                    [ContentItem::output_text("bridge text")],
                                )
                            },
                            "bridge text",
                        )]
                    };
                    for (item, delta) in &items {
                        attempt
                            .emit(ResponseEvent::OutputItemAdded(item.clone()))
                            .await;
                        attempt
                            .emit(ResponseEvent::OutputTextDelta((*delta).to_owned()))
                            .await;
                        attempt
                            .emit(ResponseEvent::OutputItemDone(item.clone()))
                            .await;
                    }
                    Ok::<_, ResponseError>(ResponsesServiceResponse::new(
                        ResponsesOutput::Generation(GenerationOutput {
                            id: "upstream-response".to_owned(),
                            status: "completed".to_owned(),
                            end_turn: Some(true),
                            final_message: Some("bridge text".to_owned()),
                            output_items: items.into_iter().map(|(item, _)| item).collect(),
                            code_calls: Vec::new(),
                            usage: None,
                            time_to_first_event_ns: 0,
                            time_to_first_output_ns: Some(0),
                            pipeline_stats: ResponsePipelineStats::default(),
                        }),
                    ))
                }
            })
        })
        .build()
        .expect("test client configuration");
    let bridge =
        Bridge::new(openai, image_auth, Model::Luna, "test instruction").expect("bridge config");
    let bridge = if let Some(base_url) = image_api_base_url {
        bridge.with_image_api_base_url(base_url)
    } else {
        bridge
    };
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("test listener");
    let address = listener.local_addr().expect("test address");
    let task = tokio::spawn(async move {
        axum::serve(listener, bridge.router())
            .await
            .expect("test server");
    });
    (format!("http://{address}{ENDPOINT}"), task)
}

fn request() -> Value {
    json!({
        "model": "gpt-5.6-luna",
        "input": "hello",
        "stream": true,
        "api_key": "dummy-key-must-not-echo"
    })
}

async fn wait_until_cancelled(cancelled: &AtomicBool) {
    for _ in 0..50 {
        if cancelled.load(Ordering::SeqCst) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(cancelled.load(Ordering::SeqCst));
}

#[tokio::test]
async fn image_generation_uses_fixed_contract_and_managed_header_shape() {
    let (upstream, image_calls, upstream_server) = start_image_upstream(false).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let cancelled = Arc::new(AtomicBool::new(false));
    let (responses_url, bridge_server) = start_bridge_mode_with_image(
        calls,
        cancelled,
        false,
        false,
        false,
        None,
        None,
        None,
        Some(OpenAiAuth::managed_chatgpt(Arc::new(TestManagedAuth))),
        Some(upstream),
        None,
    )
    .await;
    let response = Client::new()
        .post(responses_url.replace(ENDPOINT, IMAGE_ENDPOINT))
        .header("content-type", "application/json")
        .json(&json!({
            "prompt": "draw a secret sentinel",
            "api_key": "peer-compatibility-key"
        }))
        .send()
        .await
        .expect("image response");
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.json::<Value>().await.expect("image JSON"),
        json!({"image_url": "data:image/png;base64,AAAA"})
    );
    let calls = image_calls.lock().await;
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, "/images/generations");
    assert_eq!(
        calls[0].2,
        json!({
            "prompt": "draw a secret sentinel",
            "background": "auto",
            "model": "gpt-image-2",
            "quality": "auto",
            "size": "auto"
        })
    );
    assert_eq!(calls[0].1["authorization"], "Bearer managed-bearer");
    assert_eq!(calls[0].1["chatgpt-account-id"], "managed-account");
    assert_eq!(calls[0].1["x-openai-fedramp"], "true");
    assert!(
        !serde_json::to_string(&calls[0].2)
            .expect("upstream body JSON")
            .contains("peer-compatibility-key")
    );
    bridge_server.abort();
    upstream_server.abort();
}

#[tokio::test]
async fn image_edit_accepts_five_data_images_and_rejects_invalid_inputs() {
    let (upstream, image_calls, upstream_server) = start_image_upstream(false).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let cancelled = Arc::new(AtomicBool::new(false));
    let (responses_url, bridge_server) = start_bridge_mode_with_image(
        calls,
        cancelled,
        false,
        false,
        false,
        None,
        None,
        None,
        None,
        Some(upstream),
        None,
    )
    .await;
    let image_url = responses_url.replace(ENDPOINT, IMAGE_ENDPOINT);
    let images = (0..5)
        .map(|index| format!("data:image/png;base64,IMG{index}"))
        .collect::<Vec<_>>();
    let response = Client::new()
        .post(&image_url)
        .json(&json!({"prompt": "edit these", "images": images}))
        .send()
        .await
        .expect("image edit response");
    assert_eq!(response.status(), 200);
    {
        let upstream_calls = image_calls.lock().await;
        let call = &upstream_calls[0];
        assert_eq!(call.0, "/images/edits");
        assert_eq!(
            call.2["images"].as_array().expect("wrapped images").len(),
            5
        );
        assert_eq!(
            call.2["images"][0]["image_url"],
            "data:image/png;base64,IMG0"
        );
    }

    for request in [
        json!({"prompt": "six", "images": ["data:image/png;base64,A", "data:image/png;base64,B", "data:image/png;base64,C", "data:image/png;base64,D", "data:image/png;base64,E", "data:image/png;base64,F"]}),
        json!({"prompt": "remote", "images": ["https://example.test/image.png"]}),
    ] {
        let response = Client::new()
            .post(&image_url)
            .json(&request)
            .send()
            .await
            .expect("invalid image response");
        assert_eq!(response.status(), 400);
        assert!(
            response
                .text()
                .await
                .expect("invalid image body")
                .contains("invalid_request_error")
        );
    }
    assert_eq!(image_calls.lock().await.len(), 1);
    bridge_server.abort();
    upstream_server.abort();
}

#[tokio::test]
async fn image_errors_are_generic_and_do_not_echo_request_data() {
    let (upstream, image_calls, upstream_server) = start_image_upstream(true).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let cancelled = Arc::new(AtomicBool::new(false));
    let (responses_url, bridge_server) = start_bridge_mode_with_image(
        calls,
        cancelled,
        false,
        false,
        false,
        None,
        None,
        None,
        None,
        Some(upstream),
        None,
    )
    .await;
    let image_url = responses_url.replace(ENDPOINT, IMAGE_ENDPOINT);
    let response = Client::new()
        .post(&image_url)
        .header("content-type", "application/json")
        .body(r#"{"prompt":"do-not-echo-this"}"#)
        .send()
        .await
        .expect("upstream failure response");
    assert_eq!(response.status(), 502);
    let body = response.text().await.expect("upstream failure body");
    assert!(body.contains("image operation failed"));
    assert!(!body.contains("do-not-echo-this"));
    assert_eq!(image_calls.lock().await.len(), 1);

    let malformed = Client::new()
        .post(&image_url)
        .header("content-type", "application/json")
        .body("not-json")
        .send()
        .await
        .expect("malformed image response");
    assert_eq!(malformed.status(), 400);
    assert!(
        malformed
            .text()
            .await
            .expect("malformed image body")
            .contains("invalid_request_error")
    );
    bridge_server.abort();
    upstream_server.abort();
}

#[tokio::test]
async fn http_sse_and_websocket_use_native_framing_and_no_v1_alias() {
    let calls = Arc::new(AtomicUsize::new(0));
    let cancelled = Arc::new(AtomicBool::new(false));
    let (url, server) = start_bridge(calls.clone(), cancelled, false).await;
    let client = Client::new();
    let response = client
        .post(&url)
        .json(&request())
        .send()
        .await
        .expect("HTTP response");
    assert_eq!(response.status(), 200);
    let body = response.text().await.expect("SSE body");
    assert!(body.contains("\"type\":\"response.created\""));
    assert!(body.contains("response.output_text.delta"));
    assert!(body.contains("response.completed"));
    assert!(body.contains("data: [DONE]"));
    assert!(!body.contains("dummy-key-must-not-echo"));

    let missing = client
        .post(format!("{url}/v1"))
        .json(&request())
        .send()
        .await
        .expect("alias response");
    assert_eq!(missing.status(), 404);

    let ws_url = url.replacen("http", "ws", 1);
    let (mut socket, _) = connect_async(ws_url).await.expect("WebSocket connection");
    socket
        .send(Message::Text(
            json!({
                "type": "response.create",
                "model": "gpt-5.6-luna",
                "input": [{
                    "role": "user",
                    "content": [{ "type": "input_text", "text": "hello" }]
                }]
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("request frame");
    let mut event_types = Vec::new();
    while let Some(Ok(Message::Text(text))) = socket.next().await {
        let value: Value = serde_json::from_str(&text).expect("native event JSON");
        event_types.push(value["type"].as_str().unwrap_or_default().to_owned());
        if value["type"] == "response.completed" {
            break;
        }
    }
    assert!(event_types.iter().any(|kind| kind == "response.created"));
    assert!(
        event_types
            .iter()
            .any(|kind| kind == "response.output_text.delta")
    );
    assert!(event_types.iter().any(|kind| kind == "response.completed"));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    server.abort();
}

#[tokio::test]
async fn full_context_fallback_rebuilds_websocket_session() {
    let calls = Arc::new(AtomicUsize::new(0));
    let cancelled = Arc::new(AtomicBool::new(false));
    let request_inputs = Arc::new(Mutex::new(Vec::new()));
    let (url, server) = start_bridge_mode_with_image(
        calls,
        cancelled,
        false,
        false,
        false,
        None,
        None,
        None,
        None,
        None,
        Some(request_inputs.clone()),
    )
    .await;
    let (mut socket, _) = connect_async(url.replacen("http", "ws", 1))
        .await
        .expect("WebSocket connection");
    socket
        .send(Message::Text(
            json!({
                "type": "response.create",
                "model": "gpt-5.6-luna",
                "input": "first turn"
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("first request");
    while let Some(Ok(Message::Text(text))) = socket.next().await {
        if text.contains("response.completed") {
            break;
        }
    }
    socket
        .send(Message::Text(
            json!({
                "type": "response.create",
                "model": "gpt-5.6-luna",
                "instructions": "new session instructions",
                "input": [
                    {"type":"message","role":"user","content":[{"type":"input_text","text":"first turn"}]},
                    {"type":"message","role":"assistant","content":[{"type":"output_text","text":"bridge text"}]},
                    {"type":"message","role":"user","content":[{"type":"input_text","text":"second turn"}]}
                ]
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("full-context fallback request");
    while let Some(Ok(Message::Text(text))) = socket.next().await {
        if text.contains("response.completed") {
            break;
        }
    }
    let inputs = request_inputs.lock().await;
    assert_eq!(inputs.len(), 2);
    assert_eq!(inputs[0].len(), 3);
    assert_eq!(inputs[1].len(), 5);
    assert!(
        serde_json::to_string(&inputs[1])
            .unwrap()
            .contains("new session instructions")
    );
    assert!(
        serde_json::to_string(&inputs[1])
            .unwrap()
            .contains("second turn")
    );
    server.abort();
}

#[tokio::test]
async fn native_output_items_keep_their_indices() {
    let calls = Arc::new(AtomicUsize::new(0));
    let cancelled = Arc::new(AtomicBool::new(false));
    let (url, server) = start_bridge_mode(
        calls.clone(),
        cancelled,
        false,
        false,
        true,
        None,
        None,
        None,
    )
    .await;
    let body = Client::new()
        .post(&url)
        .json(&request())
        .send()
        .await
        .expect("multi-output response")
        .text()
        .await
        .expect("multi-output SSE body");
    let events = body
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .collect::<Vec<_>>();
    for event_type in [
        "response.output_item.added",
        "response.output_text.delta",
        "response.output_item.done",
    ] {
        let indices = events
            .iter()
            .filter(|event| event["type"] == event_type)
            .map(|event| event["output_index"].as_u64())
            .collect::<Vec<_>>();
        assert_eq!(indices, [Some(0), Some(1)]);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    server.abort();
}

#[tokio::test]
async fn http_sse_accepts_zstd_and_limits_expanded_requests() {
    let calls = Arc::new(AtomicUsize::new(0));
    let cancelled = Arc::new(AtomicBool::new(false));
    let (url, server) = start_bridge(calls.clone(), cancelled, false).await;
    let client = Client::new();

    let body = serde_json::to_vec(&request()).expect("request JSON");
    let compressed = zstd::stream::encode_all(body.as_slice(), 3).expect("compressed request");
    let response = client
        .post(&url)
        .header("content-encoding", "zstd")
        .body(compressed)
        .send()
        .await
        .expect("zstd response");
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response
            .text()
            .await
            .expect("zstd SSE body")
            .contains("response.completed")
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let mut oversized = request();
    oversized["input"] = Value::String("x".repeat(4 * 1024 * 1024));
    let body = serde_json::to_vec(&oversized).expect("oversized request JSON");
    let compressed =
        zstd::stream::encode_all(body.as_slice(), 3).expect("compressed oversized request");
    let response = client
        .post(&url)
        .header("content-encoding", "zstd")
        .body(compressed)
        .send()
        .await
        .expect("oversized zstd response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(
        response
            .text()
            .await
            .expect("oversized error body")
            .contains("request body is too large")
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let response = client
        .post(&url)
        .header("content-encoding", "gzip")
        .body(serde_json::to_vec(&request()).expect("plain request JSON"))
        .send()
        .await
        .expect("unsupported encoding response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(
        response
            .text()
            .await
            .expect("unsupported encoding error body")
            .contains("unsupported content encoding")
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    server.abort();
}

#[tokio::test]
async fn invalid_input_and_downstream_cancellation_stop_upstream() {
    let calls = Arc::new(AtomicUsize::new(0));
    let cancelled = Arc::new(AtomicBool::new(false));
    let (url, server) = start_bridge(calls.clone(), cancelled.clone(), true).await;
    let client = Client::new();
    let response = client
        .post(&url)
        .json(&json!({
            "model": "gpt-5.6-luna",
            "input": "hello",
            "store": true,
            "api_key": "secret-sentinel"
        }))
        .send()
        .await
        .expect("invalid response");
    let body = response.text().await.expect("invalid SSE body");
    assert!(body.contains("invalid_request_error"));
    assert!(!body.contains("secret-sentinel"));

    let mut previous_request = request();
    previous_request["previous_response_id"] = Value::String("resp_missing".to_owned());
    let response = client
        .post(&url)
        .json(&previous_request)
        .send()
        .await
        .expect("previous-response rejection");
    assert_eq!(response.status(), 400);
    assert!(
        response
            .text()
            .await
            .expect("previous-response error body")
            .contains("previous_response_id is only supported on WebSocket")
    );

    let response = client
        .post(&url)
        .json(&json!({
            "model": "gpt-5.6-luna",
            "input": [{"type": "compaction_trigger"}]
        }))
        .send()
        .await
        .expect("rich-input rejection");
    assert_eq!(response.status(), 400);
    assert!(
        response
            .text()
            .await
            .expect("rich-input error body")
            .contains("input item is outside the supported subset")
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    let response = client
        .post(&url)
        .json(&request())
        .send()
        .await
        .expect("HTTP streaming response");
    assert_eq!(response.status(), 200);
    drop(response);
    wait_until_cancelled(&cancelled).await;

    cancelled.store(false, Ordering::SeqCst);
    let ws_url = url.replacen("http", "ws", 1);
    let (mut socket, _) = connect_async(&ws_url).await.expect("WebSocket connection");
    socket
        .send(Message::Text(
            json!({
                "type": "response.create",
                "model": "gpt-5.6-luna",
                "input": "cancel me"
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("request frame");
    let _ = socket.next().await;
    socket
        .send(Message::Text("{\"type\":\"response.cancel\"}".into()))
        .await
        .expect("cancel frame");
    wait_until_cancelled(&cancelled).await;

    cancelled.store(false, Ordering::SeqCst);
    socket
        .send(Message::Text(
            json!({
                "type": "response.create",
                "model": "gpt-5.6-luna",
                "input": "disconnect me"
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("fresh request after cancellation");
    let _ = socket.next().await;
    socket.close(None).await.expect("close socket");
    wait_until_cancelled(&cancelled).await;
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    server.abort();
}

#[tokio::test]
async fn image_and_function_output_continuation_remain_typed() {
    let calls = Arc::new(AtomicUsize::new(0));
    let cancelled = Arc::new(AtomicBool::new(false));
    let image_seen = Arc::new(AtomicBool::new(false));
    let output_seen = Arc::new(AtomicBool::new(false));
    let output_image_seen = Arc::new(AtomicBool::new(false));
    let (url, server) = start_bridge_mode(
        calls.clone(),
        cancelled,
        false,
        true,
        false,
        Some(image_seen.clone()),
        Some(output_seen.clone()),
        Some(output_image_seen.clone()),
    )
    .await;
    let (mut socket, _) = connect_async(url.replacen("http", "ws", 1))
        .await
        .expect("WebSocket connection");
    socket
        .send(Message::Text(
            json!({
                "type": "response.create",
                "model": "gpt-5.6-luna",
                "input": [{"type":"message","role":"user","content":[
                    {"type":"input_text","text":"Find Paris weather"},
                    {"type":"input_image","image_url":"data:image/png;base64,AAAA"}
                ]}],
                "tools": [{"type":"function","name":"lookup","description":"look up weather","strict":null,"parameters":{"type":"object"}}]
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("first request");
    let mut first_response_id = None;
    while let Some(Ok(Message::Text(text))) = socket.next().await {
        let value: Value = serde_json::from_str(&text).expect("first native event JSON");
        if value["type"] == "response.completed" {
            first_response_id = value["response"]["id"].as_str().map(str::to_owned);
            break;
        }
    }
    socket
        .send(Message::Text(
            json!({
                "type": "response.create",
                "model": "gpt-5.6-luna",
                "previous_response_id": first_response_id.expect("first response ID"),
                "input": [
                    {"type":"message","role":"user","content":[
                        {"type":"input_text","text":"Find Paris weather"},
                        {"type":"input_image","image_url":"data:image/png;base64,AAAA"}
                    ]},
                    {"type":"function_call","id":"fc-test","name":"lookup","arguments": r#"{"city":"Paris"}"#,"call_id":"call-test"},
                    {"type":"function_call_output","call_id":"call-test","output":[
                        {"type":"input_text","text":"sunny"},
                        {"type":"input_image","image_url":"data:image/png;base64,BBBB"}
                    ]}
                ],
                "tools": [{"type":"function","name":"lookup","description":"look up weather","parameters":{"type":"object"}}]
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("continuation request");
    let mut completed = false;
    while let Some(Ok(Message::Text(text))) = socket.next().await {
        if text.contains("response.completed") {
            completed = true;
            break;
        }
    }
    assert!(completed);
    assert!(image_seen.load(Ordering::SeqCst));
    assert!(output_seen.load(Ordering::SeqCst));
    assert!(output_image_seen.load(Ordering::SeqCst));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    server.abort();
}
