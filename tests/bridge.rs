use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use kepos_codex_bridge::{Bridge, ENDPOINT};
use nanocodex_oai_api::{
    Model, OpenAi, ResponseError, ResponseEvent,
    responses::{ContentItem, MessageRole, ResponseItem},
    tower::{GenerationOutput, ResponsePipelineStats, ResponsesOutput, ResponsesServiceResponse},
};
use reqwest::Client;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tower::service_fn;

async fn start_bridge(
    calls: Arc<AtomicUsize>,
    cancelled: Arc<AtomicBool>,
    wait: bool,
) -> (String, tokio::task::JoinHandle<()>) {
    start_bridge_mode(calls, cancelled, wait, false, None, None).await
}

async fn start_bridge_mode(
    calls: Arc<AtomicUsize>,
    cancelled: Arc<AtomicBool>,
    wait: bool,
    function_round: bool,
    image_seen: Option<Arc<AtomicBool>>,
    output_seen: Option<Arc<AtomicBool>>,
) -> (String, tokio::task::JoinHandle<()>) {
    let openai = OpenAi::builder("dummy-client-key")
        .model(Model::Luna)
        .service(move || {
            let calls = Arc::clone(&calls);
            let cancelled = Arc::clone(&cancelled);
            let image_seen = image_seen.clone();
            let output_seen = output_seen.clone();
            service_fn(move |attempt: nanocodex_oai_api::tower::ResponsesAttempt| {
                let calls = Arc::clone(&calls);
                let cancelled = Arc::clone(&cancelled);
                let image_seen = image_seen.clone();
                let output_seen = output_seen.clone();
                async move {
                    let call_number = calls.fetch_add(1, Ordering::SeqCst);
                    let inputs = attempt.input_items().collect::<Vec<_>>();
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
                    let item = if function_round && call_number == 0 {
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
                    };
                    attempt
                        .emit(ResponseEvent::OutputItemAdded(item.clone()))
                        .await;
                    attempt
                        .emit(ResponseEvent::OutputTextDelta("bridge text".to_owned()))
                        .await;
                    attempt
                        .emit(ResponseEvent::OutputItemDone(item.clone()))
                        .await;
                    Ok::<_, ResponseError>(ResponsesServiceResponse::new(
                        ResponsesOutput::Generation(GenerationOutput {
                            id: "upstream-response".to_owned(),
                            status: "completed".to_owned(),
                            end_turn: Some(true),
                            final_message: Some("bridge text".to_owned()),
                            output_items: vec![item],
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
    let bridge = Bridge::new(openai, Model::Luna, "test instruction").expect("bridge config");
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
                "input": "hello"
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
async fn unsupported_values_are_framed_and_disconnect_cancels_upstream() {
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

    let ws_url = url.replacen("http", "ws", 1);
    let (mut socket, _) = connect_async(ws_url).await.expect("WebSocket connection");
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
    socket.close(None).await.expect("close socket");
    for _ in 0..50 {
        if cancelled.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(cancelled.load(Ordering::SeqCst));
    server.abort();
}

#[tokio::test]
async fn image_and_function_output_continuation_remain_typed() {
    let calls = Arc::new(AtomicUsize::new(0));
    let cancelled = Arc::new(AtomicBool::new(false));
    let image_seen = Arc::new(AtomicBool::new(false));
    let output_seen = Arc::new(AtomicBool::new(false));
    let (url, server) = start_bridge_mode(
        calls.clone(),
        cancelled,
        false,
        true,
        Some(image_seen.clone()),
        Some(output_seen.clone()),
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
                "tools": [{"type":"function","name":"lookup","description":"look up weather","parameters":{"type":"object"}}]
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
                "input": [
                    {"type":"message","role":"user","content":[
                        {"type":"input_text","text":"Find Paris weather"},
                        {"type":"input_image","image_url":"data:image/png;base64,AAAA"}
                    ]},
                    {"type":"function_call","id":"fc-test","name":"lookup","arguments": r#"{"city":"Paris"}"#,"call_id":"call-test"},
                    {"type":"function_call_output","call_id":"call-test","output":"sunny"}
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
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    server.abort();
}
