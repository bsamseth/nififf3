#![cfg(feature = "axum")]

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use axum::routing::post;
use http_body_util::BodyExt;
use nififf3::{FlowFile, FlowFileRequest, MEDIA_TYPE, StrictFlowFileRequest};
use tower::ServiceExt;

async fn echo(
    flow_file: FlowFileRequest,
) -> Result<impl axum::response::IntoResponse, nififf3::Error> {
    let flow_file = flow_file.into_bytes_async().await?;
    Ok(flow_file.into_reader())
}

async fn strict_echo(
    flow_file: StrictFlowFileRequest,
) -> Result<impl axum::response::IntoResponse, nififf3::Error> {
    let flow_file = flow_file.into_inner().into_bytes_async().await?;
    Ok(flow_file.into_reader())
}

fn app() -> Router {
    Router::new()
        .route("/echo", post(echo))
        .route("/strict", post(strict_echo))
}

fn sample_bytes() -> Vec<u8> {
    FlowFile::builder()
        .attribute("filename", "greeting.txt")
        .content(&b"hello"[..])
        .to_bytes()
}

#[tokio::test]
async fn extracts_and_responds_with_flow_files() {
    let bytes = sample_bytes();
    let response = app()
        .oneshot(
            Request::post("/echo")
                .body(Body::from(bytes.clone()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_TYPE], MEDIA_TYPE);
    assert_eq!(
        response.headers()[header::CONTENT_LENGTH],
        bytes.len().to_string().as_str()
    );
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.as_ref(), bytes.as_slice());
}

#[tokio::test]
async fn extractor_streams_chunked_bodies() {
    let bytes = sample_bytes();
    let chunks: Vec<Result<Vec<u8>, std::io::Error>> =
        bytes.chunks(3).map(|c| Ok(c.to_vec())).collect();
    let body = Body::from_stream(IterStream(chunks.into()));

    let response = app()
        .oneshot(Request::post("/echo").body(body).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.as_ref(), bytes.as_slice());
}

// Minimal stream over a queue of items, avoiding a futures-util dependency.
struct IterStream<T>(std::collections::VecDeque<T>);

impl<T: Unpin> futures_core::Stream for IterStream<T> {
    type Item = T;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<T>> {
        std::task::Poll::Ready(self.0.pop_front())
    }
}

#[tokio::test]
async fn strict_extractor_accepts_matching_content_type() {
    for content_type in [MEDIA_TYPE, "Application/FlowFile-V3; charset=utf-8"] {
        let bytes = sample_bytes();
        let response = app()
            .oneshot(
                Request::post("/strict")
                    .header(header::CONTENT_TYPE, content_type)
                    .body(Body::from(bytes.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.as_ref(), bytes.as_slice());
    }
}

#[tokio::test]
async fn strict_extractor_rejects_missing_or_wrong_content_type() {
    for content_type in [None, Some("application/json")] {
        let mut request = Request::post("/strict");
        if let Some(content_type) = content_type {
            request = request.header(header::CONTENT_TYPE, content_type);
        }
        let response = app()
            .oneshot(request.body(Body::from(sample_bytes())).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }
}

#[tokio::test]
async fn strict_extractor_rejects_invalid_body_with_400() {
    let response = app()
        .oneshot(
            Request::post("/strict")
                .header(header::CONTENT_TYPE, MEDIA_TYPE)
                .body(Body::from("not a flow file"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn invalid_body_is_rejected_with_400() {
    let response = app()
        .oneshot(
            Request::post("/echo")
                .body(Body::from("not a flow file"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert!(
        std::str::from_utf8(&body)
            .unwrap()
            .contains("invalid magic")
    );
}
