#![cfg(feature = "axum")]

use axum::Router;
use axum::body::Body;
use axum::extract::DefaultBodyLimit;
use axum::http::{Request, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::post;
use http_body_util::BodyExt;
use nififf3::{FlowFile, FlowFileRequest, Limits, MEDIA_TYPE, StrictFlowFileRequest};
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
async fn extractor_applies_default_header_limits() {
    let flow_file = FlowFile::builder()
        .attributes((0..5000).map(|i| (format!("k{i}"), "v")))
        .content(Vec::new());
    let response = app()
        .oneshot(
            Request::post("/echo")
                .body(Body::from(flow_file.to_bytes()))
                .unwrap(),
        )
        .await
        .unwrap();
    // Well-formed but over the limit, so a size complaint rather than a 400.
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert!(
        std::str::from_utf8(&body)
            .unwrap()
            .contains("attribute count")
    );
}

#[tokio::test]
async fn extractor_honours_the_default_body_limit() {
    // 64 bytes is past the header but well short of the content, so the limit
    // trips while the content streams; 8 trips while the header is still
    // being read. Both are the body being too large, not malformed.
    for limit in [8, 64] {
        let app = app().layer(DefaultBodyLimit::max(limit));
        let bytes = FlowFile::builder().content(vec![0u8; 100_000]).to_bytes();

        let response = app
            .oneshot(Request::post("/echo").body(Body::from(bytes)).unwrap())
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "a 100 KB body against a {limit} byte limit"
        );
    }
}

#[tokio::test]
async fn a_malformed_body_is_still_a_400_under_a_body_limit() {
    // The limit must not swallow the distinction: this body is small enough
    // to pass it and is simply not a flow file.
    let app = app().layer(DefaultBodyLimit::max(1024));
    let response = app
        .oneshot(
            Request::post("/echo")
                .body(Body::from("not a flow file"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_disabled_body_limit_lets_large_flow_files_through() {
    let app = app().layer(DefaultBodyLimit::disable());
    // Over axum's 2 MiB default, so this only passes because it is disabled.
    let bytes = FlowFile::builder().content(vec![7u8; 3 << 20]).to_bytes();

    let response = app
        .oneshot(
            Request::post("/echo")
                .body(Body::from(bytes.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.len(), bytes.len());
}

#[tokio::test]
async fn a_declared_content_size_over_the_limit_is_rejected_before_the_content() {
    // The header claims 5 bytes; the limit allows 4. Nothing is read past it.
    let bytes = FlowFile::builder().content(&b"hello"[..]).to_bytes();
    let limits = Limits::default().max_content_len(4);

    let err = FlowFile::parse_with_limits(bytes.as_slice(), limits).unwrap_err();
    assert!(matches!(
        err,
        nififf3::Error::ContentTooLarge { size: 5, limit: 4 }
    ));
    assert_eq!(
        err.into_response().status(),
        StatusCode::PAYLOAD_TOO_LARGE,
        "a well-formed header asking for too much is a size problem"
    );
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
