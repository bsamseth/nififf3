#![cfg(feature = "axum")]

use axum::Router;
use axum::body::Body;
use axum::extract::DefaultBodyLimit;
use axum::http::{Request, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::post;
use http_body_util::BodyExt;
use nififf3::{
    FlowFile, FlowFileRequest, FlowFilesRequest, Limits, MEDIA_TYPE, StrictFlowFileRequest,
    StrictFlowFilesRequest,
};
use tower::ServiceExt;

async fn echo(
    FlowFileRequest(flow_file): FlowFileRequest,
) -> Result<impl axum::response::IntoResponse, nififf3::Error> {
    let flow_file = flow_file.into_memory_async().await?;
    Ok(flow_file.into_reader())
}

async fn strict_echo(
    StrictFlowFileRequest(flow_file): StrictFlowFileRequest,
) -> Result<impl axum::response::IntoResponse, nififf3::Error> {
    let flow_file = flow_file.into_memory_async().await?;
    Ok(flow_file.into_reader())
}

/// The many-in case: NiFi's `PostHTTP` sends several flow files concatenated
/// under one request, which the single-flow-file extractor cannot accept.
async fn count_batch(
    FlowFilesRequest(mut flow_files): FlowFilesRequest,
) -> Result<String, nififf3::Error> {
    let mut sizes = Vec::new();
    while let Some(flow_file) = flow_files.next().await {
        sizes.push(flow_file?.size().to_string());
    }
    Ok(sizes.join(","))
}

async fn strict_count_batch(
    StrictFlowFilesRequest(mut flow_files): StrictFlowFilesRequest,
) -> Result<String, nififf3::Error> {
    let mut count = 0;
    while let Some(flow_file) = flow_files.next().await {
        flow_file?;
        count += 1;
    }
    Ok(count.to_string())
}

fn app() -> Router {
    Router::new()
        .route("/echo", post(echo))
        .route("/strict", post(strict_echo))
        .route("/batch", post(count_batch))
        .route("/strict-batch", post(strict_count_batch))
}

fn batch_bytes() -> Vec<u8> {
    let mut bytes = FlowFile::builder().content(&b"first"[..]).to_bytes();
    bytes.extend(FlowFile::builder().content(&b"second!"[..]).to_bytes());
    bytes.extend(FlowFile::builder().content(Vec::new()).to_bytes());
    bytes
}

async fn body_text(response: axum::response::Response) -> String {
    let body = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(body.to_vec()).unwrap()
}

#[tokio::test]
async fn the_batch_extractor_reads_every_flow_file_in_the_body() {
    let response = app()
        .oneshot(Request::post("/batch").body(Body::from(batch_bytes())).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_text(response).await, "5,7,0");
}

/// An empty body is a batch of none, not a failure — the same reading
/// `FlowFiles` gives a stream that ends immediately.
#[tokio::test]
async fn the_batch_extractor_accepts_an_empty_body() {
    let response = app()
        .oneshot(Request::post("/batch").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_text(response).await, "");
}

/// Nothing is read at extraction time, so a malformed body surfaces from
/// `next` — which the handler turns into the same 400 as anywhere else.
#[tokio::test]
async fn the_batch_extractor_reports_a_malformed_body_from_the_handler() {
    let response = app()
        .oneshot(
            Request::post("/batch")
                .body(Body::from("not a flow file"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(body_text(response).await.contains("invalid magic"));
}

#[tokio::test]
async fn the_strict_batch_extractor_checks_the_content_type() {
    let response = app()
        .oneshot(
            Request::post("/strict-batch")
                .body(Body::from(batch_bytes()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);

    let response = app()
        .oneshot(
            Request::post("/strict-batch")
                .header(header::CONTENT_TYPE, MEDIA_TYPE)
                .body(Body::from(batch_bytes()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_text(response).await, "3");
}

/// A batch is exactly what the single-flow-file extractor must not accept: it
/// would read the first and silently drop the rest.
#[tokio::test]
async fn the_single_extractor_rejects_a_batch_as_trailing_data() {
    let response = app()
        .oneshot(Request::post("/echo").body(Body::from(batch_bytes())).unwrap())
        .await
        .unwrap();

    // The extractor parses one flow file; echoing it back returns only that
    // one, which is why a batch needs its own extractor.
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        FlowFile::from_bytes(&body).unwrap().content().as_slice(),
        b"first"
    );
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

/// A response commits to a `Content-Length` computed from the declared size
/// before a single content byte is read, so a reader that ends early can only
/// be reported by breaking the body — completing it would hand the client a
/// flow file whose header declares more content than it carries.
#[tokio::test]
async fn a_response_whose_content_reader_ends_early_fails_the_body() {
    let flow_file = FlowFile::builder()
        .attribute("filename", "greeting.txt")
        .reader(std::io::Cursor::new(b"short".to_vec()), 10);

    let response = flow_file.into_response();
    let declared: usize = response.headers()[header::CONTENT_LENGTH]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();

    let err = response
        .into_body()
        .collect()
        .await
        .expect_err("a short content reader must not complete the body");
    assert!(
        err.to_string().contains("size mismatch"),
        "expected the truncation to be reported, got {err}"
    );
    assert!(declared > 5, "the header committed to the declared size");
}

/// The complement: a reader with more than the declared size is still cut to
/// it, and the body completes cleanly at exactly `Content-Length`.
#[tokio::test]
async fn a_response_whose_content_reader_runs_long_is_cut_to_the_declared_size() {
    let flow_file = FlowFile::builder().reader(std::io::Cursor::new(b"way too much".to_vec()), 3);

    let response = flow_file.into_response();
    let declared: usize = response.headers()[header::CONTENT_LENGTH]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();

    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.len(), declared);
    assert_eq!(FlowFile::from_bytes(&body).unwrap().content().as_slice(), b"way");
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

/// Polling a `Stream` after it has returned `None` is contractually undefined,
/// so the body adapter must not do it however hard it is read.
///
/// Driven with a flow file whose header declares more content than the body
/// carries: reads run past the end of the stream but stay inside the declared
/// size, so nothing else stops them reaching the adapter.
#[tokio::test]
async fn the_body_adapter_never_polls_the_request_stream_after_it_ends() {
    use axum::extract::FromRequest;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct EndsOnce {
        chunk: Option<Vec<u8>>,
        ended: bool,
        polled_after_end: Arc<AtomicBool>,
    }

    impl futures_core::Stream for EndsOnce {
        type Item = Result<Vec<u8>, std::io::Error>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            if self.ended {
                self.polled_after_end.store(true, Ordering::SeqCst);
            }
            let Some(chunk) = self.chunk.take() else {
                self.ended = true;
                return std::task::Poll::Ready(None);
            };
            std::task::Poll::Ready(Some(Ok(chunk)))
        }
    }

    // A header declaring ten content bytes, followed by two of them.
    let mut bytes = FlowFile::builder().content(vec![b'x'; 10]).to_bytes();
    bytes.truncate(bytes.len() - 8);

    let polled_after_end = Arc::new(AtomicBool::new(false));
    let body = Body::from_stream(EndsOnce {
        chunk: Some(bytes),
        ended: false,
        polled_after_end: Arc::clone(&polled_after_end),
    });

    let mut flow_file = FlowFileRequest::from_request(Request::post("/").body(body).unwrap(), &())
        .await
        .unwrap();
    assert_eq!(flow_file.size(), 10);

    // Read well past the end of the body, but inside the declared size.
    let mut buf = [0u8; 4];
    for _ in 0..3 {
        let _ = tokio::io::AsyncReadExt::read(flow_file.content_mut(), &mut buf).await;
    }

    assert!(
        !polled_after_end.load(Ordering::SeqCst),
        "the request body stream was polled after it returned None"
    );
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

/// The rejected content type is attacker-controlled, so the 415 body must name
/// enough of it to debug with and no more — not however much the client sent.
#[tokio::test]
async fn strict_extractor_does_not_echo_an_unbounded_content_type() {
    let long = format!("application/{}", "a".repeat(4000));
    let response = app()
        .oneshot(
            Request::post("/strict")
                .header(header::CONTENT_TYPE, &long)
                .body(Body::from(sample_bytes()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert!(
        body.len() < 256,
        "the whole header came back: {} bytes",
        body.len()
    );
    // Still enough of it to tell what was sent.
    assert!(
        std::str::from_utf8(&body)
            .unwrap()
            .contains("application/aaa")
    );
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
    let limits = Limits::recommended().with_max_content_len(4);

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
