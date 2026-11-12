//! The extractors and responses, driven through a `Router` in-process.
#![cfg(feature = "axum")]

use axum::body::Body;
use axum::extract::DefaultBodyLimit;
use axum::http::{Request, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Extension, Router};
use http_body_util::BodyExt;
use nififf3::{FlowFile, FlowFilesReaderAsync, MEDIA_TYPE, Spool, SpoolConfig, attr};
use tokio::io::{AsyncBufReadExt, BufReader};
use tower::ServiceExt;

/// The simplest handler there is: a flow file in, the same flow file out.
async fn echo(flow_file: FlowFile) -> FlowFile {
    flow_file
}

/// Validate and annotate, passing the content through untouched.
async fn admit(mut flow_file: FlowFile) -> Result<FlowFile, (StatusCode, String)> {
    if flow_file.attribute(attr::MIME_TYPE) != Some("text/csv") {
        return Err((StatusCode::UNSUPPORTED_MEDIA_TYPE, "text/csv only".into()));
    }
    if flow_file.size() == 0 {
        return Err((StatusCode::BAD_REQUEST, "nothing to admit".into()));
    }
    flow_file.set_attribute("admitted", "true");
    Ok(flow_file)
}

/// The README's lead handler: stream the content line by line, answer with
/// a new flow file built in memory.
async fn summarize(flow_file: FlowFile) -> Result<FlowFile<Vec<u8>>, (StatusCode, String)> {
    if flow_file.attribute(attr::MIME_TYPE) != Some("text/csv") {
        return Err((StatusCode::UNSUPPORTED_MEDIA_TYPE, "text/csv only".into()));
    }
    let summary = flow_file.derive();
    let failed = |err: std::io::Error| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
    let mut lines = BufReader::new(flow_file.into_content()).lines();
    let (mut columns, mut rows) = (None, 0u64);
    while let Some(line) = lines.next_line().await.map_err(failed)? {
        let fields = line.split(',').count();
        match columns {
            None => columns = Some(fields),
            Some(n) if n == fields => rows += 1,
            Some(n) => {
                let message = format!("row {} has {fields} fields, expected {n}", rows + 1);
                return Err((StatusCode::BAD_REQUEST, message));
            }
        }
    }
    Ok(summary
        .attribute(attr::MIME_TYPE, "text/plain")
        .attribute(attr::RECORD_COUNT, rows.to_string())
        .content(format!("{rows} rows, {} columns\n", columns.unwrap_or(0))))
}

/// A new flow file built in memory, which is a response as it is.
async fn shout(flow_file: FlowFile) -> Result<FlowFile<Vec<u8>>, nififf3::Error> {
    let flow_file = flow_file.into_memory_async().await?;
    Ok(flow_file
        .derive()
        .content(flow_file.content().to_ascii_uppercase()))
}

/// Where the content went, as the handler sees it.
async fn where_is_it(flow_file: FlowFile) -> String {
    let place = if flow_file.content().is_in_memory() {
        "memory"
    } else {
        "disk"
    };
    format!("{place}:{}", flow_file.size())
}

/// The many-in case: a body holding several flow files, which is what NiFi
/// sends after merging with the `FlowFile Stream, v3` format.
async fn count_batch(body: Spool) -> Result<String, nififf3::Error> {
    let mut flow_files = FlowFilesReaderAsync::new(body);
    let mut sizes = Vec::new();
    while let Some(flow_file) = flow_files.next().await? {
        sizes.push(flow_file.size().to_string());
    }
    Ok(sizes.join(","))
}

fn app() -> Router {
    Router::new()
        .route("/echo", post(echo))
        .route("/admit", post(admit))
        .route("/summarize", post(summarize))
        .route("/shout", post(shout))
        .route("/where", post(where_is_it))
        .route("/batch", post(count_batch))
}

fn sample_bytes() -> Vec<u8> {
    FlowFile::builder()
        .attribute("filename", "greeting.txt")
        .content(&b"hello"[..])
        .to_bytes()
}

fn batch_bytes() -> Vec<u8> {
    let mut bytes = FlowFile::builder().content(&b"first"[..]).to_bytes();
    bytes.extend(FlowFile::builder().content(&b"second!"[..]).to_bytes());
    bytes.extend(FlowFile::builder().content(Vec::new()).to_bytes());
    bytes
}

fn send(uri: &str, body: impl Into<Body>) -> Request<Body> {
    Request::post(uri).body(body.into()).unwrap()
}

async fn body_text(response: axum::response::Response) -> String {
    let body = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(body.to_vec()).unwrap()
}

#[tokio::test]
async fn a_flow_file_goes_in_and_comes_back_out() {
    let bytes = sample_bytes();
    let response = app().oneshot(send("/echo", bytes.clone())).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_TYPE], MEDIA_TYPE);
    assert_eq!(
        response.headers()[header::CONTENT_LENGTH],
        bytes.len().to_string().as_str()
    );
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.as_ref(), bytes.as_slice());
}

/// The README's lead handler, with the content on disk: the summary comes
/// back as a new flow file, a ragged row is a 400 naming the row, and the
/// wrong kind of flow file is a 415.
#[tokio::test]
async fn summarizing_streams_the_content_and_answers_with_a_new_flow_file() {
    let app = app().layer(Extension(SpoolConfig::new().with_max_in_memory(16)));
    let mut csv = String::from("id,name,amount\n");
    csv.extend((0..1000).map(|i| format!("{i},item {i},{}.50\n", i * 3)));
    let request = FlowFile::builder()
        .attribute(attr::MIME_TYPE, "text/csv")
        .attribute(attr::FILENAME, "items.csv")
        .content(csv.clone())
        .to_bytes();

    let response = app
        .clone()
        .oneshot(send("/summarize", request))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_TYPE], MEDIA_TYPE);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let summary = FlowFile::from_bytes(&body).unwrap();
    assert_eq!(summary.content_str().unwrap(), "1000 rows, 3 columns\n");
    assert_eq!(summary[attr::RECORD_COUNT], "1000");
    assert_eq!(summary[attr::MIME_TYPE], "text/plain");
    assert_eq!(summary[attr::FILENAME], "items.csv");

    csv.push_str("1001,ragged\n");
    let ragged = FlowFile::builder()
        .attribute(attr::MIME_TYPE, "text/csv")
        .content(csv)
        .to_bytes();
    let response = app
        .clone()
        .oneshot(send("/summarize", ragged))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        body_text(response).await,
        "row 1001 has 2 fields, expected 3"
    );

    let response = app
        .oneshot(send("/summarize", sample_bytes()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(body_text(response).await, "text/csv only");
}

/// A pass-through handler leaves disk-backed content untouched, and turns the
/// wrong kind of flow file away with its own status.
#[tokio::test]
async fn validating_and_annotating_passes_the_content_through() {
    let app = app().layer(Extension(SpoolConfig::new().with_max_in_memory(16)));
    let content = vec![b'x'; 1024];
    let csv = FlowFile::builder()
        .attribute(attr::MIME_TYPE, "text/csv")
        .content(content.clone())
        .to_bytes();

    let response = app.clone().oneshot(send("/admit", csv)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let flow_file = FlowFile::from_bytes(&body).unwrap();
    assert_eq!(flow_file["admitted"], "true");
    assert_eq!(flow_file[attr::MIME_TYPE], "text/csv");
    assert_eq!(flow_file.content(), content.as_slice());

    let response = app.oneshot(send("/admit", sample_bytes())).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(body_text(response).await, "text/csv only");
}

#[tokio::test]
async fn an_in_memory_flow_file_is_a_response() {
    let response = app().oneshot(send("/shout", sample_bytes())).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_TYPE], MEDIA_TYPE);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let flow_file = FlowFile::from_bytes(&body).unwrap();
    assert_eq!(flow_file.content(), b"HELLO");
    assert_eq!(flow_file["filename"], "greeting.txt");
}

/// The threshold in the router's extension decides where content goes, and
/// the handler can tell.
#[tokio::test]
async fn the_spool_config_in_the_router_decides_memory_or_disk() {
    let bytes = sample_bytes();

    let response = app().oneshot(send("/where", bytes.clone())).await.unwrap();
    assert_eq!(body_text(response).await, "memory:5");

    let app = app().layer(Extension(SpoolConfig::new().with_max_in_memory(4)));
    let response = app.oneshot(send("/where", bytes)).await.unwrap();
    assert_eq!(body_text(response).await, "disk:5");
}

/// A body too large for memory round-trips through the file without loss,
/// and the response streams it back with an exact `Content-Length`.
#[tokio::test]
async fn a_large_flow_file_round_trips_through_disk() {
    let content: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();
    let bytes = FlowFile::builder()
        .attribute("filename", "big.bin")
        .content(content)
        .to_bytes();

    let app = app()
        .layer(Extension(SpoolConfig::new().with_max_in_memory(1024)))
        .layer(DefaultBodyLimit::disable());
    let response = app.oneshot(send("/echo", bytes.clone())).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_LENGTH],
        bytes.len().to_string().as_str()
    );
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.as_ref(), bytes.as_slice());
}

#[tokio::test]
async fn the_extractor_streams_chunked_bodies() {
    let bytes = sample_bytes();
    let chunks: Vec<Result<Vec<u8>, std::io::Error>> =
        bytes.chunks(3).map(|c| Ok(c.to_vec())).collect();
    let body = Body::from_stream(IterStream(chunks.into()));

    let response = app().oneshot(send("/echo", body)).await.unwrap();
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
async fn a_body_that_is_not_a_flow_file_is_a_400_naming_the_content_type() {
    let request = Request::post("/echo")
        .header(header::CONTENT_TYPE, "text/plain")
        .body(Body::from("not a flow file"))
        .unwrap();
    let response = app().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let text = body_text(response).await;
    assert!(text.contains("invalid magic header"), "{text}");
    assert!(text.contains("Content-Type: text/plain"), "{text}");
}

#[tokio::test]
async fn a_truncated_body_is_a_400_with_the_size_mismatch() {
    let bytes = sample_bytes();
    let response = app()
        .oneshot(send("/echo", bytes[..bytes.len() - 2].to_vec()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let text = body_text(response).await;
    assert!(text.contains("header declares 5 bytes, got 3"), "{text}");
}

/// A batch sent to the single-flow-file extractor must not have its tail
/// silently dropped. The whole body is read, so the connection is drained,
/// and the client is told how much was over.
#[tokio::test]
async fn a_batch_sent_to_the_single_extractor_is_rejected_as_trailing_data() {
    let response = app().oneshot(send("/echo", batch_bytes())).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let text = body_text(response).await;
    assert!(text.contains("trailing bytes"), "{text}");
}

#[tokio::test]
async fn an_empty_body_is_a_400_not_a_panic() {
    let response = app().oneshot(send("/echo", Body::empty())).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let text = body_text(response).await;
    assert!(text.contains("header ended early"), "{text}");
}

#[tokio::test]
async fn the_spool_extractor_reads_every_flow_file_in_the_body() {
    let response = app().oneshot(send("/batch", batch_bytes())).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_text(response).await, "5,7,0");
}

/// An empty body is a batch of none rather than a failure.
#[tokio::test]
async fn the_spool_extractor_accepts_an_empty_body() {
    let response = app().oneshot(send("/batch", Body::empty())).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_text(response).await, "");
}

/// Nothing is parsed at extraction time, so a malformed batch surfaces from
/// the reader, and the handler turns it into the same 400 as anywhere else.
#[tokio::test]
async fn the_spool_extractor_leaves_parsing_to_the_handler() {
    let response = app()
        .oneshot(send("/batch", "not a flow file"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(body_text(response).await.contains("invalid magic"));
}

/// Both extractors read through axum's limited body, so an over-large body
/// is a 413 whether it trips while the header is being read or while the
/// content is being spooled.
#[tokio::test]
async fn the_extractors_honor_the_default_body_limit() {
    let bytes = FlowFile::builder().content(vec![0u8; 100_000]).to_bytes();
    for (uri, limit) in [("/echo", 8), ("/echo", 64), ("/batch", 64)] {
        let app = app().layer(DefaultBodyLimit::max(limit));
        let response = app.oneshot(send(uri, bytes.clone())).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "{uri} with a {limit} byte limit"
        );
        let text = body_text(response).await;
        assert!(text.contains("DefaultBodyLimit"), "{text}");
    }
}

#[tokio::test]
async fn a_malformed_body_is_still_a_400_under_a_body_limit() {
    let app = app().layer(DefaultBodyLimit::max(1024));
    let response = app.oneshot(send("/echo", "not a flow file")).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_disabled_body_limit_lets_large_flow_files_through() {
    let app = app().layer(DefaultBodyLimit::disable());
    // Over axum's 2 MiB default, so this only passes because it is disabled.
    let bytes = FlowFile::builder().content(vec![7u8; 3 << 20]).to_bytes();
    let response = app.oneshot(send("/echo", bytes.clone())).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.len(), bytes.len());
}

/// A spool directory that does not exist is the server's fault, not the
/// client's, and the status says so.
#[tokio::test]
async fn a_spool_failure_is_a_500() {
    let missing = std::env::temp_dir().join("nififf3-no-such-directory");
    let app = app().layer(Extension(
        SpoolConfig::new()
            .with_max_in_memory(0)
            .with_directory(missing),
    ));
    let response = app.oneshot(send("/echo", sample_bytes())).await.unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let text = body_text(response).await;
    assert!(text.contains("spool"), "{text}");
}

/// A response commits to a `Content-Length` computed from the declared size
/// before a single content byte is read. So a reader that ends early can only
/// be reported by breaking the body.
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
    assert!(err.to_string().contains("size mismatch"), "{err}");
    assert!(declared > 5, "the header committed to the declared size");
}

/// The complement: a reader with more than the declared size is cut to it.
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
    assert_eq!(
        FlowFile::from_bytes(&body).unwrap().content().as_slice(),
        b"way"
    );
}

/// The crate's own error type still answers as a response, for handlers
/// that use `?` on this crate's calls.
#[tokio::test]
async fn the_error_type_is_a_400_response() {
    let err = FlowFile::from_bytes(b"not a flow file").unwrap_err();
    let response = err.into_response();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(body_text(response).await.contains("invalid magic"));
}
