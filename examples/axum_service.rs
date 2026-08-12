//! Flow files over HTTP: one in, one or many out.
//!
//! Two handlers on one `Router`:
//!
//! - `POST /transform` takes one flow file in and sends one out.
//!   `StrictFlowFileRequest` parses the header and streams the body, and the
//!   answer is a flow file, which is itself an `IntoResponse`.
//! - `POST /split` takes one flow file in and sends many out, through
//!   `FlowFilesResponse`.
//!
//! The router is driven in-process with `tower`'s `oneshot`, so the example
//! runs without binding a port. In a real service the same `Router` goes to
//! `axum::serve`.
//!
//!     cargo run --features axum --example axum_service

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use axum::routing::post;
use axum::{Router, response::IntoResponse};
use http_body_util::BodyExt;
use nififf3::{Error, FlowFile, FlowFilesAsync, FlowFilesResponse, StrictFlowFileRequest};
use tower::ServiceExt;

/// One in, one out: the content is rewritten, the attributes carried over.
async fn transform(
    StrictFlowFileRequest(flow_file): StrictFlowFileRequest,
) -> Result<impl IntoResponse, Error> {
    let flow_file = flow_file.into_memory_async().await?;
    Ok(flow_file
        .derive()
        .attribute("transformed", "uppercase")
        .content(flow_file.content().to_ascii_uppercase())
        .into_reader())
}

/// One flow file in, many out: one per record.
///
/// Everything that can fail the request as a whole is checked before the
/// `FlowFilesResponse` is returned. Past that point the status is already 200,
/// so a problem with one record belongs in the body, as attributes on a flow
/// file of its own.
async fn split(
    StrictFlowFileRequest(flow_file): StrictFlowFileRequest,
) -> Result<FlowFilesResponse, Error> {
    let parent = flow_file.into_memory_async().await?;
    let mut parts = parent.fragments();

    Ok(FlowFilesResponse::new(move |mut writer| async move {
        for (offset, record) in parent.content().split(|byte| *byte == b'\n').enumerate() {
            // `record` is a reader, so its content is never copied into a part.
            let part = parts
                .next_part()
                .attribute("filename", format!("record-{offset}.txt"))
                .reader(record, record.len() as u64);
            writer.write(part).await?;
        }
        // The parts left as they were found, so the total is only known here.
        // The terminator declares it for the bundle, which is what lets
        // `MergeContent` fill its bin downstream.
        writer.write_bytes(&parts.terminate()).await?;
        Ok(())
    }))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let app = Router::new()
        .route("/transform", post(transform))
        .route("/split", post(split));

    let incoming = FlowFile::builder()
        .attribute("filename", "records.txt")
        .attribute("source", "example")
        .content(&b"alpha\nbeta\ngamma"[..])
        .to_bytes();

    // --- one in, one out ---------------------------------------------------
    let response = app
        .clone()
        .oneshot(request("/transform", incoming.clone()))
        .await?;
    println!("POST /transform -> {}", response.status());
    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await?.to_bytes();
    let answer = FlowFile::from_bytes(&body)?;
    println!(
        "  {} bytes, filename={}, transformed={}",
        answer.size(),
        answer.attributes()["filename"],
        answer.attributes()["transformed"],
    );
    assert_eq!(answer.content().as_slice(), b"ALPHA\nBETA\nGAMMA");

    // --- one in, many out --------------------------------------------------
    let response = app.clone().oneshot(request("/split", incoming)).await?;
    println!("POST /split -> {}", response.status());
    assert_eq!(response.status(), StatusCode::OK);
    // The parts are produced lazily, so the length is not known when the
    // headers go out and the body is chunked.
    assert!(!response.headers().contains_key(header::CONTENT_LENGTH));

    let body = response.into_body().collect().await?.to_bytes();
    let mut flow_files = FlowFilesAsync::new(body.as_ref());
    let mut declared = None;
    let mut count = 0;
    while let Some(part) = flow_files.next().await {
        let part = part?;
        // The last flow file is the terminator: no content, and the count for
        // the whole bundle including itself.
        match part.attributes().get("fragment.count") {
            Some(total) => {
                declared = Some(total.parse::<usize>()?);
                println!(
                    "  [{}] terminator, count={total}",
                    part.attributes()["fragment.index"]
                );
            }
            None => println!(
                "  [{}] {} = {:?}",
                part.attributes()["fragment.index"],
                part.attributes()["filename"],
                String::from_utf8_lossy(part.content()),
            ),
        }
        count += 1;
    }
    assert_eq!(count, 4, "three records and the terminator");
    assert_eq!(declared, Some(count), "the bundle declares its own size");

    // --- the status codes still available before the body starts -----------
    let response = app
        .clone()
        .oneshot(request("/split", b"not a flow file".to_vec()))
        .await?;
    println!("POST /split (bad body) -> {}", response.status());
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let no_content_type = Request::builder()
        .method("POST")
        .uri("/split")
        .body(Body::empty())?;
    let response = app.oneshot(no_content_type).await?;
    println!("POST /split (no content type) -> {}", response.status());
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    Ok(())
}

fn request(uri: &str, body: Vec<u8>) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header(header::CONTENT_TYPE, nififf3::MEDIA_TYPE)
        .body(Body::from(body))
        .unwrap()
}
