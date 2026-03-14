use futures::StreamExt;
use nifioxide::FlowFileIterator;

use anyhow::{Context, Result};
use axum::{
    body::Body,
    http::{HeaderMap, HeaderName, HeaderValue},
    response::IntoResponse,
};
use tokio::io::AsyncReadExt;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let app = axum::Router::new().route("/process", axum::routing::post(process));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:9999")
        .await
        .context("binding api server to address")?;
    axum::serve(listener, app).await?;
    Ok(())
}

// Notes
// Keep the option to _accept_ multiple flow files.
//
// However, move to one flow file per request as the intended solution.
// This because HTTP requires us to decide on a response code before we send a single other byte of
// the response body. If we don't won't to buffer the whole stream (defeating the streaming
// approach to begin with), then how do you deal with errors that happen after you've started
// streaming the response? Also a problem for single files though, but less bad.
//
// Instead try to make the single-flow-file use case more usable.
//

// Proposed solution:
// The request returns 200 unless it immediately fails parsing, due to things like wrong encoding.
// Otherwise, we return one flow file per incoming flow file. Status and potential return message
// per flow file should be conveyed in the attributes of the returned file.
//
// Requirements:
//
// 1. Make it easy to just alter the attributes of a flow file and return it, either with or
//    without the body.
// 2. Somehow allow you to _modify_ the returned flowfile.
//
//
// For the latter part there are several use cases:
//
// 1. Alter the body in a streaming fashion.
// 2. Buffer the entire contents in memory.
// 3. Buffer the entire contents to a "spooled file"
//    This could be a fallback for the memory version, with a capped memory amount.
// 4. Produce arbitrary output.

#[tracing::instrument(ret, skip_all)]
async fn process(flow_files: FlowFileIterator) -> impl IntoResponse {
    // let (mut w, body) = nifioxide::axum::make_response_stream(64 * 1024);

    let s = flow_files.filter_map(|ff| async move {
        let mut ff = match ff {
            Ok(ff) => ff,
            Err(err) => {
                tracing::error!("error from ff: {err}");
                return None;
            }
        };

        tracing::info!("Flow file with size: {}", ff.len());
        for (key, value) in ff.attributes() {
            tracing::debug!("attrib: {key}: {value}");
        }

        let mut buf = [0u8; 128];
        let n = match ff.body().read(&mut buf).await {
            Ok(n) => n,
            Err(err) => {
                tracing::error!("error from reading ff body: {err}");
                return None;
            }
        };
        tracing::debug!("read {n} bytes from file body");
        Some(Ok::<_, tokio::io::Error>(axum::body::Bytes::new()))
    });

    let body = Body::from_stream(s);

    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        HeaderName::from_static("processed-by"),
        HeaderValue::from_static("axum+nifioxide"),
    );

    (axum::http::StatusCode::OK, body).into_response()
}
