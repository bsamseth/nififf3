use nifioxide::FlowFileIterator;

use anyhow::{Context, Result};
use axum::{
    http::{HeaderMap, HeaderValue},
    response::IntoResponse,
};

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

#[tracing::instrument(ret, skip_all)]
async fn process(mut flow_files: FlowFileIterator) -> impl IntoResponse {
    loop {
        match flow_files.next_file().await {
            Ok(Some(ff)) => {
                tracing::debug!("flow file size: {}", ff.len());
                for (key, value) in ff.attributes() {
                    tracing::debug!("attrib: {key}: {value}");
                }
            }
            Ok(None) => break,
            Err(err) => return err.into_response(),
        }
    }

    let mut response_headers = HeaderMap::new();
    response_headers.insert("x-processed-by", HeaderValue::from_static("axum-stream"));

    // Streamed response body
    // let response_body = Body::from_stream(output_stream);

    (response_headers, b"Ok").into_response()
}
