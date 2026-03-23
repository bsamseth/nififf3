use anyhow::{Context, Result};
use axum::{
    http::{HeaderMap, HeaderName, HeaderValue},
    response::IntoResponse,
};
use futures::{StreamExt, TryStreamExt};
use tokio::io::AsyncReadExt;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let app = axum::Router::new()
        .route("/process-multiple", axum::routing::post(process_multiple))
        .route("/process-single", axum::routing::post(process_single));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:9999")
        .await
        .context("binding api server to address")?;
    tracing::info!("Listening on 127.0.0.1:9999");
    axum::serve(listener, app).await?;
    Ok(())
}

#[tracing::instrument(ret, skip_all)]
async fn process_single(
    ff: nifioxide::axum::StreamedFlowFileFuture,
) -> Result<impl IntoResponse, nifioxide::FlowFileParsingError> {
    let mut ff = ff.await?;

    tracing::info!("Flow file with size: {}", ff.size());
    for (key, value) in ff.attributes() {
        tracing::debug!("attrib: {key}: {value}");
    }

    let mut buf = [0u8; 128];
    let n = match ff.contents().read(&mut buf).await {
        Ok(n) => n,
        Err(err) => {
            tracing::error!("error from reading ff body: {err}");
            return Err(err.into());
        }
    };
    tracing::debug!("read {n} bytes from file body");

    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        HeaderName::from_static("processed-by"),
        HeaderValue::from_static("axum+nifioxide"),
    );

    Ok((axum::http::StatusCode::OK, response_headers))
}

#[tracing::instrument(ret, skip_all)]
async fn process_multiple(
    flow_files: nifioxide::FlowFileStream,
) -> Result<impl IntoResponse, nifioxide::FlowFileParsingError> {
    flow_files
        .then(|ff| async move {
            let mut ff = ff?;
            tracing::info!("Flow file with size: {}", ff.size());
            for (key, value) in ff.attributes() {
                tracing::debug!("attrib: {key}: {value}");
            }
            let mut buf = [0u8; 128];
            let n = ff.contents().read(&mut buf).await?;
            tracing::debug!("read {n} bytes from file body");
            Ok::<_, nifioxide::FlowFileParsingError>(())
        })
        .try_collect::<()>()
        .await?;

    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        HeaderName::from_static("processed-by"),
        HeaderValue::from_static("axum+nifioxide"),
    );

    Ok((axum::http::StatusCode::OK, response_headers))
}
