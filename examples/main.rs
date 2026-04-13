//! Showcase of what you can do with [`nifioxide`].
//!
//! You build an Axum web service in any way you'd normally do so, and just integrate the parts you
//! need from [`nifioxide`]. Each endpoint shows various parts of the API.

use anyhow::{Context, Result};
use axum::{
    http::{HeaderMap, HeaderName, HeaderValue},
    response::IntoResponse,
};
use futures::{StreamExt, TryStreamExt};
use tokio::io::AsyncReadExt;

use nifioxide::{FlowFile, FlowFileParsingError};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let app = axum::Router::new()
        .route("/single-file-input", axum::routing::post(process_single))
        .route("/multi-file-input", axum::routing::post(process_multiple))
        .route(
            "/single-file-input-multi-output",
            axum::routing::post(process_single_streamed_multi_output),
        );
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
) -> Result<impl IntoResponse, FlowFileParsingError> {
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

    let mut output = FlowFile::builder()
        .attributes(ff.attributes().clone())
        .content_from_bytes(b"hello")
        .build();
    output
        .attributes_mut()
        .insert("hello".to_string(), "world".to_string());

    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        HeaderName::from_static("processed-by"),
        HeaderValue::from_static("axum+nifioxide"),
    );
    Ok((axum::http::StatusCode::OK, response_headers, output))
}

#[tracing::instrument(ret, skip_all)]
async fn process_multiple(
    flow_files: nifioxide::FlowFileStream,
) -> Result<impl IntoResponse, FlowFileParsingError> {
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
            Ok::<_, FlowFileParsingError>(())
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

/// This shows one way to produce multiple output flow files.
///
/// This example takes the input file and returns it back in two halfs.
/// The first half is buffered straight in memory with a vector, while the second
/// half is streamed directly from the remaining HTTP payload.
#[tracing::instrument(ret, skip_all)]
async fn process_single_streamed_multi_output(
    ff: nifioxide::axum::StreamedFlowFileFuture,
) -> Result<impl IntoResponse, FlowFileParsingError> {
    let mut ff = ff.await?;

    // Here we set up a sort of pipe: Everything that is written into `w` shows up in the
    // response body. This uses an internal memory buffer with 8192 bytes of space in this example.
    // We need this pipe in order to asynchronously stream the input file back as the output. This
    // is because we need to give Axum a body that it can start to work with, but we don't want to
    // put the entire input in memory first. As such we need to spawn a separate task, and this
    // pipe provides the bridge between our main handler and the producer task.
    let (mut w, body) = nifioxide::axum::make_response_stream(8192);

    tokio::spawn(async move {
        let total_size = ff.size();
        let mut first_half = Vec::with_capacity((total_size / 2) as usize);
        ff.contents()
            .take(total_size / 2)
            .read_to_end(&mut first_half)
            .await?;
        let mut first_flow_file = FlowFile::builder()
            .attributes(ff.attributes().clone())
            .content_from_bytes(first_half)
            .build();

        let mut second_flow_file = FlowFile::builder()
            .attributes(ff.attributes().clone())
            // To make a flow file from something without a known byte size (like files or byte
            // slices), you have to specify the size explicitly. As we just give it the input stream
            // reader here, we can calculate the expected size.
            .size(total_size - first_flow_file.header().size())
            .content(ff.contents())
            // Alteratively, if we don't know the size, we would need to consume the reader and count
            // how many bytes were read. In this case you'd do either of these two:
            //
            // .content_from_reader_buffered_in_memory(ff.contents()).await?
            // .content_from_reader_buffered_in_tempfile(ff.contents()).await?
            .build();

        first_flow_file.serialize_into(&mut w).await?;
        second_flow_file.serialize_into(&mut w).await?;
        Ok::<_, tokio::io::Error>(())
    });

    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        HeaderName::from_static("processed-by"),
        HeaderValue::from_static("axum+nifioxide"),
    );
    Ok((axum::http::StatusCode::OK, response_headers, body))
}
