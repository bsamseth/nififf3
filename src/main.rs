use nifioxide::FlowFileIterator;

use anyhow::{Context, Result};
use axum::{
    http::{HeaderMap, HeaderName, HeaderValue},
    response::IntoResponse,
};
use tokio::io::AsyncWriteExt;

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

#[tracing::instrument(ret, skip_all)]
async fn process(mut flow_files: FlowFileIterator) -> impl IntoResponse {
    let (mut w, body) = nifioxide::axum::make_response_stream(64 * 1024);

    // This must be spawned separately because we need to start to stream out the response to make
    // room for more writes to w. Otherwise we would dead lock waiting for reads to happen, as
    // reads would happen after all writes are done. Spawning like this ensures progress.
    // This is a footgun -> try to make this impossible to do with a helper?
    tokio::spawn(async move {
        loop {
            match flow_files.next_file().await {
                Ok(Some(ff)) => {
                    tracing::debug!("flow file size: {}", ff.len());
                    for (key, value) in ff.attributes() {
                        tracing::debug!("attrib: {key}: {value}");
                    }

                    // let body_stream = ff.body(); // impl AsyncRead

                    // What to do about errors here?
                    w.write_all(b"a file was parsed\n").await.unwrap();
                }
                Ok(None) => break,
                // What do do abou this?!
                Err(_err) => todo!(),
            }
        }
    });

    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        HeaderName::from_static("processed-by"),
        HeaderValue::from_static("axum+nifioxide"),
    );

    (response_headers, body).into_response()
}
