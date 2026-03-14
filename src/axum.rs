//! Glue between this crate and [`axum`].

use axum::{body::Body, http::StatusCode, response::IntoResponse};
use tokio::io::AsyncWrite;
use tokio_util::io::ReaderStream;

use crate::{FlowFileIterator, FlowFileParsingError, IntoFlowFiles};

impl<S> axum::extract::FromRequest<S> for FlowFileIterator
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request(req: axum::extract::Request, _: &S) -> Result<Self, Self::Rejection> {
        if req
            .headers()
            .get("Content-Type")
            .is_none_or(|value| value.to_str().unwrap_or("") != "application/flowfile-v3")
        {
            return Err((
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "Only `Content-Type: application/flowfil-v3` is accepted.",
            ));
        }

        let stream = req.into_body().into_data_stream();
        Ok(stream.into_flow_files())
    }
}

impl IntoResponse for FlowFileParsingError {
    fn into_response(self) -> axum::response::Response {
        (StatusCode::UNPROCESSABLE_ENTITY, format!("{self}")).into_response()
    }
}

/// Make an [`AsyncWrite`] that is connected to an [`axum::body::Body`].
///
/// Writing bytes into the writer will end up in the streamed response body.
///
/// See [`tokio::io::duplex`] for the meaning of `max_buf_size`.
///
/// # Example
///
/// ```
/// # use tokio::io::AsyncWriteExt;
/// async fn streamed_response_body() -> impl axum::response::IntoResponse {
///    let (mut w, body) = nifioxide::axum::make_response_stream(1024);
///    tokio::spawn(async move {
///        let data = b"Hello, World!";
///        for _ in 0..10 {
///            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
///            w.write_all(data.as_ref()).await?;
///        }
///        Ok::<_, tokio::io::Error>(())
///    });
///    (axum::http::StatusCode::OK, body)
/// }
/// ```
pub fn make_response_stream(max_buf_size: usize) -> (impl AsyncWrite, Body) {
    let (read, write) = tokio::io::duplex(max_buf_size);
    (write, Body::from_stream(ReaderStream::new(read)))
}
