//! Glue between this crate and [`axum`].

mod extract;
mod response;

pub use extract::StreamedFlowFileFuture;

use axum::{body::Body, http::StatusCode, response::IntoResponse};
use tokio::io::AsyncWrite;
use tokio_util::io::ReaderStream;

use crate::FlowFileParsingError;

impl IntoResponse for FlowFileParsingError {
    fn into_response(self) -> axum::response::Response {
        let status_code = match self {
            FlowFileParsingError::BadMagicBytes(_)
            | FlowFileParsingError::Malformed { .. }
            | FlowFileParsingError::BrokenChannel(_)
            | FlowFileParsingError::ContentLengthLengthMismatch { .. }
            | FlowFileParsingError::FlowFileExpected => StatusCode::BAD_REQUEST,
            // If we expect one, but got excess data, then technically we parsed the (start of the)
            // input ok, so inprocessable entitiy is slightly more accurate (I think).
            FlowFileParsingError::SingleFlowFileExpected => StatusCode::UNPROCESSABLE_ENTITY,
            FlowFileParsingError::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status_code, format!("{self}")).into_response()
    }
}

/// Make an [`AsyncWrite`] that is connected to a [`axum::body::Body`].
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
