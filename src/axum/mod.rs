//! Glue between this crate and [`axum`].
//!
//! This module provides integration with the [Axum](https://github.com/tokio-rs/axum) web framework.
//! It enables parsing NiFi Flow Files from HTTP requests and generating Flow File responses.
//!
//! # HTTP Integration
//!
//! ## Extractors
//!
//! - [StreamedFlowFiles](crate::axum::StreamedFlowFiles) - Extract a stream of multiple flow files from a request body.
//! - [StreamedFlowFileFuture](crate::axum::StreamedFlowFileFuture) - Extract a single flow file, returning an error if zero or
//!   more than one is provided.
//!
//! ## Content-Type
//!
//! Both extractors require the `Content-Type: application/flowfile-v3` header. Requests with
//! other content types will receive a `415 UNSUPPORTED_MEDIA_TYPE` response.
//!
//! ## Responses
//!
//! - [`FlowFile`](crate::FlowFile) implements `IntoResponse`, enabling direct return from handlers.
//! - [`FlowFileParsingError`] implements `IntoResponse`, returning appropriate error codes.
//!
//! # Example
//!
//! ```
//! use nifioxide::axum::StreamedFlowFiles;
//! use futures::StreamExt;
//!
//! async fn handle_multiple(mut stream: StreamedFlowFiles) -> impl axum::response::IntoResponse {
//!     while let Some(result) = stream.next().await {
//!         let mut ff = result.unwrap();
//!     }
//!     "OK"
//! }
//! ```

mod extract;
mod response;
mod streamreader;

pub use extract::StreamedFlowFileFuture;
pub use streamreader::{IntoFlowFiles, StreamedFlowFile, StreamedFlowFiles};

use axum::{body::Body, http::StatusCode, response::IntoResponse};
use tokio::io::AsyncWrite;
use tokio_util::io::ReaderStream;

use crate::FlowFileParsingError;

/// HTTP response implementation for [`FlowFileParsingError`].
///
/// Maps parsing errors to appropriate HTTP status codes:
///
/// - [`BadMagicBytes`](FlowFileParsingError::BadMagicBytes): `400 Bad Request`
/// - [`Malformed`](FlowFileParsingError::Malformed): `400 Bad Request`
/// - [`BrokenChannel`](FlowFileParsingError::BrokenChannel): `400 Bad Request`
/// - [`ContentLengthLengthMismatch`](FlowFileParsingError::ContentLengthLengthMismatch): `400 Bad Request`
/// - [`FlowFileExpected`](FlowFileParsingError::FlowFileExpected): `400 Bad Request`
/// - [`SingleFlowFileExpected`](FlowFileParsingError::SingleFlowFileExpected): `422 Unprocessable Entity`
/// - [`Io`](FlowFileParsingError::Io): `500 Internal Server Error`
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
/// This is useful when you want to process some stream of data into the response, but avoid having
/// the whole response buffered in memory. Axum needs to be given a response body so that it can
/// start pulling bytes from it onto the network connection. But we also want to keep processing
/// the data stream at the same time. We solve this by spawning a task that produces bytes into a
/// writer and returning the connected body to axum.
///
/// See [`tokio::io::simplex`] for the meaning of `max_buf_size`.
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
    let (read, write) = tokio::io::simplex(max_buf_size);
    (write, Body::from_stream(ReaderStream::new(read)))
}
