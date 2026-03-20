//! Glue between this crate and [`axum`].

mod handler;

use axum::{
    body::Body,
    http::{
        StatusCode,
        header::{CONTENT_LENGTH, CONTENT_TYPE},
    },
    response::IntoResponse,
};
use futures::{FutureExt, StreamExt};
use tokio::io::AsyncWrite;
use tokio_util::io::ReaderStream;

use crate::{FlowFileIterator, FlowFileParsingError, IntoFlowFiles, StreamedFlowFile};

impl<S> axum::extract::FromRequest<S> for FlowFileIterator
where
    S: Send + Sync,
{
    type Rejection = <Self as TryFrom<axum::extract::Request>>::Error;

    async fn from_request(req: axum::extract::Request, _: &S) -> Result<Self, Self::Rejection> {
        req.try_into()
    }
}

impl TryFrom<axum::extract::Request> for FlowFileIterator {
    type Error = (StatusCode, &'static str);

    fn try_from(req: axum::extract::Request) -> Result<Self, Self::Error> {
        if req
            .headers()
            .get(CONTENT_TYPE)
            .is_none_or(|value| value.to_str().unwrap_or("") != "application/flowfile-v3")
        {
            return Err((
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "Only `Content-Type: application/flowfile-v3` is accepted.",
            ));
        }

        let maybe_content_length = req
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok());

        let stream = req.into_body().into_data_stream();
        Ok((stream, maybe_content_length).into_flow_files())
    }
}

impl<S> axum::extract::FromRequest<S> for StreamedFlowFileFuture
where
    S: Send + Sync,
{
    type Rejection = <Self as TryFrom<axum::extract::Request>>::Error;

    async fn from_request(req: axum::extract::Request, _: &S) -> Result<Self, Self::Rejection> {
        req.try_into()
    }
}

impl TryFrom<axum::extract::Request> for StreamedFlowFileFuture {
    type Error = (StatusCode, &'static str);

    fn try_from(req: axum::extract::Request) -> Result<Self, Self::Error> {
        req.try_into().map(StreamedFlowFileFuture)
    }
}

/// Extractor that when awaited gives you a single flow file.
///
/// Use this if your endpoint only wants to expect a single
pub struct StreamedFlowFileFuture(FlowFileIterator);

impl Future for StreamedFlowFileFuture {
    type Output = Result<StreamedFlowFile, FlowFileParsingError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let Some(mut ff) = std::task::ready!(self.0.next().poll_unpin(cx)) else {
            return std::task::Poll::Ready(Err(FlowFileParsingError::FlowFileExpected));
        };
        if !self.0.is_empty() {
            return std::task::Poll::Ready(Err(FlowFileParsingError::SingleFlowFileExpected));
        }

        // Finally, since we expect only a single file and the iterator will be dropped when this
        // future resolves, we shouldn't try to return the inner reader back to the iterator.
        // This would fail because the iterator, and its receiver, would be dropped already and the
        // send would fail.
        if let Ok(ff) = ff.as_mut() {
            ff.tx.take();
        }
        std::task::Poll::Ready(ff)
    }
}

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
