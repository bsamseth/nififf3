//! Glue between this crate and [`axum`].

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

use crate::{FlowFileParsingError, FlowFileStream, IntoFlowFiles, StreamedFlowFile};

impl<S> axum::extract::FromRequest<S> for FlowFileStream
where
    S: Send + Sync,
{
    type Rejection = <Self as TryFrom<axum::extract::Request>>::Error;

    async fn from_request(req: axum::extract::Request, _: &S) -> Result<Self, Self::Rejection> {
        req.try_into()
    }
}

impl TryFrom<axum::extract::Request> for FlowFileStream {
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
/// Use this if your endpoint only wants to expect a single flow file, and produce an error if zero
/// or more than one is provided instead.
///
/// # Example
///
/// ```
/// use tokio::io::AsyncReadExt;
/// use nifioxide::{FlowFileParsingError, axum::StreamedFlowFileFuture};
///
/// async fn process_single(
///     ff: StreamedFlowFileFuture,
/// ) -> Result<impl axum::response::IntoResponse, FlowFileParsingError> {
///     let mut ff = ff.await?;
///
///     println!("Flow file with size: {}", ff.size());
///     for (key, value) in ff.attributes() {
///         println!("attrib: {key}: {value}");
///     }
///
///     let mut buf = Vec::with_capacity(ff.size() as usize);
///     ff.contents().read_to_end(&mut buf).await?;
///     Ok((axum::http::StatusCode::OK, buf))
/// }
pub struct StreamedFlowFileFuture(FlowFileStream);

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
            ff.disable_automatic_return_of_internal_reader();
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

#[cfg(test)]
mod tests {
    use axum::{body::Body, http::Request};

    use super::*;

    fn make_flow_file_request(content: Vec<u8>, content_type: &str) -> Request<Body> {
        Request::builder()
            .header("Content-Type", content_type)
            .header("Content-Length", content.len())
            .body(Body::from(axum::body::Bytes::from(content)))
            .unwrap()
    }

    #[tokio::test]
    async fn flow_file_iterator_rejects_wrong_content_type() {
        let req = Request::builder()
            .header("Content-Type", "application/json")
            .body(Body::empty())
            .unwrap();

        let result: Result<FlowFileStream, _> = req.try_into();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn flow_file_iterator_accepts_flowfile_v3() {
        let req = make_flow_file_request(vec![], "application/flowfile-v3");

        let result: Result<FlowFileStream, _> = req.try_into();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn flow_file_iterator_non_empty_when_non_zero_content_length() {
        let req = Request::builder()
            .header("Content-Type", "application/flowfile-v3")
            .header("Content-Length", "12345")
            .body(Body::empty())
            .unwrap();

        let iter: FlowFileStream = req.try_into().unwrap();
        assert!(!iter.is_empty());
    }

    #[tokio::test]
    async fn flow_file_iterator_empty_when_zero_content_length() {
        let req = Request::builder()
            .header("Content-Type", "application/flowfile-v3")
            .header("Content-Length", "0")
            .body(Body::empty())
            .unwrap();

        let iter: FlowFileStream = req.try_into().unwrap();
        assert!(iter.is_empty());
    }

    #[tokio::test]
    async fn streamed_flow_file_future_requires_content() {
        let req = Request::builder()
            .header("Content-Type", "application/flowfile-v3")
            .body(Body::empty())
            .unwrap();

        let result: Result<StreamedFlowFileFuture, _> = req.try_into();
        assert!(result.is_ok());

        let ff_future = result.unwrap();
        let result = ff_future.await;
        assert!(result.is_err());
    }
}
