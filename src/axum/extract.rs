use axum::http::{
    StatusCode,
    header::{CONTENT_LENGTH, CONTENT_TYPE},
};
use futures::{FutureExt, StreamExt};

use super::streamreader::{StreamedFlowFiles, IntoFlowFiles, StreamedFlowFile};
use crate::FlowFileParsingError;

/// Axum extractor for parsing a stream of NiFi Flow Files from the request body.
///
/// This extractor reads the entire request body as a stream of NiFi Flow Files (v3).
/// Each flow file is parsed lazily - only the header is read initially, with content
/// streamed on-demand.
///
/// # Extractor Pattern
///
/// ```
/// use nifioxide::axum::StreamedFlowFiles;
/// use futures::StreamExt;
///
/// async fn handler(mut stream: StreamedFlowFiles) -> impl axum::response::IntoResponse {
///     while let Some(result) = stream.next().await {
///         match result {
///             Ok(_ff) => {
///                 // Process flow file
///             }
///             Err(e) => {
///                 return (axum::http::StatusCode::BAD_REQUEST, e.to_string());
///             }
///         }
///     }
///     (axum::http::StatusCode::OK, "OK".to_string())
/// }
/// ```
///
/// # Rejection
///
/// Returns `(StatusCode::UNSUPPORTED_MEDIA_TYPE, &str)` if the Content-Type header
/// is missing or not `application/flowfile-v3`.
impl<S> axum::extract::FromRequest<S> for StreamedFlowFiles
where
    S: Send + Sync,
{
    type Rejection = <Self as TryFrom<axum::extract::Request>>::Error;

    async fn from_request(req: axum::extract::Request, _: &S) -> Result<Self, Self::Rejection> {
        req.try_into()
    }
}

impl TryFrom<axum::extract::Request> for StreamedFlowFiles {
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

/// Axum extractor for parsing a single NiFi Flow File from the request body.
///
/// This extractor is a [`Future`] that resolves to a single [`StreamedFlowFile`].
/// It provides a convenient way to handle endpoints that expect exactly one flow file.
///
/// # Usage
///
/// As an extractor, you use it directly as a parameter. Since it's a [`Future`], you await it:
///
/// ```
/// async fn handler(
///     ff: nifioxide::axum::StreamedFlowFileFuture
/// ) -> Result<impl axum::response::IntoResponse, nifioxide::FlowFileParsingError> {
///     let mut _ff = ff.await?;  // Await the future to get the flow file
///     Ok("OK")
/// }
/// ```
///
/// # Errors
///
/// - `(StatusCode::UNSUPPORTED_MEDIA_TYPE, "Only Content-Type: application/flowfile-v3 is accepted.")` - Wrong or missing Content-Type
/// - `FlowFileParsingError::FlowFileExpected` - No flow file in request body (empty request)
/// - `FlowFileParsingError::SingleFlowFileExpected` - Multiple flow files in request body
/// - `FlowFileParsingError::*` - Other parsing errors (malformed flow file, etc.)
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
pub struct StreamedFlowFileFuture(StreamedFlowFiles);

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

        let result: Result<StreamedFlowFiles, _> = req.try_into();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn flow_file_iterator_accepts_flowfile_v3() {
        let req = make_flow_file_request(vec![], "application/flowfile-v3");

        let result: Result<StreamedFlowFiles, _> = req.try_into();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn flow_file_iterator_non_empty_when_non_zero_content_length() {
        let req = Request::builder()
            .header("Content-Type", "application/flowfile-v3")
            .header("Content-Length", "12345")
            .body(Body::empty())
            .unwrap();

        let iter: StreamedFlowFiles = req.try_into().unwrap();
        assert!(!iter.is_empty());
    }

    #[tokio::test]
    async fn flow_file_iterator_empty_when_zero_content_length() {
        let req = Request::builder()
            .header("Content-Type", "application/flowfile-v3")
            .header("Content-Length", "0")
            .body(Body::empty())
            .unwrap();

        let iter: StreamedFlowFiles = req.try_into().unwrap();
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
