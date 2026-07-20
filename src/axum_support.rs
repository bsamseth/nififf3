//! Axum integration: extract flow files from requests and turn flow files
//! into responses, streaming the content in both directions.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use axum::body::{Body, BodyDataStream};
use axum::extract::{FromRequest, Request};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures_core::Stream;
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
use tokio_util::io::ReaderStream;

use crate::{Error, FlowFile, Limits, MEDIA_TYPE};

/// A flow file extracted from an axum request.
///
/// The content is an [`AsyncRead`] streaming the request body, limited to
/// the size declared in the flow file header — the content is never
/// buffered in memory by the extractor, so arbitrarily large flow files can
/// be processed incrementally.
///
/// Request bodies are untrusted, so the header is parsed with
/// [`Limits::default`]. To use different limits, extract the raw
/// [`axum::body::Body`] and call
/// [`FlowFile::parse_async_with_limits`] on a reader over it.
///
/// ```no_run
/// use nififf3::FlowFileRequest;
///
/// async fn handler(flow_file: FlowFileRequest) -> Result<String, nififf3::Error> {
///     let flow_file = flow_file.into_bytes_async().await?;
///     Ok(format!("got {} bytes", flow_file.size()))
/// }
/// ```
pub type FlowFileRequest = FlowFile<tokio::io::Take<FlowFileBody>>;

/// [`AsyncRead`] adapter over an axum request body.
pub struct FlowFileBody {
    stream: BodyDataStream,
    chunk: Bytes,
}

impl std::fmt::Debug for FlowFileBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlowFileBody").finish_non_exhaustive()
    }
}

impl AsyncRead for FlowFileBody {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            if !this.chunk.is_empty() {
                let n = this.chunk.len().min(buf.remaining());
                buf.put_slice(&this.chunk.split_to(n));
                return Poll::Ready(Ok(()));
            }
            match Pin::new(&mut this.stream).poll_next(cx) {
                Poll::Ready(Some(Ok(chunk))) => this.chunk = chunk,
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(io::Error::other(err))),
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<S: Send + Sync> FromRequest<S> for FlowFileRequest {
    type Rejection = Error;

    async fn from_request(req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        let body = FlowFileBody {
            stream: req.into_body().into_data_stream(),
            chunk: Bytes::new(),
        };
        FlowFile::parse_async_with_limits(body, &Limits::default()).await
    }
}

/// Like [`FlowFileRequest`], but additionally requires the request to carry
/// `Content-Type: application/flowfile-v3`.
///
/// A missing or different content type is rejected with
/// `415 Unsupported Media Type` before the body is parsed. Media type
/// parameters (e.g. a `charset`) are ignored in the comparison. The wrapper
/// dereferences to the inner flow file.
///
/// ```no_run
/// use nififf3::StrictFlowFileRequest;
///
/// async fn handler(flow_file: StrictFlowFileRequest) -> Result<String, nififf3::Error> {
///     let flow_file = flow_file.into_inner().into_bytes_async().await?;
///     Ok(format!("got {} bytes", flow_file.size()))
/// }
/// ```
#[derive(Debug)]
pub struct StrictFlowFileRequest(pub FlowFileRequest);

impl StrictFlowFileRequest {
    /// Unwrap into the inner [`FlowFileRequest`].
    pub fn into_inner(self) -> FlowFileRequest {
        self.0
    }
}

impl std::ops::Deref for StrictFlowFileRequest {
    type Target = FlowFileRequest;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for StrictFlowFileRequest {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Rejection returned by the [`StrictFlowFileRequest`] extractor.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StrictRejection {
    /// The request's `Content-Type` was missing or not
    /// `application/flowfile-v3`; responds with `415 Unsupported Media Type`.
    #[error("expected content type \"application/flowfile-v3\", got {0:?}")]
    UnsupportedMediaType(Option<String>),

    /// The body was not a valid flow file; responds with `400 Bad Request`.
    #[error(transparent)]
    Parse(#[from] Error),
}

impl IntoResponse for StrictRejection {
    fn into_response(self) -> Response {
        match self {
            Self::UnsupportedMediaType(_) => {
                (StatusCode::UNSUPPORTED_MEDIA_TYPE, self.to_string()).into_response()
            }
            Self::Parse(err) => err.into_response(),
        }
    }
}

impl<S: Send + Sync> FromRequest<S> for StrictFlowFileRequest {
    type Rejection = StrictRejection;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let content_type = req
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        // Compare the media type only, ignoring any parameters.
        let media_type = content_type
            .as_deref()
            .map(|value| value.split(';').next().unwrap_or("").trim());
        if !media_type.is_some_and(|value| value.eq_ignore_ascii_case(MEDIA_TYPE)) {
            return Err(StrictRejection::UnsupportedMediaType(content_type));
        }
        Ok(Self(FlowFileRequest::from_request(req, state).await?))
    }
}

/// Respond with the flow file in binary V3 format, streaming the content.
///
/// Sets `Content-Type: application/flowfile-v3` and a `Content-Length`
/// computed from the header and the declared content size. Exactly
/// [`size`](FlowFile::size) bytes are read from the content reader.
impl<R> IntoResponse for FlowFile<R>
where
    R: AsyncRead + Send + 'static,
{
    fn into_response(self) -> Response {
        let header_bytes = self.header_bytes();
        let total = header_bytes.len() as u64 + self.size;
        let (size, _attributes, content) = self.into_parts();
        let reader = std::io::Cursor::new(header_bytes).chain(content.take(size));
        (
            [
                (header::CONTENT_TYPE, MEDIA_TYPE.to_string()),
                (header::CONTENT_LENGTH, total.to_string()),
            ],
            Body::from_stream(ReaderStream::new(reader)),
        )
            .into_response()
    }
}

/// Respond with `400 Bad Request` and the error message as the body.
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        (StatusCode::BAD_REQUEST, self.to_string()).into_response()
    }
}
