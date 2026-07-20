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

use crate::{Error, FlowFile, MEDIA_TYPE};

/// A flow file extracted from an axum request.
///
/// The content is an [`AsyncRead`] streaming the request body, limited to
/// the size declared in the flow file header — the content is never
/// buffered in memory by the extractor, so arbitrarily large flow files can
/// be processed incrementally.
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
        FlowFile::parse_async(body).await
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
