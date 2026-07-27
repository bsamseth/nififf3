//! Axum integration: extract flow files from requests and turn flow files
//! into responses, streaming the content in both directions.

use std::future::Future;
use std::io::{self, Write};
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use axum::body::{Body, BodyDataStream};
use axum::extract::{FromRequest, Request};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures_core::Stream;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, DuplexStream, ReadBuf};
use tokio::task::JoinHandle;
use tokio_util::io::{ReaderStream, SyncIoBridge};

use crate::{Error, FlowFile, FlowFilesWriter, FlowFilesWriterAsync, Limits, MEDIA_TYPE};

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

/// How much serialized output may sit between the producer and the socket
/// before writes start waiting.
const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;

type BoxFuture = Pin<Box<dyn Future<Output = crate::Result<()>> + Send>>;

enum Source {
    Producer(Box<dyn FnOnce(FlowFilesWriterAsync<ResponseSink>) -> BoxFuture + Send>),
    Blocking(Box<dyn FnOnce(FlowFilesWriter<BlockingResponseSink>) -> crate::Result<()> + Send>),
    Bytes(Vec<u8>),
}

/// A response carrying *many* flow files, concatenated as NiFi expects them.
///
/// This is the 1-to-many counterpart to [`FlowFileRequest`]: a handler takes
/// one flow file in and answers with a flow file per part it found inside.
/// The parts are produced lazily and streamed out, so neither the number of
/// parts nor the size of any one of them is bounded by memory.
///
/// # Status codes
///
/// Returning a `FlowFilesResponse` *is* the commitment to a 2xx: by the time
/// the producer runs, the status line has been sent. Validate whatever you
/// can up front, while you can still answer with a real status:
///
/// ```no_run
/// use nififf3::{Error, FlowFilesResponse, StrictFlowFileRequest};
///
/// async fn unpack(req: StrictFlowFileRequest) -> Result<FlowFilesResponse, Error> {
///     let parent = req.into_inner().into_bytes_async().await?; // 400 on a bad flow file
///     let mut parts = parent.fragments();
///     Ok(FlowFilesResponse::new(move |mut writer| async move {
///         for line in parent.content().split(|b| *b == b'\n') {
///             writer.write_bytes(&parts.next().content(line)).await?;
///         }
///         Ok(())
///     }))
/// }
/// ```
///
/// Once the producer is running, a problem with an individual part is best
/// reported *as a part* — an otherwise-empty flow file whose attributes say
/// what went wrong — so that the good parts still reach the client. That is
/// ordinary control flow in the producer's loop, so this type has no opinion
/// about it.
///
/// Returning `Err` from the producer instead aborts the response body
/// mid-stream. The client sees a truncated (chunked) response rather than a
/// clean end of body, which is detectable but carries no status code, so
/// prefer the error-part pattern above.
///
/// # When a part failure is still recoverable
///
/// The V3 format writes a part's size *before* its content, so
/// [`write`](FlowFilesWriterAsync::write) commits to the declared size before
/// reading a byte of it. Which failures you can report as attributes follows
/// from that:
///
/// - A failure found *before* the part is written — a bad archive header, an
///   entry that cannot be opened — can always become an error part.
/// - A failure found *while* streaming a part's content cannot. Its size is
///   already on the wire, so the only honest outcome is to abort the body.
///
/// To report content-level failures as attributes, read the part into memory
/// first and write it with
/// [`write_bytes`](FlowFilesWriterAsync::write_bytes): the failure surfaces
/// while nothing has been committed. That trades the streaming property for
/// one part's worth of memory, and is worth doing per-part — stream the large
/// entries, buffer the ones whose integrity you want to vouch for.
pub struct FlowFilesResponse {
    source: Source,
    buffer_size: usize,
}

impl std::fmt::Debug for FlowFilesResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlowFilesResponse")
            .field("buffer_size", &self.buffer_size)
            .finish_non_exhaustive()
    }
}

impl FlowFilesResponse {
    /// Stream flow files from an async producer.
    ///
    /// `producer` is handed a [`FlowFilesWriterAsync`] and writes parts to it
    /// in whatever order and shape it likes; the response ends when the
    /// returned future completes. Each `write` resolves once the part has
    /// been handed to the socket, so a slow client applies backpressure
    /// rather than piling up in memory.
    ///
    /// Because parts are written from a reader, their content is never
    /// buffered — this streams a decompressor straight through to the client:
    ///
    /// ```no_run
    /// # use nififf3::{Error, FlowFilesResponse, FlowFileRequest};
    /// # async fn entries() -> Option<(String, u64, tokio::io::Empty)> { None }
    /// async fn unpack(req: FlowFileRequest) -> Result<FlowFilesResponse, Error> {
    ///     let mut parts = req.fragments();
    ///     Ok(FlowFilesResponse::new(move |mut writer| async move {
    ///         while let Some((name, size, entry)) = entries().await {
    ///             writer
    ///                 .write(parts.next().attribute("filename", name).reader(entry, size))
    ///                 .await?;
    ///         }
    ///         Ok(())
    ///     }))
    /// }
    /// ```
    ///
    /// If the client disconnects, the next write fails and the producer is
    /// dropped; a producer that is working rather than writing is aborted
    /// when the response body is dropped.
    pub fn new<F, Fut>(producer: F) -> Self
    where
        F: FnOnce(FlowFilesWriterAsync<ResponseSink>) -> Fut + Send + 'static,
        Fut: Future<Output = crate::Result<()>> + Send + 'static,
    {
        Self {
            source: Source::Producer(Box::new(move |writer| Box::pin(producer(writer)))),
            buffer_size: DEFAULT_BUFFER_SIZE,
        }
    }

    /// Stream flow files from a *blocking* producer, run on a blocking
    /// thread.
    ///
    /// The same shape as [`new`](Self::new) for producers that are
    /// unavoidably synchronous. Prefer `new` where an async decoder exists:
    /// aborting a blocking producer that is mid-computation is not possible,
    /// so it runs to completion (or to its next failing write) even after the
    /// client goes away.
    ///
    /// The parent's content is an [`AsyncRead`]; wrap it in
    /// [`SyncIoBridge`](tokio_util::io::SyncIoBridge) to feed it to a
    /// synchronous decoder.
    pub fn blocking<F>(producer: F) -> Self
    where
        F: FnOnce(FlowFilesWriter<BlockingResponseSink>) -> crate::Result<()> + Send + 'static,
    {
        Self {
            source: Source::Blocking(Box::new(producer)),
            buffer_size: DEFAULT_BUFFER_SIZE,
        }
    }

    /// Stream flow files from a [`Stream`] of in-memory parts.
    ///
    /// A convenience for producers that already yield whole flow files; each
    /// part is held in memory while it is written. A `Stream` error ends the
    /// response the same way an error from [`new`](Self::new)'s producer
    /// does.
    pub fn from_stream<S>(parts: S) -> Self
    where
        S: Stream<Item = crate::Result<FlowFile<Vec<u8>>>> + Send + 'static,
    {
        Self::new(move |mut writer| async move {
            let mut parts = Box::pin(parts);
            while let Some(part) = std::future::poll_fn(|cx| parts.as_mut().poll_next(cx)).await {
                writer.write_bytes(&part?).await?;
            }
            Ok(())
        })
    }

    /// Respond with flow files that are already in memory.
    ///
    /// Unlike the streaming constructors this knows the total length up
    /// front, so the response carries an exact `Content-Length` instead of
    /// being chunked.
    ///
    /// ```
    /// use nififf3::{FlowFile, FlowFilesResponse};
    ///
    /// let parent = FlowFile::builder().attribute("filename", "pair").content(Vec::new());
    /// let mut parts = parent.fragments().with_count(2);
    /// let response = FlowFilesResponse::from_vec(vec![
    ///     parts.next().content(&b"first"[..]),
    ///     parts.next().content(&b"second"[..]),
    /// ]);
    /// ```
    pub fn from_vec(parts: Vec<FlowFile<Vec<u8>>>) -> Self {
        let mut bytes = Vec::new();
        for part in &parts {
            bytes.extend_from_slice(&part.to_bytes());
        }
        Self {
            source: Source::Bytes(bytes),
            buffer_size: DEFAULT_BUFFER_SIZE,
        }
    }

    /// Set how many serialized bytes may be in flight between the producer
    /// and the socket. Defaults to 64 KiB; ignored by
    /// [`from_vec`](Self::from_vec).
    pub fn buffer_size(mut self, bytes: usize) -> Self {
        self.buffer_size = bytes;
        self
    }
}

/// Sets `Content-Type: application/flowfile-v3`. The body is chunked, except
/// for [`FlowFilesResponse::from_vec`], which sets a `Content-Length`.
///
/// A response with no parts at all is a legitimate empty body.
impl IntoResponse for FlowFilesResponse {
    fn into_response(self) -> Response {
        let buffer_size = self.buffer_size;
        match self.source {
            Source::Bytes(bytes) => (
                [
                    (header::CONTENT_TYPE, MEDIA_TYPE.to_string()),
                    (header::CONTENT_LENGTH, bytes.len().to_string()),
                ],
                Body::from(bytes),
            )
                .into_response(),
            Source::Producer(producer) => {
                let (sink, body) = tokio::io::duplex(buffer_size);
                let writer = FlowFilesWriterAsync::new(ResponseSink { inner: sink });
                streamed(body, tokio::spawn(producer(writer)))
            }
            Source::Blocking(producer) => {
                let (sink, body) = tokio::io::duplex(buffer_size);
                let sink = SyncIoBridge::new_with_handle(sink, tokio::runtime::Handle::current());
                let handle = tokio::task::spawn_blocking(move || {
                    producer(FlowFilesWriter::new(BlockingResponseSink { inner: sink }))
                });
                streamed(body, handle)
            }
        }
    }
}

fn streamed(body: DuplexStream, producer: JoinHandle<crate::Result<()>>) -> Response {
    let stream = ProducerStream {
        inner: ReaderStream::new(body),
        producer: Some(producer),
    };
    (
        [(header::CONTENT_TYPE, MEDIA_TYPE.to_string())],
        Body::from_stream(stream),
    )
        .into_response()
}

/// The response body: serialized bytes as the producer writes them, followed
/// by the producer's own outcome.
///
/// Draining the pipe is not enough to call the response complete — a producer
/// that failed after its last successful write must still poison the stream,
/// so that the client sees a truncated body rather than a plausible one.
struct ProducerStream {
    inner: ReaderStream<DuplexStream>,
    producer: Option<JoinHandle<crate::Result<()>>>,
}

impl Stream for ProducerStream {
    type Item = crate::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let Some(producer) = this.producer.as_mut() else {
            return Poll::Ready(None);
        };
        match ready!(Pin::new(&mut this.inner).poll_next(cx)) {
            Some(Ok(chunk)) => Poll::Ready(Some(Ok(chunk))),
            Some(Err(err)) => {
                this.producer = None;
                Poll::Ready(Some(Err(err.into())))
            }
            None => {
                let outcome = ready!(Pin::new(producer).poll(cx));
                this.producer = None;
                Poll::Ready(match outcome {
                    Ok(Ok(())) => None,
                    Ok(Err(err)) => Some(Err(err)),
                    Err(join) => Some(Err(io::Error::other(join).into())),
                })
            }
        }
    }
}

impl Drop for ProducerStream {
    fn drop(&mut self) {
        // Covers a producer that is computing rather than writing; one that
        // is writing notices on its own when the read half goes away.
        if let Some(producer) = &self.producer {
            producer.abort();
        }
    }
}

/// The [`AsyncWrite`] a [`FlowFilesResponse`] producer writes into.
pub struct ResponseSink {
    inner: DuplexStream,
}

impl std::fmt::Debug for ResponseSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResponseSink").finish_non_exhaustive()
    }
}

impl AsyncWrite for ResponseSink {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// The [`Write`] a [`FlowFilesResponse::blocking`] producer writes into.
pub struct BlockingResponseSink {
    inner: SyncIoBridge<DuplexStream>,
}

impl std::fmt::Debug for BlockingResponseSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockingResponseSink")
            .finish_non_exhaustive()
    }
}

impl Write for BlockingResponseSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
