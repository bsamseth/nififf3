//! Axum integration: extract flow files from requests and turn flow files
//! into responses, streaming the content in both directions.

use std::future::Future;
use std::io::{self, Write};
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use axum::RequestExt;
use axum::body::{Body, BodyDataStream};
use axum::extract::{FromRequest, Request};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures_core::Stream;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, DuplexStream, ReadBuf};
use tokio::task::JoinHandle;
use tokio_util::io::{ReaderStream, SyncIoBridge};

use crate::{
    Error, FlowFile, FlowFilesAsync, FlowFilesWriter, FlowFilesWriterAsync, Limits, MEDIA_TYPE,
};

/// A flow file extracted from an axum request.
///
/// The content is an [`AsyncRead`] streaming the request body, limited to
/// the size declared in the flow file header — the content is never
/// buffered in memory by the extractor, so arbitrarily large flow files can
/// be processed incrementally.
///
/// Request bodies are untrusted, so the header is parsed with
/// [`Limits::recommended`]. To use different limits, extract the raw
/// [`axum::body::Body`] and call
/// [`FlowFile::parse_async_with_limits`] on a reader over it.
///
/// # Body size
///
/// The body is read through axum's
/// [`DefaultBodyLimit`](axum::extract::DefaultBodyLimit), like any other
/// extractor's — so it is capped at 2 MiB unless the router says otherwise.
/// That cap is on the bytes the client actually sends, which is the only
/// bound that means anything for a server: [`Limits`] governs the header,
/// and the header's declared content size is a claim, not a promise.
///
/// Streaming a flow file larger than the limit therefore has to say so:
///
/// ```
/// use axum::extract::DefaultBodyLimit;
/// use axum::{Router, routing::post};
/// # async fn handler(_: nififf3::FlowFileRequest) {}
///
/// let app: Router = Router::new()
///     .route("/large", post(handler))
///     .layer(DefaultBodyLimit::max(8 * 1024 * 1024 * 1024)); // or ::disable()
/// ```
///
/// Destructure it in the handler signature, as with axum's own extractors:
///
/// ```no_run
/// use nififf3::FlowFileRequest;
///
/// async fn handler(FlowFileRequest(flow_file): FlowFileRequest) -> Result<String, nififf3::Error> {
///     let flow_file = flow_file.into_memory_async().await?;
///     Ok(format!("got {} bytes", flow_file.size()))
/// }
/// ```
///
/// It also dereferences to the flow file, for reading attributes without
/// taking it apart.
#[derive(Debug)]
pub struct FlowFileRequest(pub FlowFile<tokio::io::Take<FlowFileBody>>);

impl std::ops::Deref for FlowFileRequest {
    type Target = FlowFile<tokio::io::Take<FlowFileBody>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for FlowFileRequest {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// [`AsyncRead`] adapter over an axum request body.
///
/// The extractors build one for you. Construct it directly — with
/// [`from_body`](Self::from_body) or the equivalent `From` — to parse a body
/// some other way than they do: different [`Limits`], say, or a flow file
/// followed by something that is not one.
///
/// Note that a [`Body`] taken straight off a request has *not* been through
/// axum's [`DefaultBodyLimit`](axum::extract::DefaultBodyLimit); the
/// extractors apply it by calling
/// [`RequestExt::with_limited_body`](axum::RequestExt::with_limited_body)
/// first, and anything reading a body by hand should do the same.
pub struct FlowFileBody {
    stream: BodyDataStream,
    chunk: Bytes,
    /// Whether `stream` has already reported the end of the body. Polling a
    /// [`Stream`] after it returns `None` is contractually undefined, and a
    /// reader is free to keep reading past an end it has already been told
    /// about — a flow file declaring more content than the body carries makes
    /// that the ordinary case, not a pathological one.
    ended: bool,
}

impl FlowFileBody {
    /// Read an axum request body as an [`AsyncRead`].
    ///
    /// ```no_run
    /// use axum::extract::Request;
    /// use axum::RequestExt as _;
    /// use nififf3::{FlowFileBody, FlowFilesAsync, Limits};
    ///
    /// async fn handler(request: Request) -> Result<usize, nififf3::Error> {
    ///     // `with_limited_body` is what applies `DefaultBodyLimit`.
    ///     let body = FlowFileBody::from_body(request.with_limited_body().into_body());
    ///     let mut flow_files = FlowFilesAsync::with_limits(body, Limits::recommended());
    ///
    ///     let mut count = 0;
    ///     while let Some(flow_file) = flow_files.next().await {
    ///         flow_file?;
    ///         count += 1;
    ///     }
    ///     Ok(count)
    /// }
    /// ```
    #[must_use]
    pub fn from_body(body: Body) -> Self {
        Self {
            stream: body.into_data_stream(),
            chunk: Bytes::new(),
            ended: false,
        }
    }
}

impl From<Body> for FlowFileBody {
    fn from(body: Body) -> Self {
        Self::from_body(body)
    }
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
            if this.ended {
                return Poll::Ready(Ok(()));
            }
            match Pin::new(&mut this.stream).poll_next(cx) {
                Poll::Ready(Some(Ok(chunk))) => this.chunk = chunk,
                Poll::Ready(Some(Err(err))) => {
                    this.ended = true;
                    return Poll::Ready(Err(io::Error::other(err)));
                }
                Poll::Ready(None) => {
                    this.ended = true;
                    return Poll::Ready(Ok(()));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// The request body as an [`AsyncRead`], with axum's
/// [`DefaultBodyLimit`](axum::extract::DefaultBodyLimit) applied.
///
/// `with_limited_body` is what applies it; reading `req.into_body()` directly
/// would silently opt out.
fn limited_body(req: Request) -> FlowFileBody {
    FlowFileBody::from_body(req.with_limited_body().into_body())
}

impl<S: Send + Sync> FromRequest<S> for FlowFileRequest {
    type Rejection = Error;

    async fn from_request(req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        FlowFile::parse_async_with_limits(limited_body(req), Limits::recommended())
            .await
            .map(Self)
    }
}

/// Like [`FlowFileRequest`], but additionally requires the request to carry
/// `Content-Type: application/flowfile-v3`.
///
/// A missing or different content type is rejected with
/// `415 Unsupported Media Type` before the body is parsed. Media type
/// parameters (e.g. a `charset`) are ignored in the comparison.
///
/// Wraps the same flow file [`FlowFileRequest`] does, and is used the same
/// way — destructured, or dereferenced:
///
/// ```no_run
/// use nififf3::StrictFlowFileRequest;
///
/// async fn handler(
///     StrictFlowFileRequest(flow_file): StrictFlowFileRequest,
/// ) -> Result<String, nififf3::Error> {
///     let flow_file = flow_file.into_memory_async().await?;
///     Ok(format!("got {} bytes", flow_file.size()))
/// }
/// ```
#[derive(Debug)]
pub struct StrictFlowFileRequest(pub FlowFile<tokio::io::Take<FlowFileBody>>);

impl std::ops::Deref for StrictFlowFileRequest {
    type Target = FlowFile<tokio::io::Take<FlowFileBody>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for StrictFlowFileRequest {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// How much of a rejected `Content-Type` is named in the rejection.
///
/// The value comes from the client, and it goes back to the client in an error
/// body, so it is bounded here: enough to tell what was sent, not however much
/// was sent.
const MAX_ECHOED_CONTENT_TYPE: usize = 64;

/// The head of `value`, cut to a character boundary, with an ellipsis if
/// anything was dropped.
fn abbreviated(value: &str) -> String {
    if value.len() <= MAX_ECHOED_CONTENT_TYPE {
        return value.to_owned();
    }
    let mut end = MAX_ECHOED_CONTENT_TYPE;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &value[..end])
}

/// Reject the request unless it carries `Content-Type: application/flowfile-v3`.
///
/// Compares the media type only, ignoring any parameters. The comparison sees
/// the whole header; only what travels back to the client is abbreviated.
fn require_media_type(req: &Request) -> Result<(), StrictRejection> {
    let content_type = req
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok());
    let media_type = content_type.map(|value| value.split(';').next().unwrap_or("").trim());
    if media_type.is_some_and(|value| value.eq_ignore_ascii_case(MEDIA_TYPE)) {
        Ok(())
    } else {
        Err(StrictRejection::UnsupportedMediaType(
            content_type.map(abbreviated),
        ))
    }
}

/// Rejection returned by the strict extractors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StrictRejection {
    /// The request's `Content-Type` was missing or not
    /// `application/flowfile-v3`; responds with `415 Unsupported Media Type`.
    ///
    /// The value carried here is abbreviated to the first 64 bytes, since it
    /// is reflected into the response body.
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
        require_media_type(&req)?;
        Ok(Self(FlowFileRequest::from_request(req, state).await?.0))
    }
}

/// Every flow file in a request body, read one at a time.
///
/// The many-in counterpart to [`FlowFilesResponse`], and what NiFi's own
/// `PostHTTP` sends: several flow files concatenated under one
/// `application/flowfile-v3` request. [`FlowFileRequest`] parses exactly one
/// and rejects a body with more.
///
/// Each flow file's content is buffered in memory as it is yielded — the
/// number of them is unbounded, the size of any one is bounded by
/// [`DefaultBodyLimit`](axum::extract::DefaultBodyLimit) as usual. To stream
/// the contents instead, build a [`FlowFileBody`] and drive
/// [`FlowFile::parse_next_async`] over it yourself.
///
/// Headers are parsed with [`Limits::recommended`], as for the single-flow-file
/// extractors. Parse errors surface from [`next`](FlowFilesAsync::next) rather
/// than from extraction, since nothing is read until then: the extractor itself
/// only fails if the request cannot be taken apart at all.
///
/// ```no_run
/// use nififf3::FlowFilesRequest;
///
/// async fn handler(
///     FlowFilesRequest(mut flow_files): FlowFilesRequest,
/// ) -> Result<String, nififf3::Error> {
///     let mut count = 0;
///     while let Some(flow_file) = flow_files.next().await {
///         let flow_file = flow_file?;
///         count += flow_file.size();
///     }
///     Ok(format!("{count} content bytes in total"))
/// }
/// ```
#[derive(Debug)]
pub struct FlowFilesRequest(pub FlowFilesAsync<FlowFileBody>);

impl std::ops::Deref for FlowFilesRequest {
    type Target = FlowFilesAsync<FlowFileBody>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for FlowFilesRequest {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<S: Send + Sync> FromRequest<S> for FlowFilesRequest {
    type Rejection = Error;

    async fn from_request(req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(Self(FlowFilesAsync::with_limits(
            limited_body(req),
            Limits::recommended(),
        )))
    }
}

/// [`FlowFilesRequest`], additionally requiring
/// `Content-Type: application/flowfile-v3`.
///
/// The many-in counterpart to [`StrictFlowFileRequest`], rejecting a missing or
/// different content type with `415 Unsupported Media Type` before any of the
/// body is read.
#[derive(Debug)]
pub struct StrictFlowFilesRequest(pub FlowFilesAsync<FlowFileBody>);

impl std::ops::Deref for StrictFlowFilesRequest {
    type Target = FlowFilesAsync<FlowFileBody>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for StrictFlowFilesRequest {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<S: Send + Sync> FromRequest<S> for StrictFlowFilesRequest {
    type Rejection = StrictRejection;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        require_media_type(&req)?;
        Ok(Self(FlowFilesRequest::from_request(req, state).await?.0))
    }
}

/// Respond with the flow file in binary V3 format, streaming the content.
///
/// Sets `Content-Type: application/flowfile-v3` and a `Content-Length`
/// computed from the header and the declared content size. Exactly
/// [`size`](FlowFile::size) bytes are read from the content reader.
///
/// The bound is on the *content*, so this covers every reader-backed flow
/// file but not an in-memory `FlowFile<Vec<u8>>`, since `Vec<u8>` is not an
/// [`AsyncRead`] (and coherence forbids a second impl for it). Call
/// [`into_reader`](FlowFile::into_reader) on those — it wraps the content in
/// a [`std::io::Cursor`], which is one:
///
/// ```
/// use axum::response::IntoResponse;
/// use nififf3::FlowFile;
///
/// let flow_file = FlowFile::builder().content(&b"hi"[..]);
/// let response = flow_file.into_reader().into_response();
/// # let _ = response;
/// ```
///
/// # Truncated content
///
/// The `Content-Length` is committed before a content byte has been read, so a
/// reader that ends before [`size`](FlowFile::size) cannot be reported as a
/// status — by then the headers are gone. It fails the body instead, the way
/// [`FlowFilesResponse`] does for a producer error: the client sees an aborted
/// response rather than a complete one carrying a flow file whose header
/// declares more content than it holds. A reader with *more* than `size` is
/// cut to it, as everywhere else in this crate.
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
            Body::from_stream(ExactLength {
                inner: Box::pin(ReaderStream::new(reader)),
                declared: size,
                remaining: total,
                done: false,
            }),
        )
            .into_response()
    }
}

/// Fails the body if the wrapped reader yields fewer bytes than the response
/// committed to, rather than ending it short of its `Content-Length`.
///
/// The header is served from an in-memory cursor and so always arrives whole;
/// any shortfall is content, which is what the error reports.
struct ExactLength {
    inner: Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send>>,
    declared: u64,
    remaining: u64,
    done: bool,
}

impl Stream for ExactLength {
    type Item = io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        match ready!(this.inner.as_mut().poll_next(cx)) {
            Some(Ok(chunk)) => {
                this.remaining = this.remaining.saturating_sub(chunk.len() as u64);
                Poll::Ready(Some(Ok(chunk)))
            }
            Some(Err(err)) => {
                this.done = true;
                Poll::Ready(Some(Err(err)))
            }
            None => {
                this.done = true;
                Poll::Ready(if this.remaining == 0 {
                    None
                } else {
                    // Saturating because `remaining` covers the header too:
                    // the header is served from an in-memory cursor and always
                    // drains, but nothing here depends on that being true.
                    let delivered = this.declared.saturating_sub(this.remaining);
                    Some(Err(crate::error::truncated(this.declared, delivered)))
                })
            }
        }
    }
}

/// Whether `err`, or anything in its source chain, is axum's
/// [`DefaultBodyLimit`](axum::extract::DefaultBodyLimit) being hit.
///
/// The limit is enforced by the body, several layers below the reader this
/// crate parses from, so it arrives as an opaque [`io::Error`] like any other
/// transport failure. Only the chain distinguishes it.
///
/// That makes this a guess about how axum nests its errors, checked against
/// axum 0.8 and http-body-util 0.1. If either re-wraps `LengthLimitError` the
/// check stops matching and an over-large body silently becomes a 400 rather
/// than a 413 — `extractor_honours_the_default_body_limit` in `tests/axum.rs`
/// is what notices.
fn is_body_limit(err: &(dyn std::error::Error + Send + Sync + 'static)) -> bool {
    // Matches `io::Error::get_ref`; the chain itself drops the auto traits.
    let err: &(dyn std::error::Error + 'static) = err;
    std::iter::successors(Some(err), |err| err.source())
        .any(<dyn std::error::Error + 'static>::is::<http_body_util::LengthLimitError>)
}

/// Respond with the error message as the body, under `400 Bad Request` —
/// except for the size failures, which are `413 Payload Too Large`, since the
/// input was well-formed and merely too big. Those are the [`Limits`] ones,
/// plus a body that ran past axum's
/// [`DefaultBodyLimit`](axum::extract::DefaultBodyLimit).
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = match self {
            Error::TooManyAttributes { .. }
            | Error::AttributeTooLong { .. }
            | Error::HeaderTooLarge { .. }
            | Error::ContentTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            Error::Io(ref err) if err.get_ref().is_some_and(is_body_limit) => {
                StatusCode::PAYLOAD_TOO_LARGE
            }
            _ => StatusCode::BAD_REQUEST,
        };
        (status, self.to_string()).into_response()
    }
}

/// How much serialized output may sit between the producer and the socket
/// before writes start waiting.
const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;

/// Any error, boxed: what a [`FlowFilesResponse`] producer reports failure
/// with.
///
/// The same alias as [`axum::BoxError`], and the same type the response body
/// would erase a producer's error to in any case. Because every
/// `std::error::Error` converts into it, a producer can use `?` on whatever
/// its decoder, its I/O or this crate hands it without converting between
/// error types first.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

type BoxFuture = Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send>>;

enum Source {
    Producer(Box<dyn FnOnce(FlowFilesWriterAsync<ResponseSink>) -> BoxFuture + Send>),
    Blocking(Box<dyn FnOnce(FlowFilesWriter<BlockingResponseSink>) -> Result<(), BoxError> + Send>),
    Bytes(Vec<u8>),
}

/// A response carrying *many* flow files, concatenated as NiFi expects them.
///
/// The 1-to-many counterpart to [`FlowFileRequest`]: a handler takes one flow
/// file in and answers with a flow file per part it found inside. Parts are
/// produced lazily and streamed, so neither their number nor the size of any
/// one of them is bounded by memory. See [`new`](Self::new) for the shape of a
/// handler.
///
/// # Status codes
///
/// Returning a `FlowFilesResponse` *is* the commitment to a 2xx: by the time
/// the producer runs, the status line has been sent. Validate up front, while
/// a real status code is still available, and report a problem with an
/// individual part *as a part* — a flow file whose attributes say what went
/// wrong — so the good parts still arrive. Returning `Err` from the producer
/// instead aborts the body, leaving the client with a truncated response and
/// no status to explain it.
///
/// Which failures can be reported as a part follows from the format writing a
/// part's size *before* its content:
///
/// - Found before the part is written — a bad archive header, an entry that
///   will not open — always reportable.
/// - Found while [`write`](FlowFilesWriterAsync::write) streams a part's
///   content — not reportable, since the size is already on the wire.
///
/// To vouch for a part's content, read it into memory and use
/// [`write_bytes`](FlowFilesWriterAsync::write_bytes) instead, which learns of
/// the failure before committing to anything. That is worth deciding per part:
/// stream the large ones, buffer the ones whose integrity matters.
///
/// # Panics
///
/// Turning one of the streaming variants into a response spawns the producer,
/// so it must happen inside a tokio runtime — which an axum handler always is.
/// Converting one by hand outside a runtime panics.
/// [`buffered`](Self::buffered) spawns nothing and is fine anywhere.
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
    /// in whatever shape it likes; the response ends when the future
    /// completes. A write resolves once its bytes are in the response body,
    /// which runs at most [`buffer_size`](Self::buffer_size) ahead of the
    /// socket — so a slow client applies backpressure within a buffer rather
    /// than filling memory, and writing a part from a reader never buffers its
    /// content.
    ///
    /// Failure is reported as a [`BoxError`], so `?` works directly on
    /// whatever a decoder, plain I/O or this crate returns — a producer never
    /// has to convert between error types to satisfy this signature. A
    /// producer that already has its own error type can adapt with
    /// `.map_err(Into::into)`.
    ///
    /// ```
    /// use axum::response::IntoResponse;
    /// use http_body_util::BodyExt;
    /// use nififf3::{FlowFile, FlowFilesAsync, FlowFilesResponse};
    ///
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// # #[cfg(feature = "uuid")] {
    /// let parent = FlowFile::builder()
    ///     .attribute("filename", "pair.txt")
    ///     .content(&b"first\nsecond"[..]);
    /// let mut parts = parent.fragments();
    ///
    /// let response = FlowFilesResponse::new(move |mut writer| async move {
    ///     for line in parent.content().split(|byte| *byte == b'\n') {
    ///         // `line` is a reader, so its content is never copied into a part.
    ///         writer.write(parts.next_part().reader(line, line.len() as u64)).await?;
    ///     }
    ///     Ok(())
    /// })
    /// .into_response();
    ///
    /// let body = response.into_body().collect().await.unwrap().to_bytes();
    /// let mut flow_files = FlowFilesAsync::new(body.as_ref());
    ///
    /// let first = flow_files.next().await.unwrap().unwrap();
    /// assert_eq!(first.content().as_slice(), b"first");
    /// assert_eq!(first.attributes()["fragment.index"], "1");
    /// assert_eq!(first.attributes()["segment.original.filename"], "pair.txt");
    /// # }
    /// # });
    /// ```
    ///
    /// If the client disconnects the next write fails; a producer that is
    /// working rather than writing is aborted when the body is dropped.
    pub fn new<F, Fut>(producer: F) -> Self
    where
        F: FnOnce(FlowFilesWriterAsync<ResponseSink>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        Self {
            source: Source::Producer(Box::new(move |writer| Box::pin(producer(writer)))),
            buffer_size: DEFAULT_BUFFER_SIZE,
        }
    }

    /// Stream flow files from a *blocking* producer, run on a blocking
    /// thread.
    ///
    /// The same shape as [`new`](Self::new), for producers that are
    /// unavoidably synchronous. Prefer `new` where an async decoder exists: a
    /// blocking producer cannot be aborted mid-computation, so it keeps
    /// running after the client goes away until its next write fails.
    ///
    /// The parent's content is an [`AsyncRead`]; wrap it in
    /// [`SyncIoBridge`](tokio_util::io::SyncIoBridge) to feed a synchronous
    /// decoder.
    pub fn blocking<F>(producer: F) -> Self
    where
        F: FnOnce(FlowFilesWriter<BlockingResponseSink>) -> Result<(), BoxError> + Send + 'static,
    {
        Self {
            source: Source::Blocking(Box::new(producer)),
            buffer_size: DEFAULT_BUFFER_SIZE,
        }
    }

    /// Stream flow files from a [`Stream`] of in-memory parts.
    ///
    /// A convenience for producers that already yield whole flow files. The
    /// stream's error type is free — it only has to convert into a
    /// [`BoxError`] — and a `Stream` error ends the response the same way an
    /// error from [`new`](Self::new)'s producer does.
    pub fn from_stream<S, E>(parts: S) -> Self
    where
        S: Stream<Item = Result<FlowFile<Vec<u8>>, E>> + Send + 'static,
        E: Into<BoxError> + Send,
    {
        Self::new(move |mut writer| async move {
            let mut parts = Box::pin(parts);
            while let Some(part) = std::future::poll_fn(|cx| parts.as_mut().poll_next(cx)).await {
                writer.write_bytes(&part.map_err(Into::into)?).await?;
            }
            Ok(())
        })
    }

    /// Respond with flow files that are already in memory.
    ///
    /// Unlike the streaming constructors this knows the total length up
    /// front, so the response carries an exact `Content-Length` instead of
    /// being chunked. That is what it costs: every part is serialized into one
    /// buffer before the response starts, so the whole body sits in memory at
    /// once. Use [`new`](Self::new) or [`from_stream`](Self::from_stream) when
    /// that is not a trade worth making.
    ///
    /// Takes anything iterable, so a `Vec`, an array, or a mapped iterator all
    /// work without collecting first.
    ///
    /// ```
    /// use axum::http::header;
    /// use axum::response::IntoResponse;
    /// use nififf3::{FlowFile, FlowFilesResponse};
    ///
    /// let response = FlowFilesResponse::buffered([
    ///     FlowFile::builder().content(&b"first"[..]),
    ///     FlowFile::builder().content(&b"second"[..]),
    /// ])
    /// .into_response();
    ///
    /// assert!(response.headers().contains_key(header::CONTENT_LENGTH));
    /// ```
    ///
    /// # Panics
    ///
    /// As [`FlowFile::to_bytes`]: an attribute the wire format cannot express,
    /// or a part whose declared size disagrees with its content.
    #[must_use]
    pub fn buffered(parts: impl IntoIterator<Item = FlowFile<Vec<u8>>>) -> Self {
        let parts = parts.into_iter();
        let mut bytes = Vec::new();
        // The parts arrive one at a time, so the total is not known up front;
        // reserving each one's exact length as it comes is the next best
        // thing, and keeps the body out of the doubling sequence a plain
        // `extend` would put it through.
        for part in parts {
            // A part whose length does not fit a `usize` cannot be in memory to
            // begin with, so this is unreachable; reserving nothing is still
            // the right fallback, since `write_bytes_to` grows the buffer on
            // its own and asking for `usize::MAX` would abort the process.
            let len = usize::try_from(part.serialized_len()).unwrap_or(0);
            bytes.reserve(len);
            part.write_bytes_to(&mut bytes)
                .expect("writing to a Vec cannot fail");
        }
        Self {
            source: Source::Bytes(bytes),
            buffer_size: DEFAULT_BUFFER_SIZE,
        }
    }

    /// Set how many serialized bytes may be in flight between the producer
    /// and the socket. Defaults to 64 KiB; ignored by
    /// [`buffered`](Self::buffered).
    ///
    /// Rounded up to at least one byte: a buffer of zero can never accept a
    /// write, so the producer would park on its first one and the response
    /// would hang rather than fail. One byte is pathological but it makes
    /// progress; pick a real size for a real response.
    #[must_use]
    pub fn buffer_size(mut self, bytes: usize) -> Self {
        self.buffer_size = bytes.max(1);
        self
    }
}

/// Sets `Content-Type: application/flowfile-v3`. The body is chunked, except
/// for [`FlowFilesResponse::buffered`], which sets a `Content-Length`.
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

fn streamed(body: DuplexStream, producer: JoinHandle<Result<(), BoxError>>) -> Response {
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
    producer: Option<JoinHandle<Result<(), BoxError>>>,
}

impl Stream for ProducerStream {
    type Item = Result<Bytes, BoxError>;

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
                    Err(join) => Some(Err(join.into())),
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
