//! Axum integration: extract flow files from requests and turn flow files
//! into responses.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use axum::RequestExt;
use axum::body::{Body, BodyDataStream};
use axum::extract::{FromRequest, Request};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures_core::Stream;
use tokio::io::{AsyncRead, AsyncReadExt, DuplexStream, ReadBuf};
use tokio::task::JoinHandle;
use tokio_util::io::{ReaderStream, SyncIoBridge};

use crate::spool::{self, Side};
use crate::{
    Error, FlowFile, FlowFilesWriter, FlowFilesWriterAsync, MEDIA_TYPE, Spool, SpoolConfig,
};

/// How large the chunks of a streamed response body are.
const RESPONSE_CHUNK: usize = 64 * 1024;

/// [`AsyncRead`] adapter over an axum request body.
///
/// The extractors read through one of these. Build one yourself to parse a
/// body some other way, such as to stream a request's content instead of
/// spooling it.
///
/// # Streaming a request body is a deadlock waiting to happen
///
/// NiFi's HTTP client sends the whole request before it reads any of the
/// response. A handler that streams its response while still reading the
/// request off the socket stalls as soon as the response outgrows the socket
/// buffers: the handler waits for the client to read, and the client waits
/// for the handler to finish reading. The extractors avoid this by reading
/// the whole request before the handler runs. A handler that reads this type
/// directly has to keep that in mind, and `tests/nifi_client.rs` demonstrates
/// the stall over a real socket.
///
/// A [`Body`] taken straight off a request has not been through axum's
/// [`DefaultBodyLimit`](axum::extract::DefaultBodyLimit). The extractors apply
/// it by calling
/// [`RequestExt::with_limited_body`](axum::RequestExt::with_limited_body)
/// first, and anything reading a body by hand should do the same.
///
/// ```no_run
/// use axum::extract::Request;
/// use axum::RequestExt as _;
/// use nififf3::{FlowFile, FlowFileBody};
///
/// async fn handler(request: Request) -> Result<u64, nififf3::Error> {
///     let body = FlowFileBody::new(request.with_limited_body().into_body());
///     let flow_file = FlowFile::parse_async(body).await?;
///     Ok(flow_file.size())
/// }
/// ```
pub struct FlowFileBody {
    stream: BodyDataStream,
    chunk: Bytes,
    /// Whether `stream` has already reported the end of the body. Polling a
    /// [`Stream`] after it returns `None` is contractually undefined, and a
    /// reader is free to keep reading past an end it has already been told
    /// about.
    ended: bool,
}

impl FlowFileBody {
    /// Read an axum request body as an [`AsyncRead`].
    #[must_use]
    pub fn new(body: Body) -> Self {
        Self {
            stream: body.into_data_stream(),
            chunk: Bytes::new(),
            ended: false,
        }
    }
}

impl From<Body> for FlowFileBody {
    fn from(body: Body) -> Self {
        Self::new(body)
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

/// Why a request could not be extracted into a flow file or a [`Spool`].
///
/// Each variant answers with the status it deserves: what the client sent
/// wrong is a `4xx`, and what the server could not do is a `500`. The body of
/// the response is the message, so the client's log says what happened.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Rejection {
    /// The body is not a flow file, or not exactly one. Answered with
    /// `400 Bad Request`.
    #[error("request body is not a FlowFile V3{}: {error}", described(.content_type.as_deref()))]
    Parse {
        /// What was wrong with it.
        error: Error,
        /// The request's `Content-Type`, which is the first thing to check
        /// when a client sends raw content instead of a flow file.
        content_type: Option<String>,
    },

    /// The body ran past axum's
    /// [`DefaultBodyLimit`](axum::extract::DefaultBodyLimit). Answered with
    /// `413 Payload Too Large`.
    ///
    /// The limit is 2 MiB unless the router raises it, and a flow file is
    /// often larger than that. See [`DefaultBodyLimit::max`][max].
    ///
    /// [max]: axum::extract::DefaultBodyLimit::max
    #[error(
        "request body exceeds axum's DefaultBodyLimit, which is 2 MiB unless \
         the router raises it with DefaultBodyLimit::max"
    )]
    BodyTooLarge,

    /// The body could not be read: the client went away, or the connection
    /// failed. Answered with `400 Bad Request`, for what that is worth to a
    /// client that is no longer there.
    #[error("failed to read the request body: {0}")]
    Body(#[source] io::Error),

    /// The body could not be spooled to disk. Answered with
    /// `500 Internal Server Error`, because the request was fine and the
    /// server was not. See [`SpoolConfig::with_directory`].
    #[error("failed to spool the request body to disk: {0}")]
    Spool(#[source] io::Error),
}

/// The content type as a parenthetical, for the parse rejection message.
fn described(content_type: Option<&str>) -> String {
    match content_type {
        Some(content_type) => format!(" (Content-Type: {content_type})"),
        None => String::new(),
    }
}

impl Rejection {
    /// The status this rejection answers with.
    #[must_use]
    pub fn status(&self) -> StatusCode {
        match self {
            Self::Parse { .. } | Self::Body(_) => StatusCode::BAD_REQUEST,
            Self::BodyTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::Spool(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Say what happened, since the handler never runs to say it.
    #[cfg_attr(not(feature = "tracing"), expect(clippy::unused_self))]
    fn log(&self) {
        #[cfg(feature = "tracing")]
        match self {
            Self::Spool(error) => {
                tracing::error!(error = %error, "rejecting request: spooling failed");
            }
            other => tracing::warn!(error = %other, status = %other.status(), "rejecting request"),
        }
    }

    /// Sort an error from reading or parsing the body into a rejection.
    fn from_error(error: Error, content_type: Option<&str>) -> Self {
        match error {
            Error::Io(error) if error.get_ref().is_some_and(is_body_limit) => Self::BodyTooLarge,
            Error::Io(error) => Self::Body(error),
            error => Self::Parse {
                error,
                content_type: content_type.map(str::to_owned),
            },
        }
    }

    fn from_spool(error: spool::SpoolError, content_type: Option<&str>) -> Self {
        match error.side {
            // A short body is a parse failure, and `Error::from` recovers it
            // as a `SizeMismatch`. Anything else from the source is the body.
            Side::Source => Self::from_error(Error::from(error.error), content_type),
            Side::Sink => Self::Spool(error.error),
        }
    }
}

impl IntoResponse for Rejection {
    fn into_response(self) -> Response {
        (self.status(), self.to_string()).into_response()
    }
}

/// Whether `err`, or anything in its source chain, is axum's
/// [`DefaultBodyLimit`](axum::extract::DefaultBodyLimit) being hit.
///
/// The limit is enforced by the body, several layers below the reader this
/// crate parses from, so it arrives as an opaque [`io::Error`] like any other
/// transport failure. Only the chain distinguishes it. This is a guess about
/// how axum nests its errors, checked against axum 0.8 and http-body-util 0.1.
/// If either re-wraps `LengthLimitError` the check stops matching, and an
/// over-large body becomes a 400 rather than a 413. The test that notices is
/// `the_extractor_honors_the_default_body_limit` in `tests/axum.rs`.
fn is_body_limit(err: &(dyn std::error::Error + Send + Sync + 'static)) -> bool {
    let err: &(dyn std::error::Error + 'static) = err;
    std::iter::successors(Some(err), |err| err.source())
        .any(<dyn std::error::Error + 'static>::is::<http_body_util::LengthLimitError>)
}

/// The [`SpoolConfig`] the router installed, or the default.
fn spool_config(req: &Request) -> SpoolConfig {
    req.extensions()
        .get::<SpoolConfig>()
        .cloned()
        .unwrap_or_default()
}

fn content_type(req: &Request) -> Option<String> {
    req.headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

/// The request body as an [`AsyncRead`], with axum's
/// [`DefaultBodyLimit`](axum::extract::DefaultBodyLimit) applied.
fn limited_body(req: Request) -> FlowFileBody {
    FlowFileBody::new(req.with_limited_body().into_body())
}

/// Read whatever is left of `body`, returning how much there was.
async fn drain(body: &mut FlowFileBody) -> io::Result<u64> {
    tokio::io::copy(body, &mut tokio::io::sink()).await
}

/// Extract exactly one flow file from a request, with its content read in
/// full before the handler runs.
///
/// This is the extractor to use for a service that answers NiFi. The header
/// is parsed and the content is spooled, to memory or to disk by size as the
/// [`SpoolConfig`] in the router's extensions says, or the default one if
/// there is none. A body that is not a flow file, or that holds more than
/// one, is rejected with a status before the handler runs. See [`Rejection`].
///
/// ```no_run
/// use axum::response::IntoResponse;
/// use nififf3::{Error, FlowFile};
///
/// async fn shout(flow_file: FlowFile) -> Result<impl IntoResponse, Error> {
///     let flow_file = flow_file.into_memory_async().await?;
///     Ok(flow_file
///         .derive()
///         .content(flow_file.content().to_ascii_uppercase())
///         .into_reader())
/// }
/// ```
///
/// # Why the whole request is read first
///
/// NiFi's HTTP client sends the whole request before it reads any of the
/// response. A handler that answered while the request was still arriving
/// would stall against that client as soon as the response outgrew the socket
/// buffers, with each side waiting for the other. Reading the request in full
/// first makes that impossible, and against NiFi it costs nothing, since NiFi
/// would not have read the response any sooner. [`Spool`] is the result: a
/// plain value, with no task or socket behind it.
///
/// # Body size
///
/// The body is read through axum's
/// [`DefaultBodyLimit`](axum::extract::DefaultBodyLimit), as any extractor's
/// body is, so it is capped at 2 MiB unless the router says otherwise. A flow
/// file is often larger than that:
///
/// ```
/// use axum::extract::DefaultBodyLimit;
/// use axum::{Router, routing::post};
/// # async fn handler(_: nififf3::FlowFile<nififf3::Spool>) {}
///
/// let app: Router = Router::new()
///     .route("/large", post(handler))
///     .layer(DefaultBodyLimit::max(8 << 30)); // or ::disable()
/// ```
impl<S: Send + Sync> FromRequest<S> for FlowFile {
    type Rejection = Rejection;

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            level = "info",
            name = "flow_file_request",
            skip_all,
            fields(
                uuid = tracing::field::Empty,
                filename = tracing::field::Empty,
                size = tracing::field::Empty,
            )
        )
    )]
    async fn from_request(req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        let config = spool_config(&req);
        let content_type = content_type(&req);
        let mut body = limited_body(req);
        let result = extract_one(&mut body, &config, content_type.as_deref()).await;
        match &result {
            Ok(flow_file) => {
                #[cfg(feature = "tracing")]
                tracing::info!(
                    spool = if flow_file.content().is_in_memory() {
                        "memory"
                    } else {
                        "disk"
                    },
                    "extracted flow file from request"
                );
                #[cfg(not(feature = "tracing"))]
                let _ = flow_file;
            }
            Err(rejection) => rejection.log(),
        }
        result
    }
}

async fn extract_one(
    body: &mut FlowFileBody,
    config: &SpoolConfig,
    content_type: Option<&str>,
) -> Result<FlowFile, Rejection> {
    let parsed = FlowFile::parse_async(&mut *body)
        .await
        .map_err(|error| Rejection::from_error(error, content_type))?;
    let (size, attributes, mut content) = parsed.into_parts();
    let spool = spool::spool(&mut content, Some(size), config)
        .await
        .map_err(|error| Rejection::from_spool(error, content_type))?;

    // Anything after the content is not this flow file's, and reading it
    // also leaves the connection drained, which a client that sends before
    // it reads depends on.
    let trailing = drain(body)
        .await
        .map_err(|error| Rejection::from_error(Error::Io(error), content_type))?;
    if trailing > 0 {
        return Err(Rejection::from_error(
            Error::TrailingData(trailing),
            content_type,
        ));
    }
    Ok(FlowFile::from_raw_parts(size, attributes, spool))
}

/// Extract the whole request body into a [`Spool`], read in full before the
/// handler runs.
///
/// This is the extractor for a body holding several flow files, which is
/// what NiFi sends when it merges flow files with the `FlowFile Stream, v3`
/// format and posts the result. Read them off the spool with
/// [`FlowFilesReaderAsync`](crate::FlowFilesReaderAsync), which streams each
/// content, or [`FlowFilesAsync`](crate::FlowFilesAsync), which buffers it:
///
/// ```no_run
/// use nififf3::{Error, FlowFilesReaderAsync, Spool};
///
/// async fn handler(body: Spool) -> Result<String, Error> {
///     let mut flow_files = FlowFilesReaderAsync::new(body);
///     let mut total = 0;
///     while let Some(flow_file) = flow_files.next().await? {
///         total += flow_file.size();
///     }
///     Ok(format!("{total} content bytes in total"))
/// }
/// ```
///
/// Nothing is parsed at extraction, so a body that is not flow files is
/// reported by the reader rather than by a rejection. The body is subject to
/// axum's [`DefaultBodyLimit`](axum::extract::DefaultBodyLimit) and goes to
/// memory or disk as the [`SpoolConfig`] in the router's extensions says,
/// exactly as for `FlowFile`.
impl<S: Send + Sync> FromRequest<S> for Spool {
    type Rejection = Rejection;

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "info", name = "spool_request", skip_all)
    )]
    async fn from_request(req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        let config = spool_config(&req);
        let content_type = content_type(&req);
        let mut body = limited_body(req);
        let result = spool::spool(&mut body, None, &config)
            .await
            .map_err(|error| Rejection::from_spool(error, content_type.as_deref()));
        match &result {
            Ok(spool) => {
                #[cfg(feature = "tracing")]
                tracing::info!(
                    bytes = spool.len(),
                    spool = if spool.is_in_memory() {
                        "memory"
                    } else {
                        "disk"
                    },
                    "spooled request body"
                );
                #[cfg(not(feature = "tracing"))]
                let _ = spool;
            }
            Err(rejection) => rejection.log(),
        }
        result
    }
}

/// Respond with the flow file in binary V3 format, streaming the content.
///
/// Sets `Content-Type: application/flowfile-v3` and a `Content-Length`
/// computed from the header and the declared content size. Exactly
/// [`size`](FlowFile::size) bytes are read from the content reader.
///
/// The bound is on the content, so this covers every reader-backed flow file,
/// including a plain `FlowFile` straight from the extractor. It does not
/// cover an in-memory `FlowFile<Vec<u8>>`, because `Vec<u8>` is not an
/// [`AsyncRead`] and coherence forbids a second impl for it. Call
/// [`into_reader`](FlowFile::into_reader) on one of those first:
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
/// status. It fails the body instead, so the client sees an aborted response
/// rather than a complete one carrying a flow file whose header declares more
/// content than it holds. A reader that yields more than `size` is cut to it.
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
                inner: Box::pin(ReaderStream::with_capacity(reader, RESPONSE_CHUNK)),
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
                #[cfg(feature = "tracing")]
                tracing::warn!(error = %err, "content reader failed, failing the response body");
                Poll::Ready(Some(Err(err)))
            }
            None => {
                this.done = true;
                Poll::Ready(if this.remaining == 0 {
                    None
                } else {
                    // The header is served from memory and always drains, so
                    // any shortfall is content. Saturating in case that ever
                    // stops being true.
                    let delivered = this.declared.saturating_sub(this.remaining);
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        declared = this.declared,
                        delivered,
                        "content ended early, failing the response body"
                    );
                    Some(Err(crate::error::truncated(this.declared, delivered)))
                })
            }
        }
    }
}

/// Respond with the error message as the body, under `400 Bad Request`, or
/// `413 Payload Too Large` for a body that ran past axum's
/// [`DefaultBodyLimit`](axum::extract::DefaultBodyLimit).
///
/// This is what lets a handler return `Result<_, nififf3::Error>` and use `?`
/// on this crate's calls. The extractors have a [`Rejection`] of their own,
/// which tells the server's failures from the client's.
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = match &self {
            Error::Io(err) if err.get_ref().is_some_and(is_body_limit) => {
                StatusCode::PAYLOAD_TOO_LARGE
            }
            _ => StatusCode::BAD_REQUEST,
        };
        (status, self.to_string()).into_response()
    }
}

/// How much serialized output may sit between the producer and the body
/// before the producer waits.
const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;

/// Any error, boxed: what a [`FlowFilesResponse`] producer reports failure
/// with.
///
/// The same alias as [`axum::BoxError`]. Because every `std::error::Error`
/// converts into it, a producer can use `?` on whatever its decoder, its I/O
/// or this crate hands it without converting between error types first.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The writer a [`FlowFilesResponse::new`] producer is handed.
pub type ResponseWriter = FlowFilesWriterAsync<DuplexStream>;

/// The writer a [`FlowFilesResponse::blocking`] producer is handed.
pub type BlockingResponseWriter = FlowFilesWriter<SyncIoBridge<DuplexStream>>;

type BoxFuture = Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send>>;

type AsyncProducer = Box<dyn FnOnce(ResponseWriter) -> BoxFuture + Send>;

type BlockingProducer = Box<dyn FnOnce(BlockingResponseWriter) -> Result<(), BoxError> + Send>;

enum Source {
    Producer(AsyncProducer),
    Blocking(BlockingProducer),
    Bytes(Vec<u8>),
}

/// A response carrying *many* flow files, concatenated as NiFi expects them.
///
/// The one-to-many counterpart to extracting a `FlowFile`: a handler
/// takes one flow file in and answers with a flow file per part it found
/// inside. Parts are produced lazily and streamed, so neither their number
/// nor the size of any one of them is bounded by memory. See
/// [`new`](Self::new) for the shape of a producer.
///
/// # Status codes
///
/// Returning a `FlowFilesResponse` commits you to a `2xx` status, because the
/// status line is sent before the producer runs. So validate up front, while
/// a real status code is still available. Report a problem with an individual
/// part as a part of its own, meaning a flow file whose attributes say what
/// went wrong, and the good parts still arrive. Returning `Err` from the
/// producer aborts the body instead, leaving the client with a truncated
/// response and no status to explain it.
///
/// Which failures you can report as a part follows from the format writing a
/// part's size before its content. A failure found before the part is written
/// is always reportable. A failure found while
/// [`write`](FlowFilesWriterAsync::write) streams a part's content is not,
/// because the size is already on the wire. To vouch for a part's content,
/// read it into memory and use
/// [`write_bytes`](FlowFilesWriterAsync::write_bytes), which learns of the
/// failure before committing to anything.
///
/// # How the producer runs
///
/// The producer is not spawned. The response body polls it whenever the
/// connection can take more, so it runs exactly as far as the client reads,
/// and dropping the response drops the producer at its next `await`. A
/// [`blocking`](Self::blocking) producer runs on a blocking thread instead,
/// started when the body is first polled, and cannot be stopped from outside:
/// it ends when it returns, or when its next write fails because the client
/// has gone.
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
    /// `producer` is handed a [`ResponseWriter`] and writes parts to it in
    /// whatever shape it likes, and the response ends when the future
    /// completes. A write resolves once its bytes are in the response body,
    /// and the body runs at most [`buffer_size`](Self::buffer_size) ahead of
    /// the connection, so writing a part from a reader never buffers its
    /// content.
    ///
    /// Failure is reported as a [`BoxError`], so `?` works directly on whatever
    /// a decoder, plain I/O, or this crate returns.
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
    pub fn new<F, Fut>(producer: F) -> Self
    where
        F: FnOnce(ResponseWriter) -> Fut + Send + 'static,
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
    /// blocking producer cannot be stopped when the client goes away, so it
    /// keeps running until its next write fails.
    ///
    /// A [`Spool`] is an [`AsyncRead`]; wrap it in
    /// [`SyncIoBridge`](tokio_util::io::SyncIoBridge) inside the producer to
    /// feed a synchronous decoder.
    pub fn blocking<F>(producer: F) -> Self
    where
        F: FnOnce(BlockingResponseWriter) -> Result<(), BoxError> + Send + 'static,
    {
        Self {
            source: Source::Blocking(Box::new(producer)),
            buffer_size: DEFAULT_BUFFER_SIZE,
        }
    }

    /// Stream flow files from a [`Stream`] of in-memory parts.
    ///
    /// A convenience for producers that already yield whole flow files. A
    /// `Stream` error ends the response the same way an error from
    /// [`new`](Self::new)'s producer does.
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
    /// being chunked. Every part is serialized into one buffer before the
    /// response starts, so the whole body sits in memory at once.
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
        let mut bytes = Vec::new();
        for part in parts {
            // A part whose length does not fit a `usize` cannot be in memory
            // to begin with; reserving nothing is still the right fallback.
            bytes.reserve(usize::try_from(part.serialized_len()).unwrap_or(0));
            part.write_bytes_to(&mut bytes)
                .expect("writing to a Vec cannot fail");
        }
        Self {
            source: Source::Bytes(bytes),
            buffer_size: DEFAULT_BUFFER_SIZE,
        }
    }

    /// Set how many serialized bytes may be in flight between the producer
    /// and the body. Defaults to 64 KiB; ignored by
    /// [`buffered`](Self::buffered).
    ///
    /// Rounded up to at least one byte, since a buffer of zero could never
    /// accept a write.
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
        let producer = match self.source {
            Source::Bytes(bytes) => {
                return (
                    [
                        (header::CONTENT_TYPE, MEDIA_TYPE.to_string()),
                        (header::CONTENT_LENGTH, bytes.len().to_string()),
                    ],
                    Body::from(bytes),
                )
                    .into_response();
            }
            Source::Producer(producer) => Producer::Pending(producer),
            Source::Blocking(producer) => Producer::PendingBlocking(producer),
        };
        // Only the one direction is used: the producer writes to `sink`, the
        // body reads from `pipe`. What ends the body is the sink being
        // dropped, which happens when the producer finishes. A simplex has
        // no such signal.
        let (sink, pipe) = tokio::io::duplex(buffer_size);
        let body = ProducerBody {
            pipe: ReaderStream::with_capacity(pipe, RESPONSE_CHUNK),
            sink: Some(sink),
            producer,
            bytes: 0,
            done: false,
            #[cfg(feature = "tracing")]
            span: tracing::debug_span!("flow_files_response"),
        };
        (
            [(header::CONTENT_TYPE, MEDIA_TYPE.to_string())],
            Body::from_stream(body),
        )
            .into_response()
    }
}

/// Where the producer is in its life. It starts when the body is first
/// polled, so that building a response commits nothing.
enum Producer {
    Pending(AsyncProducer),
    Running(BoxFuture),
    PendingBlocking(BlockingProducer),
    RunningBlocking(JoinHandle<Result<(), BoxError>>),
    Finished(Result<(), BoxError>),
}

/// The response body: serialized bytes as the producer writes them, followed
/// by the producer's own outcome.
///
/// Draining the pipe is not enough to call the response complete. A producer
/// that failed after its last successful write must still poison the body, so
/// that the client sees a truncated response rather than a plausible one.
struct ProducerBody {
    pipe: ReaderStream<DuplexStream>,
    /// The producer's end of the pipe, until the producer is started and
    /// takes it.
    sink: Option<DuplexStream>,
    producer: Producer,
    bytes: u64,
    done: bool,
    #[cfg(feature = "tracing")]
    span: tracing::Span,
}

impl ProducerBody {
    /// Start the producer if it has not been started, then drive it. Returns
    /// `Pending` while it is still running.
    fn poll_producer(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        loop {
            match &mut self.producer {
                Producer::Pending(_) => {
                    let Producer::Pending(start) =
                        std::mem::replace(&mut self.producer, Producer::Finished(Ok(())))
                    else {
                        unreachable!()
                    };
                    let writer = FlowFilesWriterAsync::new(self.take_sink());
                    self.producer = Producer::Running(start(writer));
                }
                Producer::PendingBlocking(_) => {
                    let Producer::PendingBlocking(start) =
                        std::mem::replace(&mut self.producer, Producer::Finished(Ok(())))
                    else {
                        unreachable!()
                    };
                    let sink = SyncIoBridge::new_with_handle(
                        self.take_sink(),
                        tokio::runtime::Handle::current(),
                    );
                    let writer = FlowFilesWriter::new(sink);
                    self.producer =
                        Producer::RunningBlocking(tokio::task::spawn_blocking(move || {
                            start(writer)
                        }));
                }
                Producer::Running(future) => {
                    let outcome = ready!(future.as_mut().poll(cx));
                    self.producer = Producer::Finished(outcome);
                }
                Producer::RunningBlocking(handle) => {
                    let outcome = match ready!(Pin::new(handle).poll(cx)) {
                        Ok(outcome) => outcome,
                        Err(join) => {
                            #[cfg(feature = "tracing")]
                            tracing::error!(error = %join, "blocking producer panicked");
                            Err(join.into())
                        }
                    };
                    self.producer = Producer::Finished(outcome);
                }
                Producer::Finished(_) => return Poll::Ready(()),
            }
        }
    }

    fn take_sink(&mut self) -> DuplexStream {
        self.sink
            .take()
            .expect("the sink is taken once, when the producer starts")
    }

    /// The producer's outcome, once it has one.
    fn outcome(&mut self) -> Option<Result<(), BoxError>> {
        match std::mem::replace(&mut self.producer, Producer::Finished(Ok(()))) {
            Producer::Finished(outcome) => Some(outcome),
            other => {
                self.producer = other;
                None
            }
        }
    }
}

impl Stream for ProducerBody {
    type Item = Result<Bytes, BoxError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        #[cfg(feature = "tracing")]
        let _entered = this.span.clone().entered();
        if this.done {
            return Poll::Ready(None);
        }
        loop {
            // Whatever the producer has written goes out first.
            match Pin::new(&mut this.pipe).poll_next(cx) {
                Poll::Ready(Some(Ok(chunk))) => {
                    this.bytes += chunk.len() as u64;
                    return Poll::Ready(Some(Ok(chunk)));
                }
                Poll::Ready(Some(Err(err))) => {
                    this.done = true;
                    #[cfg(feature = "tracing")]
                    tracing::warn!(error = %err, "response body failed mid-stream");
                    return Poll::Ready(Some(Err(err.into())));
                }
                // The sink is gone and the pipe is read dry. The producer is
                // done, or has dropped its writer and is finishing up.
                Poll::Ready(None) => break,
                Poll::Pending => {}
            }
            // Nothing buffered. A finished producer has dropped its writer,
            // which the pipe reports as its end, so reaching here finished
            // means the writer was leaked: nothing more can come.
            if matches!(this.producer, Producer::Finished(_)) {
                break;
            }
            // Run the producer until it writes something or finishes, then
            // go round again to read what it wrote.
            ready!(this.poll_producer(cx));
        }
        // The pipe is dry. The outcome decides how the body ends.
        ready!(this.poll_producer(cx));
        this.done = true;
        let outcome = this.outcome().expect("the producer has finished");
        Poll::Ready(match outcome {
            Ok(()) => {
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    bytes = this.bytes,
                    "producer finished, response body complete"
                );
                None
            }
            Err(err) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(error = %err, bytes = this.bytes, "producer failed, truncating the body");
                Some(Err(err))
            }
        })
    }
}

impl Drop for ProducerBody {
    fn drop(&mut self) {
        match &self.producer {
            Producer::Finished(_) if self.done => {}
            Producer::RunningBlocking(handle) => {
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    bytes = self.bytes,
                    "response body dropped; the blocking producer stops at its next write"
                );
                handle.abort();
            }
            _ => {
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    bytes = self.bytes,
                    "response body dropped before it completed"
                );
            }
        }
    }
}
