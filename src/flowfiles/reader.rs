use std::{collections::HashMap, io, pin::Pin};

use axum::body::BodyDataStream;
use futures::{FutureExt, Stream, TryStreamExt};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::{bytes::Bytes, io::StreamReader};

/// A NiFi Flow File V3 with a streamed body.
///
/// The attributes from the flow file are available directly in memory,
/// while the body is streamed.
pub struct FlowFile {
    size: u64,
    attributes: HashMap<String, String>,
    contents: Option<FlowFileContentReader>,
    tx: Option<tokio::sync::oneshot::Sender<FlowFileContentReader>>,
}

impl FlowFile {
    /// The length of the body of the flow file.
    ///
    /// Note that this is not how many bytes may be left in the [`Self::body()`] reader,
    /// but rather how many bytes the reader is expected to produce in total, according
    /// to the flow file header.
    #[inline]
    #[must_use]
    pub fn len(&self) -> u64 {
        self.size
    }

    /// Return `true` if the flow file self-reports to be empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// All attributes contained in the flow file.
    #[must_use]
    pub fn attributes(&self) -> &HashMap<String, String> {
        &self.attributes
    }

    /// All attributes contained in the flow file.
    #[must_use]
    pub fn attributes_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.attributes
    }

    /// A reader of the (remaining) bytes of the body of the flow file.
    ///
    /// This contains the actual file content, and is expected to yield [`Self::len()`]
    /// bytes in total. It is guaranteed to produce no more bytes than this, but a
    /// truncated file would give EOF early.
    #[expect(clippy::missing_panics_doc, reason = "never panics, or else bug")]
    pub fn body(&mut self) -> &mut FlowFileContentReader {
        self.contents.as_mut().expect("body should be present")
    }
}

impl Drop for FlowFile {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take()
            && let Some(contents) = self.contents.take()
        {
            tracing::trace!("dropping flowfile with size {}", self.len());
            if tx.send(contents).is_err() {
                tracing::error!("failed to send stream back to iterator");
            }
        }
    }
}

/// Trait for converting streams of data into a [`FlowFileIterator`].
///
/// See [`FlowFileIterator::from`] for which types can be used as a source.
pub trait IntoFlowFiles {
    fn into_flow_files(self) -> FlowFileIterator;
}

impl<S: Into<FlowFileIterator>> IntoFlowFiles for S {
    fn into_flow_files(self) -> FlowFileIterator {
        self.into()
    }
}

impl From<BodyDataStream> for FlowFileIterator {
    fn from(body: BodyDataStream) -> Self {
        let stream: BoxedByteStream = Box::pin(body.map_err(io::Error::other));
        let reader = StreamReader::new(stream);
        let state = Some(FlowFileIteratorState::Owned(reader));
        Self { state }
    }
}
/// Errors that can occur during parsing of [`FlowFile`]s.
#[derive(Debug, Error)]
pub enum FlowFileParsingError {
    /// The file did not contain the expected `b"NiFiFF3"` magic byte header.
    #[error("Incorrect flow file magic bytes, expected 'NiFiFF3' but got {0:?}")]
    BadMagicBytes([u8; 7]),
    /// IO error while parsing a flow file.
    ///
    /// The context indicates in which stage of parsing the error occured.
    #[error("Malformed flowfile: {context}: {io_error}")]
    Malformed {
        context: &'static str,
        io_error: tokio::io::Error,
    },
    /// Internal receive error while waiting to receive the stream reader back from a flow file reader.
    #[error("broken internal flow file parsing channel: {0}")]
    BrokenChannel(#[from] tokio::sync::oneshot::error::RecvError),

    /// Generic I/O error.
    #[error("IO error while processing flowfile: {0}")]
    Io(#[from] tokio::io::Error),
}

/// Boxed byte stream type
type BoxedByteStream = Pin<Box<dyn futures::Stream<Item = Result<Bytes, io::Error>> + Send>>;
type ByteStreamReader = StreamReader<BoxedByteStream, Bytes>;
type FlowFileParseResult = Result<
    (
        Option<FlowFile>,
        tokio::sync::oneshot::Receiver<FlowFileContentReader>,
    ),
    (FlowFileParsingError, ByteStreamReader),
>;

/// Parser capable of yielding successive [`FlowFile`]s from a stream of bytes.
pub struct FlowFileIterator {
    /// The state of the iterator, [`None`] only when the iterator is done.
    state: Option<FlowFileIteratorState>,
}
pub enum FlowFileIteratorState {
    Owned(ByteStreamReader),
    Parsing(Pin<Box<dyn Future<Output = FlowFileParseResult> + Send>>),
    OnLoan(tokio::sync::oneshot::Receiver<FlowFileContentReader>),
    NeedsToDrain(Pin<Box<dyn Future<Output = Result<ByteStreamReader, tokio::io::Error>> + Send>>),
}

impl Stream for FlowFileIterator {
    type Item = Result<FlowFile, FlowFileParsingError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;

        let mut this = self.as_mut();
        loop {
            tracing::trace!("ff iterator state: {:?}", this.state);

            let Some(state) = this.state.take() else {
                return Poll::Ready(None);
            };

            match state {
                FlowFileIteratorState::OnLoan(mut receiver) => match receiver.poll_unpin(cx) {
                    Poll::Ready(Ok(reader)) => {
                        if reader.inner.limit() > 0 {
                            let drain_fut = Box::pin(reader.drain());
                            this.state = Some(FlowFileIteratorState::NeedsToDrain(drain_fut));
                        } else {
                            this.state =
                                Some(FlowFileIteratorState::Owned(reader.inner.into_inner()));
                        }
                    }
                    Poll::Ready(Err(err)) => {
                        return Poll::Ready(Some(Err(err.into())));
                    }
                    Poll::Pending => {
                        this.state = Some(FlowFileIteratorState::OnLoan(receiver));
                        return Poll::Pending;
                    }
                },
                FlowFileIteratorState::NeedsToDrain(mut reader) => match reader.poll_unpin(cx) {
                    Poll::Ready(Ok(reader)) => {
                        this.state = Some(FlowFileIteratorState::Owned(reader));
                    }
                    Poll::Ready(Err(err)) => {
                        return Poll::Ready(Some(Err(err.into())));
                    }
                    Poll::Pending => {
                        this.state = Some(FlowFileIteratorState::NeedsToDrain(reader));
                        return Poll::Pending;
                    }
                },
                FlowFileIteratorState::Owned(reader) => {
                    let reader = Box::pin(parse_flow_file_from_reader(reader));
                    this.state = Some(FlowFileIteratorState::Parsing(reader));
                }
                FlowFileIteratorState::Parsing(mut parse_fut) => match parse_fut.poll_unpin(cx) {
                    Poll::Ready(Ok((None, _))) => {
                        return Poll::Ready(None);
                    }
                    Poll::Ready(Ok((Some(flow_file), receiver))) => {
                        this.state = Some(FlowFileIteratorState::OnLoan(receiver));
                        return Poll::Ready(Some(Ok(flow_file)));
                    }
                    Poll::Ready(Err((parsing_err, reader))) => {
                        this.state = Some(FlowFileIteratorState::Owned(reader));
                        return Poll::Ready(Some(Err(parsing_err)));
                    }
                    Poll::Pending => {
                        this.state = Some(FlowFileIteratorState::Parsing(parse_fut));
                        return Poll::Pending;
                    }
                },
            }
        }
    }
}

/// Parse a NiFi Flow File v3 from a reader.
///
/// # Errors
/// IO errors will propagate up. Otherwise this can return an error if the flowfile is malformed.
pub async fn parse_flow_file_from_reader(mut reader: ByteStreamReader) -> FlowFileParseResult {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let mut buf = [0u8; 7];
    if let Err(err) = reader.read_exact(&mut buf).await {
        if err.kind() == std::io::ErrorKind::UnexpectedEof {
            let _ = tx.send(FlowFileContentReader {
                inner: reader.take(0),
            });
            return Ok((None, rx));
        }
        return Err((
            FlowFileParsingError::Malformed {
                context: "Could not read 7 bytes to check for flow file magic bytes",
                io_error: err,
            },
            reader,
        ));
    }
    if &buf != b"NiFiFF3" {
        return Err((FlowFileParsingError::BadMagicBytes(buf), reader));
    }
    let n_attributes = match read_field_length(&mut reader).await {
        Ok(n) => n as usize,
        Err(io) => {
            return Err((
                FlowFileParsingError::Malformed {
                    context: "Reading number of attributes in flowfile",
                    io_error: io,
                },
                reader,
            ));
        }
    };
    let mut attributes = HashMap::with_capacity(n_attributes);
    for _ in 0..n_attributes {
        let key = match read_string(&mut reader).await {
            Ok(key) => key,
            Err(io) => {
                return Err((
                    FlowFileParsingError::Malformed {
                        context: "Reading key from attribute",
                        io_error: io,
                    },
                    reader,
                ));
            }
        };
        let value = match read_string(&mut reader).await {
            Ok(value) => value,
            Err(io) => {
                return Err((
                    FlowFileParsingError::Malformed {
                        context: "Reading value from attribute",
                        io_error: io,
                    },
                    reader,
                ));
            }
        };
        attributes.insert(key, value);
    }

    let size = match reader.read_u64().await {
        Ok(size) => size,
        Err(io) => {
            return Err((
                FlowFileParsingError::Malformed {
                    context: "Reading content length as u64",
                    io_error: io,
                },
                reader,
            ));
        }
    };

    let file_reader = FlowFileContentReader {
        inner: reader.take(size),
    };

    Ok((
        Some(FlowFile {
            size,
            attributes,
            contents: Some(file_reader),
            tx: Some(tx),
        }),
        rx,
    ))
}

/// Async reader for a single file
pub struct FlowFileContentReader {
    inner: tokio::io::Take<ByteStreamReader>,
}

impl FlowFileContentReader {
    async fn drain(mut self) -> tokio::io::Result<ByteStreamReader> {
        tokio::io::copy(&mut self, &mut tokio::io::sink()).await?;
        Ok(self.inner.into_inner())
    }
}

impl AsyncRead for FlowFileContentReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

async fn read_field_length<R: AsyncReadExt + Unpin>(r: &mut R) -> tokio::io::Result<u32> {
    let n = r.read_u16().await?;
    if n != u16::MAX {
        return Ok(u32::from(n));
    }
    r.read_u32().await
}

async fn read_string<R: AsyncReadExt + Unpin>(r: &mut R) -> tokio::io::Result<String> {
    let n = read_field_length(r).await? as usize;
    let mut string = String::with_capacity(n);
    r.take(n as u64).read_to_string(&mut string).await?;
    Ok(string)
}

impl std::fmt::Debug for FlowFileIteratorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Owned(_) => f.debug_tuple("Owned").finish_non_exhaustive(),
            Self::Parsing(_) => f.debug_tuple("Parsing").finish_non_exhaustive(),
            Self::OnLoan(_) => f.debug_tuple("OnLoan").finish_non_exhaustive(),
            Self::NeedsToDrain(_) => f.debug_tuple("NeedsToDrain").finish_non_exhaustive(),
        }
    }
}
