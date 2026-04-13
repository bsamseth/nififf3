use std::{collections::HashMap, io, ops::Deref, pin::Pin};

use axum::body::BodyDataStream;
use futures::{FutureExt, Stream, TryStreamExt};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::{bytes::Bytes, io::StreamReader};

use crate::{FlowFileHeader, FlowFileParsingError};

/// A NiFi Flow File v3 with a streamed body.
///
/// The attributes from the flow file are available directly in memory,
/// while the body is streamed. This type represents a "lock" on the underlying byte stream -
/// it owns the portion of the stream containing this flow file's content.
///
/// # Drop Behavior
///
/// When a `StreamedFlowFile` is dropped, it automatically returns the content reader to the
/// [`StreamedFlowFiles`] it was created from, allowing subsequent flow files to be parsed.
/// If you obtained this via an Axum extractor, nothing special happens on drop.
///
/// # Deref
///
/// This type implements `Deref` to [`FlowFileHeader`], allowing direct
/// access to attributes and size via the header methods.
///
/// # Usage
///
/// Access attributes and content via the inherited methods from `FlowFileHeader` and the
/// [`contents()`](Self::contents()) method:
///
/// ```
/// use nifioxide::{axum::StreamedFlowFile, FlowFileHeader};
/// use tokio::io::AsyncReadExt;
///
/// async fn process_flow_file(mut ff: StreamedFlowFile) {
///     // Access attributes via deref to FlowFileHeader
///     println!("Size: {}", ff.size());
///     println!("Filename: {}", ff.attributes().get("filename").unwrap());
///
///     // Read the content
///     let mut buf = Vec::new();
///     ff.contents().read_to_end(&mut buf).await.unwrap();
/// }
/// ```
pub struct StreamedFlowFile {
    header: FlowFileHeader,
    contents: Option<FlowFileContentReader>,
    tx: Option<tokio::sync::oneshot::Sender<FlowFileContentReader>>,
}

impl StreamedFlowFile {
    /// Get an async reader for the content of the flow file.
    ///
    /// This returns an [`impl AsyncRead`](tokio::io::AsyncRead) that provides access to the
    /// flow file's content bytes. The reader is limited to exactly [`FlowFileHeader::size()`]
    /// bytes - reading beyond that will return EOF.
    ///
    /// **Important**: You must consume all content before the stream can advance to the next
    /// flow file. The stream relies on the content being fully read to know where the next
    /// flow file begins.
    ///
    /// # Returns
    ///
    /// An `impl AsyncRead` that reads at most [`size()`](FlowFileHeader::size()) bytes.
    ///
    /// # Example
    ///
    /// ```
    /// use nifioxide::axum::StreamedFlowFile;
    /// use tokio::io::AsyncReadExt;
    ///
    /// async fn read_content(mut ff: StreamedFlowFile) -> Result<Vec<u8>, tokio::io::Error> {
    ///     let mut buf = Vec::new();
    ///     ff.contents().read_to_end(&mut buf).await?;
    ///     Ok(buf)
    /// }
    /// ```
    #[expect(clippy::missing_panics_doc, reason = "never panics, or else bug")]
    pub fn contents(&mut self) -> impl AsyncRead {
        self.contents.as_mut().expect("body should be present")
    }

    /// Prevent automatic return of content reader when this struct is dropped.
    ///
    /// Call this when the related [`StreamedFlowFiles`] will be dropped before this struct,
    /// such as when extracting a single flow file from the iterator. This is used internally
    /// by [`StreamedFlowFileFuture`](crate::axum::StreamedFlowFileFuture).
    pub(crate) fn disable_automatic_return_of_internal_reader(&mut self) {
        let _ = self.tx.take();
    }
}

impl Deref for StreamedFlowFile {
    type Target = FlowFileHeader;

    fn deref(&self) -> &Self::Target {
        &self.header
    }
}

impl Drop for StreamedFlowFile {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take()
            && let Some(contents) = self.contents.take()
        {
            tracing::trace!("dropping flowfile with size {}", self.size());
            if tx.send(contents).is_err() {
                tracing::error!("failed to send stream back to iterator");
            }
        }
    }
}

/// Trait for converting streams of data into a [`StreamedFlowFiles`].
///
/// This trait is implemented for types that can serve as a source for parsing NiFi Flow Files.
/// The most common source is an HTTP request body from Axum.
///
/// # Implementations
///
/// - [`(BodyDataStream, Option<u64>)`](axum::body::BodyDataStream) - Used when extracting from HTTP requests
/// - Custom stream readers (via `tokio_util::io::StreamReader`)
pub trait IntoFlowFiles {
    /// Convert this type into a [`StreamedFlowFiles`].
    fn into_flow_files(self) -> StreamedFlowFiles;
}

/// A stream of NiFi Flow Files parsed from a byte stream.
///
/// This implements [`futures::Stream`], yielding successive [`StreamedFlowFile`]s from the
/// underlying byte stream. Each flow file is streamed, meaning the attributes are available in
/// memory but the content is read on-demand.
///
/// # Important: Sequential Processing Required
///
/// Because each flow file's content is streamed from a shared underlying byte stream, you **must**
/// process flow files in order. You cannot collect all flow files into a collection (like `Vec`)
/// before processing them, as this will cause a deadlock - the parser cannot read subsequent
/// flow files until the current one's content is consumed.
///
/// # Creation
///
/// StreamedFlowFiles is created in one of two ways:
/// 1. Using it as an Axum extractor: `async fn handler(ff: StreamedFlowFiles)`
/// 2. Using [`IntoFlowFiles`] trait on a reader: `reader.into_flow_files()`
///
/// # Errors
///
/// The stream yields [`Result<StreamedFlowFile, FlowFileParsingError>`](FlowFileParsingError).
/// Common errors include:
/// - [`BadMagicBytes`](FlowFileParsingError::BadMagicBytes) - Data doesn't start with "NiFiFF3"
/// - [`Malformed`](FlowFileParsingError::Malformed) - Data is truncated or malformed
/// - [`ContentLengthLengthMismatch`](FlowFileParsingError::ContentLengthLengthMismatch) - Content-Length header doesn't match actual data
///
/// # Example
///
/// ```
/// use nifioxide::axum::{StreamedFlowFiles, StreamedFlowFile};
/// use futures::StreamExt;
/// use tokio::io::AsyncReadExt;
///
/// async fn process_flow_files(mut stream: StreamedFlowFiles) {
///     while let Some(result) = stream.next().await {
///         match result {
///             Ok(mut ff) => {
///                 println!("Processing flow file: size={}", ff.size());
///                 // Read content
///                 let mut buf = Vec::new();
///                 ff.contents().read_to_end(&mut buf).await.unwrap();
///                 println!("Content: {:?}", String::from_utf8_lossy(&buf));
///             }
///             Err(e) => {
///                 eprintln!("Error reading flow file: {}", e);
///             }
///         }
///     }
/// }
/// ```
pub struct StreamedFlowFiles {
    /// The state of the iterator, [`None`] only when the iterator is done.
    state: Option<StreamedFlowFilesState>,
    /// The expected length of the inner reader, as reported by Content-Length header.
    /// This might not be available, e.g. in case of chunked encoding.
    remaining_length: Option<u64>,
}
enum StreamedFlowFilesState {
    Owned(ByteStreamReader),
    Parsing(Pin<Box<dyn Future<Output = FlowFileParseResult> + Send>>),
    OnLoan(tokio::sync::oneshot::Receiver<FlowFileContentReader>),
    NeedsToDrain(Pin<Box<dyn Future<Output = Result<ByteStreamReader, tokio::io::Error>> + Send>>),
}

impl From<(BodyDataStream, Option<u64>)> for StreamedFlowFiles {
    fn from((body, maybe_content_length): (BodyDataStream, Option<u64>)) -> Self {
        let stream: BoxedByteStream = Box::pin(body.map_err(io::Error::other));
        let reader = StreamReader::new(stream);
        let state = Some(StreamedFlowFilesState::Owned(reader));
        Self {
            state,
            remaining_length: maybe_content_length,
        }
    }
}

impl<S: Into<StreamedFlowFiles>> IntoFlowFiles for S {
    fn into_flow_files(self) -> StreamedFlowFiles {
        self.into()
    }
}

/// Boxed byte stream type
type BoxedByteStream = Pin<Box<dyn futures::Stream<Item = Result<Bytes, io::Error>> + Send>>;
type ByteStreamReader = StreamReader<BoxedByteStream, Bytes>;
type FlowFileParseResult = Result<
    (
        Option<StreamedFlowFile>,
        tokio::sync::oneshot::Receiver<FlowFileContentReader>,
        u64,
    ),
    (FlowFileParsingError, ByteStreamReader),
>;

impl StreamedFlowFiles {
    /// Create a new StreamedFlowFiles from a reader.
    ///
    /// This creates a stream without knowledge of the total content length. The stream will
    /// continue reading until EOF.
    ///
    /// # Arguments
    ///
    /// * `reader` - A reader that can be converted to a `ByteStreamReader`. This is typically
    ///   created from an HTTP body stream using [`tokio_util::io::StreamReader`].
    ///
    /// # Example
    ///
    /// Most users will use the [`IntoFlowFiles`] trait instead. See its documentation for examples.
    pub fn new(reader: impl Into<ByteStreamReader>) -> Self {
        Self {
            state: Some(StreamedFlowFilesState::Owned(reader.into())),
            remaining_length: None,
        }
    }

    /// Create a new StreamedFlowFiles from a reader with known content length.
    ///
    /// This is useful when the Content-Length header is available, allowing the stream to
    /// detect when all flow files have been read (when remaining_length reaches 0).
    ///
    /// # Arguments
    ///
    /// * `reader` - A reader that can be converted to a `ByteStreamReader`.
    /// * `content_length` - The total size of the request body in bytes, typically from the
    ///   Content-Length header.
    ///
    /// # Example
    ///
    /// Most users will use the [`IntoFlowFiles`] trait instead. See its documentation for examples.
    pub fn new_with_content_length(
        reader: impl Into<ByteStreamReader>,
        content_length: u64,
    ) -> Self {
        Self {
            state: Some(StreamedFlowFilesState::Owned(reader.into())),
            remaining_length: Some(content_length),
        }
    }

    /// Returns `true` if the iterator knows that it will not yield more flow files.
    ///
    /// This can happen when:
    /// - The content length was set to 0 via [`new_with_content_length()`](Self::new_with_content_length()).
    /// - All flow files have already been consumed (the stream has ended).
    ///
    /// Note: If created via [`new()`](Self::new()) without content length, this will always
    /// return `false` until the stream is exhausted.
    pub fn is_empty(&self) -> bool {
        self.remaining_length.is_some_and(|r| r == 0) || self.state.is_none()
    }
}

impl Stream for StreamedFlowFiles {
    type Item = Result<StreamedFlowFile, FlowFileParsingError>;

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
                StreamedFlowFilesState::OnLoan(mut receiver) => match receiver.poll_unpin(cx) {
                    Poll::Ready(Ok(reader)) => {
                        if reader.inner.limit() > 0 {
                            let drain_fut = Box::pin(reader.drain());
                            this.state = Some(StreamedFlowFilesState::NeedsToDrain(drain_fut));
                        } else {
                            this.state =
                                Some(StreamedFlowFilesState::Owned(reader.inner.into_inner()));
                        }
                    }
                    Poll::Ready(Err(err)) => {
                        return Poll::Ready(Some(Err(err.into())));
                    }
                    Poll::Pending => {
                        this.state = Some(StreamedFlowFilesState::OnLoan(receiver));
                        return Poll::Pending;
                    }
                },
                StreamedFlowFilesState::NeedsToDrain(mut reader) => match reader.poll_unpin(cx) {
                    Poll::Ready(Ok(reader)) => {
                        this.state = Some(StreamedFlowFilesState::Owned(reader));
                    }
                    Poll::Ready(Err(err)) => {
                        return Poll::Ready(Some(Err(err.into())));
                    }
                    Poll::Pending => {
                        this.state = Some(StreamedFlowFilesState::NeedsToDrain(reader));
                        return Poll::Pending;
                    }
                },
                StreamedFlowFilesState::Owned(reader) => {
                    if this.remaining_length.is_some_and(|n| n == 0) {
                        tracing::trace!("Hit expected EOF, no more files in iterator");
                        return Poll::Ready(None);
                    }
                    let reader = Box::pin(parse_flow_file_from_reader(reader));
                    this.state = Some(StreamedFlowFilesState::Parsing(reader));
                }
                StreamedFlowFilesState::Parsing(mut parse_fut) => match parse_fut.poll_unpin(cx) {
                    Poll::Ready(Ok((None, _, _))) => {
                        return Poll::Ready(None);
                    }
                    Poll::Ready(Ok((Some(flow_file), receiver, total_ff_length))) => {
                        if let Some(remaining_length) = this.remaining_length.as_mut() {
                            if *remaining_length < total_ff_length {
                                return Poll::Ready(Some(Err(
                                    FlowFileParsingError::ContentLengthLengthMismatch {
                                        content_length: *remaining_length,
                                        flow_file_required: total_ff_length,
                                    },
                                )));
                            }
                            *remaining_length = remaining_length.saturating_sub(total_ff_length);
                        }
                        this.state = Some(StreamedFlowFilesState::OnLoan(receiver));
                        return Poll::Ready(Some(Ok(flow_file)));
                    }
                    Poll::Ready(Err((parsing_err, _reader))) => {
                        return Poll::Ready(Some(Err(parsing_err)));
                    }
                    Poll::Pending => {
                        this.state = Some(StreamedFlowFilesState::Parsing(parse_fut));
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
async fn parse_flow_file_from_reader(mut reader: ByteStreamReader) -> FlowFileParseResult {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let mut buf = [0u8; 7];
    if let Err(err) = reader.read_exact(&mut buf).await {
        if err.kind() == std::io::ErrorKind::UnexpectedEof {
            let _ = tx.send(FlowFileContentReader {
                inner: reader.take(0),
            });
            return Ok((None, rx, 0));
        }
        return Err((
            FlowFileParsingError::Malformed {
                context: "Could not read 7 bytes to check for flow file magic bytes",
                io_error: err,
            },
            reader,
        ));
    }

    let mut read_count = 7u64;

    if &buf != b"NiFiFF3" {
        return Err((FlowFileParsingError::BadMagicBytes(buf), reader));
    }
    let n_attributes = match read_field_length(&mut reader).await {
        Ok(n) => {
            read_count += field_length_encoded_size(n);
            n as usize
        }
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
        read_count += string_encoded_size(&key);

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
        read_count += string_encoded_size(&value);
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
    read_count += 8 + size;

    let file_reader = FlowFileContentReader {
        inner: reader.take(size),
    };

    Ok((
        Some(StreamedFlowFile {
            header: FlowFileHeader::new(size, attributes),
            contents: Some(file_reader),
            tx: Some(tx),
        }),
        rx,
        read_count,
    ))
}

/// Async reader for a single file
struct FlowFileContentReader {
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

fn field_length_encoded_size(n: u32) -> u64 {
    if n >= u32::from(u16::MAX) { 6 } else { 2 }
}
fn string_encoded_size(s: &str) -> u64 {
    let n = u32::try_from(s.len()).expect("string size less than u32::MAX");
    u64::from(n) + field_length_encoded_size(n)
}

impl std::fmt::Debug for StreamedFlowFilesState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Owned(_) => f.debug_tuple("Owned").finish_non_exhaustive(),
            Self::Parsing(_) => f.debug_tuple("Parsing").finish_non_exhaustive(),
            Self::OnLoan(_) => f.debug_tuple("OnLoan").finish_non_exhaustive(),
            Self::NeedsToDrain(_) => f.debug_tuple("NeedsToDrain").finish_non_exhaustive(),
        }
    }
}

impl std::fmt::Debug for StreamedFlowFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamedFlowFile")
            .field("header", &self.header)
            .field("contents", &"<impl AsyncRead>")
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for FlowFileContentReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlowFileContentReader")
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {

    use futures::StreamExt;
    use tokio::io::AsyncReadExt;
    use tokio_util::bytes::Bytes;

    use super::*;

    fn make_byte_stream(data: Vec<u8>) -> ByteStreamReader {
        let stream: BoxedByteStream = Box::pin(futures::stream::iter(std::iter::once(Ok(
            Bytes::from(data),
        ))));
        tokio_util::io::StreamReader::new(stream)
    }

    #[expect(clippy::cast_possible_truncation)]
    fn encode_flow_file(attributes: &[(&str, &str)], content: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend(b"NiFiFF3");

        let n_attrs = attributes.len() as u16;
        buf.extend_from_slice(&n_attrs.to_be_bytes());

        for (key, value) in attributes {
            let key_bytes = key.as_bytes();
            let value_bytes = value.as_bytes();

            buf.extend_from_slice(&(key_bytes.len() as u16).to_be_bytes());
            buf.extend_from_slice(key_bytes);

            buf.extend_from_slice(&(value_bytes.len() as u16).to_be_bytes());
            buf.extend_from_slice(value_bytes);
        }

        buf.extend_from_slice(&(content.len() as u64).to_be_bytes());
        buf.extend_from_slice(content);

        buf
    }

    #[test]
    fn field_length_encoded_size_small() {
        assert_eq!(field_length_encoded_size(0), 2);
        assert_eq!(field_length_encoded_size(u32::from(u16::MAX - 1)), 2);
    }

    #[test]
    fn field_length_encoded_size_large() {
        assert_eq!(field_length_encoded_size(u16::MAX.into()), 6);
        assert_eq!(field_length_encoded_size(u32::from(u16::MAX) + 1), 6);
    }

    #[tokio::test]
    async fn parse_empty_flow_file() {
        let data = encode_flow_file(&[], b"");
        let reader = make_byte_stream(data);
        let Ok((result, _rx, read_count)) = parse_flow_file_from_reader(reader).await else {
            panic!("expected ok result");
        };

        let ff = result.unwrap();
        assert_eq!(ff.size(), 0);
        assert!(ff.is_empty());
        assert!(ff.attributes().is_empty());
        assert_eq!(read_count, 7 + 2 + 8); // magic + attr count + size
    }

    #[tokio::test]
    async fn parse_flow_file_with_attributes() {
        let data = encode_flow_file(
            &[("filename", "test.txt"), ("mime.type", "text/plain")],
            b"content",
        );
        let reader = make_byte_stream(data);
        let Ok((result, _rx, _)) = parse_flow_file_from_reader(reader).await else {
            panic!("expected ok result");
        };

        let ff = result.unwrap();
        assert_eq!(ff.size(), 7); // "content" is 7 bytes
        assert_eq!(ff.attributes().len(), 2);
        assert_eq!(
            ff.attributes().get("filename"),
            Some(&"test.txt".to_string())
        );
        assert_eq!(
            ff.attributes().get("mime.type"),
            Some(&"text/plain".to_string())
        );
    }

    #[tokio::test]
    async fn parse_flow_file_with_content() {
        let content = b"Hello, World!";
        let data = encode_flow_file(&[], content);
        let reader = make_byte_stream(data);
        let Ok((result, _rx, _)) = parse_flow_file_from_reader(reader).await else {
            panic!("expected ok result");
        };

        let mut ff = result.unwrap();
        let mut buf = Vec::new();
        ff.contents().read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, content);
    }

    #[tokio::test]
    async fn parse_bad_magic_bytes() {
        let mut data = Vec::from(b"NOTVALID");
        data.extend_from_slice(&[0u8; 100]);

        let reader = make_byte_stream(data);
        let result = parse_flow_file_from_reader(reader).await.unwrap_err();

        match result.0 {
            FlowFileParsingError::BadMagicBytes(bytes) => {
                assert_eq!(&bytes, b"NOTVALI");
            }
            other => panic!("expected BadMagicBytes, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn parse_truncated_magic_bytes() {
        let reader = make_byte_stream(vec![0x00; 3]);
        match parse_flow_file_from_reader(reader).await {
            Ok((None, _rx, 0)) => (),
            Ok((file, _, count)) => panic!("unexpected result, got: Ok(({file:?}, _, {count}))"),
            Err((err, _)) => panic!("expected ok, got err: {err}"),
        }
    }

    #[tokio::test]
    async fn parse_empty_eof_returns_none() {
        let reader = make_byte_stream(vec![]);
        let result = parse_flow_file_from_reader(reader).await;
        let Ok((result, _rx, read_count)) = result else {
            panic!("expected an ok result");
        };

        assert!(result.is_none());
        assert_eq!(read_count, 0);
    }

    #[tokio::test]
    async fn parse_truncated_attributes() {
        let mut data = Vec::from(b"NiFiFF3".as_slice());
        data.extend_from_slice(&2u16.to_be_bytes());

        let reader = make_byte_stream(data);
        let result = parse_flow_file_from_reader(reader).await.unwrap_err();

        match result.0 {
            FlowFileParsingError::Malformed { context, .. } => {
                assert!(context.contains("attribute"));
            }
            _ => panic!("expected Malformed error"),
        }
    }

    #[tokio::test]
    async fn iterator_empty_stream() {
        let reader = make_byte_stream(vec![]);
        let mut iter = StreamedFlowFiles::new(reader);

        let next = iter.next().await;
        assert!(next.is_none());
    }

    #[tokio::test]
    async fn iterator_with_content_length_zero_is_empty() {
        let reader = make_byte_stream(vec![]);
        let iter = StreamedFlowFiles::new_with_content_length(reader, 0);

        assert!(iter.is_empty());
    }

    #[tokio::test]
    async fn iterator_single_flow_file() {
        let data = encode_flow_file(&[("key", "value")], b"body");
        let reader = make_byte_stream(data);
        let mut iter = StreamedFlowFiles::new(reader);

        {
            let ff = iter.next().await.unwrap().unwrap();
            assert_eq!(ff.attributes().get("key"), Some(&"value".to_string()));
        }

        let next = iter.next().await;
        assert!(next.is_none());
    }

    #[tokio::test]
    async fn iterator_multiple_flow_files() {
        let data1 = encode_flow_file(&[("num", "1")], b"first");
        let data2 = encode_flow_file(&[("num", "2")], b"second");

        let mut combined = data1;
        combined.extend(data2);

        let reader = make_byte_stream(combined);
        let mut iter = StreamedFlowFiles::new(reader);

        {
            let ff1 = iter.next().await.unwrap().unwrap();
            assert_eq!(ff1.attributes().get("num"), Some(&"1".to_string()));
        }

        {
            let ff2 = iter.next().await.unwrap().unwrap();
            assert_eq!(ff2.attributes().get("num"), Some(&"2".to_string()));
        }

        assert!(iter.next().await.is_none());
    }

    #[tokio::test]
    async fn iterator_content_length_mismatch() {
        let data = encode_flow_file(&[], b"extra content after header");
        let reader = make_byte_stream(data);
        let mut iter = StreamedFlowFiles::new_with_content_length(reader, 9);

        let result = iter.next().await;
        match result {
            Some(Err(FlowFileParsingError::ContentLengthLengthMismatch { .. })) => {}
            other => panic!("expected ContentLengthLengthMismatch, got {other:?}"),
        }
    }
}
