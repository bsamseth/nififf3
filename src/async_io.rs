//! Parsing and serialization over `tokio::io::AsyncRead`/`AsyncWrite`.

use std::collections::HashMap;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::format::{MAGIC, MAX_VALUE_2_BYTES};
use crate::{Error, FlowFile, Limits, Result};

async fn read_field_len<R: AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<usize> {
    let mut buf = [0u8; 2];
    reader.read_exact(&mut buf).await?;
    let len = u16::from_be_bytes(buf) as usize;
    if len < MAX_VALUE_2_BYTES {
        Ok(len)
    } else {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf).await?;
        Ok(u32::from_be_bytes(buf) as usize)
    }
}

async fn read_string<R: AsyncRead + Unpin>(
    reader: &mut R,
    max_len: Option<usize>,
) -> Result<String> {
    let len = read_field_len(reader).await?;
    if let Some(limit) = max_len
        && len > limit
    {
        return Err(Error::AttributeTooLong { len, limit });
    }
    // Grow the buffer as bytes actually arrive instead of trusting the
    // declared length, so a crafted header cannot force a huge allocation.
    let mut buf = Vec::new();
    let read = reader.take(len as u64).read_to_end(&mut buf).await?;
    if read != len {
        return Err(
            std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "attribute truncated").into(),
        );
    }
    String::from_utf8(buf).map_err(Error::InvalidAttribute)
}

pub(crate) async fn parse_header<R: AsyncRead + Unpin>(
    reader: &mut R,
    first_byte: Option<u8>,
    limits: Limits,
) -> Result<(HashMap<String, String>, u64)> {
    let mut magic = [0u8; 7];
    match first_byte {
        Some(byte) => {
            magic[0] = byte;
            reader.read_exact(&mut magic[1..]).await?;
        }
        None => {
            reader.read_exact(&mut magic).await?;
        }
    }
    if magic != MAGIC {
        return Err(Error::InvalidMagic(magic));
    }
    let count = read_field_len(reader).await?;
    if let Some(limit) = limits.max_attributes
        && count > limit
    {
        return Err(Error::TooManyAttributes { count, limit });
    }
    let mut attributes = HashMap::with_capacity(count.min(1024));
    let mut total = 0usize;
    for _ in 0..count {
        let key = read_string(reader, limits.max_attribute_len).await?;
        let value = read_string(reader, limits.max_attribute_len).await?;
        total = total.saturating_add(key.len()).saturating_add(value.len());
        if let Some(limit) = limits.max_total_attribute_len
            && total > limit
        {
            return Err(Error::HeaderTooLarge { len: total, limit });
        }
        attributes.insert(key, value);
    }
    let mut size = [0u8; 8];
    reader.read_exact(&mut size).await?;
    let size = u64::from_be_bytes(size);
    if let Some(limit) = limits.max_content_len
        && size > limit
    {
        return Err(Error::ContentTooLarge { size, limit });
    }
    Ok((attributes, size))
}

impl<R: AsyncRead + Unpin> FlowFile<tokio::io::Take<R>> {
    /// Async version of [`FlowFile::parse`]: consumes only the header and
    /// returns the content as a reader limited to the declared size.
    ///
    /// The header is read in small increments, so wrap unbuffered sources in
    /// a [`tokio::io::BufReader`].
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let bytes = FlowFile::builder().content(&b"hello"[..]).to_bytes();
    ///
    /// let flow_file = FlowFile::parse_async(bytes.as_slice()).await.unwrap();
    /// assert_eq!(flow_file.size(), 5);
    /// let flow_file = flow_file.into_memory_async().await.unwrap();
    /// assert_eq!(flow_file.content().as_slice(), b"hello");
    /// # });
    /// ```
    ///
    /// # Errors
    ///
    /// As [`FlowFile::parse`].
    pub async fn parse_async(reader: R) -> Result<Self> {
        Self::parse_async_with_limits(reader, Limits::UNLIMITED).await
    }

    /// Like [`parse_async`](Self::parse_async), but enforcing [`Limits`] on
    /// the header. Use this for untrusted input.
    ///
    /// # Errors
    ///
    /// As [`FlowFile::parse_with_limits`].
    pub async fn parse_async_with_limits(mut reader: R, limits: Limits) -> Result<Self> {
        let (attributes, size) = parse_header(&mut reader, None, limits).await?;
        Ok(FlowFile::from_raw_parts(
            size,
            attributes,
            reader.take(size),
        ))
    }
}

impl<'r, R: AsyncRead + Unpin> FlowFile<tokio::io::Take<&'r mut R>> {
    /// Async version of [`FlowFile::parse_next`]: parse the next flow file
    /// from a stream of concatenated flow files.
    ///
    /// Returns `Ok(None)` on a clean end of input. The previous flow file's
    /// content must be fully consumed before calling this again, otherwise
    /// parsing resumes in the middle of that content.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let mut bytes = FlowFile::builder().content(&b"first"[..]).to_bytes();
    /// bytes.extend(FlowFile::builder().content(&b"second"[..]).to_bytes());
    ///
    /// let mut reader = bytes.as_slice();
    /// let mut count = 0;
    /// while let Some(flow_file) = FlowFile::parse_next_async(&mut reader).await.unwrap() {
    ///     count += 1;
    ///     flow_file.into_memory_async().await.unwrap(); // consume the content
    /// }
    /// assert_eq!(count, 2);
    /// # });
    /// ```
    ///
    /// # Errors
    ///
    /// As [`FlowFile::parse_next`].
    pub async fn parse_next_async(reader: &'r mut R) -> Result<Option<Self>> {
        Self::parse_next_async_with_limits(reader, Limits::UNLIMITED).await
    }

    /// Like [`parse_next_async`](Self::parse_next_async), but enforcing
    /// [`Limits`] on the header. Use this for untrusted input.
    ///
    /// # Errors
    ///
    /// As [`FlowFile::parse_next_with_limits`].
    pub async fn parse_next_async_with_limits(
        reader: &'r mut R,
        limits: Limits,
    ) -> Result<Option<Self>> {
        let mut first = [0u8; 1];
        loop {
            match reader.read(&mut first).await {
                // A one-byte buffer, so this is the end of the stream: a
                // reader returning `Ok(0)` with room to fill is buggy, and
                // retrying one would spin rather than recover.
                Ok(0) => return Ok(None),
                Ok(_) => break,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {} // retry
                Err(e) => return Err(e.into()),
            }
        }
        let (attributes, size) = parse_header(reader, Some(first[0]), limits).await?;
        Ok(Some(FlowFile::from_raw_parts(
            size,
            attributes,
            reader.take(size),
        )))
    }
}

/// Async equivalent of [`FlowFiles`](crate::FlowFiles): reads concatenated
/// flow files one at a time, buffering each content in memory.
///
/// [`next`](Self::next) is an inherent async method rather than a
/// [`Stream`](https://docs.rs/futures-core/latest/futures_core/stream/trait.Stream.html)
/// impl, so the base type needs no extra dependency; with the `stream`
/// feature, [`into_stream`](Self::into_stream) adapts it into one. After an
/// error, `next` keeps returning `None`, since the stream position is no
/// longer trustworthy.
///
/// As with [`FlowFiles`](crate::FlowFiles), a content that ends before its
/// declared size is reported as [`Error::SizeMismatch`] directly, not wrapped
/// in [`Error::Io`].
///
/// ```
/// use nififf3::{FlowFile, FlowFilesAsync};
///
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// let mut bytes = FlowFile::builder().content(&b"first"[..]).to_bytes();
/// bytes.extend(FlowFile::builder().content(&b"second"[..]).to_bytes());
///
/// let mut flow_files = FlowFilesAsync::new(bytes.as_slice());
/// let mut contents = Vec::new();
/// while let Some(flow_file) = flow_files.next().await {
///     contents.push(flow_file.unwrap().into_content());
/// }
/// assert_eq!(contents, [b"first".to_vec(), b"second".to_vec()]);
/// # });
/// ```
#[derive(Debug)]
pub struct FlowFilesAsync<R> {
    reader: R,
    limits: Limits,
    done: bool,
}

impl<R: AsyncRead + Unpin> FlowFilesAsync<R> {
    /// Iterate over the flow files in `reader`, without header limits.
    ///
    /// The header parsing reads in small increments, so wrap unbuffered
    /// sources in a [`tokio::io::BufReader`].
    pub fn new(reader: R) -> Self {
        Self::with_limits(reader, Limits::UNLIMITED)
    }

    /// Like [`new`](Self::new), but enforcing [`Limits`] on each header.
    /// Use this for untrusted input.
    pub fn with_limits(reader: R, limits: Limits) -> Self {
        Self {
            reader,
            limits,
            done: false,
        }
    }

    /// A reference to the underlying reader.
    pub fn get_ref(&self) -> &R {
        &self.reader
    }

    /// A mutable reference to the underlying reader.
    pub fn get_mut(&mut self) -> &mut R {
        &mut self.reader
    }

    /// Consume the reader, returning the underlying one. See
    /// [`FlowFiles::into_inner`](crate::FlowFiles::into_inner) for what it is
    /// positioned at.
    pub fn into_inner(self) -> R {
        self.reader
    }

    /// Adapt into a [`Stream`](https://docs.rs/futures-core/latest/futures_core/stream/trait.Stream.html)
    /// of flow files, for composing with `StreamExt` and anything that takes
    /// a stream.
    ///
    /// Requires the `stream` feature (implied by `axum`). The stream ends the
    /// same way [`next`](Self::next) does: `None` at a clean end of input, and
    /// nothing further after an error.
    ///
    /// The result is [`Send`] exactly when the reader is, so it composes with
    /// `axum` and `tokio::spawn` without either side saying so, and a
    /// single-threaded runtime can still use a reader that is not.
    ///
    /// ```
    /// use nififf3::{FlowFile, FlowFilesAsync};
    /// use tokio_stream::StreamExt as _;
    ///
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let mut bytes = FlowFile::builder().content(&b"first"[..]).to_bytes();
    /// bytes.extend(FlowFile::builder().content(&b"second"[..]).to_bytes());
    ///
    /// let sizes: Vec<_> = FlowFilesAsync::new(bytes.as_slice())
    ///     .into_stream()
    ///     .map(|flow_file| flow_file.unwrap().size())
    ///     .collect()
    ///     .await;
    /// assert_eq!(sizes, [5, 6]);
    /// # });
    /// ```
    #[cfg(feature = "stream")]
    pub fn into_stream(self) -> impl futures_core::Stream<Item = Result<FlowFile<Vec<u8>>>> {
        // `next` borrows `self`, so the future cannot be stored beside it.
        // Passing ownership through each step keeps the state machine flat,
        // at one boxed future per flow file — noise next to buffering one.
        futures_unfold(self, |mut reader| async move {
            let item = reader.next().await?;
            Some((item, reader))
        })
    }

    /// The next flow file, or `None` at the end of the input.
    pub async fn next(&mut self) -> Option<Result<FlowFile<Vec<u8>>>> {
        if self.done {
            return None;
        }
        let result = match FlowFile::parse_next_async_with_limits(&mut self.reader, self.limits)
            .await
        {
            Ok(None) => None,
            // As in `FlowFiles::next`: the conversion recovers a truncation.
            Ok(Some(flow_file)) => Some(flow_file.into_memory_async().await.map_err(Error::from)),
            Err(err) => Some(Err(err)),
        };
        if !matches!(result, Some(Ok(_))) {
            self.done = true;
        }
        result
    }
}

/// A minimal `stream::unfold`, so that [`FlowFilesAsync::into_stream`] does
/// not pull in `futures-util` for one combinator.
#[cfg(feature = "stream")]
fn futures_unfold<T, F, Fut, I>(init: T, step: F) -> impl futures_core::Stream<Item = I>
where
    F: FnMut(T) -> Fut,
    Fut: std::future::Future<Output = Option<(I, T)>>,
{
    use std::pin::Pin;
    use std::task::{Context, Poll};

    struct Unfold<T, F, Fut, I> {
        state: Option<T>,
        pending: Option<Pin<Box<Fut>>>,
        step: F,
        _item: std::marker::PhantomData<fn() -> I>,
    }

    // Nothing here is structurally pinned: the in-flight future is boxed, so
    // its address is stable no matter where the stream itself lives, and
    // every other field is moved in and out by value.
    impl<T, F, Fut, I> Unpin for Unfold<T, F, Fut, I> {}

    impl<T, F, Fut, I> futures_core::Stream for Unfold<T, F, Fut, I>
    where
        F: FnMut(T) -> Fut,
        Fut: std::future::Future<Output = Option<(I, T)>>,
    {
        type Item = I;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<I>> {
            let this = self.get_mut();
            loop {
                if let Some(pending) = this.pending.as_mut() {
                    let done = std::task::ready!(pending.as_mut().poll(cx));
                    this.pending = None;
                    return Poll::Ready(match done {
                        Some((item, state)) => {
                            this.state = Some(state);
                            Some(item)
                        }
                        None => None,
                    });
                }
                let Some(state) = this.state.take() else {
                    return Poll::Ready(None);
                };
                this.pending = Some(Box::pin((this.step)(state)));
            }
        }
    }

    Unfold {
        state: Some(init),
        pending: None,
        step,
        _item: std::marker::PhantomData,
    }
}

/// Async equivalent of [`FlowFilesWriter`](crate::FlowFilesWriter): writes a
/// stream of concatenated flow files to an [`AsyncWrite`].
///
/// As there, a failed write poisons the writer, since the stream is left
/// mid-flow-file and appending to it would bury the failure.
///
/// # Finishing
///
/// An [`AsyncWrite`] typically has to be told when it is done, and neither
/// writing nor dropping this type tells it:
///
/// - [`shutdown`](Self::shutdown) is what finalizes a writer that has an
///   ending of its own — a compressor's trailer, a TLS `close_notify`, a
///   buffered writer's tail. Skipping it on one of those silently truncates
///   the output, and the flow files come back corrupt.
/// - [`finish`](Self::finish) flushes and returns the writer, for one that
///   only buffers.
/// - [`into_inner`](Self::into_inner) does neither, for discarding a stream
///   rather than completing it.
///
/// ```
/// use nififf3::{FlowFile, FlowFilesAsync, FlowFilesWriterAsync};
///
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// let parent = FlowFile::builder().attribute("filename", "pair").content(Vec::new());
/// let mut parts = parent.fragments().with_count(2);
///
/// let mut out = Vec::new();
/// let mut writer = FlowFilesWriterAsync::new(&mut out);
/// writer.write_bytes(&parts.next_part().content(&b"first"[..])).await.unwrap();
/// writer.write_bytes(&parts.next_part().content(&b"second"[..])).await.unwrap();
/// assert_eq!(writer.count(), 2);
///
/// let mut parsed = FlowFilesAsync::new(out.as_slice());
/// assert_eq!(parsed.next().await.unwrap().unwrap().into_content(), b"first");
/// # });
/// ```
#[derive(Debug)]
pub struct FlowFilesWriterAsync<W> {
    writer: W,
    count: u64,
    poisoned: bool,
}

impl<W: AsyncWrite + Unpin> FlowFilesWriterAsync<W> {
    /// Write flow files to `writer`.
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            count: 0,
            poisoned: false,
        }
    }

    /// Append a flow file, streaming exactly [`size`](FlowFile::size) bytes
    /// from its content reader. Returns the number of content bytes copied.
    ///
    /// The content is never buffered, so a part may be arbitrarily large.
    ///
    /// # Errors
    ///
    /// As [`FlowFile::write_to_async`]: a content reader that ends early
    /// leaves a truncated flow file behind, and poisons the writer. Use
    /// [`write_bytes`](Self::write_bytes) for content whose length must be
    /// verified before anything is committed.
    pub async fn write<R: AsyncRead + Unpin>(
        &mut self,
        flow_file: FlowFile<R>,
    ) -> std::io::Result<u64> {
        self.guard()?;
        let result = flow_file.write_to_async(&mut self.writer).await;
        let copied = self.poison_on_err(result)?;
        self.count += 1;
        Ok(copied)
    }

    /// Append an in-memory flow file, whose size is known to be correct.
    ///
    /// # Errors
    ///
    /// Whatever the writer returns — which, since it may have accepted part
    /// of the flow file first, also poisons the writer.
    pub async fn write_bytes(&mut self, flow_file: &FlowFile<Vec<u8>>) -> std::io::Result<u64> {
        self.guard()?;
        let bytes = flow_file.to_bytes();
        let result = self.writer.write_all(&bytes).await;
        self.poison_on_err(result)?;
        self.count += 1;
        Ok(flow_file.size)
    }

    fn guard(&self) -> std::io::Result<()> {
        if self.poisoned {
            return Err(crate::error::poisoned());
        }
        Ok(())
    }

    fn poison_on_err<T>(&mut self, result: std::io::Result<T>) -> std::io::Result<T> {
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    /// Flush the underlying writer.
    ///
    /// # Errors
    ///
    /// Whatever the underlying writer returns. A failed flush poisons the
    /// writer, since bytes that never reached the stream leave it
    /// mid-flow-file exactly as a failed write does.
    pub async fn flush(&mut self) -> std::io::Result<()> {
        let result = self.writer.flush().await;
        self.poison_on_err(result)
    }

    /// Shut the underlying writer down, finalizing it.
    ///
    /// For a plain sink this is [`flush`](Self::flush) and little else, but a
    /// writer that encodes — a compressor, a TLS session, a framed transport —
    /// emits its ending here and nowhere else. Dropping the writer does not
    /// produce it, so a stream that skips this is truncated in a way the
    /// reader on the far end will report as a corrupt flow file, not as a
    /// missing trailer.
    ///
    /// # Errors
    ///
    /// Whatever the underlying writer returns; as [`flush`](Self::flush), a
    /// failure poisons the writer.
    pub async fn shutdown(&mut self) -> std::io::Result<()> {
        let result = self.writer.shutdown().await;
        self.poison_on_err(result)
    }

    /// Flush and return the underlying writer.
    ///
    /// For a writer that finalizes itself, call [`shutdown`](Self::shutdown)
    /// first — or instead, followed by [`into_inner`](Self::into_inner), since
    /// shutting down and then flushing is the wrong order.
    ///
    /// # Errors
    ///
    /// Whatever flushing the underlying writer returns, in which case the
    /// writer is dropped. To keep hold of it either way, call
    /// [`flush`](Self::flush) and then [`into_inner`](Self::into_inner).
    pub async fn finish(mut self) -> std::io::Result<W> {
        self.flush().await?;
        Ok(self.writer)
    }

    /// Whether a write has failed, leaving the stream mid-flow-file. See
    /// [`FlowFilesWriter::is_poisoned`](crate::FlowFilesWriter::is_poisoned).
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// The number of flow files written so far, counting only the complete
    /// ones.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// A reference to the underlying writer.
    pub fn get_ref(&self) -> &W {
        &self.writer
    }

    /// A mutable reference to the underlying writer.
    pub fn get_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    /// Consume the writer, returning the underlying one *without flushing or
    /// shutting it down*.
    ///
    /// See [`finish`](Self::finish) and [`shutdown`](Self::shutdown) for
    /// completing a stream; this is for taking the writer back after a failure
    /// in order to discard or truncate what was produced.
    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<R: AsyncRead + Unpin> FlowFile<R> {
    /// Async version of [`FlowFile::write_to`]: serialize the flow file to a
    /// writer, reading exactly [`size`](FlowFile::size) bytes from the
    /// content reader. Returns the number of content bytes copied, and
    /// consumes the flow file for the same reason.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let content = &b"hello"[..]; // any `impl AsyncRead + Unpin`
    /// let flow_file = FlowFile::builder().reader(content, 5);
    ///
    /// let mut out = Vec::new();
    /// flow_file.write_to_async(&mut out).await.unwrap();
    /// assert_eq!(FlowFile::from_bytes(&out).unwrap().size(), 5);
    /// # });
    /// ```
    ///
    /// # Errors
    ///
    /// As [`FlowFile::write_to`].
    ///
    /// # Panics
    ///
    /// As [`FlowFile::write_to`]: an attribute longer than `u32::MAX` bytes
    /// cannot be expressed in the wire format.
    pub async fn write_to_async<W: AsyncWrite + Unpin>(
        mut self,
        writer: &mut W,
    ) -> std::io::Result<u64> {
        writer.write_all(&self.header_bytes()).await?;
        let copied = tokio::io::copy(&mut (&mut self.content).take(self.size), writer).await?;
        if copied != self.size {
            return Err(crate::error::truncated(self.size, copied));
        }
        Ok(copied)
    }

    /// Async version of [`FlowFile::into_memory`]: reads the content to
    /// completion and validates its length against the declared size.
    ///
    /// # Errors
    ///
    /// As [`FlowFile::into_memory`].
    pub async fn into_memory_async(mut self) -> std::io::Result<FlowFile<Vec<u8>>> {
        let mut content = Vec::new();
        let read = (&mut self.content)
            .take(self.size)
            .read_to_end(&mut content)
            .await? as u64;
        if read != self.size {
            return Err(crate::error::truncated(self.size, read));
        }
        Ok(FlowFile::from_raw_parts(
            self.size,
            self.attributes,
            content,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// As in the sync tests: the payload, not the message.
    fn is_poisoned_error(err: &std::io::Error) -> bool {
        err.kind() == std::io::ErrorKind::BrokenPipe
            && matches!(
                err.get_ref().and_then(|e| e.downcast_ref::<Error>()),
                Some(Error::WriterPoisoned)
            )
    }

    fn sample() -> FlowFile<Vec<u8>> {
        FlowFile::builder()
            .attribute("a", "b")
            .attribute("path", "x")
            .content(&b"hello"[..])
    }

    #[tokio::test]
    async fn async_roundtrip_matches_sync() {
        let expected = sample().to_bytes();

        let mut out = Vec::new();
        let copied = sample()
            .into_reader()
            .write_to_async(&mut out)
            .await
            .unwrap();
        assert_eq!(copied, 5);
        assert_eq!(out, expected);

        let parsed = FlowFile::parse_async(expected.as_slice()).await.unwrap();
        assert_eq!(parsed.size(), 5);
        let parsed = parsed.into_memory_async().await.unwrap();
        assert_eq!(parsed.attributes()["path"], "x");
        assert_eq!(parsed.content().as_slice(), b"hello");
    }

    #[tokio::test]
    async fn flow_files_async_reads_all_and_fuses_after_error() {
        let mut bytes = sample().to_bytes();
        bytes.extend(sample().to_bytes());
        bytes.extend_from_slice(b"garbage");
        let mut flow_files = FlowFilesAsync::new(bytes.as_slice());
        assert!(flow_files.next().await.unwrap().is_ok());
        assert!(flow_files.next().await.unwrap().is_ok());
        assert!(matches!(
            flow_files.next().await,
            Some(Err(Error::InvalidMagic(_)))
        ));
        assert!(flow_files.next().await.is_none());
    }

    #[tokio::test]
    async fn the_async_reader_comes_back_out_positioned_after_the_flow_files() {
        let mut bytes = sample().to_bytes();
        bytes.extend_from_slice(b"and then something else");

        let mut flow_files = FlowFilesAsync::new(bytes.as_slice());
        assert!(flow_files.next().await.unwrap().is_ok());

        let mut trailer = Vec::new();
        flow_files
            .into_inner()
            .read_to_end(&mut trailer)
            .await
            .unwrap();
        assert_eq!(trailer, b"and then something else");
    }

    #[tokio::test]
    async fn async_limits_reject_oversized_attributes() {
        let bytes = FlowFile::builder()
            .attribute("key", "a value larger than the limit")
            .content(Vec::new())
            .to_bytes();

        let limits = Limits::recommended().with_max_attribute_len(8);
        assert!(matches!(
            FlowFile::parse_async_with_limits(bytes.as_slice(), limits).await,
            Err(Error::AttributeTooLong { limit: 8, .. })
        ));
    }

    #[tokio::test]
    async fn async_limits_reject_an_oversized_declared_content_size() {
        let bytes = sample().to_bytes();
        let limits = Limits::recommended().with_max_content_len(4);

        assert!(matches!(
            FlowFile::parse_async_with_limits(bytes.as_slice(), limits).await,
            Err(Error::ContentTooLarge { size: 5, limit: 4 })
        ));
    }

    #[tokio::test]
    async fn async_readers_report_truncation_as_a_size_mismatch() {
        let bytes = sample().to_bytes();
        let truncated = &bytes[..bytes.len() - 2];

        let err = FlowFilesAsync::new(truncated).next().await.unwrap().unwrap_err();
        assert!(
            matches!(
                err,
                Error::SizeMismatch {
                    expected: 5,
                    actual: 3
                }
            ),
            "{err:?}"
        );
    }

    #[cfg(feature = "stream")]
    #[tokio::test]
    async fn into_stream_yields_every_flow_file_then_ends() {
        use tokio_stream::StreamExt as _;

        let mut bytes = sample().to_bytes();
        bytes.extend(sample().to_bytes());

        let collected: Vec<_> = FlowFilesAsync::new(bytes.as_slice())
            .into_stream()
            .collect::<Vec<_>>()
            .await;
        assert_eq!(collected.len(), 2);
        for flow_file in collected {
            assert_eq!(flow_file.unwrap().content().as_slice(), b"hello");
        }
    }

    /// The stream inherits `Send` from the reader rather than demanding it, so
    /// a single-threaded runtime can use a reader that is not `Send` while
    /// `axum` and `tokio::spawn` still get a `Send` stream from one that is.
    #[cfg(feature = "stream")]
    #[tokio::test]
    async fn into_stream_follows_the_reader_on_send() {
        use tokio_stream::StreamExt as _;

        struct NotSend {
            bytes: std::io::Cursor<Vec<u8>>,
            _not_send: std::rc::Rc<()>,
        }

        fn assert_send<T: Send>(_: &T) {}

        impl AsyncRead for NotSend {
            fn poll_read(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
                buf: &mut tokio::io::ReadBuf<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::pin::Pin::new(&mut self.bytes).poll_read(cx, buf)
            }
        }

        let collected: Vec<_> = FlowFilesAsync::new(NotSend {
            bytes: std::io::Cursor::new(sample().to_bytes()),
            _not_send: std::rc::Rc::new(()),
        })
        .into_stream()
        .collect::<Vec<_>>()
        .await;
        assert_eq!(collected.len(), 1);
        assert_eq!(collected[0].as_ref().unwrap().content().as_slice(), b"hello");

        assert_send(&FlowFilesAsync::new(std::io::Cursor::new(Vec::new())).into_stream());
    }

    #[cfg(feature = "stream")]
    #[tokio::test]
    async fn into_stream_stops_after_an_error() {
        use tokio_stream::StreamExt as _;

        let mut bytes = sample().to_bytes();
        bytes.extend_from_slice(b"garbage");

        let collected: Vec<_> = FlowFilesAsync::new(bytes.as_slice())
            .into_stream()
            .collect::<Vec<_>>()
            .await;
        assert_eq!(collected.len(), 2, "one flow file, then the error");
        assert!(collected[0].is_ok());
        assert!(matches!(collected[1], Err(Error::InvalidMagic(_))));
    }

    #[tokio::test]
    async fn a_failed_write_poisons_the_async_writer() {
        // Declares 10 bytes but the reader holds 6, so the write truncates.
        let mut out = Vec::new();
        let mut writer = FlowFilesWriterAsync::new(&mut out);

        let err = writer
            .write(FlowFile::builder().reader(&b"short"[..], 10))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
        assert!(writer.is_poisoned());

        let err = writer
            .write_bytes(&FlowFile::builder().content(&b"ok"[..]))
            .await
            .unwrap_err();
        assert!(is_poisoned_error(&err), "{err:?}");
        assert_eq!(writer.count(), 0);
    }

    /// An `AsyncWrite` that records what it was asked to do. The distinction
    /// that matters is flush versus shutdown: a writer that encodes emits its
    /// ending only on the latter.
    #[derive(Default)]
    struct Recording {
        bytes: Vec<u8>,
        flushes: usize,
        shutdowns: usize,
    }

    impl AsyncWrite for Recording {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            self.bytes.extend_from_slice(buf);
            std::task::Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            self.flushes += 1;
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            self.shutdowns += 1;
            std::task::Poll::Ready(Ok(()))
        }
    }

    async fn write_one(writer: &mut FlowFilesWriterAsync<Recording>) {
        writer
            .write_bytes(&FlowFile::builder().content(&b"content"[..]))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn finishing_flushes_and_shutting_down_shuts_down() {
        let mut writer = FlowFilesWriterAsync::new(Recording::default());
        write_one(&mut writer).await;
        let inner = writer.finish().await.unwrap();
        assert_eq!((inner.flushes, inner.shutdowns), (1, 0));
        let mut written = FlowFilesAsync::new(inner.bytes.as_slice());
        assert!(written.next().await.is_some(), "the flow file survived");

        let mut writer = FlowFilesWriterAsync::new(Recording::default());
        write_one(&mut writer).await;
        writer.shutdown().await.unwrap();
        assert_eq!(writer.into_inner().shutdowns, 1);

        // Neither happens on its own, which is the whole reason both exist.
        let mut writer = FlowFilesWriterAsync::new(Recording::default());
        write_one(&mut writer).await;
        let inner = writer.into_inner();
        assert_eq!((inner.flushes, inner.shutdowns), (0, 0));
    }

    /// The case these methods exist for, against a real encoder: gzip writes
    /// its trailer on shutdown and nowhere else, so a stream that skips it
    /// cannot be read back at all.
    #[tokio::test]
    async fn shutdown_finalizes_a_real_encoder() {
        use async_compression::tokio::bufread::GzipDecoder;
        use async_compression::tokio::write::GzipEncoder;

        async fn compressed(shut_down: bool) -> Vec<u8> {
            let mut writer = FlowFilesWriterAsync::new(GzipEncoder::new(Vec::new()));
            writer
                .write_bytes(&FlowFile::builder().content(&b"compressed"[..]))
                .await
                .unwrap();
            if shut_down {
                writer.shutdown().await.unwrap();
            }
            writer.into_inner().into_inner()
        }

        async fn decompress(bytes: &[u8]) -> std::io::Result<Vec<u8>> {
            let mut out = Vec::new();
            GzipDecoder::new(tokio::io::BufReader::new(bytes))
                .read_to_end(&mut out)
                .await?;
            Ok(out)
        }

        let flow_file = FlowFile::from_bytes(&decompress(&compressed(true).await).await.unwrap())
            .expect("a shut-down encoder produces a readable stream");
        assert_eq!(flow_file.content().as_slice(), b"compressed");

        assert!(
            decompress(&compressed(false).await).await.is_err(),
            "without shutdown the trailer never arrives and the data is lost"
        );
    }

    #[tokio::test]
    async fn a_failed_flush_poisons_the_async_writer() {
        struct FlushFails;

        impl AsyncWrite for FlushFails {
            fn poll_write(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
                buf: &[u8],
            ) -> std::task::Poll<std::io::Result<usize>> {
                std::task::Poll::Ready(Ok(buf.len()))
            }

            fn poll_flush(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready(Err(std::io::Error::other("the device went away")))
            }

            fn poll_shutdown(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready(Ok(()))
            }
        }

        let mut writer = FlowFilesWriterAsync::new(FlushFails);
        writer
            .write_bytes(&FlowFile::builder().content(&b"first"[..]))
            .await
            .unwrap();

        assert!(writer.flush().await.is_err());
        assert!(writer.is_poisoned());
        let err = writer
            .write_bytes(&FlowFile::builder().content(&b"second"[..]))
            .await
            .unwrap_err();
        assert!(is_poisoned_error(&err), "{err:?}");
    }

    #[tokio::test]
    async fn async_truncated_content_is_a_size_mismatch() {
        let bytes = sample().to_bytes();
        let truncated = &bytes[..bytes.len() - 2];
        let parsed = FlowFile::parse_async(truncated).await.unwrap();
        let err = parsed.into_memory_async().await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
        assert!(matches!(
            err.get_ref().and_then(|e| e.downcast_ref::<Error>()),
            Some(Error::SizeMismatch {
                expected: 5,
                actual: 3
            })
        ));
    }
}
