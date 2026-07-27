//! Parsing and serialization over `tokio::io::AsyncRead`/`AsyncWrite`.

use std::collections::HashMap;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::format::{MAGIC, MAX_VALUE_2_BYTES};
use crate::{Error, FlowFile, Limits, Result};

async fn read_field_len<R: AsyncRead + Unpin>(reader: &mut R) -> Result<usize> {
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
    Ok(String::from_utf8(buf)?)
}

pub(crate) async fn parse_header<R: AsyncRead + Unpin>(
    reader: &mut R,
    first_byte: Option<u8>,
    limits: &Limits,
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
    for _ in 0..count {
        let key = read_string(reader, limits.max_attribute_len).await?;
        let value = read_string(reader, limits.max_attribute_len).await?;
        attributes.insert(key, value);
    }
    let mut size = [0u8; 8];
    reader.read_exact(&mut size).await?;
    Ok((attributes, u64::from_be_bytes(size)))
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
    /// let flow_file = flow_file.into_bytes_async().await.unwrap();
    /// assert_eq!(flow_file.content().as_slice(), b"hello");
    /// # });
    /// ```
    pub async fn parse_async(reader: R) -> Result<Self> {
        Self::parse_async_with_limits(reader, &Limits::UNLIMITED).await
    }

    /// Like [`parse_async`](Self::parse_async), but enforcing [`Limits`] on
    /// the header. Use this for untrusted input.
    pub async fn parse_async_with_limits(mut reader: R, limits: &Limits) -> Result<Self> {
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
    ///     flow_file.into_bytes_async().await.unwrap(); // consume the content
    /// }
    /// assert_eq!(count, 2);
    /// # });
    /// ```
    pub async fn parse_next_async(reader: &'r mut R) -> Result<Option<Self>> {
        Self::parse_next_async_with_limits(reader, &Limits::UNLIMITED).await
    }

    /// Like [`parse_next_async`](Self::parse_next_async), but enforcing
    /// [`Limits`] on the header. Use this for untrusted input.
    pub async fn parse_next_async_with_limits(
        reader: &'r mut R,
        limits: &Limits,
    ) -> Result<Option<Self>> {
        let mut first = [0u8; 1];
        loop {
            match reader.read(&mut first).await {
                Ok(0) => return Ok(None),
                Ok(_) => break,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
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
/// This is not a [`Stream`](https://docs.rs/futures-core/latest/futures_core/stream/trait.Stream.html);
/// call [`next`](Self::next) in a loop instead. After an error, `next` keeps
/// returning `None`, since the stream position is no longer trustworthy.
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

    /// The next flow file, or `None` at the end of the input.
    pub async fn next(&mut self) -> Option<Result<FlowFile<Vec<u8>>>> {
        if self.done {
            return None;
        }
        let result =
            match FlowFile::parse_next_async_with_limits(&mut self.reader, &self.limits).await {
                Ok(None) => None,
                Ok(Some(flow_file)) => Some(flow_file.into_bytes_async().await),
                Err(err) => Some(Err(err)),
            };
        if !matches!(result, Some(Ok(_))) {
            self.done = true;
        }
        result
    }
}

/// Async equivalent of [`FlowFilesWriter`](crate::FlowFilesWriter): writes a
/// stream of concatenated flow files to an [`AsyncWrite`].
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
/// writer.write_bytes(&parts.next().content(&b"first"[..])).await.unwrap();
/// writer.write_bytes(&parts.next().content(&b"second"[..])).await.unwrap();
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
}

impl<W: AsyncWrite + Unpin> FlowFilesWriterAsync<W> {
    /// Write flow files to `writer`.
    pub fn new(writer: W) -> Self {
        Self { writer, count: 0 }
    }

    /// Append a flow file, streaming exactly [`size`](FlowFile::size) bytes
    /// from its content reader. Returns the number of content bytes copied.
    ///
    /// The content is never buffered, so a part may be arbitrarily large.
    pub async fn write<R: AsyncRead + Unpin>(&mut self, mut flow_file: FlowFile<R>) -> Result<u64> {
        let copied = flow_file.write_to_async(&mut self.writer).await?;
        self.count += 1;
        Ok(copied)
    }

    /// Append an in-memory flow file, whose size is known to be correct.
    pub async fn write_bytes(&mut self, flow_file: &FlowFile<Vec<u8>>) -> Result<u64> {
        self.writer.write_all(&flow_file.to_bytes()).await?;
        self.count += 1;
        Ok(flow_file.size)
    }

    /// The number of flow files written so far.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// A mutable reference to the underlying writer.
    pub fn get_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    /// Consume the writer, returning the underlying one.
    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<R: AsyncRead + Unpin> FlowFile<R> {
    /// Async version of [`FlowFile::write_to`]: serialize the flow file to a
    /// writer, reading exactly [`size`](FlowFile::size) bytes from the
    /// content reader. Returns the number of content bytes copied.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let content = &b"hello"[..]; // any `impl AsyncRead + Unpin`
    /// let mut flow_file = FlowFile::builder().reader(content, 5);
    ///
    /// let mut out = Vec::new();
    /// flow_file.write_to_async(&mut out).await.unwrap();
    /// assert_eq!(FlowFile::from_bytes(&out).unwrap().size(), 5);
    /// # });
    /// ```
    pub async fn write_to_async<W: AsyncWrite + Unpin>(&mut self, writer: &mut W) -> Result<u64> {
        writer.write_all(&self.header_bytes()).await?;
        let copied = tokio::io::copy(&mut (&mut self.content).take(self.size), writer).await?;
        if copied != self.size {
            return Err(Error::SizeMismatch {
                expected: self.size,
                actual: copied,
            });
        }
        Ok(copied)
    }

    /// Async version of [`FlowFile::into_bytes`]: reads the content to
    /// completion and validates its length against the declared size.
    pub async fn into_bytes_async(mut self) -> Result<FlowFile<Vec<u8>>> {
        let mut content = Vec::new();
        let read = (&mut self.content)
            .take(self.size)
            .read_to_end(&mut content)
            .await? as u64;
        if read != self.size {
            return Err(Error::SizeMismatch {
                expected: self.size,
                actual: read,
            });
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
        let parsed = parsed.into_bytes_async().await.unwrap();
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
    async fn async_limits_reject_oversized_attributes() {
        let bytes = FlowFile::builder()
            .attribute("key", "a value larger than the limit")
            .content(Vec::new())
            .to_bytes();

        let limits = Limits::default().max_attribute_len(8);
        assert!(matches!(
            FlowFile::parse_async_with_limits(bytes.as_slice(), &limits).await,
            Err(Error::AttributeTooLong { limit: 8, .. })
        ));
    }

    #[tokio::test]
    async fn async_truncated_content_is_a_size_mismatch() {
        let bytes = sample().to_bytes();
        let truncated = &bytes[..bytes.len() - 2];
        let parsed = FlowFile::parse_async(truncated).await.unwrap();
        assert!(matches!(
            parsed.into_bytes_async().await,
            Err(Error::SizeMismatch {
                expected: 5,
                actual: 3
            })
        ));
    }
}
