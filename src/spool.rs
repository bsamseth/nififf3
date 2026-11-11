//! Content held in memory or in a temporary file, whichever its size calls
//! for.

use std::io::{self, Cursor, SeekFrom};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{
    AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter, ReadBuf,
};

use crate::{FlowFile, FlowFileBuilder};

/// How much goes to or from the temporary file per operation. Every
/// `tokio::fs` call is a hop to a blocking thread, so this amortizes them.
const FILE_BUFFER: usize = 128 * 1024;

/// Where spooled content goes, by size.
///
/// Content up to [`max_in_memory`](Self::max_in_memory) bytes is held in
/// memory. Anything larger goes to an anonymous temporary file, created in
/// [`directory`](Self::directory) or in the system's temporary directory.
///
/// The defaults suit most services. To change them for the axum extractors,
/// add the config to the router as an extension:
///
/// ```
/// # #[cfg(feature = "axum")] {
/// use axum::{Extension, Router};
/// use nififf3::SpoolConfig;
///
/// let app: Router = Router::new()
///     .layer(Extension(SpoolConfig::new().with_max_in_memory(64 << 20)));
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpoolConfig {
    max_in_memory: usize,
    directory: Option<PathBuf>,
}

impl SpoolConfig {
    /// What [`max_in_memory`](Self::max_in_memory) is unless set: 4 MiB.
    pub const DEFAULT_MAX_IN_MEMORY: usize = 4 << 20;

    /// The default configuration. The same as [`Default::default`], but
    /// `const`.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_in_memory: Self::DEFAULT_MAX_IN_MEMORY,
            directory: None,
        }
    }

    /// Hold content up to `bytes` long in memory, and spool anything larger
    /// to disk. Zero sends everything to disk.
    #[must_use]
    pub fn with_max_in_memory(mut self, bytes: usize) -> Self {
        self.max_in_memory = bytes;
        self
    }

    /// Create temporary files in `directory` rather than in the system's
    /// temporary directory.
    ///
    /// Use it when the system directory is small or on the wrong disk. The
    /// files are anonymous, meaning they are unlinked as soon as they are
    /// created where the platform allows it, so nothing is left behind if the
    /// process dies.
    #[must_use]
    pub fn with_directory(mut self, directory: impl Into<PathBuf>) -> Self {
        self.directory = Some(directory.into());
        self
    }

    /// The largest content held in memory.
    #[must_use]
    pub fn max_in_memory(&self) -> usize {
        self.max_in_memory
    }

    /// Where temporary files go, or `None` for the system default.
    #[must_use]
    pub fn directory(&self) -> Option<&Path> {
        self.directory.as_deref()
    }
}

impl Default for SpoolConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Content that has been read in full, held in memory or in a temporary file
/// depending on its size. See [`SpoolConfig`].
///
/// The axum extractor for `FlowFile<Spool>` produces one for every request,
/// and so do [`FlowFile::spool_async`] and [`FlowFileBuilder::spool_async`].
/// Whatever produced it has already read the content to the end, so a
/// `Spool` never waits on a network or on another task. Reading it yields the
/// content from the current position, and it can be rewound and read again.
///
/// It is an
/// [`AsyncRead`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncRead.html)
/// and an
/// [`AsyncSeek`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncSeek.html),
/// so it feeds any async decoder, including ones that seek. For a synchronous
/// decoder, wrap it in `tokio_util::io::SyncIoBridge` on a blocking thread.
/// When the content is in memory, [`as_bytes`](Self::as_bytes) gives it
/// without a read.
///
/// Dropping it releases the memory or the file.
#[derive(Debug)]
pub struct Spool {
    inner: Inner,
    len: u64,
}

#[derive(Debug)]
enum Inner {
    Memory(Cursor<Vec<u8>>),
    File(BufReader<tokio::fs::File>),
}

impl Spool {
    /// Spool content that is already in memory.
    #[must_use]
    pub fn from_vec(bytes: Vec<u8>) -> Self {
        Self {
            len: bytes.len() as u64,
            inner: Inner::Memory(Cursor::new(bytes)),
        }
    }

    /// Read `reader` to its end into a spool.
    ///
    /// The content goes to memory or to disk as `config` says. Use
    /// [`from_reader_exact`](Self::from_reader_exact) when the length is known
    /// in advance, which is the case for a flow file's content.
    ///
    /// ```
    /// use nififf3::{Spool, SpoolConfig};
    ///
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let spool = Spool::from_reader(&b"hello"[..], &SpoolConfig::default()).await?;
    /// assert_eq!(spool.len(), 5);
    /// assert_eq!(spool.as_bytes(), Some(&b"hello"[..]));
    /// # Ok::<(), std::io::Error>(())
    /// # }).unwrap();
    /// ```
    ///
    /// # Errors
    ///
    /// Whatever the reader reports, or the temporary file cannot be created
    /// or written.
    pub async fn from_reader<R: AsyncRead + Unpin>(
        mut reader: R,
        config: &SpoolConfig,
    ) -> io::Result<Self> {
        spool(&mut reader, None, config)
            .await
            .map_err(|err| err.error)
    }

    /// Read exactly `size` bytes from `reader` into a spool.
    ///
    /// Knowing the size up front means content over the threshold goes
    /// straight to disk, instead of into memory first.
    ///
    /// # Errors
    ///
    /// As [`from_reader`](Self::from_reader), plus an
    /// [`UnexpectedEof`](io::ErrorKind::UnexpectedEof) carrying
    /// [`Error::SizeMismatch`](crate::Error::SizeMismatch) if the reader ends
    /// before `size` bytes. Nothing checks for bytes beyond `size`; they are
    /// left in the reader.
    pub async fn from_reader_exact<R: AsyncRead + Unpin>(
        mut reader: R,
        size: u64,
        config: &SpoolConfig,
    ) -> io::Result<Self> {
        spool(&mut reader, Some(size), config)
            .await
            .map_err(|err| err.error)
    }

    /// The length of the content in bytes.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Whether the content is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Whether the content is held in memory rather than in a file.
    #[must_use]
    pub fn is_in_memory(&self) -> bool {
        matches!(self.inner, Inner::Memory(_))
    }

    /// The whole content, if it is in memory. The read position does not
    /// matter here.
    #[must_use]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match &self.inner {
            Inner::Memory(cursor) => Some(cursor.get_ref()),
            Inner::File(_) => None,
        }
    }

    /// The whole content as a `Vec`, reading the file if there is one. The
    /// read position does not matter here.
    ///
    /// # Errors
    ///
    /// If the file cannot be read.
    pub async fn into_bytes(mut self) -> io::Result<Vec<u8>> {
        match self.inner {
            Inner::Memory(cursor) => Ok(cursor.into_inner()),
            Inner::File(ref mut file) => {
                file.rewind().await?;
                let mut bytes = Vec::new();
                crate::async_io::read_declared(file, self.len, &mut bytes).await?;
                Ok(bytes)
            }
        }
    }

    /// Move the read position back to the start, to read the content again.
    ///
    /// # Errors
    ///
    /// If seeking the file fails.
    pub async fn rewind(&mut self) -> io::Result<()> {
        match &mut self.inner {
            Inner::Memory(cursor) => {
                cursor.set_position(0);
                Ok(())
            }
            Inner::File(file) => file.rewind().await.map(|_| ()),
        }
    }
}

impl From<Vec<u8>> for Spool {
    fn from(bytes: Vec<u8>) -> Self {
        Self::from_vec(bytes)
    }
}

impl AsyncRead for Spool {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut self.get_mut().inner {
            Inner::Memory(cursor) => Pin::new(cursor).poll_read(cx, buf),
            Inner::File(file) => Pin::new(file).poll_read(cx, buf),
        }
    }
}

impl AsyncSeek for Spool {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        match &mut self.get_mut().inner {
            Inner::Memory(cursor) => Pin::new(cursor).start_seek(position),
            Inner::File(file) => Pin::new(file).start_seek(position),
        }
    }

    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        match &mut self.get_mut().inner {
            Inner::Memory(cursor) => Pin::new(cursor).poll_complete(cx),
            Inner::File(file) => Pin::new(file).poll_complete(cx),
        }
    }
}

/// Which end of the copy failed. The axum extractor answers a failing client
/// and a failing disk with different status codes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Side {
    /// The reader being spooled.
    Source,
    /// The temporary file.
    Sink,
}

#[derive(Debug)]
pub(crate) struct SpoolError {
    /// Only the axum extractors act on the side; the public spooling calls
    /// flatten the error.
    #[cfg_attr(not(feature = "axum"), allow(dead_code))]
    pub(crate) side: Side,
    pub(crate) error: io::Error,
}

fn source(error: io::Error) -> SpoolError {
    SpoolError {
        side: Side::Source,
        error,
    }
}

fn sink(error: io::Error) -> SpoolError {
    SpoolError {
        side: Side::Sink,
        error,
    }
}

/// Read `reader` into a spool: exactly `expected` bytes when it is given,
/// otherwise to the end.
#[cfg_attr(
    feature = "tracing",
    tracing::instrument(
        level = "debug",
        name = "spool",
        skip_all,
        fields(expected, destination = tracing::field::Empty, bytes = tracing::field::Empty)
    )
)]
pub(crate) async fn spool<R: AsyncRead + Unpin>(
    reader: &mut R,
    expected: Option<u64>,
    config: &SpoolConfig,
) -> Result<Spool, SpoolError> {
    let threshold = config.max_in_memory as u64;
    let mut memory = Vec::new();
    match expected {
        // The size is known and small: exactly that much, into memory.
        Some(size) if size <= threshold => {
            let read = crate::async_io::read_declared(reader, size, &mut memory)
                .await
                .map_err(source)?;
            if read != size {
                return Err(source(crate::error::truncated(size, read)));
            }
            return Ok(finished(Spool::from_vec(memory)));
        }
        // The size is known and large: straight to disk.
        Some(_) => {}
        // The size is unknown: memory first, and disk if it runs past the
        // threshold. One byte past is all it takes to know.
        None => {
            let probe = threshold.saturating_add(1);
            reader
                .take(probe)
                .read_to_end(&mut memory)
                .await
                .map_err(source)?;
            if memory.len() as u64 <= threshold {
                return Ok(finished(Spool::from_vec(memory)));
            }
        }
    }

    let file = create_file(config.directory.clone()).await.map_err(sink)?;
    let mut writer = BufWriter::with_capacity(FILE_BUFFER, file);
    writer.write_all(&memory).await.map_err(sink)?;
    let mut total = memory.len() as u64;
    drop(memory);

    let mut buf = vec![0u8; FILE_BUFFER];
    loop {
        let want = match expected {
            Some(size) => {
                usize::try_from(size - total).map_or(buf.len(), |left| left.min(buf.len()))
            }
            None => buf.len(),
        };
        if want == 0 {
            break;
        }
        let read = reader.read(&mut buf[..want]).await.map_err(source)?;
        if read == 0 {
            if let Some(size) = expected {
                return Err(source(crate::error::truncated(size, total)));
            }
            break;
        }
        writer.write_all(&buf[..read]).await.map_err(sink)?;
        total += read as u64;
    }
    writer.flush().await.map_err(sink)?;
    let mut file = writer.into_inner();
    file.rewind().await.map_err(sink)?;
    Ok(finished(Spool {
        inner: Inner::File(BufReader::with_capacity(FILE_BUFFER, file)),
        len: total,
    }))
}

/// Record where the content ended up, then hand it on.
fn finished(spool: Spool) -> Spool {
    #[cfg(feature = "tracing")]
    {
        let span = tracing::Span::current();
        span.record(
            "destination",
            if spool.is_in_memory() {
                "memory"
            } else {
                "disk"
            },
        );
        span.record("bytes", spool.len);
        tracing::debug!("spooled content");
    }
    spool
}

/// An anonymous temporary file, created off the async threads since creating
/// one is a blocking call.
async fn create_file(directory: Option<PathBuf>) -> io::Result<tokio::fs::File> {
    let file = tokio::task::spawn_blocking(move || match directory {
        Some(directory) => tempfile::tempfile_in(directory),
        None => tempfile::tempfile(),
    })
    .await
    .map_err(io::Error::other)??;
    Ok(tokio::fs::File::from_std(file))
}

impl<R: AsyncRead + Unpin> FlowFile<R> {
    /// Read the content to its end into a [`Spool`], with the default
    /// [`SpoolConfig`].
    ///
    /// Use it to take a flow file off a stream, such as a request body, and
    /// hold it in full without deciding between memory and disk yourself.
    /// Exactly [`size`](FlowFile::size) bytes are read.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let bytes = FlowFile::builder()
    ///     .attribute("filename", "big.tar")
    ///     .content(&b"contents"[..])
    ///     .to_bytes();
    ///
    /// let spooled = FlowFile::parse_async(bytes.as_slice()).await?.spool_async().await?;
    /// assert_eq!(spooled.attribute("filename"), Some("big.tar"));
    /// assert_eq!(spooled.content().as_bytes(), Some(&b"contents"[..]));
    /// # Ok::<(), nififf3::Error>(())
    /// # }).unwrap();
    /// ```
    ///
    /// # Errors
    ///
    /// As [`Spool::from_reader_exact`].
    pub async fn spool_async(self) -> io::Result<FlowFile<Spool>> {
        self.spool_async_with(&SpoolConfig::new()).await
    }

    /// As [`spool_async`](Self::spool_async), with a [`SpoolConfig`] of your
    /// own.
    ///
    /// # Errors
    ///
    /// As [`Spool::from_reader_exact`].
    pub async fn spool_async_with(self, config: &SpoolConfig) -> io::Result<FlowFile<Spool>> {
        let (size, attributes, content) = self.into_parts();
        let spool = Spool::from_reader_exact(content, size, config).await?;
        Ok(FlowFile::from_raw_parts(size, attributes, spool))
    }
}

impl FlowFileBuilder {
    /// Finish the build by reading an [`AsyncRead`] to its end into a
    /// [`Spool`], with the default [`SpoolConfig`].
    ///
    /// Use it when the content size is not known up front and may be large:
    /// the output of a compressor, for instance. The size is what was read.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let reader = &b"length unknown ahead of time"[..];
    /// let flow_file = FlowFile::builder().spool_async(reader).await?;
    /// assert_eq!(flow_file.size(), 28);
    /// # Ok::<(), std::io::Error>(())
    /// # }).unwrap();
    /// ```
    ///
    /// # Errors
    ///
    /// As [`Spool::from_reader`].
    pub async fn spool_async(self, content: impl AsyncRead + Unpin) -> io::Result<FlowFile<Spool>> {
        self.spool_async_with(content, &SpoolConfig::new()).await
    }

    /// As [`spool_async`](Self::spool_async), with a [`SpoolConfig`] of your
    /// own.
    ///
    /// # Errors
    ///
    /// As [`Spool::from_reader`].
    pub async fn spool_async_with(
        self,
        content: impl AsyncRead + Unpin,
        config: &SpoolConfig,
    ) -> io::Result<FlowFile<Spool>> {
        let spool = Spool::from_reader(content, config).await?;
        let size = spool.len();
        Ok(self.reader(spool, size))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Truncation is the point: the low byte of each step is the sample.
    #[allow(clippy::cast_possible_truncation)]
    fn noise(len: usize) -> Vec<u8> {
        let mut s = 0x9E37_79B9_7F4A_7C15u64;
        (0..len)
            .map(|_| {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                (s >> 24) as u8
            })
            .collect()
    }

    fn small() -> SpoolConfig {
        SpoolConfig::new().with_max_in_memory(1024)
    }

    /// The threshold decides where the content lives, whichever way the
    /// length is learned, and the content reads back the same from both.
    #[tokio::test]
    async fn the_threshold_picks_memory_or_disk_and_both_read_back() {
        for len in [0usize, 1, 1023, 1024, 1025, 5 * FILE_BUFFER + 7] {
            let content = noise(len);
            let in_memory = len <= 1024;

            let exact = Spool::from_reader_exact(content.as_slice(), len as u64, &small())
                .await
                .unwrap();
            assert_eq!(exact.is_in_memory(), in_memory, "exact, {len}");
            assert_eq!(exact.len(), len as u64);
            assert_eq!(exact.into_bytes().await.unwrap(), content, "exact, {len}");

            let to_end = Spool::from_reader(content.as_slice(), &small())
                .await
                .unwrap();
            assert_eq!(to_end.is_in_memory(), in_memory, "to end, {len}");
            assert_eq!(to_end.len(), len as u64);
            let mut read = Vec::new();
            let mut to_end = to_end;
            to_end.read_to_end(&mut read).await.unwrap();
            assert_eq!(read, content, "to end, {len}");
        }
    }

    #[tokio::test]
    async fn a_spool_can_be_rewound_and_read_again() {
        for len in [10usize, 3 * FILE_BUFFER] {
            let content = noise(len);
            let mut spool = Spool::from_reader(content.as_slice(), &small())
                .await
                .unwrap();

            let mut first = Vec::new();
            spool.read_to_end(&mut first).await.unwrap();
            let mut nothing = Vec::new();
            spool.read_to_end(&mut nothing).await.unwrap();
            assert!(nothing.is_empty(), "exhausted until rewound");

            spool.rewind().await.unwrap();
            let mut second = Vec::new();
            spool.read_to_end(&mut second).await.unwrap();
            assert_eq!(first, content);
            assert_eq!(second, content);

            // And `AsyncSeek` works as the trait, not only through `rewind`.
            spool.seek(SeekFrom::Start(len as u64 - 3)).await.unwrap();
            let mut tail = Vec::new();
            spool.read_to_end(&mut tail).await.unwrap();
            assert_eq!(tail, &content[len - 3..]);
        }
    }

    /// A source that ends before the declared size is an error, at both
    /// sizes, and the error says which side failed.
    #[tokio::test]
    async fn an_exact_spool_reports_a_short_source() {
        for (declared, available) in [(100u64, 60usize), (5000, 3000)] {
            let content = noise(available);
            let mut reader = content.as_slice();
            let err = spool(&mut reader, Some(declared), &small())
                .await
                .unwrap_err();
            assert_eq!(err.side, Side::Source, "{declared}");
            assert_eq!(err.error.kind(), io::ErrorKind::UnexpectedEof);
            assert!(matches!(
                crate::Error::from(err.error),
                crate::Error::SizeMismatch { expected, actual }
                    if expected == declared && actual == available as u64
            ));
        }
    }

    #[tokio::test]
    async fn an_exact_spool_leaves_what_follows_in_the_reader() {
        let mut reader: &[u8] = b"hello, world";
        let spool = Spool::from_reader_exact(&mut reader, 5, &small())
            .await
            .unwrap();
        assert_eq!(spool.as_bytes(), Some(&b"hello"[..]));
        assert_eq!(reader, b", world");
    }

    #[tokio::test]
    async fn a_failing_sink_is_told_apart_from_a_failing_source() {
        let missing = std::env::temp_dir().join("nififf3-no-such-directory");
        let config = SpoolConfig::new()
            .with_max_in_memory(0)
            .with_directory(missing);
        let mut reader: &[u8] = b"anything";
        let err = spool(&mut reader, None, &config).await.unwrap_err();
        assert_eq!(err.side, Side::Sink);
    }

    #[tokio::test]
    async fn the_configured_directory_is_used() {
        let dir = std::env::temp_dir().join("nififf3-spool-test");
        std::fs::create_dir_all(&dir).unwrap();
        let config = SpoolConfig::new()
            .with_max_in_memory(0)
            .with_directory(&dir);
        assert_eq!(config.directory(), Some(dir.as_path()));
        assert_eq!(config.max_in_memory(), 0);

        let spool = Spool::from_reader(&b"to disk"[..], &config).await.unwrap();
        assert!(!spool.is_in_memory());
        assert_eq!(spool.into_bytes().await.unwrap(), b"to disk");
    }

    #[tokio::test]
    async fn spooling_a_flow_file_keeps_its_attributes_and_size() {
        let content = noise(3000);
        let bytes = FlowFile::builder()
            .attribute("filename", "big.bin")
            .content(content.clone())
            .to_bytes();

        let parsed = FlowFile::parse_async(bytes.as_slice()).await.unwrap();
        let spooled = parsed.spool_async_with(&small()).await.unwrap();
        assert_eq!(spooled.size(), 3000);
        assert_eq!(spooled.attribute("filename"), Some("big.bin"));
        assert!(!spooled.content().is_in_memory());
        assert_eq!(
            spooled.into_memory_async().await.unwrap().content(),
            &content
        );
    }

    #[tokio::test]
    async fn the_builder_takes_its_size_from_what_was_read() {
        let flow_file = FlowFile::builder()
            .attribute("k", "v")
            .spool_async(&b"spooled"[..])
            .await
            .unwrap();
        assert_eq!(flow_file.size(), 7);
        let mut out = Vec::new();
        flow_file.write_to_async(&mut out).await.unwrap();
        assert_eq!(
            out,
            FlowFile::builder()
                .attribute("k", "v")
                .content(&b"spooled"[..])
                .to_bytes()
        );
    }
}
