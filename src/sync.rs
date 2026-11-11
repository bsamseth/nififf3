//! Parsing and serialization over `std::io::Read`/`Write`.

use std::io::{self, Read, Write};

use crate::format::{self, Header, HeaderDecoder};
use crate::{Content, Error, FlowFile, Result};

/// Read into `buf` until it is full or the reader ends, retrying
/// interruptions. Returns how many bytes arrived.
fn fill(reader: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
    Ok(filled)
}

/// Read a header off the front of `reader`.
///
/// `Ok(None)` means the reader ended before the first byte, which is the
/// clean end of a stream of flow files. Ending anywhere later is
/// [`Error::TruncatedHeader`].
pub(crate) fn read_header(reader: &mut impl Read) -> Result<Option<Header>> {
    let mut decoder = HeaderDecoder::new();
    loop {
        let chunk = decoder.next_read();
        let wanted = chunk.len();
        let got = fill(reader, chunk)?;
        if got < wanted {
            if got == 0 && !decoder.started() {
                return Ok(None);
            }
            return Err(Error::TruncatedHeader {
                needed: wanted - got,
                expecting: decoder.expecting(),
            });
        }
        if let Some(header) = decoder.advance()? {
            return Ok(Some(header));
        }
    }
}

/// The error for a stream that ended where a flow file had to begin.
fn missing_header() -> Error {
    Error::TruncatedHeader {
        needed: format::MAGIC.len(),
        expecting: "the magic header",
    }
}

/// Read up to `len` bytes into `buf`, returning how many arrived.
///
/// Falling short is not an error here, because the callers report it in
/// different ways. See [`format::RESERVE_AHEAD`] for why this is not
/// `take(len).read_to_end(buf)`.
pub(crate) fn read_declared(
    reader: &mut impl Read,
    len: u64,
    buf: &mut Vec<u8>,
) -> io::Result<u64> {
    let mut done = 0u64;
    while done < len {
        let left = usize::try_from(len - done).unwrap_or(usize::MAX);
        let step = format::reserve_step(usize::try_from(done).unwrap_or(usize::MAX), left);
        buf.reserve_exact(step);
        let read = reader.take(step as u64).read_to_end(buf)?;
        if read == 0 {
            break; // the source ended early
        }
        done += read as u64;
    }
    Ok(done)
}

impl<R: Read> FlowFile<Content<R>> {
    /// Parse a flow file from a reader, consuming only the header.
    ///
    /// The returned flow file's content is the reader, limited to the declared
    /// content size, and it reports the reader ending early as an error. See
    /// [`Content`]. Nothing is buffered, so the content can be any size.
    ///
    /// The header is read in small increments, so wrap an unbuffered source in
    /// a [`std::io::BufReader`]. A file or a socket is unbuffered.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let bytes = FlowFile::builder().content(&b"hello"[..]).to_bytes();
    ///
    /// // Only the header is consumed; the content can be read incrementally.
    /// let flow_file = FlowFile::parse(bytes.as_slice()).unwrap();
    /// assert_eq!(flow_file.size(), 5);
    /// let flow_file = flow_file.into_memory().unwrap();
    /// assert_eq!(flow_file.content().as_slice(), b"hello");
    /// ```
    ///
    /// # Errors
    ///
    /// [`Error::InvalidMagic`] if the input does not begin with `NiFiFF3`,
    /// [`Error::InvalidAttribute`] for an attribute that is not UTF-8,
    /// [`Error::TruncatedHeader`] if the input ends inside the header, and
    /// [`Error::Io`] for anything the reader reports. Nothing here depends on
    /// the content, which has not been read yet.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            level = "debug",
            name = "parse",
            skip_all,
            fields(
                uuid = tracing::field::Empty,
                filename = tracing::field::Empty,
                size = tracing::field::Empty,
            )
        )
    )]
    pub fn parse(mut reader: R) -> Result<Self> {
        let header = read_header(&mut reader)?.ok_or_else(missing_header)?;
        let size = header.size;
        Ok(FlowFile::parsed(header, Content::new(reader, size)))
    }
}

impl<'r, R: Read> FlowFile<Content<&'r mut R>> {
    /// Parse the next flow file from a stream of concatenated flow files.
    ///
    /// Returns `Ok(None)` on a clean end of input, meaning the stream ended
    /// exactly where a flow file would have begun.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let mut bytes = FlowFile::builder().content(&b"first"[..]).to_bytes();
    /// bytes.extend(FlowFile::builder().content(&b"second"[..]).to_bytes());
    ///
    /// let mut reader = bytes.as_slice();
    /// let mut count = 0;
    /// while let Some(flow_file) = FlowFile::parse_next(&mut reader).unwrap() {
    ///     count += 1;
    ///     flow_file.into_memory().unwrap(); // consume the content
    /// }
    /// assert_eq!(count, 2);
    /// ```
    ///
    /// # Every flow file's content must be consumed
    ///
    /// The returned flow file's content is the reader itself, positioned at the
    /// first content byte, and the next flow file begins where that content
    /// ends. So each content has to be consumed before the next flow file is
    /// parsed: [`into_memory`](FlowFile::into_memory) reads it into a buffer,
    /// [`write_to`](FlowFile::write_to) copies it out, and
    /// [`skip_content`](FlowFile::skip_content) discards it.
    ///
    /// Dropping a flow file with its content unread leaves the reader inside
    /// that content, and the next call parses the content as though it were a
    /// flow file. Usually that fails. It does not fail when the content itself
    /// begins with a valid header, which happens when one flow file carries
    /// another, and then you get back a plausible flow file that was never
    /// sent.
    ///
    /// [`FlowFilesReader`] does this bookkeeping for you, and is the better
    /// choice unless you need the reader back between flow files.
    ///
    /// # Errors
    ///
    /// As [`FlowFile::parse`]. A stream ending part-way through a header is
    /// [`Error::TruncatedHeader`] rather than `Ok(None)`, because only a clean
    /// boundary ends the iteration.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            level = "debug",
            name = "parse_next",
            skip_all,
            fields(
                uuid = tracing::field::Empty,
                filename = tracing::field::Empty,
                size = tracing::field::Empty,
            )
        )
    )]
    pub fn parse_next(reader: &'r mut R) -> Result<Option<Self>> {
        let Some(header) = read_header(reader)? else {
            #[cfg(feature = "tracing")]
            tracing::trace!("stream ended on a flow file boundary");
            return Ok(None);
        };
        let size = header.size;
        Ok(Some(FlowFile::parsed(header, Content::new(reader, size))))
    }
}

impl<R: Read> FlowFile<R> {
    /// Serialize the flow file to a writer, reading exactly [`size`] bytes
    /// from the content reader.
    ///
    /// Returns the number of content bytes copied. It takes `self`, because it
    /// reads the content to the end and a second call would write a second
    /// header and then fail. Read whatever you need from
    /// [`attributes`](FlowFile::attributes) first.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let content = &b"hello"[..]; // any `impl Read`
    /// let flow_file = FlowFile::builder().reader(content, 5);
    ///
    /// let mut out = Vec::new();
    /// flow_file.write_to(&mut out).unwrap();
    /// assert_eq!(FlowFile::from_bytes(&out).unwrap().size(), 5);
    /// ```
    ///
    /// # Errors
    ///
    /// Only I/O. A content reader that ends before `size` bytes gives an
    /// [`UnexpectedEof`](io::ErrorKind::UnexpectedEof) carrying an
    /// [`Error::SizeMismatch`], and anything else comes from the writer. Either
    /// way, the header has already been written, along with whatever content
    /// was copied before the failure.
    ///
    /// # Panics
    ///
    /// If an attribute key or value is longer than `u32::MAX` bytes, which
    /// the wire format cannot express. As [`FlowFile::to_bytes`].
    ///
    /// [`size`]: FlowFile::size
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            level = "debug",
            name = "write_to",
            skip_all,
            fields(
                uuid = self.attribute(crate::attr::UUID),
                filename = self.attribute(crate::attr::FILENAME),
                size = self.size,
            )
        )
    )]
    pub fn write_to<W: Write>(mut self, writer: &mut W) -> io::Result<u64> {
        writer.write_all(&self.header_bytes())?;
        let copied = io::copy(&mut (&mut self.content).take(self.size), writer)?;
        if copied != self.size {
            // The header is already out, so the stream now holds a flow file
            // that promises more than it carries.
            #[cfg(feature = "tracing")]
            tracing::warn!(
                copied,
                "content ended early, leaving a truncated flow file in the stream"
            );
            return Err(crate::error::truncated(self.size, copied));
        }
        #[cfg(feature = "tracing")]
        tracing::debug!(copied, "wrote flow file");
        Ok(copied)
    }

    /// Discard the content, consuming exactly [`size`] bytes from the reader.
    ///
    /// Call this when only the attributes were wanted from a flow file
    /// [`parse_next`](FlowFile::parse_next) produced, so that the stream is
    /// left where the next flow file begins.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let mut bytes = FlowFile::builder().attribute("n", "1").content(&b"aaa"[..]).to_bytes();
    /// bytes.extend(FlowFile::builder().attribute("n", "2").content(&b"bbb"[..]).to_bytes());
    ///
    /// let mut reader = bytes.as_slice();
    /// let mut names = Vec::new();
    /// while let Some(flow_file) = FlowFile::parse_next(&mut reader)? {
    ///     names.push(flow_file.attribute("n").unwrap().to_string());
    ///     flow_file.skip_content()?; // the content was not wanted, but must go
    /// }
    ///
    /// assert_eq!(names, ["1", "2"]);
    /// # Ok::<(), nififf3::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Only I/O. Content that ends before [`size`] is
    /// [`UnexpectedEof`](io::ErrorKind::UnexpectedEof) carrying an
    /// [`Error::SizeMismatch`], as [`into_memory`](Self::into_memory).
    ///
    /// [`size`]: FlowFile::size
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            level = "trace",
            name = "skip_content",
            skip_all,
            fields(
                uuid = self.attribute(crate::attr::UUID),
                filename = self.attribute(crate::attr::FILENAME),
                size = self.size,
            )
        )
    )]
    pub fn skip_content(mut self) -> io::Result<u64> {
        let skipped = io::copy(&mut (&mut self.content).take(self.size), &mut io::sink())?;
        if skipped != self.size {
            return Err(crate::error::truncated(self.size, skipped));
        }
        #[cfg(feature = "tracing")]
        tracing::trace!(skipped, "discarded content");
        Ok(skipped)
    }

    /// Read the content to completion, producing an in-memory flow file.
    ///
    /// This moves the content into memory and serializes nothing. It is the
    /// inverse of [`into_reader`](FlowFile::into_reader). The similarly named
    /// [`to_bytes`](FlowFile::to_bytes) serializes the whole flow file, header
    /// and all, to the wire format.
    ///
    /// It validates that exactly [`size`] bytes of content were available.
    ///
    /// # Errors
    ///
    /// Only I/O. Content that ends early is
    /// [`UnexpectedEof`](io::ErrorKind::UnexpectedEof) carrying an
    /// [`Error::SizeMismatch`].
    ///
    /// [`size`]: FlowFile::size
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            level = "trace",
            name = "into_memory",
            skip_all,
            fields(
                uuid = self.attribute(crate::attr::UUID),
                filename = self.attribute(crate::attr::FILENAME),
                size = self.size,
            )
        )
    )]
    pub fn into_memory(mut self) -> io::Result<FlowFile<Vec<u8>>> {
        let mut content = Vec::new();
        let read = read_declared(&mut self.content, self.size, &mut content)?;
        if read != self.size {
            return Err(crate::error::truncated(self.size, read));
        }
        #[cfg(feature = "tracing")]
        tracing::trace!(read, "read content into memory");
        Ok(FlowFile::from_raw_parts(
            self.size,
            self.attributes,
            content,
        ))
    }
}

impl FlowFile<Vec<u8>> {
    /// Read one whole flow file from a reader, content included.
    ///
    /// This is [`parse`](FlowFile::parse) followed by
    /// [`into_memory`](FlowFile::into_memory), with the two error types already
    /// reconciled, so a truncated content arrives as [`Error::SizeMismatch`]
    /// the way [`from_bytes`](FlowFile::from_bytes) reports it.
    ///
    /// Exactly one flow file is read, and the reader is left on the byte after
    /// it, so trailing bytes are not this flow file's business.
    /// [`from_bytes`](FlowFile::from_bytes) differs there: it is given a buffer
    /// that must hold one flow file and nothing else. To read several in a row,
    /// use [`FlowFiles`], which is this call in a loop.
    ///
    /// The header is read in small increments, so wrap an unbuffered source in
    /// a [`std::io::BufReader`].
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let bytes = FlowFile::builder()
    ///     .attribute("filename", "greeting.txt")
    ///     .content(&b"hello"[..])
    ///     .to_bytes();
    ///
    /// let flow_file = FlowFile::from_reader(bytes.as_slice())?;
    /// assert_eq!(flow_file.content().as_slice(), b"hello");
    /// assert_eq!(flow_file.attribute("filename"), Some("greeting.txt"));
    /// # Ok::<(), nififf3::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// As [`parse`](FlowFile::parse) for the header, plus
    /// [`Error::SizeMismatch`] if the content ends before the declared
    /// [`size`](FlowFile::size).
    pub fn from_reader<R: Read>(reader: R) -> Result<Self> {
        Ok(FlowFile::parse(reader)?.into_memory()?)
    }

    /// Serialize the flow file to a writer.
    ///
    /// This is the in-memory counterpart to [`write_to`](FlowFile::write_to),
    /// which is bounded on `R: Read` and so does not apply to a `Vec<u8>`. It
    /// takes `&self`, because writing in-memory content does not exhaust it,
    /// and returns the number of content bytes written.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let flow_file = FlowFile::builder().content(&b"hello"[..]);
    /// let mut out = Vec::new();
    /// flow_file.write_bytes_to(&mut out)?;
    ///
    /// assert_eq!(FlowFile::from_bytes(&out)?, flow_file);
    /// # Ok::<(), nififf3::Error>(())
    /// ```
    ///
    /// The header and the content go out as two writes rather than one, so
    /// that the content is never copied into a second buffer. Wrap an
    /// unbuffered writer in a [`std::io::BufWriter`] if the syscalls matter
    /// more than the copy.
    ///
    /// # Errors
    ///
    /// Whatever the writer returns. A failure after the header has gone out
    /// leaves a partial flow file in the stream, as
    /// [`write_to`](FlowFile::write_to) does. [`FlowFilesWriter`] tracks that
    /// for you.
    ///
    /// # Panics
    ///
    /// As [`to_bytes`](FlowFile::to_bytes): for an attribute the wire format
    /// cannot express, or a declared size that disagrees with the content.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            level = "debug",
            name = "write_bytes_to",
            skip_all,
            fields(
                uuid = self.attribute(crate::attr::UUID),
                filename = self.attribute(crate::attr::FILENAME),
                size = self.size,
            )
        )
    )]
    pub fn write_bytes_to<W: Write>(&self, writer: &mut W) -> io::Result<u64> {
        self.assert_size_matches();
        writer.write_all(&self.header_bytes())?;
        writer.write_all(&self.content)?;
        #[cfg(feature = "tracing")]
        tracing::debug!(written = self.size, "wrote flow file");
        Ok(self.size)
    }
}

/// Iterator over a stream of concatenated flow files, buffering each content.
///
/// Yields each flow file with its content in memory, and ends on a clean end
/// of input. After an error the iterator is fused, meaning it keeps returning
/// `None`, because the stream position is no longer trustworthy.
///
/// Buffering each content is what lets this be an ordinary [`Iterator`]
/// yielding owned flow files. When a content is too large for that, use
/// [`FlowFilesReader`], which streams instead.
///
/// A content that ends before its declared size is reported as
/// [`Error::SizeMismatch`] directly, the way [`FlowFile::from_bytes`] does.
///
/// ```
/// use nififf3::{FlowFile, FlowFiles};
///
/// let mut bytes = FlowFile::builder().content(&b"first"[..]).to_bytes();
/// bytes.extend(FlowFile::builder().content(&b"second"[..]).to_bytes());
///
/// let contents: Vec<_> = FlowFiles::new(bytes.as_slice())
///     .map(|flow_file| flow_file.unwrap().into_content())
///     .collect();
/// assert_eq!(contents, [b"first".to_vec(), b"second".to_vec()]);
/// ```
#[derive(Debug)]
pub struct FlowFiles<R> {
    reader: R,
    done: bool,
}

impl<R: Read> FlowFiles<R> {
    /// Iterate over the flow files in `reader`.
    ///
    /// The header parsing reads in small increments, so wrap an unbuffered
    /// source in a [`std::io::BufReader`].
    pub fn new(reader: R) -> Self {
        Self {
            reader,
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

    /// Consume the iterator, returning the underlying reader.
    ///
    /// The reader is left wherever iteration stopped. After a `None` that is
    /// the end of the flow files and the start of whatever follows them.
    /// Stopping early leaves the reader part-way through a flow file, and
    /// after an error its position is not meaningful at all.
    ///
    /// ```
    /// use nififf3::{FlowFile, FlowFiles};
    /// use std::io::Read;
    ///
    /// let mut bytes = FlowFile::builder().content(&b"first"[..]).to_bytes();
    /// bytes.extend_from_slice(b"and then something else");
    ///
    /// let mut flow_files = FlowFiles::new(bytes.as_slice());
    /// flow_files.next().unwrap()?;
    ///
    /// let mut trailer = Vec::new();
    /// flow_files.into_inner().read_to_end(&mut trailer)?;
    /// assert_eq!(trailer, b"and then something else");
    /// # Ok::<(), nififf3::Error>(())
    /// ```
    pub fn into_inner(self) -> R {
        self.reader
    }
}

impl<R: Read> Iterator for FlowFiles<R> {
    type Item = Result<FlowFile<Vec<u8>>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let result = match FlowFile::parse_next(&mut self.reader) {
            Ok(None) => None,
            // The conversion recovers a truncation as `SizeMismatch`.
            Ok(Some(flow_file)) => Some(flow_file.into_memory().map_err(Error::from)),
            Err(err) => Some(Err(err)),
        };
        if !matches!(result, Some(Ok(_))) {
            self.done = true;
        }
        result
    }
}

impl<R: Read> std::iter::FusedIterator for FlowFiles<R> {}

/// Reads a stream of concatenated flow files, streaming their content and
/// keeping the stream positioned for you.
///
/// This is the streaming counterpart to [`FlowFiles`]. Reach for it when a flow
/// file's content is too big to hold in memory. It hands you a flow file whose
/// content is the stream itself, so it is read as you read it, and never
/// twice. Each call to [`next`](Self::next) picks up whatever the last flow
/// file left behind and skips it, so reading none of a content, some of it, or
/// all of it are equally correct:
///
/// ```
/// use nififf3::{FlowFile, FlowFilesReader};
///
/// let mut bytes = FlowFile::builder().attribute("n", "1").content(&b"aaaa"[..]).to_bytes();
/// bytes.extend(FlowFile::builder().attribute("n", "2").content(&b"bbbb"[..]).to_bytes());
///
/// // Only the attributes are wanted; the content is simply not read.
/// let mut flow_files = FlowFilesReader::new(bytes.as_slice());
/// let mut names = Vec::new();
/// while let Some(flow_file) = flow_files.next()? {
///     names.push(flow_file.attribute("n").unwrap().to_string());
/// }
///
/// assert_eq!(names, ["1", "2"]);
/// # Ok::<(), nififf3::Error>(())
/// ```
///
/// The flow files this yields borrow the stream they came from, so this is
/// not an [`Iterator`], and holding a flow file past the next call does not
/// compile. After an error, `next` keeps returning `None`.
#[derive(Debug)]
pub struct FlowFilesReader<R> {
    reader: R,
    /// Declared content size of the flow file last handed out.
    size: u64,
    /// How much of that content has not been read. Maintained by
    /// [`StreamedContent`] as it is read, rather than on drop, so that a
    /// content which is leaked rather than dropped leaves the count correct.
    unread: u64,
    done: bool,
}

/// The content of a flow file from a [`FlowFilesReader`]: the stream itself,
/// limited to this flow file's content.
///
/// Reading it is optional. Whatever is left is skipped when the next flow file
/// is asked for. As with [`Content`], a stream that ends early is an error.
#[derive(Debug)]
pub struct StreamedContent<'a, R> {
    inner: Content<&'a mut R>,
    unread: &'a mut u64,
}

impl<R: Read> Read for StreamedContent<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buf)?;
        *self.unread = self.inner.remaining();
        Ok(read)
    }
}

impl<R: Read> FlowFilesReader<R> {
    /// Read flow files from `reader`.
    ///
    /// The header parsing reads in small increments, so wrap an unbuffered
    /// source in a [`std::io::BufReader`].
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            size: 0,
            unread: 0,
            done: false,
        }
    }

    /// The next flow file, or `None` at the end of the stream.
    ///
    /// Anything left unread of the previous flow file's content is skipped
    /// first, so you are never responsible for the stream's position.
    ///
    /// # Errors
    ///
    /// As [`FlowFile::parse_next`], plus [`Error::SizeMismatch`] if the stream
    /// ends part-way through content this call had to skip.
    #[expect(
        clippy::should_implement_trait,
        reason = "`Iterator` cannot yield an item that borrows the iterator, \
                  and `while let Some(..) = reader.next()?` is the point"
    )]
    pub fn next(&mut self) -> Result<Option<FlowFile<StreamedContent<'_, R>>>> {
        if self.done {
            return Ok(None);
        }
        let result = self
            .discard_unread()
            .and_then(|()| read_header(&mut self.reader));
        let header = match result {
            Ok(Some(header)) => header,
            Ok(None) => {
                self.done = true;
                return Ok(None);
            }
            Err(err) => {
                self.done = true;
                return Err(err);
            }
        };

        self.size = header.size;
        self.unread = header.size;
        let Self { reader, unread, .. } = self;
        let content = StreamedContent {
            inner: Content::new(reader, header.size),
            unread,
        };
        Ok(Some(FlowFile::parsed(header, content)))
    }

    /// Read past whatever of the last flow file's content was left.
    fn discard_unread(&mut self) -> Result<()> {
        if self.unread == 0 {
            return Ok(());
        }
        #[cfg(feature = "tracing")]
        tracing::trace!(
            unread = self.unread,
            "skipping the rest of the previous flow file's content"
        );
        let skipped = io::copy(&mut (&mut self.reader).take(self.unread), &mut io::sink())?;
        let unread = std::mem::replace(&mut self.unread, 0);
        if skipped != unread {
            // Report against the whole content, not the part being skipped:
            // what the header declared is what did not arrive.
            return Err(Error::SizeMismatch {
                expected: self.size,
                actual: self.size - unread + skipped,
            });
        }
        Ok(())
    }

    /// A reference to the underlying reader.
    pub fn get_ref(&self) -> &R {
        &self.reader
    }

    /// A mutable reference to the underlying reader.
    ///
    /// The stream may be positioned part-way through a flow file's content,
    /// at whatever the last one handed out did not read.
    pub fn get_mut(&mut self) -> &mut R {
        &mut self.reader
    }

    /// Consume the reader, returning the underlying one. See
    /// [`get_mut`](Self::get_mut) for where it is positioned.
    pub fn into_inner(self) -> R {
        self.reader
    }
}

/// Writes a stream of concatenated flow files, the counterpart to
/// [`FlowFiles`].
///
/// A failed write leaves a partial flow file in the stream, so the writer
/// refuses every write after one. If it appended instead, the next flow
/// file's header would be read back as the truncated flow file's content,
/// which buries the failure rather than reporting it. See
/// [`is_poisoned`](Self::is_poisoned).
///
/// # Finishing
///
/// This type writes straight through and buffers nothing of its own, but the
/// writer underneath it may, and neither writing nor dropping this one flushes
/// it. Finish with [`finish`](Self::finish), which flushes and hands the writer
/// back. [`into_inner`](Self::into_inner) does not flush at all, on purpose, so
/// you can discard a stream rather than complete it.
///
/// ```
/// use nififf3::{FlowFile, FlowFilesWriter};
///
/// let mut out = Vec::new();
/// let mut writer = FlowFilesWriter::new(&mut out);
/// writer.write_bytes(&FlowFile::builder().content(&b"first"[..]))?;
/// writer.write_bytes(&FlowFile::builder().content(&b"second"[..]))?;
/// assert_eq!(writer.count(), 2);
///
/// # use nififf3::FlowFiles;
/// let parsed: Vec<_> = FlowFiles::new(out.as_slice()).collect::<Result<_, _>>()?;
/// assert_eq!(parsed.len(), 2);
/// # Ok::<(), nififf3::Error>(())
/// ```
#[derive(Debug)]
pub struct FlowFilesWriter<W> {
    writer: W,
    count: u64,
    poisoned: bool,
}

impl<W: Write> FlowFilesWriter<W> {
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
    /// # Errors
    ///
    /// As [`FlowFile::write_to`]. A content reader that ends early leaves a
    /// truncated flow file behind, and poisons the writer. Use
    /// [`write_bytes`](Self::write_bytes) for content whose length must be
    /// verified before anything is committed.
    pub fn write<R: Read>(&mut self, flow_file: FlowFile<R>) -> io::Result<u64> {
        self.guard()?;
        let result = flow_file.write_to(&mut self.writer);
        let copied = self.poison_on_err(result)?;
        self.count += 1;
        Ok(copied)
    }

    /// Append an in-memory flow file, whose size is known to be correct.
    ///
    /// # Errors
    ///
    /// Whatever the writer returns. That also poisons this writer, because the
    /// one underneath may have accepted part of the flow file first.
    ///
    /// # Panics
    ///
    /// As [`FlowFile::to_bytes`]: an attribute the wire format cannot express,
    /// or a declared size disagreeing with the content.
    pub fn write_bytes(&mut self, flow_file: &FlowFile<Vec<u8>>) -> io::Result<u64> {
        self.guard()?;
        let result = flow_file.write_bytes_to(&mut self.writer);
        let written = self.poison_on_err(result)?;
        self.count += 1;
        Ok(written)
    }

    fn guard(&self) -> io::Result<()> {
        if self.poisoned {
            return Err(crate::error::poisoned());
        }
        Ok(())
    }

    fn poison_on_err<T>(&mut self, result: io::Result<T>) -> io::Result<T> {
        if let Err(err) = &result {
            #[cfg(feature = "tracing")]
            tracing::warn!(
                error = %err,
                written = self.count,
                "write failed, poisoning the writer"
            );
            #[cfg(not(feature = "tracing"))]
            let _ = err;
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
    pub fn flush(&mut self) -> io::Result<()> {
        let result = self.writer.flush();
        self.poison_on_err(result)
    }

    /// Flush and return the underlying writer: the ordinary way to finish.
    ///
    /// # Errors
    ///
    /// Whatever flushing the underlying writer returns, in which case the
    /// writer is dropped. To keep hold of it either way, call
    /// [`flush`](Self::flush) and then [`into_inner`](Self::into_inner).
    pub fn finish(mut self) -> io::Result<W> {
        self.flush()?;
        Ok(self.writer)
    }

    /// Whether a write has failed, leaving the stream mid-flow-file.
    ///
    /// Once this is true every further write fails without touching the
    /// underlying writer. [`into_inner`](Self::into_inner) still hands it
    /// back, for a caller that wants to discard or truncate the output.
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

    /// Consume the writer, returning the underlying one without flushing it.
    ///
    /// To finish a stream, use [`finish`](Self::finish) instead. This is for
    /// taking the writer back after a failure in order to discard or truncate
    /// what was produced.
    pub fn into_inner(self) -> W {
        self.writer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_poisoned_error(err: &io::Error) -> bool {
        err.kind() == io::ErrorKind::BrokenPipe
            && matches!(
                err.get_ref().and_then(|e| e.downcast_ref::<Error>()),
                Some(Error::WriterPoisoned)
            )
    }

    pub(crate) fn sample_flow_file() -> FlowFile<Vec<u8>> {
        FlowFile::builder()
            .attribute("a", "b")
            .attribute("path", "x")
            .content(&b"hello"[..])
    }

    pub(crate) fn sample_bytes() -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(b"NiFiFF3");
        v.extend_from_slice(&[0x00, 0x02]);
        v.extend_from_slice(&[0x00, 0x01]);
        v.extend_from_slice(b"a");
        v.extend_from_slice(&[0x00, 0x01]);
        v.extend_from_slice(b"b");
        v.extend_from_slice(&[0x00, 0x04]);
        v.extend_from_slice(b"path");
        v.extend_from_slice(&[0x00, 0x01]);
        v.extend_from_slice(b"x");
        v.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 5]);
        v.extend_from_slice(b"hello");
        v
    }

    #[test]
    fn serializes_to_golden_bytes() {
        assert_eq!(sample_flow_file().to_bytes(), sample_bytes());
    }

    #[test]
    fn from_bytes_roundtrip() {
        let parsed = FlowFile::from_bytes(&sample_bytes()).unwrap();
        assert_eq!(parsed.size(), 5);
        assert_eq!(parsed.attributes().len(), 2);
        assert_eq!(parsed.attributes()["a"], "b");
        assert_eq!(parsed.attributes()["path"], "x");
        assert_eq!(parsed.content().as_slice(), b"hello");
    }

    #[test]
    fn parse_is_lazy_and_limits_content() {
        let bytes = sample_bytes();
        let flow_file = FlowFile::parse(bytes.as_slice()).unwrap();
        assert_eq!(flow_file.size(), 5);
        assert_eq!(flow_file.content().remaining(), 5);
        let flow_file = flow_file.into_memory().unwrap();
        assert_eq!(flow_file.content().as_slice(), b"hello");
    }

    /// Every source ends up reading through the same decoder, so a reader
    /// that delivers one byte per call has to agree with a slice.
    #[test]
    fn parsing_through_a_dribbling_reader_matches_the_slice() {
        struct Dribble<'a>(&'a [u8]);
        impl Read for Dribble<'_> {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                if self.0.is_empty() || buf.is_empty() {
                    return Ok(0);
                }
                buf[0] = self.0[0];
                self.0 = &self.0[1..];
                Ok(1)
            }
        }

        let flow_file = FlowFile::builder()
            .attributes((0..50).map(|i| (format!("k{i}"), "v".repeat(i * 3000))))
            .content(vec![9u8; 100_000]);
        let bytes = flow_file.to_bytes();
        assert_eq!(FlowFile::from_reader(Dribble(&bytes)).unwrap(), flow_file);
    }

    #[test]
    fn write_to_matches_to_bytes() {
        let flow_file = sample_flow_file().into_reader();
        let mut out = Vec::new();
        let copied = flow_file.write_to(&mut out).unwrap();
        assert_eq!(copied, 5);
        assert_eq!(out, sample_bytes());
    }

    #[test]
    fn write_bytes_to_matches_write_to() {
        let flow_file = sample_flow_file();

        let mut buffered = Vec::new();
        let written = flow_file.write_bytes_to(&mut buffered).unwrap();
        assert_eq!(written, 5);

        let mut streamed = Vec::new();
        flow_file.into_reader().write_to(&mut streamed).unwrap();
        assert_eq!(buffered, streamed);
        assert_eq!(buffered, sample_bytes());
    }

    #[test]
    fn parse_next_reads_concatenated_flow_files() {
        let mut bytes = sample_bytes();
        bytes.extend_from_slice(&sample_bytes());
        let mut reader = bytes.as_slice();
        let mut count = 0;
        while let Some(flow_file) = FlowFile::parse_next(&mut reader).unwrap() {
            let flow_file = flow_file.into_memory().unwrap();
            assert_eq!(flow_file.content().as_slice(), b"hello");
            count += 1;
        }
        assert_eq!(count, 2);
    }

    #[test]
    fn skip_content_walks_a_stream_without_reading_it() {
        let mut bytes = sample_bytes();
        bytes.extend_from_slice(&sample_bytes());

        let mut reader = bytes.as_slice();
        let mut count = 0;
        while let Some(flow_file) = FlowFile::parse_next(&mut reader).unwrap() {
            assert_eq!(flow_file.attribute("path"), Some("x"));
            assert_eq!(flow_file.skip_content().unwrap(), 5);
            count += 1;
        }
        assert_eq!(count, 2);
    }

    /// The hazard the docs describe: a flow file dropped with its content
    /// unread leaves the reader inside that content. When the content is an
    /// envelope, that produces a plausible wrong answer rather than an error.
    #[test]
    fn dropping_content_unread_reparses_the_content_itself() {
        let inner = FlowFile::builder()
            .attribute("who", "inner")
            .content(&b"payload"[..])
            .to_bytes();
        let mut bytes = FlowFile::builder()
            .attribute("who", "envelope")
            .content(inner)
            .to_bytes();
        bytes.extend(
            FlowFile::builder()
                .attribute("who", "second")
                .content(Vec::new())
                .to_bytes(),
        );

        let mut reader = bytes.as_slice();
        let mut seen = Vec::new();
        while let Ok(Some(flow_file)) = FlowFile::parse_next(&mut reader) {
            seen.push(flow_file.attribute("who").unwrap().to_string());
        }
        assert_eq!(seen, ["envelope", "inner"]);

        let mut reader = bytes.as_slice();
        let mut seen = Vec::new();
        while let Some(flow_file) = FlowFile::parse_next(&mut reader).unwrap() {
            seen.push(flow_file.attribute("who").unwrap().to_string());
            flow_file.skip_content().unwrap();
        }
        assert_eq!(seen, ["envelope", "second"]);
    }

    /// Reading none of a content, some of it, or all of it must leave the
    /// stream in the same place, which is where the next flow file starts.
    #[test]
    fn the_streaming_reader_positions_the_stream_however_much_is_read() {
        let mut bytes = FlowFile::builder()
            .attribute("n", "1")
            .content(&b"aaaa"[..])
            .to_bytes();
        bytes.extend(
            FlowFile::builder()
                .attribute("n", "2")
                .content(&b"bbbb"[..])
                .to_bytes(),
        );
        bytes.extend(
            FlowFile::builder()
                .attribute("n", "3")
                .content(&b"cccc"[..])
                .to_bytes(),
        );

        let mut flow_files = FlowFilesReader::new(bytes.as_slice());
        let mut seen = Vec::new();
        while let Some(flow_file) = flow_files.next().unwrap() {
            seen.push(flow_file.attribute("n").unwrap().to_string());
        }
        assert_eq!(seen, ["1", "2", "3"]);

        let mut flow_files = FlowFilesReader::new(bytes.as_slice());
        let mut seen = Vec::new();
        while let Some(mut flow_file) = flow_files.next().unwrap() {
            let mut byte = [0u8; 1];
            flow_file.content_mut().read_exact(&mut byte).unwrap();
            seen.push((flow_file.attribute("n").unwrap().to_string(), byte[0]));
        }
        assert_eq!(
            seen,
            [("1".into(), b'a'), ("2".into(), b'b'), ("3".into(), b'c')] as [(String, u8); 3]
        );

        let mut flow_files = FlowFilesReader::new(bytes.as_slice());
        let mut seen = Vec::new();
        while let Some(flow_file) = flow_files.next().unwrap() {
            seen.push(flow_file.into_memory().unwrap().into_content());
        }
        assert_eq!(seen, [b"aaaa".to_vec(), b"bbbb".to_vec(), b"cccc".to_vec()]);
    }

    #[test]
    fn the_streaming_reader_is_not_fooled_by_nested_flow_files() {
        let inner = FlowFile::builder()
            .attribute("who", "inner")
            .content(&b"payload"[..])
            .to_bytes();
        let mut bytes = FlowFile::builder()
            .attribute("who", "envelope")
            .content(inner)
            .to_bytes();
        bytes.extend(
            FlowFile::builder()
                .attribute("who", "second")
                .content(Vec::new())
                .to_bytes(),
        );

        let mut flow_files = FlowFilesReader::new(bytes.as_slice());
        let mut seen = Vec::new();
        while let Some(flow_file) = flow_files.next().unwrap() {
            seen.push(flow_file.attribute("who").unwrap().to_string());
        }
        assert_eq!(seen, ["envelope", "second"]);
    }

    /// A stream that stops inside content nobody read is still a truncated
    /// stream, and has to be reported rather than passed over in silence.
    #[test]
    fn the_streaming_reader_reports_a_truncated_content_it_skipped() {
        let mut bytes = sample_bytes();
        bytes.truncate(bytes.len() - 2);

        let mut flow_files = FlowFilesReader::new(bytes.as_slice());
        assert!(flow_files.next().unwrap().is_some());
        assert!(matches!(
            flow_files.next(),
            Err(Error::SizeMismatch {
                expected: 5,
                actual: 3
            })
        ));
        assert!(flow_files.next().unwrap().is_none(), "fused");
    }

    /// A content read through the streaming reader reports truncation itself,
    /// at the read that reaches it, so a decoder consuming it fails there.
    #[test]
    fn a_streamed_content_that_ends_early_errs_on_read() {
        let mut bytes = sample_bytes();
        bytes.truncate(bytes.len() - 2);

        let mut flow_files = FlowFilesReader::new(bytes.as_slice());
        let mut flow_file = flow_files.next().unwrap().unwrap();
        let mut out = Vec::new();
        let err = flow_file.content_mut().read_to_end(&mut out).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(out, b"hel");
    }

    #[test]
    fn from_reader_matches_from_bytes() {
        let bytes = sample_bytes();
        assert_eq!(
            FlowFile::from_reader(bytes.as_slice()).unwrap(),
            FlowFile::from_bytes(&bytes).unwrap()
        );

        let truncated = &bytes[..bytes.len() - 2];
        assert!(matches!(
            FlowFile::from_reader(truncated),
            Err(Error::SizeMismatch {
                expected: 5,
                actual: 3
            })
        ));
    }

    #[test]
    fn from_reader_takes_one_flow_file_and_leaves_the_rest() {
        let mut bytes = sample_bytes();
        bytes.extend_from_slice(b"trailer");
        let mut reader = bytes.as_slice();
        assert!(FlowFile::from_reader(&mut reader).is_ok());
        assert_eq!(reader, b"trailer");
    }

    #[test]
    fn flow_files_iterator_fuses_after_error() {
        let mut bytes = sample_bytes();
        bytes.extend_from_slice(b"garbage");
        let mut flow_files = FlowFiles::new(bytes.as_slice());
        assert!(flow_files.next().unwrap().is_ok());
        assert!(matches!(
            flow_files.next(),
            Some(Err(Error::InvalidMagic(_)))
        ));
        assert!(flow_files.next().is_none());
    }

    #[test]
    fn bad_magic_is_rejected() {
        let err = FlowFile::from_bytes(b"not a flow file at all").unwrap_err();
        assert!(matches!(err, Error::InvalidMagic(_)));
        assert!(err.to_string().contains("not a f"), "{err}");
    }

    #[test]
    fn truncated_content_is_a_size_mismatch() {
        let bytes = sample_bytes();
        assert!(matches!(
            FlowFile::from_bytes(&bytes[..bytes.len() - 2]),
            Err(Error::SizeMismatch {
                expected: 5,
                actual: 3
            })
        ));

        let parsed = FlowFile::parse(&bytes[..bytes.len() - 2]).unwrap();
        let err = parsed.into_memory().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert!(matches!(
            err.get_ref().and_then(|e| e.downcast_ref::<Error>()),
            Some(Error::SizeMismatch {
                expected: 5,
                actual: 3
            })
        ));
    }

    #[test]
    fn truncated_header_names_the_missing_piece() {
        let bytes = sample_bytes();
        let err = FlowFile::from_bytes(&bytes[..10]).unwrap_err();
        assert!(
            matches!(
                err,
                Error::TruncatedHeader {
                    expecting: "an attribute key length",
                    ..
                }
            ),
            "{err:?}"
        );
        // A reader ending at the same place says the same thing.
        let from_reader = FlowFile::parse(&bytes[..10]).unwrap_err();
        assert_eq!(from_reader.to_string(), err.to_string());
    }

    #[test]
    fn an_empty_stream_ends_cleanly_but_a_partial_magic_does_not() {
        let mut empty: &[u8] = b"";
        assert!(FlowFile::parse_next(&mut empty).unwrap().is_none());

        let mut partial: &[u8] = b"NiFi";
        assert!(matches!(
            FlowFile::parse_next(&mut partial),
            Err(Error::TruncatedHeader {
                needed: 3,
                expecting: "the magic header"
            })
        ));
    }

    #[test]
    fn trailing_data_is_rejected() {
        let mut bytes = sample_bytes();
        bytes.push(0);
        assert!(matches!(
            FlowFile::from_bytes(&bytes),
            Err(Error::TrailingData(1))
        ));
    }

    #[test]
    fn readers_still_report_plain_io_errors_as_io() {
        struct Broken;
        impl Read for Broken {
            fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::new(io::ErrorKind::ConnectionReset, "gone"))
            }
        }
        assert!(matches!(FlowFile::parse(Broken), Err(Error::Io(_))));
    }

    #[test]
    fn a_failed_write_poisons_the_writer() {
        let mut out = Vec::new();
        let mut writer = FlowFilesWriter::new(&mut out);

        // Declares 10 bytes but the reader holds 5, so the write truncates.
        let err = writer
            .write(FlowFile::builder().reader(&b"short"[..], 10))
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert!(writer.is_poisoned());

        let err = writer.write_bytes(&sample_flow_file()).unwrap_err();
        assert!(is_poisoned_error(&err), "{err:?}");
        assert_eq!(writer.count(), 0);
    }

    #[test]
    fn a_healthy_writer_is_never_poisoned() {
        let mut writer = FlowFilesWriter::new(Vec::new());
        writer.write_bytes(&sample_flow_file()).unwrap();
        writer.write(sample_flow_file().into_reader()).unwrap();
        assert!(!writer.is_poisoned());
        assert_eq!(writer.count(), 2);
        let bytes = writer.finish().unwrap();
        assert_eq!(FlowFiles::new(bytes.as_slice()).count(), 2);
    }

    /// Both halves of the reservation schedule: it has to size the buffer
    /// well for content that really arrives, without ever sizing it from a
    /// number the input has not backed with bytes.
    #[test]
    fn content_is_buffered_at_its_exact_size_without_trusting_the_declaration() {
        let content = vec![7u8; 3 * format::RESERVE_AHEAD + 17];
        let bytes = FlowFile::builder().content(content.clone()).to_bytes();
        let parsed = FlowFile::parse(bytes.as_slice())
            .unwrap()
            .into_memory()
            .unwrap();
        assert_eq!(parsed.content(), &content);
        assert_eq!(parsed.content().capacity(), content.len(), "exact");

        // A header claiming far more than arrives.
        let mut lying = FlowFile::builder().content(vec![1u8; 100]).to_bytes();
        let size_at = lying.len() - 100 - 8;
        lying[size_at..size_at + 8].copy_from_slice(&(1u64 << 40).to_be_bytes());
        let err = FlowFile::parse(lying.as_slice())
            .unwrap()
            .into_memory()
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn large_attribute_uses_extended_field_length() {
        let value = "v".repeat(0x1_0000);
        let flow_file = FlowFile::builder()
            .attribute("k", &value)
            .content(Vec::new());
        let bytes = flow_file.to_bytes();
        assert!(bytes[7 + 2 + 2 + 1..].starts_with(&[0xFF, 0xFF]));
        assert_eq!(FlowFile::from_bytes(&bytes).unwrap(), flow_file);
    }

    #[test]
    fn empty_attributes_and_content() {
        let flow_file = FlowFile::builder().empty();
        let bytes = flow_file.to_bytes();
        assert_eq!(bytes.len(), 7 + 2 + 8);
        assert_eq!(FlowFile::from_bytes(&bytes).unwrap(), flow_file);
    }
}
