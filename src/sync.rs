//! Parsing and serialization over `std::io::Read`/`Write`.

use std::collections::HashMap;
use std::io::{self, Read, Write};

use crate::format::{MAGIC, MAX_VALUE_2_BYTES};
use crate::{Error, FlowFile, Limits, Result};

fn read_field_len(reader: &mut impl Read) -> io::Result<usize> {
    let mut buf = [0u8; 2];
    reader.read_exact(&mut buf)?;
    let len = u16::from_be_bytes(buf) as usize;
    if len < MAX_VALUE_2_BYTES {
        Ok(len)
    } else {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        Ok(u32::from_be_bytes(buf) as usize)
    }
}

fn read_string(reader: &mut impl Read, max_len: Option<usize>) -> Result<String> {
    let len = read_field_len(reader)?;
    if let Some(limit) = max_len
        && len > limit
    {
        return Err(Error::AttributeTooLong { len, limit });
    }
    // Grow the buffer as bytes actually arrive instead of trusting the
    // declared length, so a crafted header cannot force a huge allocation.
    let mut buf = Vec::new();
    let read = reader.take(len as u64).read_to_end(&mut buf)?;
    if read != len {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "attribute truncated").into());
    }
    String::from_utf8(buf).map_err(Error::InvalidAttribute)
}

/// Read the header, returning the attributes and the declared content size.
/// `first_byte` is a byte already consumed from the reader, if any.
pub(crate) fn parse_header(
    reader: &mut impl Read,
    first_byte: Option<u8>,
    limits: Limits,
) -> Result<(HashMap<String, String>, u64)> {
    let mut magic = [0u8; 7];
    match first_byte {
        Some(byte) => {
            magic[0] = byte;
            reader.read_exact(&mut magic[1..])?;
        }
        None => reader.read_exact(&mut magic)?,
    }
    if magic != MAGIC {
        return Err(Error::InvalidMagic(magic));
    }
    let count = read_field_len(reader)?;
    if let Some(limit) = limits.max_attributes
        && count > limit
    {
        return Err(Error::TooManyAttributes { count, limit });
    }
    let mut attributes = HashMap::with_capacity(count.min(1024));
    let mut total = 0usize;
    for _ in 0..count {
        let key = read_string(reader, limits.max_attribute_len)?;
        let value = read_string(reader, limits.max_attribute_len)?;
        total = total.saturating_add(key.len()).saturating_add(value.len());
        if let Some(limit) = limits.max_total_attribute_len
            && total > limit
        {
            return Err(Error::HeaderTooLarge { len: total, limit });
        }
        attributes.insert(key, value);
    }
    let mut size = [0u8; 8];
    reader.read_exact(&mut size)?;
    let size = u64::from_be_bytes(size);
    if let Some(limit) = limits.max_content_len
        && size > limit
    {
        return Err(Error::ContentTooLarge { size, limit });
    }
    Ok((attributes, size))
}

impl<R: Read> FlowFile<io::Take<R>> {
    /// Parse a flow file from a reader, consuming only the header.
    ///
    /// The returned flow file's content is the reader, limited to the
    /// declared content size. Reading fewer bytes than [`size`] before the
    /// reader ends means the input was truncated; [`FlowFile::into_memory`]
    /// checks this for you.
    ///
    /// The header is read in small increments, so wrap unbuffered sources
    /// (files, sockets) in a [`std::io::BufReader`].
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
    /// [`Error::InvalidAttribute`] for an attribute that is not UTF-8, or
    /// [`Error::Io`] if the header ends early. Nothing here depends on the
    /// content, which has not been read yet.
    ///
    /// [`size`]: FlowFile::size
    pub fn parse(reader: R) -> Result<Self> {
        Self::parse_with_limits(reader, Limits::UNLIMITED)
    }

    /// Like [`parse`](Self::parse), but enforcing [`Limits`] on the header.
    ///
    /// Use this for untrusted input; see [`Limits`] for the threat model.
    ///
    /// # Errors
    ///
    /// As [`parse`](Self::parse), plus [`Error::TooManyAttributes`] or
    /// [`Error::AttributeTooLong`] when the header exceeds `limits`.
    pub fn parse_with_limits(mut reader: R, limits: Limits) -> Result<Self> {
        let (attributes, size) = parse_header(&mut reader, None, limits)?;
        Ok(FlowFile::from_raw_parts(
            size,
            attributes,
            reader.take(size),
        ))
    }
}

impl<'r, R: Read> FlowFile<io::Take<&'r mut R>> {
    /// Parse the next flow file from a stream of concatenated flow files.
    ///
    /// Returns `Ok(None)` on a clean end of input.
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
    /// The returned flow file's content *is* the reader, positioned at the
    /// first content byte. Nothing else can read the stream until that content
    /// is dealt with, and the next flow file begins where it ends — so each one
    /// has to be consumed before the next is parsed:
    ///
    /// - [`into_memory`](FlowFile::into_memory) reads it into a buffer;
    /// - [`write_to`](FlowFile::write_to) copies it straight out;
    /// - [`skip_content`](FlowFile::skip_content) throws it away, which is what
    ///   to call when only the attributes were wanted.
    ///
    /// Holding a flow file past the next call is a compile error — it borrows
    /// the reader — but *dropping* one with its content unread is not, and that
    /// is the mistake to watch for. The reader is then left where the content
    /// starts, so the next call parses the content as though it were a flow
    /// file. Usually that is an error. It is worse when it is not: a flow file
    /// whose content begins with a valid header — an envelope carrying another
    /// flow file, say — yields something plausible that was never sent. What
    /// happens after that depends on the rest of the content, so the damage
    /// ranges from one phantom record in a stream that otherwise reads fine to
    /// losing everything that followed.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// // An envelope: one flow file whose content is another, then a second
    /// // ordinary flow file after it.
    /// let inner = FlowFile::builder().attribute("who", "inner").content(&b"payload"[..]).to_bytes();
    /// let mut bytes = FlowFile::builder().attribute("who", "envelope").content(inner).to_bytes();
    /// bytes.extend(FlowFile::builder().attribute("who", "second").content(Vec::new()).to_bytes());
    ///
    /// let mut reader = bytes.as_slice();
    /// let mut seen = Vec::new();
    /// while let Some(flow_file) = FlowFile::parse_next(&mut reader)? {
    ///     seen.push(flow_file.attribute("who").unwrap().to_string());
    ///     flow_file.skip_content()?;
    /// }
    /// assert_eq!(seen, ["envelope", "second"]);
    ///
    /// // Without that `skip_content`, the second read starts inside the
    /// // envelope's content and finds the flow file nested in it — while the
    /// // one that really came next is never reached.
    /// # Ok::<(), nififf3::Error>(())
    /// ```
    ///
    /// Two types remove the question entirely, and one of them should usually
    /// be preferred to calling this directly: [`FlowFiles`] reads each content
    /// into memory as it goes, and [`FlowFilesReader`] streams the content but
    /// keeps the stream positioned for you, skipping whatever a flow file left
    /// unread. `parse_next` is the primitive underneath them — reach for it
    /// when you need the reader back between flow files, or are driving the
    /// stream yourself.
    ///
    /// # Errors
    ///
    /// As [`FlowFile::parse`]. Note that a stream ending part-way through a
    /// header is [`Error::Io`], not `Ok(None)` — only a clean boundary ends
    /// the iteration.
    pub fn parse_next(reader: &'r mut R) -> Result<Option<Self>> {
        Self::parse_next_with_limits(reader, Limits::UNLIMITED)
    }

    /// Like [`parse_next`](Self::parse_next), but enforcing [`Limits`] on
    /// the header. Use this for untrusted input.
    ///
    /// # Errors
    ///
    /// As [`parse_next`](Self::parse_next), plus [`Error::TooManyAttributes`]
    /// or [`Error::AttributeTooLong`] when a header exceeds `limits`.
    pub fn parse_next_with_limits(reader: &'r mut R, limits: Limits) -> Result<Option<Self>> {
        let mut first = [0u8; 1];
        loop {
            match reader.read(&mut first) {
                // A one-byte buffer, so this is the end of the stream: a
                // reader returning `Ok(0)` with room to fill is buggy, and
                // retrying one would spin rather than recover.
                Ok(0) => return Ok(None),
                Ok(_) => break,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {} // retry
                Err(e) => return Err(e.into()),
            }
        }
        let (attributes, size) = parse_header(reader, Some(first[0]), limits)?;
        Ok(Some(FlowFile::from_raw_parts(
            size,
            attributes,
            reader.take(size),
        )))
    }
}

impl<R: Read> FlowFile<R> {
    /// Serialize the flow file to a writer, reading exactly [`size`] bytes
    /// from the content reader.
    ///
    /// Returns the number of content bytes copied. This consumes the flow
    /// file, because it is a one-shot: the content reader is left exhausted,
    /// so a second call would write a second header and then fail — after
    /// committing those bytes to the stream. Read whatever you need from
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
    /// Only I/O: nothing here inspects the flow file's structure. A content
    /// reader that ends before `size` bytes is
    /// [`UnexpectedEof`](io::ErrorKind::UnexpectedEof) carrying an
    /// [`Error::SizeMismatch`]; anything else comes from the writer. Either
    /// way the header — and whatever content was copied before the failure —
    /// has already been written.
    ///
    /// # Panics
    ///
    /// If an attribute key or value is longer than `u32::MAX` bytes, which
    /// the wire format cannot express. As [`FlowFile::to_bytes`].
    ///
    /// [`size`]: FlowFile::size
    pub fn write_to<W: Write>(mut self, writer: &mut W) -> io::Result<u64> {
        writer.write_all(&self.header_bytes())?;
        let copied = io::copy(&mut (&mut self.content).take(self.size), writer)?;
        if copied != self.size {
            return Err(crate::error::truncated(self.size, copied));
        }
        Ok(copied)
    }

    /// Discard the content, consuming exactly [`size`] bytes from the reader.
    ///
    /// What to call when only the attributes were wanted. For a flow file
    /// parsed out of a stream by [`parse_next`](FlowFile::parse_next), leaving
    /// the content unread is not free: the reader stays where the content
    /// begins, and the next parse starts from there. See that method for what
    /// goes wrong.
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
    pub fn skip_content(mut self) -> io::Result<u64> {
        let skipped = io::copy(&mut (&mut self.content).take(self.size), &mut io::sink())?;
        if skipped != self.size {
            return Err(crate::error::truncated(self.size, skipped));
        }
        Ok(skipped)
    }

    /// Read the content to completion, producing an in-memory flow file.
    ///
    /// The inverse of [`into_reader`](FlowFile::into_reader), and not to be
    /// confused with [`to_bytes`](FlowFile::to_bytes): this moves the *content*
    /// into memory and serializes nothing, while `to_bytes` serializes the
    /// whole flow file — header and all — to the wire format.
    ///
    /// Validates that exactly [`size`] bytes of content were available.
    ///
    /// # Errors
    ///
    /// Only I/O; the header was already validated by whatever produced this
    /// flow file. Content that ends early is
    /// [`UnexpectedEof`](io::ErrorKind::UnexpectedEof) carrying an
    /// [`Error::SizeMismatch`].
    ///
    /// [`size`]: FlowFile::size
    pub fn into_memory(mut self) -> io::Result<FlowFile<Vec<u8>>> {
        let mut content = Vec::new();
        let read = (&mut self.content)
            .take(self.size)
            .read_to_end(&mut content)? as u64;
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

impl FlowFile<Vec<u8>> {
    /// Serialize the flow file to a writer.
    ///
    /// The in-memory counterpart to [`write_to`](FlowFile::write_to), which is
    /// bounded on `R: Read` and so does not apply here — `Vec<u8>` is not a
    /// reader, and two inherent methods of the same name cannot coexist even
    /// though only one of them could ever apply. Hence the name.
    ///
    /// Takes `&self` rather than consuming, since nothing is exhausted by
    /// writing in-memory content. Returns the number of content bytes written.
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
    /// # Errors
    ///
    /// Whatever the writer returns.
    ///
    /// # Panics
    ///
    /// As [`to_bytes`](FlowFile::to_bytes): an attribute the wire format
    /// cannot express, or a declared size disagreeing with the content.
    pub fn write_bytes_to<W: Write>(&self, writer: &mut W) -> io::Result<u64> {
        writer.write_all(&self.to_bytes())?;
        Ok(self.size)
    }
}

/// Iterator over a stream of concatenated flow files.
///
/// Yields each flow file with its content buffered in memory, and ends on a
/// clean end of input. After an error the iterator is fused (keeps returning
/// `None`), since the stream position is no longer trustworthy.
///
/// Buffering is the trade: it is what lets this be an ordinary [`Iterator`]
/// yielding owned flow files. When a content is too large for that, use
/// [`FlowFilesReader`], which streams instead and is just as safe; see its
/// docs for the three-way choice between them and [`FlowFile::parse_next`].
///
/// Because this buffers, it reports a content that ends before its declared
/// size as [`Error::SizeMismatch`] directly, the way
/// [`FlowFile::from_bytes`] does — not wrapped in [`Error::Io`].
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
    limits: Limits,
    done: bool,
}

impl<R: Read> FlowFiles<R> {
    /// Iterate over the flow files in `reader`, without header limits.
    ///
    /// The header parsing reads in small increments, so wrap unbuffered
    /// sources (files, sockets) in a [`std::io::BufReader`].
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

    /// Consume the iterator, returning the underlying reader.
    ///
    /// The reader is left wherever iteration stopped: after a `None` that is
    /// the end of the flow files and the start of whatever follows them, which
    /// is what makes this useful — a trailer, or the next section of a
    /// multiplexed stream. Stopping early instead leaves it part-way through a
    /// flow file, and after an error the position is not meaningful at all.
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
        let result = match FlowFile::parse_next_with_limits(&mut self.reader, self.limits) {
            Ok(None) => None,
            // The conversion recovers a truncation as `SizeMismatch`, so
            // buffering here reports it the way `from_bytes` does.
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

/// Reads a stream of concatenated flow files *without* buffering their
/// content, keeping the stream positioned for you.
///
/// The streaming counterpart to [`FlowFiles`], and the one to reach for when a
/// flow file's content is too big to hold in memory. Where `FlowFiles` reads
/// each content into a `Vec` and hands you an owned flow file, this hands you
/// one whose content is the stream itself — so it is read as you read it, and
/// never twice.
///
/// That laziness is what makes [`FlowFile::parse_next`] sharp to use directly:
/// there, a flow file dropped with its content unread leaves the stream inside
/// that content, and the next parse reads it as though it were a flow file.
/// Here it does not matter. Each call picks up whatever the last flow file left
/// behind and skips it, so reading none of the content, some of it, or all of
/// it are equally correct:
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
/// # Choosing between the three
///
/// | | content | use when |
/// | --- | --- | --- |
/// | [`FlowFiles`] | read into memory for you | the contents fit, and an owned `FlowFile<Vec<u8>>` per flow file is what you want |
/// | `FlowFilesReader` | streamed, positioned for you | a content may be too large to buffer |
/// | [`FlowFile::parse_next`] | streamed, positioned by you | you need the reader back between flow files, or are driving the stream yourself |
///
/// Only `FlowFiles` implements [`Iterator`]: the flow files this yields borrow
/// the stream they came from, which no `Iterator` can express. In a `while let`
/// loop that costs nothing, and it is what stops one being held past the next
/// call — that is a compile error, not a corrupt read.
///
/// After an error, `next` keeps returning `None`, as [`FlowFiles`] does: the
/// position in the stream is no longer trustworthy.
#[derive(Debug)]
pub struct FlowFilesReader<R> {
    reader: R,
    limits: Limits,
    /// Declared content size of the flow file last handed out.
    size: u64,
    /// How much of that content has not been read. Maintained by [`StreamedContent`]
    /// as it is read, rather than on drop, so that a content which is leaked
    /// rather than dropped leaves the count correct.
    unread: u64,
    done: bool,
}

/// The content of a flow file from a [`FlowFilesReader`]: the stream itself,
/// limited to this flow file's content.
///
/// Reading it is optional. Whatever is left goes when the next flow file is
/// asked for.
#[derive(Debug)]
pub struct StreamedContent<'a, R> {
    inner: io::Take<&'a mut R>,
    unread: &'a mut u64,
}

impl<R: Read> Read for StreamedContent<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buf)?;
        *self.unread = self.unread.saturating_sub(read as u64);
        Ok(read)
    }
}

impl<R: Read> FlowFilesReader<R> {
    /// Read flow files from `reader`, without header limits.
    ///
    /// The header parsing reads in small increments, so wrap unbuffered
    /// sources (files, sockets) in a [`std::io::BufReader`].
    pub fn new(reader: R) -> Self {
        Self::with_limits(reader, Limits::UNLIMITED)
    }

    /// Like [`new`](Self::new), but enforcing [`Limits`] on each header.
    /// Use this for untrusted input.
    pub fn with_limits(reader: R, limits: Limits) -> Self {
        Self {
            reader,
            limits,
            size: 0,
            unread: 0,
            done: false,
        }
    }

    /// The next flow file, or `None` at the end of the stream.
    ///
    /// Anything left unread of the previous flow file's content is skipped
    /// first, so the caller is never responsible for the stream's position.
    ///
    /// Returns `Result<Option<_>>` rather than the `Option<Result<_>>` an
    /// [`Iterator`] would, matching [`FlowFile::parse_next`] — which this
    /// replaces — so that `while let Some(flow_file) = reader.next()?` reads
    /// with one `?` and no inner match.
    ///
    /// # Errors
    ///
    /// As [`FlowFile::parse_next`], plus [`Error::SizeMismatch`] if the stream
    /// ends part-way through content this call had to skip.
    #[expect(
        clippy::should_implement_trait,
        reason = "`Iterator` cannot yield an item borrowing the iterator, which \
                  is the whole point here; `next` is what a lending iterator's \
                  method is called, it is what the async twin already calls it, \
                  and `while let Some(..) = reader.next()?` does work — unlike \
                  `Fragments::next_part`, which was renamed because it did not"
    )]
    pub fn next(&mut self) -> Result<Option<FlowFile<StreamedContent<'_, R>>>> {
        if self.done {
            return Ok(None);
        }
        if let Err(err) = self.discard_unread() {
            self.done = true;
            return Err(err);
        }

        let mut first = [0u8; 1];
        loop {
            match self.reader.read(&mut first) {
                // As in `parse_next`: a one-byte buffer, so this is the end.
                Ok(0) => {
                    self.done = true;
                    return Ok(None);
                }
                Ok(_) => break,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {} // retry
                Err(e) => {
                    self.done = true;
                    return Err(e.into());
                }
            }
        }

        let (attributes, size) =
            match parse_header(&mut self.reader, Some(first[0]), self.limits) {
                Ok(header) => header,
                Err(err) => {
                    self.done = true;
                    return Err(err);
                }
            };

        self.size = size;
        self.unread = size;
        // Disjoint borrows of two fields, which is what lets the content carry
        // both the stream and the counter that tracks it.
        let Self {
            reader, unread, ..
        } = self;
        Ok(Some(FlowFile::from_raw_parts(
            size,
            attributes,
            StreamedContent {
                inner: reader.take(size),
                unread,
            },
        )))
    }

    /// Read past whatever of the last flow file's content was left.
    fn discard_unread(&mut self) -> Result<()> {
        if self.unread == 0 {
            return Ok(());
        }
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
    /// Note that the stream may be positioned part-way through a flow file's
    /// content — whatever the last one handed out did not read. [`next`](Self::next)
    /// accounts for that; anything reading around it does not.
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
/// refuses every write after one, the way [`FlowFiles`] stops reading after an
/// error — appending to a stream that is mid-record would bury the failure
/// rather than report it, since the next record's header is indistinguishable
/// from the content the truncated one still expects. See
/// [`is_poisoned`](Self::is_poisoned).
///
/// # Finishing
///
/// This type writes straight through and buffers nothing of its own, but the
/// writer underneath it may, and neither writing nor dropping this one flushes
/// it. Finish with [`finish`](Self::finish), which flushes and hands the writer
/// back; [`flush`](Self::flush) does it without giving up the writer, and
/// [`into_inner`](Self::into_inner) deliberately skips it, for discarding a
/// stream rather than completing it.
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
    /// As [`FlowFile::write_to`]: a content reader that ends early leaves a
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
    /// Whatever the writer returns — which, since it may have accepted part
    /// of the flow file first, also poisons the writer.
    pub fn write_bytes(&mut self, flow_file: &FlowFile<Vec<u8>>) -> io::Result<u64> {
        self.guard()?;
        let bytes = flow_file.to_bytes();
        let result = self.writer.write_all(&bytes);
        self.poison_on_err(result)?;
        self.count += 1;
        Ok(flow_file.size)
    }

    fn guard(&self) -> io::Result<()> {
        if self.poisoned {
            return Err(crate::error::poisoned());
        }
        Ok(())
    }

    fn poison_on_err<T>(&mut self, result: io::Result<T>) -> io::Result<T> {
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    /// Flush the underlying writer.
    ///
    /// Nothing else here does: this type holds no buffer of its own, so
    /// whatever the writer underneath keeps is only pushed out when asked.
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
    /// ```
    /// use nififf3::{FlowFile, FlowFilesWriter};
    ///
    /// let mut writer = FlowFilesWriter::new(Vec::new());
    /// writer.write_bytes(&FlowFile::builder().content(&b"only"[..]))?;
    /// let bytes = writer.finish()?;
    /// # assert!(bytes.starts_with(b"NiFiFF3"));
    /// # Ok::<(), std::io::Error>(())
    /// ```
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

    /// Consume the writer, returning the underlying one *without flushing it*.
    ///
    /// For finishing a stream, use [`finish`](Self::finish). This is the
    /// escape hatch for the other case: taking the writer back after a failure
    /// in order to discard or truncate what was produced, where flushing the
    /// tail of a truncated flow file is the last thing wanted.
    pub fn into_inner(self) -> W {
        self.writer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A poisoned write is recognisable by its payload rather than its
    /// message: the point of `Error::WriterPoisoned` is that a caller can tell
    /// it from any other write failure without matching on text.
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
        let flow_file = flow_file.into_memory().unwrap();
        assert_eq!(flow_file.content().as_slice(), b"hello");
    }

    #[test]
    fn write_to_matches_to_bytes() {
        let flow_file = sample_flow_file().into_reader();
        let mut out = Vec::new();
        let copied = flow_file.write_to(&mut out).unwrap();
        assert_eq!(copied, 5);
        assert_eq!(out, sample_bytes());
    }

    /// The in-memory writer has to produce exactly what the reader-based one
    /// does, or the two ways to write a flow file would disagree.
    #[test]
    fn write_bytes_to_matches_write_to() {
        let flow_file = sample_flow_file();

        let mut buffered = Vec::new();
        let written = flow_file.write_bytes_to(&mut buffered).unwrap();
        assert_eq!(written, 5, "content bytes, as `write_to` reports");

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

    /// The attributes-only loop, which is the case that has no reason to read
    /// the content and so the one most likely to forget.
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

    /// The documented hazard, pinned: a flow file dropped with its content
    /// unread leaves the reader inside that content, and an envelope — a flow
    /// file carrying another — turns that into a plausible wrong answer rather
    /// than an error. If this ever stops being true, the warning on
    /// `parse_next` needs to change with it.
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
            // Deliberately no `skip_content`.
        }
        // The nested flow file surfaces as though it had been sent; parsing
        // then lands inside *its* content, fails, and the flow file that
        // really came second is never reached.
        assert_eq!(seen, ["envelope", "inner"]);

        // Skipping instead walks the stream that was actually sent.
        let mut reader = bytes.as_slice();
        let mut seen = Vec::new();
        while let Some(flow_file) = FlowFile::parse_next(&mut reader).unwrap() {
            seen.push(flow_file.attribute("who").unwrap().to_string());
            flow_file.skip_content().unwrap();
        }
        assert_eq!(seen, ["envelope", "second"]);
    }

    /// The whole point: reading none, some or all of a content must leave the
    /// stream in the same place — the one the next flow file starts at.
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

        // Read nothing at all.
        let mut flow_files = FlowFilesReader::new(bytes.as_slice());
        let mut seen = Vec::new();
        while let Some(flow_file) = flow_files.next().unwrap() {
            seen.push(flow_file.attribute("n").unwrap().to_string());
        }
        assert_eq!(seen, ["1", "2", "3"]);

        // Read one byte of each, leaving the rest.
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

        // Read all of it.
        let mut flow_files = FlowFilesReader::new(bytes.as_slice());
        let mut seen = Vec::new();
        while let Some(flow_file) = flow_files.next().unwrap() {
            seen.push(flow_file.into_memory().unwrap().into_content());
        }
        assert_eq!(seen, [b"aaaa".to_vec(), b"bbbb".to_vec(), b"cccc".to_vec()]);
    }

    /// The envelope that misleads `parse_next` when its content is dropped
    /// unread is simply not a problem here.
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
        let mut bytes = FlowFile::builder()
            .attribute("n", "1")
            .content(&b"aaaa"[..])
            .to_bytes();
        bytes.truncate(bytes.len() - 2);

        let mut flow_files = FlowFilesReader::new(bytes.as_slice());
        assert!(flow_files.next().unwrap().is_some(), "the header is intact");
        assert!(
            matches!(
                flow_files.next(),
                Err(Error::SizeMismatch {
                    expected: 4,
                    actual: 2
                })
            ),
            "the skip must notice the content ran out"
        );
        assert!(flow_files.next().unwrap().is_none(), "fused after the error");
    }

    #[test]
    fn flow_files_iterator_fuses_after_error() {
        let mut bytes = sample_bytes();
        bytes.extend_from_slice(b"garbage");
        let mut iter = FlowFiles::new(bytes.as_slice());
        assert!(iter.next().unwrap().is_ok());
        assert!(matches!(iter.next(), Some(Err(Error::InvalidMagic(_)))));
        assert!(iter.next().is_none());
        assert!(iter.next().is_none());
    }

    /// Reading flow files off the front of a stream that holds something else
    /// afterwards, which is what handing the reader back is for.
    #[test]
    fn the_reader_comes_back_out_positioned_after_the_flow_files() {
        let mut bytes = sample_bytes();
        bytes.extend_from_slice(b"and then something else");

        let mut flow_files = FlowFiles::new(bytes.as_slice());
        assert!(flow_files.next().unwrap().is_ok());
        assert_eq!(flow_files.get_ref().len(), b"and then something else".len());

        let mut trailer = Vec::new();
        flow_files.into_inner().read_to_end(&mut trailer).unwrap();
        assert_eq!(trailer, b"and then something else");
    }

    #[test]
    fn bad_magic_is_rejected() {
        let mut bytes = sample_bytes();
        bytes[0] = b'X';
        assert!(matches!(
            FlowFile::from_bytes(&bytes),
            Err(Error::InvalidMagic(_))
        ));
    }

    #[test]
    fn truncated_content_is_a_size_mismatch() {
        let bytes = sample_bytes();
        let truncated = &bytes[..bytes.len() - 2];
        assert!(matches!(
            FlowFile::from_bytes(truncated),
            Err(Error::SizeMismatch {
                expected: 5,
                actual: 3
            })
        ));
        // `into_memory` parses nothing, so it reports the same condition as an
        // io error — with the structured error still recoverable from it.
        let parsed = FlowFile::parse(truncated).unwrap();
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
    fn readers_report_truncation_the_same_way_from_bytes_does() {
        let bytes = sample_bytes();
        let truncated = &bytes[..bytes.len() - 2];

        // Both entry points yield the structured error, not `Io` wrapping it.
        for err in [
            FlowFile::from_bytes(truncated).unwrap_err(),
            FlowFiles::new(truncated).next().unwrap().unwrap_err(),
        ] {
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
    }

    #[test]
    fn readers_still_report_plain_io_errors_as_io() {
        struct Failing;
        impl Read for Failing {
            fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::new(io::ErrorKind::ConnectionReset, "gone"))
            }
        }

        let err = FlowFiles::new(Failing).next().unwrap().unwrap_err();
        assert!(
            matches!(err, Error::Io(ref e) if e.kind() == io::ErrorKind::ConnectionReset),
            "{err:?}"
        );
    }

    #[test]
    fn limits_reject_an_oversized_declared_content_size() {
        let bytes = sample_flow_file().to_bytes();
        let limits = Limits::recommended().with_max_content_len(4);

        assert!(matches!(
            FlowFile::parse_with_limits(bytes.as_slice(), limits),
            Err(Error::ContentTooLarge { size: 5, limit: 4 })
        ));
        // The check is on the declared size, so it fires before any content
        // is read — a header alone is enough to trip it.
        let header_only = &bytes[..bytes.len() - 5];
        assert!(matches!(
            FlowFile::parse_with_limits(header_only, limits),
            Err(Error::ContentTooLarge { size: 5, limit: 4 })
        ));
        assert!(FlowFile::parse_with_limits(bytes.as_slice(), Limits::recommended()).is_ok());
    }

    /// A reader that yields `available` bytes and then ends, so a flow file
    /// declaring more than that is truncated part-way through its content.
    struct Short(usize);

    impl Read for Short {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let n = self.0.min(buf.len());
            self.0 -= n;
            buf[..n].fill(b'x');
            Ok(n)
        }
    }

    #[test]
    fn a_failed_write_poisons_the_writer() {
        let mut out = Vec::new();
        let mut writer = FlowFilesWriter::new(&mut out);

        let err = writer
            .write(FlowFile::builder().attribute("n", "1").reader(Short(3), 10))
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert!(writer.is_poisoned());
        assert_eq!(writer.count(), 0, "the failed one does not count");

        // Appending here would let the next header be read as the truncated
        // record's content, producing a plausible but wrong flow file.
        let err = writer
            .write_bytes(&FlowFile::builder().attribute("n", "2").content(&b"ok"[..]))
            .unwrap_err();
        assert!(is_poisoned_error(&err), "{err:?}");
        assert_eq!(writer.count(), 0);

        // Nothing was appended after the failure: the header plus the 3
        // bytes the reader did produce, and not a byte more.
        let stream = writer.into_inner();
        assert_eq!(
            stream.len(),
            FlowFile::builder().attribute("n", "1").content(Vec::new()).to_bytes().len() + 3
        );
    }

    #[test]
    fn a_healthy_writer_is_never_poisoned() {
        let mut out = Vec::new();
        let mut writer = FlowFilesWriter::new(&mut out);
        writer
            .write_bytes(&FlowFile::builder().content(&b"first"[..]))
            .unwrap();
        writer
            .write(FlowFile::builder().reader(&b"second"[..], 6))
            .unwrap();

        assert!(!writer.is_poisoned());
        assert_eq!(writer.count(), 2);
        assert_eq!(FlowFiles::new(out.as_slice()).count(), 2);
    }

    /// A writer that records what it was asked to do, standing in for one
    /// that buffers — a `BufWriter`, a compressor, a socket.
    #[derive(Default)]
    struct Recording {
        bytes: Vec<u8>,
        flushes: usize,
    }

    impl Write for Recording {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.bytes.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            self.flushes += 1;
            Ok(())
        }
    }

    #[test]
    fn finish_flushes_and_into_inner_does_not() {
        let write_one = |writer: &mut FlowFilesWriter<Recording>| {
            writer
                .write_bytes(&FlowFile::builder().content(&b"content"[..]))
                .unwrap();
        };

        let mut writer = FlowFilesWriter::new(Recording::default());
        write_one(&mut writer);
        let inner = writer.finish().unwrap();
        assert_eq!(inner.flushes, 1);
        assert_eq!(FlowFiles::new(inner.bytes.as_slice()).count(), 1);

        // The escape hatch stays an escape hatch: nothing is pushed out, so a
        // half-written stream can be discarded instead of completed.
        let mut writer = FlowFilesWriter::new(Recording::default());
        write_one(&mut writer);
        assert_eq!(writer.into_inner().flushes, 0);
    }

    #[test]
    fn a_failed_flush_poisons_the_writer() {
        struct FlushFails;

        impl Write for FlushFails {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                Ok(buf.len())
            }

            fn flush(&mut self) -> io::Result<()> {
                Err(io::Error::other("the device went away"))
            }
        }

        let mut writer = FlowFilesWriter::new(FlushFails);
        writer
            .write_bytes(&FlowFile::builder().content(&b"first"[..]))
            .unwrap();

        assert!(writer.flush().is_err());
        // Bytes the writer accepted may never have reached the stream, so it
        // is mid-flow-file for the same reason a failed write leaves it there.
        assert!(writer.is_poisoned());
        let err = writer
            .write_bytes(&FlowFile::builder().content(&b"second"[..]))
            .unwrap_err();
        assert!(is_poisoned_error(&err), "{err:?}");
    }

    #[test]
    fn from_bytes_limits_apply_to_the_header() {
        let bytes = sample_flow_file().to_bytes();

        assert!(matches!(
            FlowFile::from_bytes_with_limits(&bytes, Limits::recommended().with_max_attributes(1)),
            Err(Error::TooManyAttributes { count: 2, limit: 1 })
        ));
        assert!(matches!(
            FlowFile::from_bytes_with_limits(&bytes, Limits::recommended().with_max_content_len(4)),
            Err(Error::ContentTooLarge { size: 5, limit: 4 })
        ));
        assert!(FlowFile::from_bytes_with_limits(&bytes, Limits::recommended()).is_ok());
    }

    #[test]
    fn content_mut_reads_incrementally_and_keeps_the_attributes() {
        let bytes = sample_bytes();
        let mut flow_file = FlowFile::parse(bytes.as_slice()).unwrap();

        let mut head = [0u8; 2];
        flow_file.content_mut().read_exact(&mut head).unwrap();
        assert_eq!(&head, b"he");
        assert_eq!(flow_file.attributes()["path"], "x");

        // The rest is still there, and the flow file still knows its size.
        assert_eq!(flow_file.size(), 5);
        let mut rest = Vec::new();
        flow_file.content_mut().read_to_end(&mut rest).unwrap();
        assert_eq!(rest, b"llo");
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
    fn truncated_header_is_an_io_error() {
        let bytes = sample_bytes();
        assert!(matches!(
            FlowFile::from_bytes(&bytes[..10]),
            Err(Error::Io(_))
        ));
    }

    #[test]
    fn limits_reject_excess_attribute_count() {
        let flow_file = FlowFile::builder()
            .attributes((0..10).map(|i| (format!("k{i}"), "v")))
            .content(Vec::new());
        let bytes = flow_file.to_bytes();

        let limits = Limits::recommended().with_max_attributes(5);
        assert!(matches!(
            FlowFile::parse_with_limits(bytes.as_slice(), limits),
            Err(Error::TooManyAttributes {
                count: 10,
                limit: 5
            })
        ));
        assert!(FlowFile::parse_with_limits(bytes.as_slice(), Limits::recommended()).is_ok());
    }

    /// The aggregate the per-attribute limits cannot express: each attribute
    /// here is tiny, and together they are not.
    #[test]
    fn limits_reject_an_oversized_header_in_total() {
        let bytes = FlowFile::builder()
            .attributes((0..10).map(|i| (format!("k{i}"), "v".repeat(100))))
            .content(Vec::new())
            .to_bytes();

        let limits = Limits::UNLIMITED.with_max_total_attribute_len(256);
        assert!(matches!(
            FlowFile::parse_with_limits(bytes.as_slice(), limits),
            Err(Error::HeaderTooLarge { limit: 256, .. })
        ));
        // Each one on its own is well inside the per-attribute limits, so
        // nothing but the total would have caught this.
        assert!(
            FlowFile::parse_with_limits(
                bytes.as_slice(),
                Limits::UNLIMITED
                    .with_max_attributes(10)
                    .with_max_attribute_len(1024)
            )
            .is_ok()
        );
    }

    #[test]
    fn limits_reject_oversized_attributes() {
        let bytes = FlowFile::builder()
            .attribute("key", "a value larger than the limit")
            .content(Vec::new())
            .to_bytes();

        let limits = Limits::recommended().with_max_attribute_len(8);
        assert!(matches!(
            FlowFile::parse_with_limits(bytes.as_slice(), limits),
            Err(Error::AttributeTooLong { limit: 8, .. })
        ));
    }

    #[test]
    fn declared_length_beyond_input_fails_without_allocating() {
        // Header declaring a ~4 GiB attribute key, but hardly any actual data.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"NiFiFF3");
        bytes.extend_from_slice(&[0x00, 0x01]); // one attribute
        bytes.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00]); // key length
        bytes.extend_from_slice(b"tiny");
        let err = FlowFile::parse(bytes.as_slice()).unwrap_err();
        assert!(matches!(err, Error::Io(ref e) if e.kind() == io::ErrorKind::UnexpectedEof));
    }

    #[test]
    fn invalid_utf8_attribute_is_rejected() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"NiFiFF3");
        bytes.extend_from_slice(&[0x00, 0x01]);
        bytes.extend_from_slice(&[0x00, 0x01, 0xFF]); // invalid UTF-8 key
        bytes.extend_from_slice(&[0x00, 0x01, b'v']);
        bytes.extend_from_slice(&[0; 8]);
        assert!(matches!(
            FlowFile::from_bytes(&bytes),
            Err(Error::InvalidAttribute(_))
        ));
    }

    #[test]
    fn large_attribute_uses_extended_field_length() {
        let value = "v".repeat(70_000);
        let flow_file = FlowFile::builder()
            .attribute("big", &value)
            .content(&b""[..]);
        let bytes = flow_file.to_bytes();
        // key, then extended length marker for the value
        let marker = [
            0x00, 0x03, b'b', b'i', b'g', 0xFF, 0xFF, 0x00, 0x01, 0x11, 0x70,
        ];
        assert!(bytes.windows(marker.len()).any(|w| w == marker));
        let parsed = FlowFile::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.attributes()["big"], value);
    }

    #[test]
    fn empty_attributes_and_content() {
        let bytes = FlowFile::builder().content(Vec::new()).to_bytes();
        let mut expected = b"NiFiFF3".to_vec();
        expected.extend_from_slice(&[0x00, 0x00]);
        expected.extend_from_slice(&[0; 8]);
        assert_eq!(bytes, expected);
        let parsed = FlowFile::from_bytes(&bytes).unwrap();
        assert!(parsed.attributes().is_empty());
        assert_eq!(parsed.size(), 0);
    }
}
