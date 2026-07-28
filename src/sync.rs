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
    Ok(String::from_utf8(buf)?)
}

/// Read the header, returning the attributes and the declared content size.
/// `first_byte` is a byte already consumed from the reader, if any.
pub(crate) fn parse_header(
    reader: &mut impl Read,
    first_byte: Option<u8>,
    limits: &Limits,
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
    for _ in 0..count {
        let key = read_string(reader, limits.max_attribute_len)?;
        let value = read_string(reader, limits.max_attribute_len)?;
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
    /// reader ends means the input was truncated; [`FlowFile::into_bytes`]
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
    /// let flow_file = flow_file.into_bytes().unwrap();
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
        Self::parse_with_limits(reader, &Limits::UNLIMITED)
    }

    /// Like [`parse`](Self::parse), but enforcing [`Limits`] on the header.
    ///
    /// Use this for untrusted input; see [`Limits`] for the threat model.
    ///
    /// # Errors
    ///
    /// As [`parse`](Self::parse), plus [`Error::TooManyAttributes`] or
    /// [`Error::AttributeTooLong`] when the header exceeds `limits`.
    pub fn parse_with_limits(mut reader: R, limits: &Limits) -> Result<Self> {
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
    /// Returns `Ok(None)` on a clean end of input. The previous flow file's
    /// content must be fully consumed before calling this again, otherwise
    /// parsing resumes in the middle of that content.
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
    ///     flow_file.into_bytes().unwrap(); // consume the content
    /// }
    /// assert_eq!(count, 2);
    /// ```
    ///
    /// # Errors
    ///
    /// As [`FlowFile::parse`]. Note that a stream ending part-way through a
    /// header is [`Error::Io`], not `Ok(None)` — only a clean boundary ends
    /// the iteration.
    pub fn parse_next(reader: &'r mut R) -> Result<Option<Self>> {
        Self::parse_next_with_limits(reader, &Limits::UNLIMITED)
    }

    /// Like [`parse_next`](Self::parse_next), but enforcing [`Limits`] on
    /// the header. Use this for untrusted input.
    ///
    /// # Errors
    ///
    /// As [`parse_next`](Self::parse_next), plus [`Error::TooManyAttributes`]
    /// or [`Error::AttributeTooLong`] when a header exceeds `limits`.
    pub fn parse_next_with_limits(reader: &'r mut R, limits: &Limits) -> Result<Option<Self>> {
        let mut first = [0u8; 1];
        loop {
            match reader.read(&mut first) {
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
    /// [`size`]: FlowFile::size
    pub fn write_to<W: Write>(mut self, writer: &mut W) -> io::Result<u64> {
        writer.write_all(&self.header_bytes())?;
        let copied = io::copy(&mut (&mut self.content).take(self.size), writer)?;
        if copied != self.size {
            return Err(crate::error::truncated(self.size, copied));
        }
        Ok(copied)
    }

    /// Read the content to completion, producing an in-memory flow file.
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
    pub fn into_bytes(mut self) -> io::Result<FlowFile<Vec<u8>>> {
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

/// Iterator over a stream of concatenated flow files.
///
/// Yields each flow file with its content buffered in memory, and ends on a
/// clean end of input. After an error the iterator is fused (keeps returning
/// `None`), since the stream position is no longer trustworthy. To stream
/// contents instead of buffering them, use [`FlowFile::parse_next`] directly.
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
}

impl<R: Read> Iterator for FlowFiles<R> {
    type Item = Result<FlowFile<Vec<u8>>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let result = match FlowFile::parse_next_with_limits(&mut self.reader, &self.limits) {
            Ok(None) => None,
            Ok(Some(flow_file)) => {
                Some(flow_file.into_bytes().map_err(crate::error::unwrap_io))
            }
            Err(err) => Some(Err(err)),
        };
        if !matches!(result, Some(Ok(_))) {
            self.done = true;
        }
        result
    }
}

impl<R: Read> std::iter::FusedIterator for FlowFiles<R> {}

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
/// ```
/// use nififf3::{FlowFile, FlowFilesWriter};
///
/// let parent = FlowFile::builder().attribute("filename", "pair").content(Vec::new());
/// let mut parts = parent.fragments().with_count(2);
///
/// let mut out = Vec::new();
/// let mut writer = FlowFilesWriter::new(&mut out);
/// writer.write_bytes(&parts.next().content(&b"first"[..]))?;
/// writer.write_bytes(&parts.next().content(&b"second"[..]))?;
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

    /// A mutable reference to the underlying writer.
    pub fn get_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    /// Consume the writer, returning the underlying one.
    pub fn into_inner(self) -> W {
        self.writer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let flow_file = flow_file.into_bytes().unwrap();
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

    #[test]
    fn parse_next_reads_concatenated_flow_files() {
        let mut bytes = sample_bytes();
        bytes.extend_from_slice(&sample_bytes());
        let mut reader = bytes.as_slice();
        let mut count = 0;
        while let Some(flow_file) = FlowFile::parse_next(&mut reader).unwrap() {
            let flow_file = flow_file.into_bytes().unwrap();
            assert_eq!(flow_file.content().as_slice(), b"hello");
            count += 1;
        }
        assert_eq!(count, 2);
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
        // `into_bytes` parses nothing, so it reports the same condition as an
        // io error — with the structured error still recoverable from it.
        let parsed = FlowFile::parse(truncated).unwrap();
        let err = parsed.into_bytes().unwrap_err();
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
        let limits = Limits::default().max_content_len(4);

        assert!(matches!(
            FlowFile::parse_with_limits(bytes.as_slice(), &limits),
            Err(Error::ContentTooLarge { size: 5, limit: 4 })
        ));
        // The check is on the declared size, so it fires before any content
        // is read — a header alone is enough to trip it.
        let header_only = &bytes[..bytes.len() - 5];
        assert!(matches!(
            FlowFile::parse_with_limits(header_only, &limits),
            Err(Error::ContentTooLarge { size: 5, limit: 4 })
        ));
        assert!(FlowFile::parse_with_limits(bytes.as_slice(), &Limits::default()).is_ok());
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
        assert!(err.to_string().contains("poisoned"));
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

    #[test]
    fn from_bytes_limits_apply_to_the_header() {
        let bytes = sample_flow_file().to_bytes();

        assert!(matches!(
            FlowFile::from_bytes_with_limits(&bytes, &Limits::default().max_attributes(1)),
            Err(Error::TooManyAttributes { count: 2, limit: 1 })
        ));
        assert!(matches!(
            FlowFile::from_bytes_with_limits(&bytes, &Limits::default().max_content_len(4)),
            Err(Error::ContentTooLarge { size: 5, limit: 4 })
        ));
        assert!(FlowFile::from_bytes_with_limits(&bytes, &Limits::default()).is_ok());
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

        let limits = Limits::default().max_attributes(5);
        assert!(matches!(
            FlowFile::parse_with_limits(bytes.as_slice(), &limits),
            Err(Error::TooManyAttributes {
                count: 10,
                limit: 5
            })
        ));
        assert!(FlowFile::parse_with_limits(bytes.as_slice(), &Limits::default()).is_ok());
    }

    #[test]
    fn limits_reject_oversized_attributes() {
        let bytes = FlowFile::builder()
            .attribute("key", "a value larger than the limit")
            .content(Vec::new())
            .to_bytes();

        let limits = Limits::default().max_attribute_len(8);
        assert!(matches!(
            FlowFile::parse_with_limits(bytes.as_slice(), &limits),
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
