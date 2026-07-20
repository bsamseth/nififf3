//! Parsing and serialization over `std::io::Read`/`Write`.

use std::collections::HashMap;
use std::io::{self, Read, Write};

use crate::format::{MAGIC, MAX_VALUE_2_BYTES};
use crate::{Error, FlowFile, Limits, Result};

fn read_field_len(reader: &mut impl Read) -> Result<usize> {
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
    Ok((attributes, u64::from_be_bytes(size)))
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
    /// [`size`]: FlowFile::size
    pub fn parse(reader: R) -> Result<Self> {
        Self::parse_with_limits(reader, &Limits::UNLIMITED)
    }

    /// Like [`parse`](Self::parse), but enforcing [`Limits`] on the header.
    ///
    /// Use this for untrusted input; see [`Limits`] for the threat model.
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
    pub fn parse_next(reader: &'r mut R) -> Result<Option<Self>> {
        Self::parse_next_with_limits(reader, &Limits::UNLIMITED)
    }

    /// Like [`parse_next`](Self::parse_next), but enforcing [`Limits`] on
    /// the header. Use this for untrusted input.
    pub fn parse_next_with_limits(reader: &'r mut R, limits: &Limits) -> Result<Option<Self>> {
        let mut first = [0u8; 1];
        loop {
            match reader.read(&mut first) {
                Ok(0) => return Ok(None),
                Ok(_) => break,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
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
    /// Returns the number of content bytes copied. If the content reader
    /// ends before `size` bytes were read, an [`Error::SizeMismatch`] is
    /// returned (with the writer left partially written).
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let content = &b"hello"[..]; // any `impl Read`
    /// let mut flow_file = FlowFile::builder().reader(content, 5);
    ///
    /// let mut out = Vec::new();
    /// flow_file.write_to(&mut out).unwrap();
    /// assert_eq!(FlowFile::from_bytes(&out).unwrap().size(), 5);
    /// ```
    ///
    /// [`size`]: FlowFile::size
    pub fn write_to<W: Write>(&mut self, writer: &mut W) -> Result<u64> {
        writer.write_all(&self.header_bytes())?;
        let copied = io::copy(&mut (&mut self.content).take(self.size), writer)?;
        if copied != self.size {
            return Err(Error::SizeMismatch {
                expected: self.size,
                actual: copied,
            });
        }
        Ok(copied)
    }

    /// Read the content to completion, producing an in-memory flow file.
    ///
    /// Validates that exactly [`size`] bytes of content were available.
    ///
    /// [`size`]: FlowFile::size
    pub fn into_bytes(mut self) -> Result<FlowFile<Vec<u8>>> {
        let mut content = Vec::new();
        let read = (&mut self.content)
            .take(self.size)
            .read_to_end(&mut content)? as u64;
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

/// Iterator over a stream of concatenated flow files.
///
/// Yields each flow file with its content buffered in memory, and ends on a
/// clean end of input. After an error the iterator is fused (keeps returning
/// `None`), since the stream position is no longer trustworthy. To stream
/// contents instead of buffering them, use [`FlowFile::parse_next`] directly.
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
            Ok(Some(flow_file)) => Some(flow_file.into_bytes()),
            Err(err) => Some(Err(err)),
        };
        if !matches!(result, Some(Ok(_))) {
            self.done = true;
        }
        result
    }
}

impl<R: Read> std::iter::FusedIterator for FlowFiles<R> {}

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
        let mut flow_file = sample_flow_file().into_reader();
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
        let parsed = FlowFile::parse(truncated).unwrap();
        assert!(matches!(
            parsed.into_bytes(),
            Err(Error::SizeMismatch {
                expected: 5,
                actual: 3
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
