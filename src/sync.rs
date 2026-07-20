//! Parsing and serialization over `std::io::Read`/`Write`.

use std::collections::HashMap;
use std::io::{self, Read, Write};

use crate::format::{MAGIC, MAX_VALUE_2_BYTES};
use crate::{Error, FlowFile, Result};

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

fn read_string(reader: &mut impl Read) -> Result<String> {
    let len = read_field_len(reader)?;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    Ok(String::from_utf8(buf)?)
}

/// Read the header, returning the attributes and the declared content size.
/// `first_byte` is a byte already consumed from the reader, if any.
pub(crate) fn parse_header(
    reader: &mut impl Read,
    first_byte: Option<u8>,
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
    let mut attributes = HashMap::with_capacity(count.min(1024));
    for _ in 0..count {
        let key = read_string(reader)?;
        let value = read_string(reader)?;
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
    /// [`size`]: FlowFile::size
    pub fn parse(mut reader: R) -> Result<Self> {
        let (attributes, size) = parse_header(&mut reader, None)?;
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
    pub fn parse_next(reader: &'r mut R) -> Result<Option<Self>> {
        let mut first = [0u8; 1];
        loop {
            match reader.read(&mut first) {
                Ok(0) => return Ok(None),
                Ok(_) => break,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            }
        }
        let (attributes, size) = parse_header(reader, Some(first[0]))?;
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
