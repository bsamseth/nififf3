//! Encoding primitives for the FlowFile V3 binary format.
//!
//! Layout, matching NiFi's `FlowFilePackagerV3`:
//!
//! 1. Magic header: the 7 ASCII bytes `NiFiFF3`.
//! 2. Attribute count as a *field length*.
//! 3. For each attribute: key, then value, each as a field length followed
//!    by that many UTF-8 bytes.
//! 4. Content size as an 8-byte big-endian integer.
//! 5. The content bytes.
//!
//! A *field length* is 2 bytes big-endian; values >= 0xFFFF are written as
//! the marker `0xFF 0xFF` followed by the value as 4 bytes big-endian.

use std::collections::HashMap;

pub(crate) const MAGIC: [u8; 7] = *b"NiFiFF3";
pub(crate) const MAX_VALUE_2_BYTES: usize = 0xFFFF;

pub(crate) fn write_field_len(buf: &mut Vec<u8>, len: usize) {
    if let Ok(short) = u16::try_from(len)
        && len < MAX_VALUE_2_BYTES
    {
        buf.extend_from_slice(&short.to_be_bytes());
    } else {
        let len = u32::try_from(len).expect("field length exceeds u32::MAX");
        buf.extend_from_slice(&[0xFF, 0xFF]);
        buf.extend_from_slice(&len.to_be_bytes());
    }
}

pub(crate) fn write_string(buf: &mut Vec<u8>, value: &str) {
    write_field_len(buf, value.len());
    buf.extend_from_slice(value.as_bytes());
}

/// Serialize the header (everything before the content bytes).
///
/// Attributes are written in sorted key order so the output is
/// deterministic; NiFi itself does not require any particular order.
pub(crate) fn encode_header(attributes: &HashMap<String, String>, size: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);
    buf.extend_from_slice(&MAGIC);
    write_field_len(&mut buf, attributes.len());
    let mut keys: Vec<&String> = attributes.keys().collect();
    keys.sort();
    for key in keys {
        write_string(&mut buf, key);
        write_string(&mut buf, &attributes[key]);
    }
    buf.extend_from_slice(&size.to_be_bytes());
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field_len(len: usize) -> Vec<u8> {
        let mut buf = Vec::new();
        write_field_len(&mut buf, len);
        buf
    }

    #[test]
    fn short_field_lengths_use_two_bytes() {
        assert_eq!(field_len(0), [0x00, 0x00]);
        assert_eq!(field_len(5), [0x00, 0x05]);
        assert_eq!(field_len(0xFFFE), [0xFF, 0xFE]);
    }

    #[test]
    fn long_field_lengths_use_marker_and_four_bytes() {
        assert_eq!(field_len(0xFFFF), [0xFF, 0xFF, 0x00, 0x00, 0xFF, 0xFF]);
        assert_eq!(field_len(70_000), [0xFF, 0xFF, 0x00, 0x01, 0x11, 0x70]);
    }

    #[test]
    fn header_layout() {
        let attributes = HashMap::from([("path".to_string(), "x".to_string())]);
        let header = encode_header(&attributes, 5);
        let mut expected = b"NiFiFF3".to_vec();
        expected.extend_from_slice(&[0x00, 0x01]); // one attribute
        expected.extend_from_slice(&[0x00, 0x04]);
        expected.extend_from_slice(b"path");
        expected.extend_from_slice(&[0x00, 0x01]);
        expected.extend_from_slice(b"x");
        expected.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 5]);
        assert_eq!(header, expected);
    }
}
