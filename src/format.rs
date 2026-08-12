//! Encoding primitives for the FlowFile V3 binary format.
//!
//! The layout matches NiFi's `FlowFilePackagerV3`:
//!
//! 1. Magic header: the 7 ASCII bytes `NiFiFF3`.
//! 2. Attribute count, as a field length.
//! 3. For each attribute: key, then value, each as a field length followed
//!    by that many UTF-8 bytes.
//! 4. Content size, as an 8-byte big-endian integer.
//! 5. The content bytes.
//!
//! A field length is 2 bytes big-endian. A value of 0xFFFF or above is
//! written as the marker `0xFF 0xFF`, followed by the value as 4 bytes
//! big-endian.
//!
//! The extended form encodes a `u32`, but NiFi reads it into a Java `int`. So
//! a length at or above `i32::MAX` comes back negative there, and fails on the
//! array allocation. The interoperable ceiling is half the encodable one. No
//! ordinary attribute comes near either, but the format appears to offer 4
//! GiB, and only 2 GiB of that travels.

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

/// How many bytes [`write_field_len`] writes for `len`. That is two bytes, or
/// six in the extended form.
const fn field_len_bytes(len: usize) -> usize {
    if len < MAX_VALUE_2_BYTES { 2 } else { 6 }
}

/// The exact number of bytes [`encode_header`] will produce.
///
/// This costs one pass over the attributes doing arithmetic, and the pass pays
/// for itself. A header buffer and a whole serialized flow file can both be
/// allocated once at the right size, rather than grown into. It is also the
/// answer [`FlowFile::serialized_len`](crate::FlowFile::serialized_len) gives,
/// so it lives here rather than being written out again at each site.
pub(crate) fn header_len(attributes: &HashMap<String, String>) -> usize {
    let fields: usize = attributes
        .iter()
        .map(|(key, value)| {
            field_len_bytes(key.len()) + key.len() + field_len_bytes(value.len()) + value.len()
        })
        .sum();
    MAGIC.len() + field_len_bytes(attributes.len()) + fields + size_of::<u64>()
}

/// Serialize the header, meaning everything before the content bytes.
///
/// Attributes are written in sorted key order, so the output is
/// deterministic. NiFi itself accepts any order.
pub(crate) fn encode_header(attributes: &HashMap<String, String>, size: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(header_len(attributes));
    encode_header_into(&mut buf, attributes, size);
    buf
}

/// Serialize the header into a buffer the caller has already sized.
///
/// Use this to serialize a whole flow file into one allocation. The header and
/// the content go into the same buffer, so the header does not need a buffer
/// of its own and a copy out of it.
pub(crate) fn encode_header_into(
    buf: &mut Vec<u8>,
    attributes: &HashMap<String, String>,
    size: u64,
) {
    buf.extend_from_slice(&MAGIC);
    write_field_len(buf, attributes.len());
    // Taken as pairs rather than as keys to look up again. Sorting borrowed
    // entries costs one small allocation. Sorting the keys and indexing the
    // map costs a hash and a probe per attribute, and on a header with many
    // attributes that is most of the work of writing one. The sort is
    // `sort_unstable` because map keys are distinct, so there are no equal
    // elements whose order stability could preserve. It also needs no scratch
    // buffer of its own, where `sort` does.
    let mut entries: Vec<(&str, &str)> = attributes
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    entries.sort_unstable_by_key(|(key, _)| *key);
    for (key, value) in entries {
        write_string(buf, key);
        write_string(buf, value);
    }
    buf.extend_from_slice(&size.to_be_bytes());
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

    /// The predicted length has to be the produced length exactly. If it is
    /// not, every buffer sized from it either reallocates or over-reserves,
    /// and `serialized_len` gives the wrong answer to a caller computing a
    /// `Content-Length`.
    #[test]
    fn the_predicted_header_length_is_the_produced_one() {
        let cases: [Vec<(String, String)>; 5] = [
            vec![],
            vec![("k".into(), "v".into())],
            vec![("path".into(), "x".into()), ("a".into(), "b".into())],
            // Multi-byte characters, so byte lengths are not character counts.
            vec![("é→🙂".into(), "🙂".into())],
            // Both sides of the two-byte field length boundary.
            vec![
                ("short".into(), "v".repeat(0xFFFE)),
                ("long".into(), "v".repeat(0xFFFF)),
                ("v".repeat(0x1_0000), "x".into()),
            ],
        ];

        for attributes in cases {
            let map: HashMap<String, String> = attributes.into_iter().collect();
            assert_eq!(
                encode_header(&map, 7).len(),
                header_len(&map),
                "{} attributes",
                map.len()
            );
        }
    }

    /// A header with enough attributes to cross the two-byte boundary on the
    /// attribute count as well. That is a different branch of the same
    /// arithmetic from the one the case above covers.
    #[test]
    fn the_predicted_length_holds_across_the_count_boundary() {
        for count in [0xFFFE, 0xFFFF, 0x1_0000] {
            let map: HashMap<String, String> = (0..count)
                .map(|i| (format!("k{i}"), "v".to_string()))
                .collect();
            assert_eq!(encode_header(&map, 0).len(), header_len(&map), "{count}");
        }
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
