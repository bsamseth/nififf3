//! The FlowFile V3 wire format: encoding a header, and decoding one a few
//! bytes at a time.
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

use crate::{Error, Result};

pub(crate) const MAGIC: [u8; 7] = *b"NiFiFF3";

/// The two-byte field length that announces a four-byte one.
const EXTENDED_LEN: u16 = 0xFFFF;

/// How much to reserve for a declared length before any of it has arrived to
/// justify the allocation.
///
/// Nothing in this crate sizes a buffer from a declared length alone. A
/// corrupt header can claim four gigabytes over a four-byte input, and
/// allocating for the claim would abort the process on garbage. So the first
/// reservation is capped here, and every one after it is capped by how much
/// has already arrived. Bytes that have arrived are the part a header cannot
/// fake. Anything at or under this size is allocated once, at exactly its
/// size, and that covers every ordinary attribute and most content.
pub(crate) const RESERVE_AHEAD: usize = 64 * 1024;

/// How large the next reservation may be, given how much has arrived and how
/// much is still to come.
pub(crate) fn reserve_step(done: usize, left: usize) -> usize {
    left.min(done.max(RESERVE_AHEAD))
}

/// A decoded header: everything that precedes the content.
#[derive(Debug)]
pub(crate) struct Header {
    pub(crate) attributes: HashMap<String, String>,
    pub(crate) size: u64,
}

/// Which of the two strings of an attribute is being decoded.
#[derive(Clone, Copy, Debug)]
enum Part {
    Key,
    Value,
}

#[derive(Clone, Copy, Debug)]
enum Stage {
    Magic,
    Count,
    CountExt,
    Len(Part),
    LenExt(Part),
    Bytes(Part),
    Size,
    Done,
}

/// Decodes a header incrementally, so one decoder serves readers of every
/// kind: a byte slice, a `std::io::Read` and a `tokio::io::AsyncRead`.
///
/// The protocol is a loop of two calls. [`next_read`](Self::next_read) hands
/// out a buffer that the driver fills completely, from wherever its bytes come
/// from, and [`advance`](Self::advance) decodes what was filled. The buffers
/// are exactly as large as the next piece of the header, so the driver never
/// reads past the header into the content. Attribute bytes are handed out in
/// chunks bounded by [`RESERVE_AHEAD`] and by what has already arrived, so a
/// corrupt length costs an allocation proportional to real input, never to
/// the claim.
pub(crate) struct HeaderDecoder {
    stage: Stage,
    attributes: HashMap<String, String>,
    /// Attribute pairs still to decode, counting the one in progress.
    remaining: usize,
    /// The key of the pair in progress, once decoded.
    key: Option<String>,
    /// The bytes of the string in progress, as far as they have arrived.
    field: Vec<u8>,
    /// The declared length of the string in progress.
    field_len: usize,
    /// Room for the fixed-size pieces: magic, lengths and the content size.
    scratch: [u8; 8],
    /// Whether any byte at all has been decoded. A stream that ends before
    /// the first one ended cleanly, on a flow file boundary.
    started: bool,
}

impl HeaderDecoder {
    pub(crate) fn new() -> Self {
        Self {
            stage: Stage::Magic,
            attributes: HashMap::new(),
            remaining: 0,
            key: None,
            field: Vec::new(),
            field_len: 0,
            scratch: [0; 8],
            started: false,
        }
    }

    /// Whether any byte has been decoded yet.
    pub(crate) fn started(&self) -> bool {
        self.started
    }

    /// What the next read is for, in words, for an error that says where the
    /// input ended.
    pub(crate) fn expecting(&self) -> &'static str {
        match self.stage {
            Stage::Magic => "the magic header",
            Stage::Count | Stage::CountExt => "the attribute count",
            Stage::Len(Part::Key) | Stage::LenExt(Part::Key) => "an attribute key length",
            Stage::Len(Part::Value) | Stage::LenExt(Part::Value) => "an attribute value length",
            Stage::Bytes(Part::Key) => "an attribute key",
            Stage::Bytes(Part::Value) => "an attribute value",
            Stage::Size => "the content size",
            Stage::Done => "nothing",
        }
    }

    /// The buffer to fill next. It is empty once the header is complete.
    ///
    /// Fill all of it, then call [`advance`](Self::advance). Calling this
    /// twice without an `advance` in between hands out a second buffer for the
    /// same piece, so do not.
    pub(crate) fn next_read(&mut self) -> &mut [u8] {
        match self.stage {
            Stage::Magic => &mut self.scratch[..MAGIC.len()],
            Stage::Count | Stage::Len(_) => &mut self.scratch[..2],
            Stage::CountExt | Stage::LenExt(_) => &mut self.scratch[..4],
            Stage::Bytes(_) => {
                let have = self.field.len();
                let step = reserve_step(have, self.field_len - have);
                self.field.reserve_exact(step);
                self.field.resize(have + step, 0);
                &mut self.field[have..]
            }
            Stage::Size => &mut self.scratch[..8],
            Stage::Done => &mut self.scratch[..0],
        }
    }

    /// Decode the buffer the last [`next_read`](Self::next_read) handed out.
    /// Returns the header once the last piece of it has been decoded.
    pub(crate) fn advance(&mut self) -> Result<Option<Header>> {
        self.started = true;
        match self.stage {
            Stage::Magic => {
                let mut magic = [0u8; 7];
                magic.copy_from_slice(&self.scratch[..7]);
                if magic != MAGIC {
                    return Err(Error::InvalidMagic(magic));
                }
                self.stage = Stage::Count;
            }
            Stage::Count => match self.short() {
                EXTENDED_LEN => self.stage = Stage::CountExt,
                count => self.start_attributes(count as usize),
            },
            Stage::CountExt => self.start_attributes(self.long()),
            Stage::Len(part) => match self.short() {
                EXTENDED_LEN => self.stage = Stage::LenExt(part),
                len => self.start_field(part, len as usize)?,
            },
            Stage::LenExt(part) => self.start_field(part, self.long())?,
            Stage::Bytes(part) => {
                if self.field.len() == self.field_len {
                    self.finish_field(part)?;
                }
            }
            Stage::Size => {
                let size = u64::from_be_bytes(self.scratch);
                self.stage = Stage::Done;
                return Ok(Some(Header {
                    attributes: std::mem::take(&mut self.attributes),
                    size,
                }));
            }
            Stage::Done => {}
        }
        Ok(None)
    }

    fn short(&self) -> u16 {
        u16::from_be_bytes([self.scratch[0], self.scratch[1]])
    }

    fn long(&self) -> usize {
        // `u32` always fits a `usize` on the targets this crate supports.
        u32::from_be_bytes([
            self.scratch[0],
            self.scratch[1],
            self.scratch[2],
            self.scratch[3],
        ]) as usize
    }

    fn start_attributes(&mut self, count: usize) {
        // Sized from the claim only up to a point. Past it the map grows as
        // attributes actually arrive.
        self.attributes = HashMap::with_capacity(count.min(1024));
        self.remaining = count;
        self.stage = if count == 0 {
            Stage::Size
        } else {
            Stage::Len(Part::Key)
        };
    }

    fn start_field(&mut self, part: Part, len: usize) -> Result<()> {
        self.field = Vec::new();
        self.field_len = len;
        if len == 0 {
            self.finish_field(part)
        } else {
            self.stage = Stage::Bytes(part);
            Ok(())
        }
    }

    fn finish_field(&mut self, part: Part) -> Result<()> {
        let text =
            String::from_utf8(std::mem::take(&mut self.field)).map_err(Error::InvalidAttribute)?;
        match part {
            Part::Key => {
                self.key = Some(text);
                self.stage = Stage::Len(Part::Value);
            }
            Part::Value => {
                let key = self.key.take().expect("a value follows its key");
                self.attributes.insert(key, text);
                self.remaining -= 1;
                self.stage = if self.remaining == 0 {
                    Stage::Size
                } else {
                    Stage::Len(Part::Key)
                };
            }
        }
        Ok(())
    }
}

/// Decode a header from the front of `bytes`, returning it and how many bytes
/// it occupied.
pub(crate) fn decode_header(bytes: &[u8]) -> Result<(Header, usize)> {
    let mut decoder = HeaderDecoder::new();
    let mut rest = bytes;
    loop {
        let chunk = decoder.next_read();
        if rest.len() < chunk.len() {
            return Err(Error::TruncatedHeader {
                needed: chunk.len() - rest.len(),
                expecting: decoder.expecting(),
            });
        }
        let (now, later) = rest.split_at(chunk.len());
        chunk.copy_from_slice(now);
        rest = later;
        if let Some(header) = decoder.advance()? {
            return Ok((header, bytes.len() - rest.len()));
        }
    }
}

pub(crate) fn write_field_len(buf: &mut Vec<u8>, len: usize) {
    if let Ok(short) = u16::try_from(len)
        && short < EXTENDED_LEN
    {
        buf.extend_from_slice(&short.to_be_bytes());
    } else {
        let len = u32::try_from(len).expect("field length exceeds u32::MAX");
        buf.extend_from_slice(&EXTENDED_LEN.to_be_bytes());
        buf.extend_from_slice(&len.to_be_bytes());
    }
}

fn write_string(buf: &mut Vec<u8>, value: &str) {
    write_field_len(buf, value.len());
    buf.extend_from_slice(value.as_bytes());
}

/// How many bytes [`write_field_len`] writes for `len`: two, or six in the
/// extended form.
const fn field_len_bytes(len: usize) -> usize {
    if len < EXTENDED_LEN as usize { 2 } else { 6 }
}

/// The exact number of bytes [`encode_header`] will produce.
///
/// One pass of arithmetic over the attributes, which pays for itself: a header
/// buffer and a whole serialized flow file can both be allocated once at the
/// right size. It is also what
/// [`FlowFile::serialized_len`](crate::FlowFile::serialized_len) reports.
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
pub(crate) fn encode_header(attributes: &HashMap<String, String>, size: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(header_len(attributes));
    encode_header_into(&mut buf, attributes, size);
    buf
}

/// Serialize the header into a buffer the caller has already sized.
///
/// Attributes are written in sorted key order, so the output is
/// deterministic. NiFi itself accepts any order.
pub(crate) fn encode_header_into(
    buf: &mut Vec<u8>,
    attributes: &HashMap<String, String>,
    size: u64,
) {
    buf.extend_from_slice(&MAGIC);
    write_field_len(buf, attributes.len());
    // Sorted as borrowed pairs rather than as keys looked up again: one small
    // allocation instead of a hash and a probe per attribute. Keys are
    // distinct, so the unstable sort has no equal elements to reorder.
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
    /// attribute count as well, which is a different branch of the same
    /// arithmetic.
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

    /// Whatever the encoder writes, the decoder reads back, including the
    /// extended lengths and an empty value.
    #[test]
    fn the_decoder_undoes_the_encoder() {
        let attributes: HashMap<String, String> = [
            ("k".to_string(), "v".to_string()),
            ("empty".to_string(), String::new()),
            ("long".to_string(), "x".repeat(0x1_0000)),
            ("é→🙂".to_string(), "🙂".to_string()),
        ]
        .into_iter()
        .collect();
        let mut bytes = encode_header(&attributes, 1234);
        bytes.extend_from_slice(b"content follows");

        let (header, consumed) = decode_header(&bytes).unwrap();
        assert_eq!(header.attributes, attributes);
        assert_eq!(header.size, 1234);
        assert_eq!(&bytes[consumed..], b"content follows");
    }

    /// The decoder is incremental, so feeding it one byte at a time through a
    /// reader must give the same answer as feeding it the slice.
    #[test]
    fn the_decoder_is_the_same_byte_by_byte() {
        let attributes: HashMap<String, String> = (0..40)
            .map(|i| (format!("key-{i}"), "value".repeat(i)))
            .collect();
        let bytes = encode_header(&attributes, 7);

        let mut decoder = HeaderDecoder::new();
        let mut source = bytes.iter().copied();
        let header = loop {
            let chunk = decoder.next_read();
            for slot in chunk.iter_mut() {
                *slot = source.next().expect("the header is complete");
            }
            if let Some(header) = decoder.advance().unwrap() {
                break header;
            }
        };
        assert_eq!(header.attributes, attributes);
        assert_eq!(header.size, 7);
        assert!(source.next().is_none(), "nothing read past the header");
    }

    /// Each place the input can end is named, so the error says what was
    /// missing rather than just that something was.
    #[test]
    fn a_truncated_header_says_what_was_expected() {
        let attributes = HashMap::from([("key".to_string(), "value".to_string())]);
        let bytes = encode_header(&attributes, 5);

        let expectations = [
            (3, "the magic header"),
            (8, "the attribute count"),
            (10, "an attribute key length"),
            (12, "an attribute key"),
            (15, "an attribute value length"),
            (18, "an attribute value"),
            (25, "the content size"),
        ];
        for (cut, expecting) in expectations {
            match decode_header(&bytes[..cut]) {
                Err(Error::TruncatedHeader { expecting: got, .. }) => {
                    assert_eq!(got, expecting, "cut at {cut}");
                }
                other => panic!("cut at {cut}: {other:?}"),
            }
        }
        assert!(decode_header(&bytes).is_ok());
    }

    /// A length the input never backs with bytes must not be allocated for.
    #[test]
    fn a_huge_declared_length_costs_no_allocation() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&MAGIC);
        bytes.extend_from_slice(&[0x00, 0x01]); // one attribute
        bytes.extend_from_slice(&[0xFF, 0xFF]); // extended key length
        bytes.extend_from_slice(&u32::MAX.to_be_bytes());
        bytes.extend_from_slice(b"tiny");

        let mut decoder = HeaderDecoder::new();
        let mut rest = bytes.as_slice();
        let mut largest = 0;
        let err = loop {
            let chunk = decoder.next_read();
            largest = largest.max(chunk.len());
            if rest.len() < chunk.len() {
                break Error::TruncatedHeader {
                    needed: chunk.len() - rest.len(),
                    expecting: decoder.expecting(),
                };
            }
            let (now, later) = rest.split_at(chunk.len());
            chunk.copy_from_slice(now);
            rest = later;
            if let Err(err) = decoder.advance() {
                break err;
            }
        };
        assert!(matches!(err, Error::TruncatedHeader { .. }), "{err:?}");
        assert!(
            largest <= RESERVE_AHEAD,
            "asked for {largest} bytes at once"
        );
    }

    #[test]
    fn bad_magic_is_rejected_before_anything_else() {
        assert!(matches!(
            decode_header(b"NOT-FF3 and then a lot more bytes"),
            Err(Error::InvalidMagic(_))
        ));
    }

    #[test]
    fn invalid_utf8_in_an_attribute_is_rejected() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&MAGIC);
        bytes.extend_from_slice(&[0x00, 0x01, 0x00, 0x02, 0xFF, 0xFE]);
        bytes.extend_from_slice(&[0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert!(matches!(
            decode_header(&bytes),
            Err(Error::InvalidAttribute(_))
        ));
    }
}
