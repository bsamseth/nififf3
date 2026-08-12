//! Generative checks over the parser.
//!
//! Every other test names a specific input. These two say something about all
//! of them: hostile bytes must be rejected rather than cause a panic, and
//! anything this crate serializes must parse back identically.
//!
//! They are deterministic by construction, because they use a fixed-seed
//! generator rather than a fuzzing dependency. So a failure reproduces on the
//! next run, instead of being a one-off nobody can chase.

use nififf3::{FlowFile, FlowFiles, Limits};

/// A small linear congruential generator. The randomness is poor, but the
/// coverage is adequate and the sequence is the same every run.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 ^ (self.0 >> 33)
    }

    /// A value in `0..n`, or 0 when `n` is 0.
    fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            return 0;
        }
        let value = self.next_u64() % n as u64;
        usize::try_from(value).expect("a value below a usize fits in a usize")
    }

    fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| (self.next_u64() & 0xff) as u8).collect()
    }

    /// A short string, drawn from a set that includes multi-byte characters so
    /// the byte lengths the format writes are not the character counts.
    fn text(&mut self, len: usize) -> String {
        const ALPHABET: [&str; 8] = ["a", "Z", "0", ".", "_", "é", "→", "🙂"];
        (0..len)
            .map(|_| ALPHABET[self.below(ALPHABET.len())])
            .collect()
    }
}

/// A well-formed flow file, to be damaged.
fn plausible(rng: &mut Rng) -> Vec<u8> {
    let attributes: Vec<(String, String)> = (0..rng.below(4))
        .map(|_| {
            let key_len = rng.below(8) + 1;
            let key = rng.text(key_len);
            let value_len = rng.below(12);
            (key, rng.text(value_len))
        })
        .collect();
    let content_len = rng.below(40);
    FlowFile::builder()
        .attributes(attributes)
        .content(rng.bytes(content_len))
        .to_bytes()
}

/// The parser's contract for untrusted input is that it may reject anything,
/// but it may not panic, hang, or allocate beyond what the input provides.
///
/// Pure noise mostly dies on the magic or on an absurd attribute count, which
/// says little. So two thirds of these are damaged flow files instead: a
/// well-formed one with bytes flipped, or one cut short at an arbitrary
/// offset. That is what carries the generator past the header and into the
/// field lengths, the UTF-8 decoding, and the declared content size.
#[test]
fn arbitrary_input_is_rejected_rather_than_fatal() {
    let mut rng = Rng(0x5eed_1234);

    for i in 0..4000 {
        let bytes = match i % 3 {
            // Noise, half of it wearing a valid magic header.
            0 => {
                let len = rng.below(64) + 8;
                let mut bytes = rng.bytes(len);
                if i % 2 == 0 {
                    bytes[..7].copy_from_slice(b"NiFiFF3");
                }
                bytes
            }
            // A valid flow file with a few bytes corrupted.
            1 => {
                let mut bytes = plausible(&mut rng);
                for _ in 0..=rng.below(4) {
                    let at = rng.below(bytes.len());
                    bytes[at] = (rng.next_u64() & 0xff) as u8;
                }
                bytes
            }
            // A valid flow file that stops part-way.
            _ => {
                let mut bytes = plausible(&mut rng);
                let keep = rng.below(bytes.len());
                bytes.truncate(keep);
                bytes
            }
        };

        for limits in [Limits::UNLIMITED, Limits::recommended()] {
            // Whatever these return, returning at all is the assertion.
            let _ = FlowFile::from_bytes_with_limits(&bytes, limits);
            let _ = FlowFile::from_vec_with_limits(bytes.clone(), limits);
            let _ = FlowFile::parse_with_limits(bytes.as_slice(), limits)
                .and_then(|flow_file| flow_file.into_memory().map_err(Into::into));
            // Also has to terminate: the iterator fuses after an error rather
            // than trying to resynchronise on a stream it cannot trust.
            let _ = FlowFiles::with_limits(bytes.as_slice(), limits).count();
        }
    }
}

/// Anything this crate writes, it can read back. That holds for attributes and
/// content alike, whatever they contain.
#[test]
fn what_is_serialized_parses_back_identically() {
    let mut rng = Rng(0xd0d0_5678);

    for _ in 0..300 {
        let attributes: Vec<(String, String)> = (0..rng.below(8))
            .map(|_| {
                let key_len = rng.below(12) + 1;
                let key = rng.text(key_len);
                let value_len = rng.below(20);
                let value = rng.text(value_len);
                (key, value)
            })
            .collect();
        let content_len = rng.below(300);
        let content = rng.bytes(content_len);

        let flow_file = FlowFile::builder()
            .attributes(attributes)
            .content(content.clone());
        let bytes = flow_file.to_bytes();

        assert_eq!(FlowFile::from_bytes(&bytes).unwrap(), flow_file);
        assert_eq!(FlowFile::from_vec(bytes.clone()).unwrap(), flow_file);

        // And back-to-back, the way NiFi sends them.
        let mut stream = bytes.clone();
        stream.extend_from_slice(&bytes);
        let parsed: Vec<_> = FlowFiles::new(stream.as_slice())
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(parsed, [flow_file.clone(), flow_file]);
    }
}

/// The format switches from a two-byte length to a marker plus four bytes at
/// `0xFFFF`, for attribute lengths *and* the attribute count. Both sides of
/// each boundary have to survive the trip.
#[test]
fn the_field_length_boundary_round_trips() {
    for len in [0, 1, 0xFFFE, 0xFFFF, 0x1_0000, 0x1_0001] {
        let value = "v".repeat(len);
        let flow_file = FlowFile::builder().attribute("k", &value).content(Vec::new());
        let parsed = FlowFile::from_bytes(&flow_file.to_bytes()).unwrap();
        assert_eq!(parsed, flow_file, "value of {len} bytes");
    }

    for count in [0xFFFE, 0xFFFF, 0x1_0000] {
        let flow_file = FlowFile::builder()
            .attributes((0..count).map(|i| (format!("k{i}"), "v")))
            .content(Vec::new());
        let parsed = FlowFile::from_bytes(&flow_file.to_bytes()).unwrap();
        assert_eq!(parsed.attributes().len(), count, "{count} attributes");
    }
}
