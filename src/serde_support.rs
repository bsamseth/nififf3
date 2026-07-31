//! `Serialize`/`Deserialize` impls for in-memory flow files.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::io::Read;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{Error, FlowFile};

/// Serializes as a struct with the fields `size`, `attributes` (in sorted
/// key order), and `content` (base64 encoded), e.g. as JSON:
///
/// ```json
/// {"size":5,"attributes":{"filename":"greeting.txt"},"content":"aGVsbG8="}
/// ```
///
/// # Panics
///
/// If `size` disagrees with the content's actual length, as
/// [`FlowFile::to_bytes`](crate::FlowFile::to_bytes) does — only reachable by
/// breaking [`map_content`](crate::FlowFile::map_content)'s contract. The check
/// is here because `Deserialize` rejects that mismatch, so without it this
/// would emit JSON its own reader refuses.
impl Serialize for FlowFile<Vec<u8>> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Repr<'a> {
            size: u64,
            attributes: BTreeMap<&'a str, &'a str>,
            content: String,
        }

        assert_eq!(
            self.size,
            self.content.len() as u64,
            "declared size does not match the content; see FlowFile::with_size"
        );
        Repr {
            size: self.size,
            attributes: self
                .attributes
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect(),
            content: BASE64.encode(&self.content),
        }
        .serialize(serializer)
    }
}

/// Deserializes the structure produced by the `Serialize` impl. The `size`
/// field may be omitted; when present it must match the decoded content
/// length. Missing `attributes` default to an empty map, and missing `content`
/// to no content — so `{}` is a valid empty flow file, and every value this
/// impl can produce round-trips.
impl<'de> Deserialize<'de> for FlowFile<Vec<u8>> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Repr {
            #[serde(default)]
            size: Option<u64>,
            #[serde(default)]
            attributes: HashMap<String, String>,
            #[serde(default)]
            content: String,
        }

        let repr = Repr::deserialize(deserializer)?;
        let content = BASE64
            .decode(repr.content.as_bytes())
            .map_err(serde::de::Error::custom)?;
        if let Some(size) = repr.size
            && size != content.len() as u64
        {
            return Err(serde::de::Error::custom(format!(
                "size field ({size}) does not match decoded content length ({})",
                content.len()
            )));
        }
        Ok(FlowFile::from_raw_parts(
            content.len() as u64,
            repr.attributes,
            content,
        ))
    }
}

/// A reader-backed flow file, made serializable.
///
/// [`Serialize`] is implemented for `FlowFile<Vec<u8>>` only, because
/// serializing takes `&self` and reading a reader takes `&mut`. This wrapper
/// bridges that, and streams the content through the base64 encoder as it goes
/// — so what is held in memory is the encoded string, not the content *and*
/// its encoding.
///
/// ```
/// use nififf3::{FlowFile, StreamingFlowFile};
///
/// let bytes = FlowFile::builder()
///     .attribute("filename", "greeting.txt")
///     .content(&b"hello"[..])
///     .to_bytes();
///
/// // The content is never buffered as bytes, only as base64.
/// let flow_file = FlowFile::parse(bytes.as_slice())?;
/// let json = serde_json::to_string(&StreamingFlowFile::new(flow_file))?;
///
/// assert_eq!(
///     json,
///     r#"{"size":5,"attributes":{"filename":"greeting.txt"},"content":"aGVsbG8="}"#
/// );
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// Serializing consumes the content, so it works once. A second attempt reads
/// nothing and fails rather than emitting a flow file with the wrong content.
#[derive(Debug)]
pub struct StreamingFlowFile<R>(RefCell<FlowFile<R>>);

impl<R> StreamingFlowFile<R> {
    /// Wrap a flow file for serialization.
    #[must_use]
    pub fn new(flow_file: FlowFile<R>) -> Self {
        Self(RefCell::new(flow_file))
    }

    /// Unwrap, giving back the flow file and whatever is left of its content.
    #[must_use]
    pub fn into_inner(self) -> FlowFile<R> {
        self.0.into_inner()
    }
}

impl<R: Read> Serialize for StreamingFlowFile<R> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Repr<'a> {
            size: u64,
            attributes: BTreeMap<&'a str, &'a str>,
            content: String,
        }

        let mut flow_file = self.0.borrow_mut();
        let size = flow_file.size;

        let mut encoder = base64::write::EncoderStringWriter::new(&BASE64);
        let read = std::io::copy(&mut (&mut flow_file.content).take(size), &mut encoder)
            .map_err(serde::ser::Error::custom)?;
        if read != size {
            return Err(serde::ser::Error::custom(Error::SizeMismatch {
                expected: size,
                actual: read,
            }));
        }

        Repr {
            size,
            attributes: flow_file
                .attributes
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect(),
            content: encoder.into_inner(),
        }
        .serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use crate::FlowFile;

    fn sample() -> FlowFile<Vec<u8>> {
        FlowFile::builder()
            .attribute("filename", "greeting.txt")
            .content(&b"hello"[..])
    }

    #[test]
    fn serializes_to_the_cli_json_shape() {
        let json = serde_json::to_string(&sample()).unwrap();
        assert_eq!(
            json,
            r#"{"size":5,"attributes":{"filename":"greeting.txt"},"content":"aGVsbG8="}"#
        );
    }

    #[test]
    fn deserializes_with_and_without_size() {
        for json in [
            r#"{"size":5,"attributes":{"filename":"greeting.txt"},"content":"aGVsbG8="}"#,
            r#"{"attributes":{"filename":"greeting.txt"},"content":"aGVsbG8="}"#,
        ] {
            let flow_file: FlowFile<Vec<u8>> = serde_json::from_str(json).unwrap();
            assert_eq!(flow_file.size(), 5);
            assert_eq!(flow_file.attributes()["filename"], "greeting.txt");
            assert_eq!(flow_file.content().as_slice(), b"hello");
        }
    }

    /// An empty flow file is an ordinary one, so it has to survive the trip.
    /// `content` is omitted from the shape only when there is none.
    #[test]
    fn an_empty_flow_file_round_trips() {
        let empty = FlowFile::builder().content(Vec::new());
        let json = serde_json::to_string(&empty).unwrap();
        assert_eq!(
            serde_json::from_str::<FlowFile<Vec<u8>>>(&json).unwrap(),
            empty
        );

        for json in ["{}", r#"{"attributes":{}}"#, r#"{"size":0}"#] {
            let flow_file: FlowFile<Vec<u8>> = serde_json::from_str(json).unwrap();
            assert_eq!(flow_file, empty, "{json}");
        }
    }

    /// The streaming form has to produce byte-for-byte what the in-memory one
    /// does, or the two would describe the same flow file differently.
    #[test]
    fn streaming_matches_the_in_memory_serialization() {
        use crate::StreamingFlowFile;

        let bytes = sample().to_bytes();
        let parsed = FlowFile::parse(bytes.as_slice()).unwrap();

        assert_eq!(
            serde_json::to_string(&StreamingFlowFile::new(parsed)).unwrap(),
            serde_json::to_string(&sample()).unwrap()
        );
    }

    /// Content that ends early would otherwise serialize to a flow file whose
    /// declared size does not match what it carries — which this impl's own
    /// `Deserialize` would then reject.
    #[test]
    fn streaming_a_truncated_content_fails() {
        use crate::StreamingFlowFile;

        let bytes = sample().to_bytes();
        let parsed = FlowFile::parse(&bytes[..bytes.len() - 2]).unwrap();

        let err = serde_json::to_string(&StreamingFlowFile::new(parsed)).unwrap_err();
        assert!(err.to_string().contains("size mismatch"), "{err}");
    }

    /// The reader is consumed, so serializing twice must fail rather than
    /// quietly emit an empty content for the same declared size.
    #[test]
    fn streaming_twice_fails_rather_than_lying() {
        use crate::StreamingFlowFile;

        let bytes = sample().to_bytes();
        let streaming = StreamingFlowFile::new(FlowFile::parse(bytes.as_slice()).unwrap());

        assert!(serde_json::to_string(&streaming).is_ok());
        assert!(serde_json::to_string(&streaming).is_err());
    }

    #[test]
    fn rejects_mismatched_size() {
        let json = r#"{"size":3,"attributes":{},"content":"aGVsbG8="}"#;
        let err = serde_json::from_str::<FlowFile<Vec<u8>>>(json).unwrap_err();
        assert!(err.to_string().contains("does not match"));
    }

    #[test]
    fn rejects_invalid_base64() {
        let json = r#"{"attributes":{},"content":"not base64!!"}"#;
        assert!(serde_json::from_str::<FlowFile<Vec<u8>>>(json).is_err());
    }
}
