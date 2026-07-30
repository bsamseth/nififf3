#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
#![forbid(unsafe_code)]
// Label feature-gated items in the rendered docs ("Available on crate feature
// `tokio` only"). `docs.rs` passes `--cfg docsrs`; a plain `cargo doc` on
// stable does not, so neither attribute is seen there.
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, doc(auto_cfg))]

use std::collections::HashMap;

pub mod attr;

mod builder;
mod error;
mod format;
mod fragments;
mod limits;
mod sync;

#[cfg(feature = "tokio")]
mod async_io;
#[cfg(feature = "axum")]
mod axum_support;
#[cfg(feature = "serde")]
mod serde_support;

pub use builder::FlowFileBuilder;
pub use error::Error;
pub use fragments::Fragments;
pub use limits::Limits;
pub use sync::{FlowFiles, FlowFilesWriter};

#[cfg(feature = "tokio")]
pub use async_io::{FlowFilesAsync, FlowFilesWriterAsync};

/// The [`Stream`] trait
/// [`FlowFilesAsync::into_stream`] returns, re-exported so that naming the
/// bound does not mean adding a `futures-core` dependency of your own — and
/// one whose version has to match this crate's.
#[cfg(feature = "stream")]
pub use futures_core::Stream;

#[cfg(feature = "axum")]
pub use axum_support::{
    BlockingResponseSink, BoxError, FlowFileBody, FlowFileRequest, FlowFilesResponse, ResponseSink,
    StrictFlowFileRequest, StrictRejection,
};

/// Convenience alias for results produced by this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// The media type of a flow file V3, as used by NiFi.
pub const MEDIA_TYPE: &str = "application/flowfile-v3";

/// A NiFi flow file: a content payload with associated string attributes.
///
/// The type is generic over the container of the content, `R`. Parsing
/// produces either an in-memory [`FlowFile<Vec<u8>>`] (via
/// [`FlowFile::from_bytes`]) or a lazy variant whose content is a
/// size-limited reader (via [`FlowFile::parse`], and its `tokio` twin
/// `parse_async`).
///
/// # Where to start
///
/// That genericity spreads the entry points over several `impl` blocks below,
/// each headed by a `Self` type rather than by what it is for. This is the
/// index:
///
/// | | in memory | from a reader | many, concatenated |
/// | --- | --- | --- | --- |
/// | **parse** | [`from_bytes`] | [`parse`] | [`FlowFiles`], or [`parse_next`] |
/// | **serialize** | [`to_bytes`] | [`write_to`] | [`FlowFilesWriter`] |
#[cfg_attr(
    feature = "tokio",
    doc = "| **parse, async** | — | [`parse_async`] | [`FlowFilesAsync`], or [`parse_next_async`] |"
)]
#[cfg_attr(
    feature = "tokio",
    doc = "| **serialize, async** | — | [`write_to_async`] | [`FlowFilesWriterAsync`] |"
)]
///
/// [`builder`](Self::builder) creates one from scratch, [`derive`](Self::derive)
/// from another flow file's attributes, and [`fragments`](Self::fragments)
/// splits one into many. The `*_with_limits` variants of every parsing entry
/// point take [`Limits`], and are what to use on untrusted input.
///
/// [`from_bytes`]: Self::from_bytes
/// [`parse`]: Self::parse
/// [`parse_next`]: Self::parse_next
/// [`to_bytes`]: Self::to_bytes
/// [`write_to`]: Self::write_to
#[cfg_attr(feature = "tokio", doc = "[`parse_async`]: Self::parse_async")]
#[cfg_attr(
    feature = "tokio",
    doc = "[`parse_next_async`]: Self::parse_next_async"
)]
#[cfg_attr(feature = "tokio", doc = "[`write_to_async`]: Self::write_to_async")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowFile<R> {
    pub(crate) size: u64,
    pub(crate) attributes: HashMap<String, String>,
    pub(crate) content: R,
}

impl FlowFile<()> {
    /// Start building a flow file. See [`FlowFileBuilder`].
    #[must_use]
    pub fn builder() -> FlowFileBuilder {
        FlowFileBuilder::new()
    }
}

impl<R> FlowFile<R> {
    pub(crate) fn from_raw_parts(
        size: u64,
        attributes: HashMap<String, String>,
        content: R,
    ) -> Self {
        Self {
            size,
            attributes,
            content,
        }
    }

    /// The length of the content in bytes, as declared in the header.
    ///
    /// This is the single source of truth for how many content bytes every
    /// serializer writes and every reader-based operation consumes. Every
    /// constructor in this crate keeps it in step with the content; the one
    /// way to break that is [`map_content`](Self::map_content) with a
    /// function that changes the length, which is what
    /// [`with_size`](Self::with_size) is for.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// The value of one attribute, if it is set.
    ///
    /// The [`attr`] module names the ones NiFi gives a meaning to.
    ///
    /// ```
    /// use nififf3::{FlowFile, attr};
    ///
    /// let flow_file = FlowFile::builder()
    ///     .attribute(attr::FILENAME, "greeting.txt")
    ///     .content(&b"hello"[..]);
    ///
    /// assert_eq!(flow_file.attribute(attr::FILENAME), Some("greeting.txt"));
    /// assert_eq!(flow_file.attribute(attr::MIME_TYPE), None);
    /// ```
    pub fn attribute(&self, key: &str) -> Option<&str> {
        self.attributes.get(key).map(String::as_str)
    }

    /// The attributes of the flow file.
    ///
    /// For a single value, [`attribute`](Self::attribute) says what a missing
    /// one means instead of panicking; the [`attr`] module names the
    /// well-known keys.
    pub fn attributes(&self) -> &HashMap<String, String> {
        &self.attributes
    }

    /// Mutable access to the attributes of the flow file.
    pub fn attributes_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.attributes
    }

    /// The content container.
    pub fn content(&self) -> &R {
        &self.content
    }

    /// Mutable access to the content container.
    ///
    /// The way to read a reader-backed flow file's content incrementally
    /// while keeping the flow file — and so its attributes — around;
    /// [`into_content`](Self::into_content) gives up the latter.
    ///
    /// ```
    /// use nififf3::FlowFile;
    /// use std::io::Read;
    ///
    /// let bytes = FlowFile::builder()
    ///     .attribute("filename", "greeting.txt")
    ///     .content(&b"hello"[..])
    ///     .to_bytes();
    ///
    /// let mut flow_file = FlowFile::parse(bytes.as_slice()).unwrap();
    /// let mut head = [0u8; 2];
    /// flow_file.content_mut().read_exact(&mut head).unwrap();
    ///
    /// assert_eq!(&head, b"he");
    /// assert_eq!(flow_file.attributes()["filename"], "greeting.txt");
    /// ```
    pub fn content_mut(&mut self) -> &mut R {
        &mut self.content
    }

    /// Consume the flow file, returning the content container.
    pub fn into_content(self) -> R {
        self.content
    }

    /// Consume the flow file, returning `(size, attributes, content)`.
    pub fn into_parts(self) -> (u64, HashMap<String, String>, R) {
        (self.size, self.attributes, self.content)
    }

    /// Build a flow file from the parts [`into_parts`](Self::into_parts)
    /// yields, for putting one back together after taking it apart.
    ///
    /// Like [`FlowFileBuilder::reader`], this takes `size` on trust: it must be
    /// the number of bytes `content` will yield, since it is what every
    /// serializer declares and every reader-based operation consumes. Prefer
    /// [`FlowFile::builder`] when building one from scratch — the builder's
    /// finishers derive the size rather than asking for it.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let flow_file = FlowFile::builder()
    ///     .attribute("filename", "greeting.txt")
    ///     .content(&b"hello"[..]);
    ///
    /// let (size, mut attributes, content) = flow_file.into_parts();
    /// attributes.insert("seen".to_string(), "true".to_string());
    /// let flow_file = FlowFile::from_parts(size, attributes, content);
    ///
    /// assert_eq!(flow_file.attribute("seen"), Some("true"));
    /// assert_eq!(flow_file.size(), 5);
    /// ```
    #[must_use]
    pub fn from_parts(size: u64, attributes: HashMap<String, String>, content: R) -> Self {
        Self::from_raw_parts(size, attributes, content)
    }

    /// Transform the content container, keeping size and attributes.
    ///
    /// The declared [`size`](Self::size) is carried over unchanged, so `f`
    /// must produce a container holding the same *number of bytes* — wrapping
    /// one reader in another, as `Cursor::new` or `BufReader::new` do.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let flow_file = FlowFile::builder().content(&b"hi"[..]);
    /// let flow_file = flow_file.map_content(std::io::Cursor::new);
    /// assert_eq!(flow_file.size(), 2);
    /// ```
    ///
    /// For a transform that changes the length — a decoder, a decompressor —
    /// chain [`with_size`](Self::with_size), since the format needs the new
    /// size before it can write any of the new content:
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let flow_file = FlowFile::builder().content(&b"hi"[..]);
    /// let flow_file = flow_file
    ///     .map_content(|content| content.repeat(3))
    ///     .with_size(6);
    /// assert_eq!(flow_file.size(), 6);
    /// ```
    #[must_use]
    pub fn map_content<T>(self, f: impl FnOnce(R) -> T) -> FlowFile<T> {
        FlowFile {
            size: self.size,
            attributes: self.attributes,
            content: f(self.content),
        }
    }

    /// Declare a different content [`size`](Self::size), keeping attributes
    /// and content.
    ///
    /// Needed after a [`map_content`](Self::map_content) that changed the
    /// content's length, and only then: everything else in this crate keeps
    /// the size correct on its own. Declaring a size the content does not
    /// match is how a flow file is corrupted, so `size` must be the exact
    /// number of bytes the new container yields.
    #[must_use]
    pub fn with_size(mut self, size: u64) -> Self {
        self.size = size;
        self
    }

    /// Start building a new flow file carrying this one's attributes.
    ///
    /// The [`uuid`](attr::UUID) attribute is replaced with a freshly generated
    /// one, since in NiFi it identifies a single flow file — use
    /// [`derive_keep_uuid`](Self::derive_keep_uuid) to copy it verbatim. Every
    /// other attribute is inherited as-is; set one on the returned builder to
    /// override it, or
    /// [`without_attribute`](FlowFileBuilder::without_attribute) to drop it.
    ///
    /// Only the attributes are borrowed, so the parent stays available to have
    /// its content read.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let parent = FlowFile::builder()
    ///     .attribute("filename", "report.csv")
    ///     .attribute("source", "upload")
    ///     .content(&b"a,b\n1,2\n"[..]);
    ///
    /// let child = parent.derive()
    ///     .attribute("filename", "report.header.csv")
    ///     .content(&b"a,b\n"[..]);
    ///
    /// assert_eq!(child.attributes()["source"], "upload");
    /// assert_eq!(child.attributes()["filename"], "report.header.csv");
    /// assert!(child.attributes().contains_key("uuid")); // freshly generated
    /// ```
    ///
    /// To produce many flow files from one, use [`fragments`](Self::fragments),
    /// which adds NiFi's fragment attributes on top of this.
    pub fn derive(&self) -> FlowFileBuilder {
        self.derive_keep_uuid()
            .attribute(attr::UUID, uuid::Uuid::new_v4().to_string())
    }

    /// Like [`derive`](Self::derive), but copying the [`uuid`](attr::UUID)
    /// attribute unchanged rather than generating a new one.
    ///
    /// Appropriate when the result represents the *same* flow file rather
    /// than a new one — a re-encoded or re-compressed payload, say.
    pub fn derive_keep_uuid(&self) -> FlowFileBuilder {
        FlowFileBuilder::new().attributes(self.attributes.clone())
    }

    /// Split this flow file into many, numbering the results with NiFi's
    /// fragment attributes. See [`Fragments`].
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let parent = FlowFile::builder()
    ///     .attribute("filename", "pair.txt")
    ///     .content(&b"first\nsecond"[..]);
    ///
    /// let mut parts = parent.fragments().with_count(2);
    /// let children: Vec<_> = parent
    ///     .content()
    ///     .split(|b| *b == b'\n')
    ///     .map(|line| parts.next().content(line))
    ///     .collect();
    ///
    /// assert_eq!(children[0].attributes()["fragment.index"], "1");
    /// assert_eq!(children[1].attributes()["fragment.index"], "2");
    /// ```
    pub fn fragments(&self) -> Fragments {
        Fragments::new(&self.attributes)
    }

    /// The serialized header (everything up to the content) for this flow file.
    ///
    /// # Panics
    ///
    /// If an attribute key or value is longer than `u32::MAX` bytes. A field
    /// length in this format is at most 4 bytes, so such an attribute cannot
    /// be written at all — the builder accepts any `String`, and this is
    /// where the format's ceiling is enforced.
    pub(crate) fn header_bytes(&self) -> Vec<u8> {
        format::encode_header(&self.attributes, self.size)
    }
}

impl FlowFile<Vec<u8>> {
    /// Parse a flow file from a byte slice holding exactly one flow file.
    ///
    /// The declared content size is validated against the actual number of
    /// bytes present: too few bytes is a [`Error::SizeMismatch`], extra bytes
    /// after the content is a [`Error::TrailingData`]. To read several
    /// concatenated flow files from one buffer, use [`FlowFile::parse_next`]
    /// instead.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let bytes = FlowFile::builder()
    ///     .attribute("filename", "greeting.txt")
    ///     .content(&b"hello"[..])
    ///     .to_bytes();
    ///
    /// let flow_file = FlowFile::from_bytes(&bytes).unwrap();
    /// assert_eq!(flow_file.attributes()["filename"], "greeting.txt");
    /// assert_eq!(flow_file.content().as_slice(), b"hello");
    /// ```
    ///
    /// # Errors
    ///
    /// [`Error::InvalidMagic`] or [`Error::InvalidAttribute`] for a malformed
    /// header, [`Error::Io`] for one that ends part-way through,
    /// [`Error::SizeMismatch`] if fewer content bytes are present than the
    /// header declares, and [`Error::TrailingData`] if more.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Self::from_bytes_with_limits(bytes, Limits::UNLIMITED)
    }

    /// Like [`from_bytes`](Self::from_bytes), but enforcing [`Limits`] on the
    /// header. Use this for untrusted input.
    ///
    /// The buffer already bounds how much there is to read, so this matters
    /// less here than for the streaming parsers — but a 1 KiB buffer can
    /// still declare tens of thousands of attributes, and
    /// [`max_content_len`](Limits::max_content_len) rejects an oversized
    /// declared size without walking the header first.
    ///
    /// # Errors
    ///
    /// As [`from_bytes`](Self::from_bytes), plus [`Error::TooManyAttributes`],
    /// [`Error::AttributeTooLong`] or [`Error::ContentTooLarge`] when the
    /// header exceeds `limits`.
    pub fn from_bytes_with_limits(bytes: &[u8], limits: Limits) -> Result<Self> {
        let mut reader = bytes;
        let (attributes, size) = sync::parse_header(&mut reader, None, limits)?;
        let actual = reader.len() as u64;
        if actual < size {
            return Err(Error::SizeMismatch {
                expected: size,
                actual,
            });
        }
        if actual > size {
            return Err(Error::TrailingData(actual - size));
        }
        Ok(Self::from_raw_parts(size, attributes, reader.to_vec()))
    }

    /// Serialize the flow file to the binary V3 format.
    ///
    /// Declares [`size`](Self::size) bytes of content, the same value
    /// [`write_to`](Self::write_to) would stream.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let bytes = FlowFile::builder().content(&b"hi"[..]).to_bytes();
    /// assert!(bytes.starts_with(b"NiFiFF3"));
    /// assert!(bytes.ends_with(b"hi"));
    /// ```
    ///
    /// # Panics
    ///
    /// If an attribute key or value exceeds `u32::MAX` bytes: a field length
    /// in this format is at most 4 bytes, so such an attribute cannot be
    /// written at all.
    ///
    /// Also if `size` disagrees with the content's actual length — only
    /// reachable by breaking [`map_content`](Self::map_content)'s contract,
    /// and checked in every build rather than only in debug, because the
    /// alternative is emitting a flow file that only the reader at the far end
    /// finds to be corrupt.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        assert_eq!(
            self.size,
            self.content.len() as u64,
            "declared size does not match the content; see FlowFile::with_size"
        );
        let mut buf = format::encode_header(&self.attributes, self.size);
        buf.extend_from_slice(&self.content);
        buf
    }

    /// Wrap the content in a [`std::io::Cursor`], which implements both
    /// `std::io::Read` and (with the `tokio` feature) `tokio::io::AsyncRead`.
    ///
    /// Useful for handing an in-memory flow file to reader-based APIs such
    /// as the axum response integration.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let flow_file = FlowFile::builder().content(&b"hi"[..]).into_reader();
    /// let mut out = Vec::new();
    /// flow_file.write_to(&mut out).unwrap(); // reader-based serialization
    /// ```
    pub fn into_reader(self) -> FlowFile<std::io::Cursor<Vec<u8>>> {
        self.map_content(std::io::Cursor::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A length-changing `map_content` needs `with_size`; once it has one,
    /// every serializer agrees on how much content there is.
    #[test]
    fn with_size_keeps_the_serializers_in_step() {
        let flow_file = FlowFile::builder()
            .attribute("k", "v")
            .content(&b"hi"[..])
            .map_content(|content| content.repeat(3))
            .with_size(6);

        assert_eq!(flow_file.size(), 6);

        let buffered = flow_file.to_bytes();
        let mut streamed = Vec::new();
        flow_file
            .clone()
            .into_reader()
            .write_to(&mut streamed)
            .unwrap();
        assert_eq!(buffered, streamed);

        let parsed = FlowFile::from_bytes(&buffered).unwrap();
        assert_eq!(parsed.size(), 6);
        assert_eq!(parsed.content().as_slice(), b"hihihi");
    }

    #[test]
    fn to_bytes_declares_the_size_write_to_would_stream() {
        let flow_file = FlowFile::builder().content(&b"hello"[..]);
        let mut streamed = Vec::new();
        flow_file
            .clone()
            .into_reader()
            .write_to(&mut streamed)
            .unwrap();
        assert_eq!(flow_file.to_bytes(), streamed);
    }

    /// Checked in every build, not just debug: a flow file whose declared
    /// size disagrees with its content serializes to bytes that only the
    /// reader at the far end discovers to be corrupt.
    #[test]
    #[should_panic(expected = "declared size does not match the content")]
    fn to_bytes_refuses_a_size_that_disagrees_with_the_content() {
        let _ = FlowFile::builder()
            .content(&b"hi"[..])
            .with_size(99)
            .to_bytes();
    }

    #[test]
    fn parts_round_trip_through_from_parts() {
        let flow_file = FlowFile::builder()
            .attribute("filename", "greeting.txt")
            .content(&b"hello"[..]);

        let (size, attributes, content) = flow_file.clone().into_parts();
        assert_eq!(FlowFile::from_parts(size, attributes, content), flow_file);
    }

    #[test]
    fn flow_files_compare_on_every_part() {
        let flow_file = FlowFile::builder().attribute("k", "v").content(&b"hi"[..]);

        assert_eq!(flow_file, flow_file.clone());
        assert_ne!(
            flow_file,
            FlowFile::builder().attribute("k", "w").content(&b"hi"[..])
        );
        assert_ne!(
            flow_file,
            FlowFile::builder().attribute("k", "v").content(&b"no"[..])
        );
        // The declared size is part of the identity, not just the content.
        assert_ne!(flow_file, flow_file.clone().with_size(1));
    }

    #[test]
    fn attribute_reports_a_missing_key_rather_than_panicking() {
        let flow_file = FlowFile::builder()
            .attribute(attr::FILENAME, "greeting.txt")
            .content(Vec::new());

        assert_eq!(flow_file.attribute(attr::FILENAME), Some("greeting.txt"));
        assert_eq!(flow_file.attribute(attr::MIME_TYPE), None);
    }

    /// `map_content` is for containers that hold the same bytes, and carries
    /// the size across untouched.
    #[test]
    fn map_content_preserves_the_size() {
        let flow_file = FlowFile::builder().content(&b"hi"[..]);
        assert_eq!(flow_file.map_content(std::io::Cursor::new).size(), 2);
    }
}
