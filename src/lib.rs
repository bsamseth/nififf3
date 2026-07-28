#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

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

#[cfg(feature = "axum")]
pub use axum_support::{
    BlockingResponseSink, FlowFileBody, FlowFileRequest, FlowFilesResponse, ResponseSink,
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
/// size-limited reader (via [`FlowFile::parse`] and, with the `tokio`
/// feature, [`FlowFile::parse_async`]).
#[derive(Debug, Clone)]
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
    pub fn size(&self) -> u64 {
        self.size
    }

    /// The attributes of the flow file.
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

    /// Consume the flow file, returning the content container.
    pub fn into_content(self) -> R {
        self.content
    }

    /// Consume the flow file, returning `(size, attributes, content)`.
    pub fn into_parts(self) -> (u64, HashMap<String, String>, R) {
        (self.size, self.attributes, self.content)
    }

    /// Transform the content container, keeping size and attributes.
    ///
    /// The declared [`size`](Self::size) is carried over unchanged, so the
    /// new container should hold (or produce) the same content.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let flow_file = FlowFile::builder().content(&b"hi"[..]);
    /// let flow_file = flow_file.map_content(std::io::Cursor::new);
    /// assert_eq!(flow_file.size(), 2);
    /// ```
    pub fn map_content<T>(self, f: impl FnOnce(R) -> T) -> FlowFile<T> {
        FlowFile {
            size: self.size,
            attributes: self.attributes,
            content: f(self.content),
        }
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
    /// header, [`Error::SizeMismatch`] if fewer content bytes are present than
    /// the header declares, and [`Error::TrailingData`] if more.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut reader = bytes;
        let (attributes, size) = sync::parse_header(&mut reader, None, &Limits::UNLIMITED)?;
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
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let bytes = FlowFile::builder().content(&b"hi"[..]).to_bytes();
    /// assert!(bytes.starts_with(b"NiFiFF3"));
    /// assert!(bytes.ends_with(b"hi"));
    /// ```
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = format::encode_header(&self.attributes, self.content.len() as u64);
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
    /// let mut flow_file = FlowFile::builder().content(&b"hi"[..]).into_reader();
    /// let mut out = Vec::new();
    /// flow_file.write_to(&mut out).unwrap(); // reader-based serialization
    /// ```
    pub fn into_reader(self) -> FlowFile<std::io::Cursor<Vec<u8>>> {
        self.map_content(std::io::Cursor::new)
    }
}
