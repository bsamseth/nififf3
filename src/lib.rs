#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

use std::collections::HashMap;

mod builder;
mod error;
mod format;
mod sync;

#[cfg(feature = "tokio")]
mod async_io;
#[cfg(feature = "axum")]
mod axum_support;

pub use builder::FlowFileBuilder;
pub use error::Error;

#[cfg(feature = "axum")]
pub use axum_support::{FlowFileBody, FlowFileRequest};

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
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut reader = bytes;
        let (attributes, size) = sync::parse_header(&mut reader, None)?;
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
