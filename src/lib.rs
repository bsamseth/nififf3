//! Utilities for working with NiFi's FlowFile V3 file format.
//!
//! A flow file consists of a set of string attributes and a binary content
//! payload. This crate parses and serializes the V3 binary format
//! (`application/flowfile-v3`), compatible with NiFi's `FlowFilePackagerV3`
//! and `FlowFileUnpackagerV3`.
//!
//! # Example
//!
//! ```
//! use nififf3::FlowFile;
//!
//! let flow_file = FlowFile::builder()
//!     .attribute("filename", "greeting.txt")
//!     .content(&b"Hello, NiFi!"[..]);
//! let bytes = flow_file.to_bytes();
//!
//! let parsed = FlowFile::from_bytes(&bytes).unwrap();
//! assert_eq!(parsed.attributes()["filename"], "greeting.txt");
//! assert_eq!(parsed.content().as_slice(), b"Hello, NiFi!");
//! ```
//!
//! Parsing from a reader is lazy: [`FlowFile::parse`] only consumes the
//! header, and the content is exposed as a reader limited to the declared
//! content size. The async equivalents ([`FlowFile::parse_async`],
//! [`FlowFile::write_to_async`]) are available behind the `tokio` feature,
//! and axum request/response integration behind the `axum` feature.

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
    pub fn into_reader(self) -> FlowFile<std::io::Cursor<Vec<u8>>> {
        self.map_content(std::io::Cursor::new)
    }
}
