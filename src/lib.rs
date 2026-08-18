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
#[cfg(all(feature = "tokio", feature = "tempfile"))]
mod spool;

pub use builder::FlowFileBuilder;
pub use error::Error;
pub use fragments::FragmentKeys;

#[cfg(feature = "uuid")]
pub use fragments::Fragments;
pub use limits::Limits;
pub use sync::{FlowFiles, FlowFilesReader, FlowFilesWriter, StreamedContent};

#[cfg(feature = "tokio")]
pub use async_io::{
    FlowFilesAsync, FlowFilesReaderAsync, FlowFilesWriterAsync, StreamedContentAsync,
};

/// The [`Stream`] trait [`FlowFilesAsync::into_stream`] returns.
///
/// It is re-exported so that naming the bound does not mean adding a
/// `futures-core` dependency of your own, whose version would then have to
/// match this crate's.
#[cfg(feature = "stream")]
pub use futures_core::Stream;

#[cfg(feature = "axum")]
pub use axum_support::{
    BlockingResponseSink, BoxError, FlowFileBody, FlowFileRequest, FlowFilesRequest,
    FlowFilesResponse, ResponseSink, StrictFlowFileRequest, StrictFlowFilesRequest,
    StrictRejection,
};

#[cfg(all(feature = "tokio", feature = "tempfile"))]
pub use spool::SpooledContent;

#[cfg(feature = "serde")]
pub use serde_support::StreamingFlowFile;

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
/// Being generic spreads the entry points over several `impl` blocks below,
/// and each block is headed by a `Self` type rather than by what it is for.
/// This table indexes them by what they do:
///
/// | | in memory | from a reader | many, concatenated |
/// | --- | --- | --- | --- |
/// | **parse** | [`from_bytes`], [`from_vec`] | [`from_reader`] buffers, [`parse`] streams | [`FlowFiles`] buffers, [`FlowFilesReader`] streams, [`parse_next`] is the primitive |
/// | **serialize** | [`to_bytes`] | [`write_to`] | [`FlowFilesWriter`] |
#[cfg_attr(
    feature = "tokio",
    doc = "| **parse, async** | | [`from_reader_async`] buffers, [`parse_async`] streams | [`FlowFilesAsync`], [`FlowFilesReaderAsync`], [`parse_next_async`] |"
)]
#[cfg_attr(
    feature = "tokio",
    doc = "| **serialize, async** | | [`write_to_async`] | [`FlowFilesWriterAsync`] |"
)]
///
/// [`builder`](Self::builder) creates a flow file from scratch, `derive`
/// creates one from another flow file's attributes, and `fragments` splits one
/// into many. Every parsing entry point has a `*_with_limits` variant that
/// takes [`Limits`], and those are what to use on untrusted input.
///
/// [`from_bytes`]: Self::from_bytes
/// [`from_vec`]: Self::from_vec
/// [`from_reader`]: Self::from_reader
/// [`parse`]: Self::parse
/// [`parse_next`]: Self::parse_next
/// [`to_bytes`]: Self::to_bytes
/// [`write_to`]: Self::write_to
#[cfg_attr(feature = "tokio", doc = "[`parse_async`]: Self::parse_async")]
#[cfg_attr(
    feature = "tokio",
    doc = "[`from_reader_async`]: Self::from_reader_async"
)]
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
    /// constructor in this crate keeps it in step with the content. The one
    /// way to break that is [`map_content`](Self::map_content) with a function
    /// that changes the length, and [`with_size`](Self::with_size) is how you
    /// put it right again.
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
    /// For a single value, use [`attribute`](Self::attribute). It returns
    /// `None` for a missing key, where indexing this map panics. The [`attr`]
    /// module names the well-known keys.
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
    /// Use this to read a reader-backed flow file's content incrementally
    /// while keeping the flow file itself, and so its attributes.
    /// [`into_content`](Self::into_content) gives the flow file up instead.
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
    /// This takes `size` on trust, as [`FlowFileBuilder::reader`] does. It must
    /// be the number of bytes `content` will yield, because it is what every
    /// serializer declares and every reader-based operation consumes. When you
    /// build a flow file from scratch, prefer [`FlowFile::builder`], whose
    /// finishers derive the size instead of asking you for it.
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
    /// The declared [`size`](Self::size) is carried over unchanged, so `f` must
    /// produce a container holding the same number of bytes. Wrapping one
    /// reader in another does that, as `Cursor::new` and `BufReader::new` do.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let flow_file = FlowFile::builder().content(&b"hi"[..]);
    /// let flow_file = flow_file.map_content(std::io::Cursor::new);
    /// assert_eq!(flow_file.size(), 2);
    /// ```
    ///
    /// For a transform that changes the length, such as a decoder or a
    /// decompressor, chain [`with_size`](Self::with_size). The format needs the
    /// new size before it can write any of the new content:
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

    /// Transform the content container and its declared [`size`](Self::size)
    /// together, for a transform that changes the length.
    ///
    /// `f` returns the new container along with how many bytes it holds, so the
    /// size and the content cannot drift apart. Doing the same thing as
    /// [`map_content`](Self::map_content) followed by
    /// [`with_size`](Self::with_size) leaves you to remember the second call.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let flow_file = FlowFile::builder().content(&b"hi"[..]);
    /// let flow_file = flow_file.map_content_sized(|content| {
    ///     let repeated = content.repeat(3);
    ///     let len = repeated.len() as u64;
    ///     (repeated, len)
    /// });
    ///
    /// assert_eq!(flow_file.size(), 6);
    /// assert_eq!(flow_file.content().as_slice(), b"hihihi");
    /// ```
    #[must_use]
    pub fn map_content_sized<T>(self, f: impl FnOnce(R) -> (T, u64)) -> FlowFile<T> {
        let (content, size) = f(self.content);
        FlowFile {
            size,
            attributes: self.attributes,
            content,
        }
    }

    /// Declare a different content [`size`](Self::size), keeping attributes
    /// and content.
    ///
    /// You need this after a [`map_content`](Self::map_content) that changed
    /// the content's length, and at no other time, because everything else in
    /// this crate keeps the size correct on its own. `size` must be the exact
    /// number of bytes the new container yields. Declaring a size the content
    /// does not match is how a flow file becomes corrupt.
    #[must_use]
    pub fn with_size(mut self, size: u64) -> Self {
        self.size = size;
        self
    }

    /// Start building a new flow file carrying this one's attributes.
    ///
    /// The [`uuid`](attr::UUID) attribute is replaced with a freshly generated
    /// one, because in NiFi it identifies a single flow file. Use
    /// [`derive_keep_uuid`](Self::derive_keep_uuid) to copy it verbatim. Every
    /// other attribute is inherited as it is. Set one on the returned builder
    /// to override it, or call
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
    #[cfg(feature = "uuid")]
    pub fn derive(&self) -> FlowFileBuilder {
        self.derive_keep_uuid()
            .attribute(attr::UUID, uuid::Uuid::new_v4().to_string())
    }

    /// Start building a new flow file carrying this one's attributes,
    /// including its [`uuid`](attr::UUID) unchanged.
    ///
    /// Use it when the result represents the same flow file rather than a new
    /// one, such as a re-encoded or re-compressed payload. `derive` generates
    /// a fresh `uuid` instead.
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
    ///     .map(|line| parts.next_part().content(line))
    ///     .collect();
    ///
    /// assert_eq!(children[0].attributes()["fragment.index"], "1");
    /// assert_eq!(children[1].attributes()["fragment.index"], "2");
    /// ```
    #[cfg(feature = "uuid")]
    pub fn fragments(&self) -> Fragments {
        Fragments::new(&self.attributes)
    }

    /// How many bytes this flow file serializes to: the header plus
    /// [`size`](Self::size).
    ///
    /// It is computed from the attributes and the declared size, without
    /// serializing anything. So it answers the question a `Content-Length`
    /// header, a size-limited sink, or a pre-sized buffer has to answer before
    /// the bytes exist. The answer is exact rather than an estimate: it is the
    /// length [`to_bytes`] produces, and the number of bytes [`write_to`] and
    /// [`write_bytes_to`] write.
    ///
    /// Neither part of the sum depends on the content itself, so this works
    /// for any content container. That includes a reader-backed flow file that
    /// has not been read, and there is no other way to measure one of those
    /// without reading it.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let flow_file = FlowFile::builder()
    ///     .attribute("filename", "greeting.txt")
    ///     .content(&b"hello"[..]);
    ///
    /// assert_eq!(flow_file.serialized_len(), flow_file.to_bytes().len() as u64);
    ///
    /// // And without content to serialize: a reader is measured the same way.
    /// let streamed = FlowFile::builder()
    ///     .attribute("filename", "greeting.txt")
    ///     .reader(&b"hello"[..], 5);
    /// assert_eq!(streamed.serialized_len(), flow_file.serialized_len());
    /// ```
    ///
    /// For a stream of flow files the totals add up, since the format
    /// concatenates them with nothing in between.
    ///
    /// # Panics
    ///
    /// This never panics. Note though that an attribute longer than `u32::MAX`
    /// bytes is counted here and rejected by the serializers. So a length this
    /// reports is not on its own a promise that the flow file can be written.
    ///
    /// [`to_bytes`]: FlowFile::to_bytes
    /// [`write_to`]: FlowFile::write_to
    /// [`write_bytes_to`]: FlowFile::write_bytes_to
    pub fn serialized_len(&self) -> u64 {
        format::header_len(&self.attributes) as u64 + self.size
    }

    /// The serialized header (everything up to the content) for this flow file.
    ///
    /// # Panics
    ///
    /// If an attribute key or value is longer than `u32::MAX` bytes. A field
    /// length in this format is at most 4 bytes, so such an attribute cannot
    /// be written at all. The builder accepts any `String`, and this is where
    /// the format's ceiling is enforced.
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
    /// less here than it does for the streaming parsers. Even so, a 1 KiB
    /// buffer can declare tens of thousands of attributes.
    /// [`max_content_len`](Limits::max_content_len) also rejects an oversized
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

    /// Parse a flow file from a `Vec` holding exactly one, reusing its
    /// allocation for the content.
    ///
    /// The header is parsed and then removed from the front of `bytes`, so the
    /// content stays in the allocation it arrived in. Use this when the bytes
    /// are already owned, which is the common case for anything read into
    /// memory. [`from_bytes`](Self::from_bytes) copies the content out of the
    /// slice it is given instead.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let bytes = FlowFile::builder()
    ///     .attribute("filename", "greeting.txt")
    ///     .content(&b"hello"[..])
    ///     .to_bytes();
    ///
    /// let flow_file = FlowFile::from_vec(bytes).unwrap();
    /// assert_eq!(flow_file.content().as_slice(), b"hello");
    /// ```
    ///
    /// # Errors
    ///
    /// As [`from_bytes`](Self::from_bytes).
    pub fn from_vec(bytes: Vec<u8>) -> Result<Self> {
        Self::from_vec_with_limits(bytes, Limits::UNLIMITED)
    }

    /// Like [`from_vec`](Self::from_vec), but enforcing [`Limits`] on the
    /// header. Use this for untrusted input.
    ///
    /// # Errors
    ///
    /// As [`from_bytes_with_limits`](Self::from_bytes_with_limits).
    pub fn from_vec_with_limits(mut bytes: Vec<u8>, limits: Limits) -> Result<Self> {
        let (attributes, size, header_len) = {
            let mut reader = bytes.as_slice();
            let (attributes, size) = sync::parse_header(&mut reader, None, limits)?;
            (attributes, size, bytes.len() - reader.len())
        };
        let actual = (bytes.len() - header_len) as u64;
        if actual < size {
            return Err(Error::SizeMismatch {
                expected: size,
                actual,
            });
        }
        if actual > size {
            return Err(Error::TrailingData(actual - size));
        }
        // Shifts the content down over the header rather than allocating a
        // second buffer for it.
        bytes.drain(..header_len);
        Ok(Self::from_raw_parts(size, attributes, bytes))
    }

    /// Serialize the flow file to the binary V3 format.
    ///
    /// This writes the whole flow file, header included.
    /// [`into_memory`](Self::into_memory) does something different: it reads a
    /// reader-backed content into memory, and serializes nothing.
    ///
    /// The output declares [`size`](Self::size) bytes of content, the same
    /// value [`write_to`](Self::write_to) would stream.
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
    /// It also panics if `size` disagrees with the content's actual length.
    /// You can only reach that by breaking
    /// [`map_content`](Self::map_content)'s contract. The check runs in every
    /// build rather than only in debug, because the alternative is emitting a
    /// flow file that only the reader at the far end finds to be corrupt.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        assert_eq!(
            self.size,
            self.content.len() as u64,
            "declared size does not match the content; see FlowFile::with_size"
        );
        // One allocation for header and content together: the length is known
        // exactly before either is written, so nothing here grows.
        let mut buf = Vec::with_capacity(format::header_len(&self.attributes) + self.content.len());
        format::encode_header_into(&mut buf, &self.attributes, self.size);
        buf.extend_from_slice(&self.content);
        buf
    }

    /// Transform in-memory content, deriving the new [`size`](Self::size) from
    /// what `f` returns.
    ///
    /// This is the common length-changing case, such as decompressing,
    /// re-encoding, or rewriting. The new length is simply the new content's,
    /// so nothing has to declare it. Use
    /// [`map_content_sized`](Self::map_content_sized) when the result is a
    /// reader whose length only you know.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let flow_file = FlowFile::builder()
    ///     .attribute("filename", "greeting.txt")
    ///     .content(&b"hello"[..])
    ///     .map_bytes(|content| content.to_ascii_uppercase());
    ///
    /// assert_eq!(flow_file.content().as_slice(), b"HELLO");
    /// assert_eq!(flow_file.size(), 5);
    /// assert_eq!(flow_file.attribute("filename"), Some("greeting.txt"));
    /// ```
    #[must_use]
    pub fn map_bytes(self, f: impl FnOnce(Vec<u8>) -> Vec<u8>) -> Self {
        self.map_content_sized(|content| {
            let content = f(content);
            let size = content.len() as u64;
            (content, size)
        })
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

    /// A length-changing `map_content` needs `with_size`. Once it has one,
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

    /// The check runs in every build, not only in debug. A flow file whose
    /// declared size disagrees with its content serializes to bytes that only
    /// the reader at the far end discovers to be corrupt.
    #[test]
    #[should_panic(expected = "declared size does not match the content")]
    fn to_bytes_refuses_a_size_that_disagrees_with_the_content() {
        let _ = FlowFile::builder()
            .content(&b"hi"[..])
            .with_size(99)
            .to_bytes();
    }

    /// The sized forms exist so that the size follows the content, instead of
    /// being a second thing to remember.
    #[test]
    fn the_sized_maps_keep_the_size_with_the_content() {
        let flow_file = FlowFile::builder()
            .attribute("k", "v")
            .content(&b"hi"[..])
            .map_bytes(|content| content.repeat(3));
        assert_eq!(flow_file.size(), 6);
        assert_eq!(flow_file.content().as_slice(), b"hihihi");
        assert_eq!(flow_file.attribute("k"), Some("v"), "attributes carried");

        // Same answer as spelling it out by hand, which is what these replace.
        let by_hand = FlowFile::builder()
            .attribute("k", "v")
            .content(&b"hi"[..])
            .map_content(|content| content.repeat(3))
            .with_size(6);
        assert_eq!(flow_file, by_hand);

        // And the general form, for a container whose length only `f` knows.
        let sized = FlowFile::builder()
            .content(&b"hi"[..])
            .map_content_sized(|content| (std::io::Cursor::new(content.repeat(2)), 4));
        assert_eq!(sized.size(), 4);
    }

    /// The owning parse has to agree with the borrowing one exactly, since the
    /// only difference is meant to be who owns the bytes.
    #[test]
    fn from_vec_matches_from_bytes() {
        let bytes = FlowFile::builder()
            .attribute("filename", "greeting.txt")
            .content(&b"hello"[..])
            .to_bytes();

        assert_eq!(
            FlowFile::from_vec(bytes.clone()).unwrap(),
            FlowFile::from_bytes(&bytes).unwrap()
        );

        // Including how it rejects things.
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(matches!(
            FlowFile::from_vec(trailing),
            Err(Error::TrailingData(1))
        ));
        assert!(matches!(
            FlowFile::from_vec(bytes[..bytes.len() - 2].to_vec()),
            Err(Error::SizeMismatch {
                expected: 5,
                actual: 3
            })
        ));
        assert!(matches!(
            FlowFile::from_vec_with_limits(bytes, Limits::UNLIMITED.with_max_content_len(4)),
            Err(Error::ContentTooLarge { size: 5, limit: 4 })
        ));
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
