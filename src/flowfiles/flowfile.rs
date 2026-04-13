use std::collections::HashMap;

use tokio::io::{AsyncRead, AsyncReadExt};

use super::FlowFileHeader;

/// A NiFi Flow File v3.
///
/// This is generic over the type that holds the content. The header is kept in memory as a
/// [`FlowFileHeader`]. The content type `C` can be anything that holds the flow file's binary data,
/// such as a `Vec<u8>`, `tokio::fs::File`, or any type implementing [`tokio::io::AsyncRead`].
///
/// # Usage
///
/// For creating new flow files, use [`FlowFile::builder()`] for a fluent API, or [`FlowFile::new()`]
/// for direct construction:
///
/// ```
/// use nifioxide::FlowFile;
/// use std::collections::HashMap;
///
/// // Using builder pattern - content_from_bytes infers size, set attributes afterwards
/// let flow_file = FlowFile::builder()
///     .content_from_bytes(b"file content here")
///     .attributes(HashMap::new())
///     .build();
///
/// // Using direct constructor
/// let mut attrs = HashMap::new();
/// attrs.insert("filename".to_string(), "test.txt".to_string());
/// let flow_file = FlowFile::new(1024u64, attrs, std::io::Cursor::new(b"content"));
/// ```
///
/// For reading flow files from a stream, see [`crate::axum::StreamedFlowFiles`] and [`crate::axum::StreamedFlowFile`].
pub struct FlowFile<C> {
    pub(crate) header: FlowFileHeader,
    /// The content of the flow file.
    pub(crate) content: C,
}

impl FlowFile<()> {
    /// Create an empty flow file with no attributes.
    ///
    /// This creates a flow file with zero content size and an empty attribute map.
    /// The content type is set to `()` (unit type), indicating no content.
    ///
    /// # Example
    ///
    /// ```
    /// use nifioxide::FlowFile;
    ///
    /// let ff = FlowFile::empty();
    /// assert!(ff.is_empty());
    /// assert!(ff.attributes().is_empty());
    /// ```
    #[must_use]
    pub fn empty() -> Self {
        Self::empty_with_attributes(HashMap::default())
    }

    /// Create an empty flow file with the given attributes.
    ///
    /// # Arguments
    ///
    /// * `attributes` - A map of key-value pairs representing the flow file attributes.
    ///
    /// # Example
    ///
    /// ```
    /// use nifioxide::FlowFile;
    /// use std::collections::HashMap;
    ///
    /// let mut attrs = HashMap::new();
    /// attrs.insert("filename".to_string(), "test.txt".to_string());
    /// let ff = FlowFile::empty_with_attributes(attrs);
    /// assert_eq!(ff.attributes().get("filename"), Some(&"test.txt".to_string()));
    /// ```
    #[must_use]
    pub fn empty_with_attributes(attributes: HashMap<String, String>) -> Self {
        Self {
            header: FlowFileHeader::new(0, attributes),
            content: (),
        }
    }

    /// Start building a new flow file using the builder pattern.
    ///
    /// The builder requires setting `size`, `attributes`, and `content` before calling the `build()` method.
    ///
    /// # Example
    ///
    /// ```
    /// use nifioxide::FlowFile;
    /// use std::collections::HashMap;
    ///
    /// let ff = FlowFile::builder()
    ///     .content_from_bytes(b"file content")
    ///     .attributes(HashMap::new())
    ///     .build();
    /// ```
    #[must_use]
    pub fn builder() -> FlowFileBuilder<Unset, Unset, Unset> {
        FlowFileBuilder {
            size: Unset,
            attributes: Unset,
            content: Unset,
        }
    }
}

impl<C> FlowFile<C> {
    /// Create a new flow file with the given size, attributes, and content.
    ///
    /// # Arguments
    ///
    /// * `size` - The size in bytes of the content.
    /// * `attributes` - A map of key-value pairs representing the flow file attributes.
    /// * `content` - The content of the flow file. This can be any type that holds the data.
    ///
    /// Note that in order for the resulting flow file to be well defined, the provided size must
    /// match the actual number of bytes in the content.
    ///
    /// # Example
    ///
    /// ```
    /// use nifioxide::FlowFile;
    /// use std::collections::HashMap;
    ///
    /// let mut attrs = HashMap::new();
    /// attrs.insert("filename".to_string(), "test.txt".to_string());
    /// let content = b"content";
    /// let ff = FlowFile::new(content.len() as u64, attrs, std::io::Cursor::new(content));
    /// ```
    pub fn new(
        size: impl Into<u64>,
        attributes: impl Into<HashMap<String, String>>,
        content: C,
    ) -> Self {
        Self {
            header: (size, attributes).into(),
            content,
        }
    }

    /// Get a reference to the flow file header.
    ///
    /// This provides access to the size and attributes of the flow file.
    ///
    /// # Example
    ///
    /// ```
    /// use nifioxide::FlowFile;
    ///
    /// let ff: FlowFile<()> = FlowFile::empty();
    /// let header = ff.header();
    /// ```
    pub fn header(&self) -> &FlowFileHeader {
        &self.header
    }

    /// Get a mutable reference to the flow file header.
    ///
    /// This allows modifying the size and attributes of the flow file.
    pub fn header_mut(&mut self) -> &mut FlowFileHeader {
        &mut self.header
    }

    /// Get a reference to the content of the flow file.
    pub fn content(&self) -> &C {
        &self.content
    }

    /// Get a mutable reference to the content of the flow file.
    pub fn content_mut(&mut self) -> &mut C {
        &mut self.content
    }

    /// Decompose the flow file into its header and content components.
    ///
    /// This is useful when you need to serialize them separately or transform the content.
    ///
    /// # Example
    ///
    /// ```
    /// use nifioxide::FlowFile;
    /// use std::io::Cursor;
    ///
    /// let ff = FlowFile::new(1024u64, std::collections::HashMap::new(), Cursor::new(vec![1, 2, 3]));
    /// let (header, content) = ff.into_parts();
    /// ```
    pub fn into_parts(self) -> (FlowFileHeader, C) {
        (self.header, self.content)
    }
}

impl<C> std::ops::Deref for FlowFile<C> {
    type Target = FlowFileHeader;

    fn deref(&self) -> &Self::Target {
        &self.header
    }
}
impl<C> std::ops::DerefMut for FlowFile<C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.header
    }
}

impl<R: AsyncRead + Unpin> FlowFile<R> {
    /// Serialize the flow file into a writer.
    ///
    /// This will write the header using [`FlowFileHeader::serialize_header_into`], followed by
    /// using [`tokio::io::copy`] to copy the bytes from the content into the writer.
    ///
    /// # Errors
    ///
    /// Returns a [`tokio::io::Error`] if writing fails. See the above functions for more details.
    pub async fn serialize_into<W: tokio::io::AsyncWrite + Unpin>(
        &mut self,
        mut w: W,
    ) -> tokio::io::Result<()> {
        self.header.serialize_header_into(&mut w).await?;
        tokio::io::copy(&mut self.content, &mut w).await?;
        Ok(())
    }
}

#[doc(hidden)]
pub struct Unset;

#[doc(hidden)]
pub struct Set<T>(T);

/// Builder for constructing a [`FlowFile`] with a fluent API.
///
/// Use [`FlowFile::builder()`] to create a new builder, then chain methods to set
/// the size, attributes, and content. Call [`build()`](Self::build()) to create the final `FlowFile`.
///
/// # Required Fields
///
/// All three fields must be set before calling `build()`:
/// - `size` - The content size in bytes
/// - `attributes` - Key-value pairs representing the flow file attributes
/// - `content` - The actual content data
///
/// # Example
///
/// ```
/// use nifioxide::FlowFile;
///
/// // Build from bytes - content_from_bytes sets size, then set attributes
/// let ff = FlowFile::builder()
///     .content_from_bytes(b"Hello World!")
///     .attributes([("filename".to_string(), "test.txt".to_string())])
///     .build();
///
/// // Build from file (requires tokio)
/// async fn build_from_file() {
///     let ff = FlowFile::builder()
///         .content_from_file(tokio::fs::File::open("data.bin").await.unwrap())
///         .await
///         .unwrap()
///         .attributes([("filename".to_string(), "data.bin".to_string())])
///         .build();
/// }
/// ```
#[doc(hidden)]
pub struct FlowFileBuilder<Size, Attributes, Content> {
    size: Size,
    attributes: Attributes,
    content: Content,
}
impl<S, C> FlowFileBuilder<S, Unset, C> {
    /// Set the attributes for the flow file.
    ///
    /// # Arguments
    ///
    /// * `attributes` - A map of key-value pairs. Can be any type that converts to `HashMap<String, String>`.
    ///
    /// # Example
    ///
    /// ```
    /// use nifioxide::FlowFile;
    /// use std::collections::HashMap;
    ///
    /// let mut attrs = HashMap::new();
    /// attrs.insert("filename".to_string(), "test.txt".to_string());
    ///
    /// let ff = FlowFile::builder()
    ///     .attributes(std::collections::HashMap::new())
    ///     .content_from_bytes(b"content")
    ///     .build();
    /// ```
    pub fn attributes(
        self,
        attributes: impl Into<HashMap<String, String>>,
    ) -> FlowFileBuilder<S, Set<HashMap<String, String>>, C> {
        FlowFileBuilder {
            size: self.size,
            attributes: Set(attributes.into()),
            content: self.content,
        }
    }
}
impl<A, C> FlowFileBuilder<Unset, A, C> {
    /// Set the content size (in bytes) for the flow file.
    ///
    /// # Arguments
    ///
    /// * `size` - The size of the content in bytes. Can be any type that converts to `u64`.
    ///
    /// # Example
    ///
    /// ```
    /// use nifioxide::FlowFile;
    /// use std::collections::HashMap;
    ///
    /// let ff = FlowFile::builder()
    ///     .size(1024u64)
    ///     .attributes(HashMap::new())
    ///     .content(std::io::Cursor::new(b"content"))
    ///     .build();
    /// ```
    pub fn size(self, size: impl Into<u64>) -> FlowFileBuilder<Set<u64>, A, C> {
        FlowFileBuilder {
            size: Set(size.into()),
            attributes: self.attributes,
            content: self.content,
        }
    }
}
impl<S, A> FlowFileBuilder<S, A, Unset> {
    /// Set the content for the flow file.
    ///
    /// # Arguments
    ///
    /// * `content` - The content. Can be any type that holds the flow file data.
    ///
    /// Note: For in-memory content, consider using [`content_from_bytes()`](Self::content_from_bytes())
    /// which automatically calculates the size.
    pub fn content<C>(self, content: C) -> FlowFileBuilder<S, A, Set<C>> {
        FlowFileBuilder {
            size: self.size,
            attributes: self.attributes,
            content: Set(content),
        }
    }
}

impl<C> FlowFileBuilder<Set<u64>, Set<HashMap<String, String>>, Set<C>> {
    /// Build the final [`FlowFile`].
    ///
    /// This method is only available when all three fields (size, attributes, content) have been set.
    ///
    /// # Example
    ///
    /// ```
    /// use nifioxide::FlowFile;
    ///
    /// let ff = FlowFile::builder()
    ///     .content_from_bytes(b"Hello World!")
    ///     .attributes([("key".to_string(), "value".to_string())])
    ///     .build();
    /// ```
    pub fn build(self) -> FlowFile<C> {
        FlowFile {
            header: FlowFileHeader::new(self.size.0, self.attributes.0),
            content: self.content.0,
        }
    }
}

impl<A> FlowFileBuilder<Unset, A, Unset> {
    /// Set content from a byte slice, automatically calculating the size.
    ///
    /// This is a convenience method that wraps the content in a [`std::io::Cursor`] and automatically
    /// sets the size based on the content length.
    ///
    /// # Arguments
    ///
    /// * `content` - Any type that converts to `Vec<u8>` (e.g., `&[u8]`, `Vec<u8>`).
    ///
    /// # Example
    ///
    /// ```
    /// use nifioxide::FlowFile;
    /// use std::collections::HashMap;
    ///
    /// let ff = FlowFile::builder()
    ///     .content_from_bytes(b"file content here")
    ///     .attributes(HashMap::new())
    ///     .build();
    /// ```
    pub fn content_from_bytes(
        self,
        content: impl Into<Vec<u8>>,
    ) -> FlowFileBuilder<Set<u64>, A, Set<std::io::Cursor<Vec<u8>>>> {
        let content = content.into();
        FlowFileBuilder {
            size: Set(content.len() as u64),
            attributes: self.attributes,
            content: Set(std::io::Cursor::new(content)),
        }
    }

    /// Set content from a file, automatically determining the size.
    ///
    /// This method opens the file, reads its metadata to determine the size, and prepares it
    /// as the content. The file must be a [`tokio::fs::File`].
    ///
    /// # Arguments
    ///
    /// * `content`: A file that can be converted to `tokio::fs::File` (e.g., a path via
    ///   [`tokio::fs::File::open`]) or an already-opened file.
    ///
    /// # Errors
    ///
    /// Returns a [`tokio::io::Error`] if:
    /// - The file cannot be opened or converted.
    /// - The file metadata (size) cannot be read.
    ///
    /// # Example
    ///
    /// ```
    /// use nifioxide::FlowFile;
    /// use std::collections::HashMap;
    ///
    /// async fn build_from_file() -> Result<(), tokio::io::Error> {
    ///     let ff = FlowFile::builder()
    ///         .content_from_file(tokio::fs::File::open("data.bin").await?)
    ///         .await?
    ///         .attributes(HashMap::new())
    ///         .build();
    ///     Ok(())
    /// }
    /// ```
    pub async fn content_from_file(
        self,
        content: impl Into<tokio::fs::File>,
    ) -> tokio::io::Result<FlowFileBuilder<Set<u64>, A, Set<tokio::fs::File>>> {
        let file = content.into();
        let size = file.metadata().await?.len();
        Ok(FlowFileBuilder {
            size: Set(size),
            attributes: self.attributes,
            content: Set(file),
        })
    }

    /// Set content from a reader, buffering it entirely into memory.
    ///
    /// This reads the entire reader into a `Vec<u8>`, automatically calculating the size.
    /// The content is then wrapped in a [`std::io::Cursor`] for reading.
    ///
    /// # Arguments
    ///
    /// * `reader` - Any async reader implementing [`AsyncRead`](tokio::io::AsyncRead) + [`Unpin`].
    ///
    /// # Errors
    ///
    /// Returns a [`tokio::io::Error`] if reading from the reader fails.
    ///
    /// # Example
    ///
    /// ```
    /// use nifioxide::FlowFile;
    /// use std::io::Cursor;
    ///
    /// async fn build_from_reader() -> Result<(), tokio::io::Error> {
    ///     let reader = Cursor::new(b"some stream data");
    ///     let ff = FlowFile::builder()
    ///         .attributes([("source".to_string(), "stream".to_string())])
    ///         .content_from_reader_buffered_in_memory(reader)
    ///         .await?
    ///         .build();
    ///     Ok(())
    /// }
    /// ```
    pub async fn content_from_reader_buffered_in_memory<R: AsyncRead + Unpin>(
        self,
        mut reader: R,
    ) -> tokio::io::Result<FlowFileBuilder<Set<u64>, A, Set<std::io::Cursor<Vec<u8>>>>> {
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await?;
        Ok(FlowFileBuilder {
            size: Set(buf.len() as u64),
            attributes: self.attributes,
            content: Set(std::io::Cursor::new(buf)),
        })
    }
}

#[cfg(feature = "tempfile")]
impl<A> FlowFileBuilder<Unset, A, Unset> {
    /// Set content from a reader, buffering it to a temporary file.
    ///
    /// This is useful for handling large content that you don't want to keep entirely in memory.
    /// The reader's content is copied to a temporary file, which is then used as the content source.
    /// The file is seeked back to the beginning so it can be read.
    ///
    /// Requires the `tempfile` feature to be enabled.
    ///
    /// # Arguments
    ///
    /// * `reader` - Any async reader implementing [`AsyncRead`](tokio::io::AsyncRead) + [`Unpin`].
    ///
    /// # Errors
    ///
    /// Returns a [`tokio::io::Error`] if:
    /// - Creating the temporary file fails.
    /// - Reading from the reader fails.
    /// - Seeking to the beginning of the file fails.
    ///
    /// # Example
    ///
    /// ```
    /// use nifioxide::FlowFile;
    /// use std::io::Cursor;
    ///
    /// async fn build_from_tempfile() -> Result<(), tokio::io::Error> {
    ///     let reader = Cursor::new(b"large content stream");
    ///     let ff = FlowFile::builder()
    ///         .attributes([("source".to_string(), "stream".to_string())])
    ///         .content_from_reader_buffered_in_tempfile(reader)
    ///         .await?
    ///         .build();
    ///     Ok(())
    /// }
    /// ```
    pub async fn content_from_reader_buffered_in_tempfile<R: AsyncRead + Unpin>(
        self,
        mut reader: R,
    ) -> tokio::io::Result<FlowFileBuilder<Set<u64>, A, Set<tokio::fs::File>>> {
        use std::io::SeekFrom;
        use tokio::io::AsyncSeekExt;

        let mut file: tokio::fs::File = tempfile::tempfile()?.into();
        let size = tokio::io::copy(&mut reader, &mut file).await?;
        file.seek(SeekFrom::Start(0)).await?;
        Ok(FlowFileBuilder {
            size: Set(size),
            attributes: self.attributes,
            content: Set(file),
        })
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for FlowFile<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlowFile")
            .field("header", &self.header)
            .field("content", &self.content)
            .finish()
    }
}
