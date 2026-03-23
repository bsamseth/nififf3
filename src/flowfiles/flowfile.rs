use std::collections::HashMap;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};

use super::FlowFileHeader;

/// A NiFi Flow File v3.
///
/// This is generic over the type that holds the content. The header is kept in memory as a
/// [`FlowFileHeader`].
pub struct FlowFile<C> {
    header: FlowFileHeader,
    /// The content of the flow file.
    content: C,
}

impl FlowFile<()> {
    #[must_use]
    pub fn empty() -> Self {
        Self::empty_with_attributes(HashMap::default())
    }
    #[must_use]
    pub fn empty_with_attributes(attributes: HashMap<String, String>) -> Self {
        Self {
            header: FlowFileHeader::new(0, attributes),
            content: (),
        }
    }
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
    pub fn header(&self) -> &FlowFileHeader {
        &self.header
    }
    pub fn header_mut(&mut self) -> &mut FlowFileHeader {
        &mut self.header
    }
    pub fn content(&self) -> &C {
        &self.content
    }
    pub fn content_mut(&mut self) -> &mut C {
        &mut self.content
    }
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

impl<R: AsyncRead + Unpin> FlowFile<R> {
    /// Write the flow file to the provided writer.
    ///
    /// This will write the header, followed by using [`tokio::io::copy`] to copy the content into
    /// the writer. For more control of how the writing is done, use [`Self::into_parts`] and
    /// serialize each part.
    ///
    /// # Errors
    /// Forwards any [`tokio::io::Error`]s that occur.
    pub async fn serialize_into<W: AsyncWrite + Unpin>(
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

#[doc(hidden)]
pub struct FlowFileBuilder<Size, Attributes, Content> {
    size: Size,
    attributes: Attributes,
    content: Content,
}
impl<S, C> FlowFileBuilder<S, Unset, C> {
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
    pub fn size(self, size: impl Into<u64>) -> FlowFileBuilder<Set<u64>, A, C> {
        FlowFileBuilder {
            size: Set(size.into()),
            attributes: self.attributes,
            content: self.content,
        }
    }
}
impl<S, A> FlowFileBuilder<S, A, Unset> {
    pub fn content<C>(self, content: C) -> FlowFileBuilder<S, A, Set<C>> {
        FlowFileBuilder {
            size: self.size,
            attributes: self.attributes,
            content: Set(content),
        }
    }
}

impl<C> FlowFileBuilder<Set<u64>, Set<HashMap<String, String>>, Set<C>> {
    pub fn build(self) -> FlowFile<C> {
        FlowFile {
            header: FlowFileHeader::new(self.size.0, self.attributes.0),
            content: self.content.0,
        }
    }
}

impl<A> FlowFileBuilder<Unset, A, Unset> {
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

    pub async fn content_from_file(
        self,
        content: impl Into<tokio::fs::File>,
    ) -> tokio::io::Result<FlowFileBuilder<Set<u64>, A, Set<tokio::fs::File>>> {
        let file = content.into();
        let size = file.metadata().await?.len();
        Ok(FlowFileBuilder {
            size: Set(size as u64),
            attributes: self.attributes,
            content: Set(file),
        })
    }

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
