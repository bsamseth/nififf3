use std::collections::HashMap;

use tokio::io::{AsyncRead, AsyncWrite};

use super::FlowFileHeader;

pub struct OutputFlowFile<R> {
    header: FlowFileHeader,
    /// The content of the flow file.
    content: R,
}

impl OutputFlowFile<()> {
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
}

impl<R> OutputFlowFile<R> {
    pub fn new(
        size: impl Into<u64>,
        attributes: impl Into<HashMap<String, String>>,
        content: R,
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
    pub fn content(&self) -> &R {
        &self.content
    }
    pub fn content_mut(&mut self) -> &mut R {
        &mut self.content
    }
    pub fn into_parts(self) -> (FlowFileHeader, R) {
        (self.header, self.content)
    }
}

impl<R: AsyncRead + Unpin> OutputFlowFile<R> {
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
        self.header.serialize_into(&mut w).await?;
        tokio::io::copy(&mut self.content, &mut w).await?;
        Ok(())
    }
}
