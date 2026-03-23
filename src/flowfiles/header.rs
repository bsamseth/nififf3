use std::collections::HashMap;

use tokio::io::{AsyncWrite, AsyncWriteExt};

/// Representation of the header of a NiFi Flow File v3.
///
/// A NiFi Flow File v3 header contains, when decoded, all the attributes attached to the content,
/// as well as the size in bytes of the content.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FlowFileHeader {
    size: u64,
    attributes: HashMap<String, String>,
}

impl FlowFileHeader {
    /// Create a new flow file header.
    ///
    /// The size is the number of bytes in the content of the flow file, not including the
    /// size of this header itself.
    #[must_use]
    pub fn new(size: u64, attributes: HashMap<String, String>) -> Self {
        Self { size, attributes }
    }

    /// The length in bytes of the content of the flow file this header describes.
    ///
    /// Note that this is not how many bytes may be left in the related content (for stateful
    /// content readers, such as a file with a cursor, or a tcp connection), but rather how many
    /// bytes the content is expected to contain in total.
    #[doc(alias = "len")]
    #[must_use]
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Return `true` if the flow file self-reports to be empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// All attributes contained in the flow file.
    #[must_use]
    pub fn attributes(&self) -> &HashMap<String, String> {
        &self.attributes
    }

    /// All attributes contained in the flow file.
    #[must_use]
    pub fn attributes_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.attributes
    }
}

impl<S: Into<u64>, T: Into<HashMap<String, String>>> From<(S, T)> for FlowFileHeader {
    fn from((size, attributes): (S, T)) -> Self {
        Self {
            size: size.into(),
            attributes: attributes.into(),
        }
    }
}

impl FlowFileHeader {
    /// Serialize the header into the provided writer.
    ///
    /// # Errors
    /// Write errors from the writer are propagated up.
    pub async fn serialize_header_into<W: AsyncWrite + Unpin>(
        &self,
        mut writer: W,
    ) -> tokio::io::Result<()> {
        writer.write_all(b"NiFiFF3").await?;
        write_field_length(&mut writer, self.attributes.len()).await?;

        for (key, value) in &self.attributes {
            write_string(&mut writer, key).await?;
            write_string(&mut writer, value).await?;
        }

        writer.write_u64(self.size).await?;

        Ok(())
    }
}

async fn write_field_length<W: AsyncWrite + Unpin>(w: &mut W, len: usize) -> tokio::io::Result<()> {
    let Ok(len) = u32::try_from(len) else {
        return Err(tokio::io::Error::other("Field length exceeds u32::MAX"));
    };
    if let Ok(len) = u16::try_from(len) {
        return w.write_u16(len).await;
    }
    w.write_u16(u16::MAX).await?;
    w.write_u32(len).await
}

async fn write_string<W: AsyncWrite + Unpin>(w: &mut W, s: &str) -> tokio::io::Result<()> {
    write_field_length(w, s.len()).await?;
    w.write_all(s.as_bytes()).await
}
