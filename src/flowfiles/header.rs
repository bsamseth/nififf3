use std::collections::HashMap;

use tokio::io::{AsyncWrite, AsyncWriteExt};

/// Representation of the header of a NiFi Flow File v3.
///
/// A NiFi Flow File v3 header contains, when decoded, all the attributes attached to the content,
/// as well as the size in bytes of the content. The binary header format is (if you care):
///
/// ```text
/// Magic Bytes:     "NiFiFF3" (7 bytes)
/// Attribute Count: u16 or u16+u32 (if count >= 65535)
/// Attributes:      Repeated (key_len: u16/u16+u32, key: bytes, value_len: u16/u16+u32, value: bytes)
/// Content Size:    u64
/// ```
///
/// # Example
///
/// ```
/// use nifioxide::FlowFileHeader;
/// use std::collections::HashMap;
///
/// let mut attrs = HashMap::new();
/// attrs.insert("filename".to_string(), "test.txt".to_string());
/// let header = FlowFileHeader::new(1024, attrs);
/// assert_eq!(header.size(), 1024);
/// assert_eq!(header.attributes().get("filename"), Some(&"test.txt".to_string()));
/// ```
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
    ///
    /// # Arguments
    ///
    /// * `size` - The content size in bytes.
    /// * `attributes` - A map of key-value pairs representing the flow file attributes.
    ///
    /// # Example
    ///
    /// ```
    /// use nifioxide::FlowFileHeader;
    /// use std::collections::HashMap;
    ///
    /// let mut attrs = HashMap::new();
    /// attrs.insert("filename".to_string(), "test.txt".to_string());
    /// let header = FlowFileHeader::new(1024, attrs);
    /// ```
    #[must_use]
    pub fn new(size: u64, attributes: HashMap<String, String>) -> Self {
        Self { size, attributes }
    }

    /// Get the content length in bytes of the flow file this header describes.
    ///
    /// Note that this is not how many bytes may be left in the related content (for stateful
    /// content readers, such as a file with a cursor, or a TCP connection), but rather how many
    /// bytes the content is expected to contain in total.
    ///
    /// # Example
    ///
    /// ```
    /// use nifioxide::FlowFileHeader;
    ///
    /// let header = FlowFileHeader::new(2048, std::collections::HashMap::new());
    /// assert_eq!(header.size(), 2048);
    /// ```
    #[doc(alias = "len")]
    #[must_use]
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Return `true` if the flow file self-reports to be empty (content size is 0).
    ///
    /// # Example
    ///
    /// ```
    /// use nifioxide::FlowFileHeader;
    ///
    /// let header = FlowFileHeader::new(0, std::collections::HashMap::new());
    /// assert!(header.is_empty());
    ///
    /// let header = FlowFileHeader::new(100, std::collections::HashMap::new());
    /// assert!(!header.is_empty());
    /// ```
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Get an immutable reference to all attributes contained in the flow file.
    ///
    /// # Example
    ///
    /// ```
    /// use nifioxide::FlowFileHeader;
    ///
    /// let header = FlowFileHeader::new(100, std::collections::HashMap::new());
    /// for (key, value) in header.attributes() {
    ///     println!("{}: {}", key, value);
    /// }
    /// ```
    #[must_use]
    pub fn attributes(&self) -> &HashMap<String, String> {
        &self.attributes
    }

    /// Get a mutable reference to all attributes contained in the flow file.
    ///
    /// This allows modifying the attributes after the header is created.
    ///
    /// # Example
    ///
    /// ```
    /// use nifioxide::FlowFileHeader;
    ///
    /// let mut header = FlowFileHeader::new(100, std::collections::HashMap::new());
    /// header.attributes_mut().insert("new.key".to_string(), "new.value".to_string());
    /// ```
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
    /// Serialize the header into the provided writer in NiFi Flow File v3 binary format.
    ///
    /// This writes the following to the writer:
    /// 1. Magic bytes: `b"NiFiFF3"` (7 bytes)
    /// 2. Attribute count: either `u16` (if < 65535) or `u16::MAX` followed by `u32`
    /// 3. For each attribute: key length, key bytes, value length, value bytes
    /// 4. Content size as `u64`
    ///
    /// # Arguments
    ///
    /// * `writer` - The async writer to serialize the header to.
    ///
    /// # Errors
    ///
    /// Returns a [`tokio::io::Error`] if writing fails. This can occur if:
    /// - The writer fails to write any part of the header.
    /// - The attribute map contains more than `u32::MAX` entries.
    /// - Any attribute key or value length exceeds `u32::MAX`.
    ///
    /// # Example
    ///
    /// ```
    /// use nifioxide::FlowFileHeader;
    /// use std::collections::HashMap;
    ///
    /// async fn serialize_header() -> Result<(), tokio::io::Error> {
    ///     let mut attrs = HashMap::new();
    ///     attrs.insert("filename".to_string(), "test.txt".to_string());
    ///     let header = FlowFileHeader::new(1024, attrs);
    ///
    ///     let mut buf = Vec::new();
    ///     header.serialize_header_into(&mut buf).await?;
    ///     Ok(())
    /// }
    /// ```
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
