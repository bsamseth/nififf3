use std::collections::HashMap;

use crate::FlowFile;

/// Builder for [`FlowFile`]s.
///
/// Attributes are added first; supplying the content finishes the build.
///
/// ```
/// use nififf3::FlowFile;
///
/// let flow_file = FlowFile::builder()
///     .attribute("filename", "data.bin")
///     .content(vec![1, 2, 3]);
/// assert_eq!(flow_file.size(), 3);
/// ```
#[derive(Debug, Default, Clone)]
pub struct FlowFileBuilder {
    attributes: HashMap<String, String>,
}

impl FlowFileBuilder {
    /// Create an empty builder. Equivalent to [`FlowFile::builder`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a single attribute, replacing any previous value for the key.
    #[must_use]
    pub fn attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }

    /// Add all attributes from an iterator of key-value pairs.
    #[must_use]
    pub fn attributes<K, V>(mut self, attributes: impl IntoIterator<Item = (K, V)>) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.attributes
            .extend(attributes.into_iter().map(|(k, v)| (k.into(), v.into())));
        self
    }

    /// Remove an attribute, if present.
    ///
    /// Mostly useful after [`FlowFile::derive`](crate::FlowFile::derive) or
    /// [`Fragments::next`](crate::Fragments::next), to drop an inherited
    /// attribute that does not apply to the new flow file.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let parent = FlowFile::builder()
    ///     .attribute("filename", "archive.tar")
    ///     .content(Vec::new());
    ///
    /// let child = parent.derive().without_attribute("filename").content(Vec::new());
    /// assert!(!child.attributes().contains_key("filename"));
    /// ```
    #[must_use]
    pub fn without_attribute(mut self, key: &str) -> Self {
        self.attributes.remove(key);
        self
    }

    /// Finish the build with in-memory content; the size is the content length.
    pub fn content(self, content: impl Into<Vec<u8>>) -> FlowFile<Vec<u8>> {
        let content = content.into();
        FlowFile::from_raw_parts(content.len() as u64, self.attributes, content)
    }

    /// Finish the build with content from a reader.
    ///
    /// The V3 format stores the content size before the content itself, so
    /// the size must be known up front; `size` is the number of bytes that
    /// will be read from `content` when the flow file is serialized.
    ///
    /// If the size is not known, use [`buffered`](Self::buffered) or (with
    /// the `tempfile` feature) [`tempfile`](Self::tempfile) to spool the
    /// reader first.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let content = &b"hello"[..]; // any `impl Read`
    /// let mut flow_file = FlowFile::builder().reader(content, 5);
    /// let mut out = Vec::new();
    /// flow_file.write_to(&mut out).unwrap();
    /// ```
    pub fn reader<R>(self, content: R, size: u64) -> FlowFile<R> {
        FlowFile::from_raw_parts(size, self.attributes, content)
    }

    /// Finish the build by reading `content` to completion into memory.
    ///
    /// Useful when the content size is not known up front. The whole
    /// content is held in memory; for large content prefer
    /// [`tempfile`](Self::tempfile) (behind the `tempfile` feature).
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let reader = &b"length unknown ahead of time"[..];
    /// let flow_file = FlowFile::builder().buffered(reader).unwrap();
    /// assert_eq!(flow_file.size(), 28);
    /// ```
    ///
    /// # Errors
    ///
    /// Any error from reading `content` to the end.
    pub fn buffered(self, mut content: impl std::io::Read) -> std::io::Result<FlowFile<Vec<u8>>> {
        let mut buf = Vec::new();
        content.read_to_end(&mut buf)?;
        Ok(self.content(buf))
    }

    /// Finish the build by spooling `content` into an anonymous temporary
    /// file, which becomes the flow file's content.
    ///
    /// Useful when the content size is not known up front and may be too
    /// large to buffer in memory. The file is deleted when the returned
    /// flow file (or its content) is dropped.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let reader = &b"spooled to disk"[..];
    /// let mut flow_file = FlowFile::builder().tempfile(reader).unwrap();
    /// assert_eq!(flow_file.size(), 15);
    /// let mut out = Vec::new();
    /// flow_file.write_to(&mut out).unwrap();
    /// ```
    ///
    /// # Errors
    ///
    /// Any error from creating the temporary file, or from copying `content`
    /// into it.
    #[cfg(feature = "tempfile")]
    pub fn tempfile(
        self,
        mut content: impl std::io::Read,
    ) -> std::io::Result<FlowFile<std::fs::File>> {
        use std::io::Seek;

        let mut file = ::tempfile::tempfile()?;
        let size = std::io::copy(&mut content, &mut file)?;
        file.rewind()?;
        Ok(FlowFile::from_raw_parts(size, self.attributes, file))
    }

    /// Finish the build by spooling `content` into a
    /// [`SpooledTempFile`](tempfile::SpooledTempFile): the content is held
    /// in memory up to `max_memory` bytes, and rolled over to an anonymous
    /// temporary file beyond that.
    ///
    /// A middle ground between [`buffered`](Self::buffered) (always memory)
    /// and [`tempfile`](Self::tempfile) (always disk) for content of
    /// unknown size that is usually small.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let reader = &b"stays in memory"[..];
    /// let mut flow_file = FlowFile::builder().spooled(reader, 64 * 1024).unwrap();
    /// assert_eq!(flow_file.size(), 15);
    /// let mut out = Vec::new();
    /// flow_file.write_to(&mut out).unwrap();
    /// ```
    ///
    /// # Errors
    ///
    /// Any error from copying `content` into the spool, including creating
    /// the temporary file once `max_memory` is exceeded.
    #[cfg(feature = "tempfile")]
    pub fn spooled(
        self,
        mut content: impl std::io::Read,
        max_memory: usize,
    ) -> std::io::Result<FlowFile<tempfile::SpooledTempFile>> {
        use std::io::Seek;

        let mut file = tempfile::SpooledTempFile::new(max_memory);
        let size = std::io::copy(&mut content, &mut file)?;
        file.rewind()?;
        Ok(FlowFile::from_raw_parts(size, self.attributes, file))
    }

    /// Async version of [`buffered`](Self::buffered): reads an
    /// [`AsyncRead`](tokio::io::AsyncRead) to completion into memory.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let reader = &b"hello"[..]; // any `impl AsyncRead + Unpin`
    /// let flow_file = FlowFile::builder().buffered_async(reader).await.unwrap();
    /// assert_eq!(flow_file.size(), 5);
    /// # });
    /// ```
    ///
    /// # Errors
    ///
    /// Any error from reading `content` to the end.
    #[cfg(feature = "tokio")]
    pub async fn buffered_async(
        self,
        mut content: impl tokio::io::AsyncRead + Unpin,
    ) -> std::io::Result<FlowFile<Vec<u8>>> {
        use tokio::io::AsyncReadExt;

        let mut buf = Vec::new();
        content.read_to_end(&mut buf).await?;
        Ok(self.content(buf))
    }

    /// Async version of [`tempfile`](Self::tempfile): spools an
    /// [`AsyncRead`](tokio::io::AsyncRead) into an anonymous temporary file.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let reader = &b"spooled to disk"[..]; // any `impl AsyncRead + Unpin`
    /// let flow_file = FlowFile::builder().tempfile_async(reader).await.unwrap();
    /// assert_eq!(flow_file.size(), 15);
    /// # });
    /// ```
    ///
    /// # Errors
    ///
    /// Any error from creating the temporary file, or from copying `content`
    /// into it.
    #[cfg(all(feature = "tokio", feature = "tempfile"))]
    pub async fn tempfile_async(
        self,
        mut content: impl tokio::io::AsyncRead + Unpin,
    ) -> std::io::Result<FlowFile<tokio::fs::File>> {
        use tokio::io::AsyncSeekExt;

        let mut file = tokio::fs::File::from_std(::tempfile::tempfile()?);
        let size = tokio::io::copy(&mut content, &mut file).await?;
        file.rewind().await?;
        Ok(FlowFile::from_raw_parts(size, self.attributes, file))
    }
}

#[cfg(test)]
mod tests {
    use crate::FlowFile;

    #[test]
    fn buffered_reads_content_and_sets_size() {
        let flow_file = FlowFile::builder()
            .attribute("k", "v")
            .buffered(&b"hello"[..])
            .unwrap();
        assert_eq!(flow_file.size(), 5);
        assert_eq!(flow_file.content().as_slice(), b"hello");
        assert_eq!(flow_file.attributes()["k"], "v");
    }

    #[cfg(feature = "tempfile")]
    #[test]
    fn tempfile_spools_content_and_sets_size() {
        let mut flow_file = FlowFile::builder()
            .attribute("k", "v")
            .tempfile(&b"hello"[..])
            .unwrap();
        assert_eq!(flow_file.size(), 5);
        let mut out = Vec::new();
        flow_file.write_to(&mut out).unwrap();
        let expected = FlowFile::builder()
            .attribute("k", "v")
            .content(&b"hello"[..]);
        assert_eq!(out, expected.to_bytes());
    }

    #[cfg(feature = "tempfile")]
    #[test]
    fn spooled_rolls_to_disk_beyond_the_memory_limit() {
        let small = FlowFile::builder().spooled(&b"hi"[..], 1024).unwrap();
        assert_eq!(small.size(), 2);
        assert!(!small.content().is_rolled());

        let big_content = vec![7u8; 4096];
        let mut big = FlowFile::builder()
            .spooled(big_content.as_slice(), 1024)
            .unwrap();
        assert_eq!(big.size(), 4096);
        assert!(big.content().is_rolled());

        let mut out = Vec::new();
        big.write_to(&mut out).unwrap();
        let parsed = FlowFile::from_bytes(&out).unwrap();
        assert_eq!(parsed.into_content(), big_content);
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn buffered_async_reads_content_and_sets_size() {
        let flow_file = FlowFile::builder()
            .buffered_async(&b"hello"[..])
            .await
            .unwrap();
        assert_eq!(flow_file.size(), 5);
        assert_eq!(flow_file.content().as_slice(), b"hello");
    }

    #[cfg(all(feature = "tokio", feature = "tempfile"))]
    #[tokio::test]
    async fn tempfile_async_spools_content_and_sets_size() {
        let mut flow_file = FlowFile::builder()
            .attribute("k", "v")
            .tempfile_async(&b"hello"[..])
            .await
            .unwrap();
        assert_eq!(flow_file.size(), 5);
        let mut out = Vec::new();
        flow_file.write_to_async(&mut out).await.unwrap();
        let expected = FlowFile::builder()
            .attribute("k", "v")
            .content(&b"hello"[..]);
        assert_eq!(out, expected.to_bytes());
    }
}
