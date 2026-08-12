use std::collections::HashMap;

use crate::{FlowFile, FragmentKeys, attr};

/// Builder for [`FlowFile`]s.
///
/// Add attributes first. Supplying the content finishes the build.
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
    ///
    /// The [`attr`] module names the keys NiFi gives a meaning to.
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
    /// Mostly useful after `FlowFile::derive` or
    /// `Fragments::next_part`, to drop an inherited
    /// attribute that does not apply to the new flow file.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let flow_file = FlowFile::builder()
    ///     .attribute("filename", "archive.tar")
    ///     .attribute("source", "upload")
    ///     .without_attribute("filename")
    ///     .content(Vec::new());
    ///
    /// assert_eq!(flow_file.attribute("filename"), None);
    /// assert_eq!(flow_file.attribute("source"), Some("upload"));
    /// ```
    #[must_use]
    pub fn without_attribute(mut self, key: &str) -> Self {
        self.attributes.remove(key);
        self
    }

    /// Undo what `Fragments` added, dropping the fragment attributes and
    /// restoring [`filename`](crate::attr::FILENAME) from
    /// [`segment.original.filename`](crate::attr::SEGMENT_ORIGINAL_FILENAME).
    ///
    /// Use it at the tail of a merge. Once you have reassembled the content of
    /// a fragment set, build the result from any one part, because every part
    /// carries the parent's attributes. Calling this then makes the result look
    /// like the flow file the split started from, the way NiFi's
    /// `MergeContent` does in `defragment` mode.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// // One part of a split, as it would arrive.
    /// let part = FlowFile::builder()
    ///     .attribute("filename", "record-0")
    ///     .attribute("source", "upload")
    ///     .attribute("fragment.identifier", "8f14e45f")
    ///     .attribute("fragment.index", "1")
    ///     .attribute("fragment.count", "2")
    ///     .attribute("segment.original.filename", "records.csv")
    ///     .content(&b"a"[..]);
    ///
    /// let merged = part.derive_keep_uuid().defragment().content(&b"a\nb"[..]);
    ///
    /// assert_eq!(merged.attribute("filename"), Some("records.csv"));
    /// assert_eq!(merged.attribute("source"), Some("upload")); // still inherited
    /// assert_eq!(merged.attribute("fragment.index"), None);
    /// assert_eq!(merged.attribute("segment.original.filename"), None);
    /// ```
    ///
    /// This handles the default attribute keys. For a split that used custom
    /// ones, hand the same [`FragmentKeys`] to
    /// [`defragment_with`](Self::defragment_with).
    #[must_use]
    pub fn defragment(self) -> Self {
        self.defragment_with(&FragmentKeys::default())
    }

    /// Undo a split that numbered its parts with custom keys.
    ///
    /// This is the other end of `Fragments::with_keys`. The same value that
    /// decided what to write decides what to undo, so a custom split is as
    /// reversible as a default one.
    ///
    /// ```
    /// use nififf3::{FlowFile, FragmentKeys};
    ///
    /// let keys = FragmentKeys::default()
    ///     .index_attribute("split.n")
    ///     .original_filename_attribute("split.parent");
    ///
    /// // A part numbered with those keys, as the split left it.
    /// let part = FlowFile::builder()
    ///     .attribute("filename", "record-0")
    ///     .attribute("split.n", "1")
    ///     .attribute("split.parent", "records.csv")
    ///     .content(&b"a"[..]);
    ///
    /// let merged = part
    ///     .derive_keep_uuid()
    ///     .defragment_with(&keys)
    ///     .content(&b"a\nb"[..]);
    ///
    /// assert_eq!(merged.attribute("filename"), Some("records.csv"));
    /// assert_eq!(merged.attribute("split.n"), None);
    /// assert_eq!(merged.attribute("split.parent"), None);
    /// ```
    #[must_use]
    pub fn defragment_with(mut self, keys: &FragmentKeys) -> Self {
        if let Some(filename) = self.attributes.remove(&keys.original_filename) {
            self.attributes.insert(attr::FILENAME.to_string(), filename);
        }
        for key in [&keys.identifier, &keys.index, &keys.count] {
            self.attributes.remove(key);
        }
        self
    }

    /// Finish the build with in-memory content. The size is the content's
    /// length.
    #[must_use]
    pub fn content(self, content: impl Into<Vec<u8>>) -> FlowFile<Vec<u8>> {
        let content = content.into();
        FlowFile::from_raw_parts(content.len() as u64, self.attributes, content)
    }

    /// Finish the build with no content at all.
    ///
    /// A flow file that carries only attributes is a normal thing in NiFi. The
    /// terminator of a fragment set is one example. `content(Vec::new())`
    /// builds the same value, but it reads as a buffer that happens to have
    /// nothing in it.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let done = FlowFile::builder()
    ///     .attribute("filename", "batch-17")
    ///     .attribute("status", "complete")
    ///     .empty();
    ///
    /// assert_eq!(done.size(), 0);
    /// assert_eq!(done.attribute("status"), Some("complete"));
    /// ```
    #[must_use]
    pub fn empty(self) -> FlowFile<Vec<u8>> {
        self.content(Vec::new())
    }

    /// Finish the build with content from a reader.
    ///
    /// The V3 format stores the content size before the content itself, so the
    /// size has to be known up front. `size` is the number of bytes that will
    /// be read from `content` when the flow file is serialized.
    ///
    /// If you don't know the size, use [`buffered`](Self::buffered) to read the
    /// content into memory first. With the `tempfile` feature, `tempfile` and
    /// `spooled` spool it to disk instead.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let content = &b"hello"[..]; // any `impl Read`
    /// let flow_file = FlowFile::builder().reader(content, 5);
    /// let mut out = Vec::new();
    /// flow_file.write_to(&mut out).unwrap();
    /// ```
    #[must_use]
    pub fn reader<R>(self, content: R, size: u64) -> FlowFile<R> {
        FlowFile::from_raw_parts(size, self.attributes, content)
    }

    /// Finish the build by reading `content` to completion into memory.
    ///
    /// Use it when the content size is not known up front. The whole content is
    /// held in memory. For large content prefer `tempfile`, or `spooled` to
    /// stay in memory up to a bound. Both are behind the `tempfile` feature.
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
    /// Use it when the content size is not known up front, and may be too large
    /// to buffer in memory. The file is deleted when the returned flow file is
    /// dropped, or when its content is.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let reader = &b"spooled to disk"[..];
    /// let flow_file = FlowFile::builder().tempfile(reader).unwrap();
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
    /// [`SpooledTempFile`](tempfile::SpooledTempFile). The content is held in
    /// memory up to `max_memory` bytes, and rolled over to an anonymous
    /// temporary file beyond that.
    ///
    /// Use it for content of unknown size that is usually small.
    /// [`buffered`](Self::buffered) always stays in memory, and
    /// [`tempfile`](Self::tempfile) always goes to disk.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let reader = &b"stays in memory"[..];
    /// let flow_file = FlowFile::builder().spooled(reader, 64 * 1024).unwrap();
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

    /// Finish the build by reading an [`AsyncRead`](tokio::io::AsyncRead) to
    /// completion into memory. This is the async version of
    /// [`buffered`](Self::buffered).
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

    /// Finish the build by spooling an [`AsyncRead`](tokio::io::AsyncRead) into
    /// an anonymous temporary file. This is the async version of
    /// [`tempfile`](Self::tempfile).
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

    #[cfg(feature = "uuid")]
    #[test]
    fn defragment_undoes_what_fragments_added() {
        let parent = FlowFile::builder()
            .attribute("filename", "records.csv")
            .attribute("source", "upload")
            .content(&b"a\nb"[..]);

        let mut parts = parent.fragments().with_count(2);
        let part = parts.next_part().attribute("filename", "record-0").content(&b"a"[..]);

        let merged = part.derive().defragment().content(&b"a\nb"[..]);
        let attributes = merged.attributes();

        assert_eq!(attributes["filename"], "records.csv", "restored");
        assert_eq!(attributes["source"], "upload", "still inherited");
        for key in [
            "fragment.identifier",
            "fragment.index",
            "fragment.count",
            "segment.original.filename",
        ] {
            assert!(!attributes.contains_key(key), "{key} should be gone");
        }
    }

    /// A custom split has to be as reversible as a default one. The same
    /// `FragmentKeys` writes the parts and undoes them.
    #[cfg(feature = "uuid")]
    #[test]
    fn defragment_with_undoes_a_custom_split_round_trip() {
        use crate::FragmentKeys;

        let keys = FragmentKeys::default()
            .identifier_attribute("split.id")
            .index_attribute("split.n")
            .count_attribute("split.total")
            .original_filename_attribute("split.parent");

        let parent = FlowFile::builder()
            .attribute("filename", "records.csv")
            .attribute("source", "upload")
            .content(&b"a\nb"[..]);

        let part = parent
            .fragments()
            .with_keys(keys.clone())
            .with_count(2)
            .next_part()
            .attribute("filename", "record-0")
            .content(&b"a"[..]);
        assert_eq!(part.attribute("split.n"), Some("1"));
        assert_eq!(part.attribute("split.parent"), Some("records.csv"));

        let merged = part.derive().defragment_with(&keys).content(&b"a\nb"[..]);
        assert_eq!(merged.attribute("filename"), Some("records.csv"), "restored");
        assert_eq!(merged.attribute("source"), Some("upload"), "still inherited");
        for key in ["split.id", "split.n", "split.total", "split.parent"] {
            assert_eq!(merged.attribute(key), None, "{key} should be gone");
        }
    }

    /// The default keys are the default value of the same thing.
    #[cfg(feature = "uuid")]
    #[test]
    fn defragment_is_defragment_with_the_default_keys() {
        use crate::FragmentKeys;

        let part = FlowFile::builder()
            .attribute("filename", "records.csv")
            .content(Vec::new())
            .fragments()
            .next_part()
            .content(Vec::new());

        // `derive` mints a fresh uuid each call, so compare on everything else.
        assert_eq!(
            part.derive_keep_uuid().defragment().content(Vec::new()),
            part.derive_keep_uuid()
                .defragment_with(&FragmentKeys::default())
                .content(Vec::new())
        );
    }

    #[cfg(feature = "uuid")]
    #[test]
    fn defragment_leaves_a_filename_alone_when_there_was_no_split() {
        let merged = FlowFile::builder()
            .attribute("filename", "plain.txt")
            .content(Vec::new())
            .derive()
            .defragment()
            .content(Vec::new());
        assert_eq!(merged.attributes()["filename"], "plain.txt");
    }

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
        let flow_file = FlowFile::builder()
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
        let big = FlowFile::builder()
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
        let flow_file = FlowFile::builder()
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
