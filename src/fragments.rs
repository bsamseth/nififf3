//! Deriving many flow files from one.

use std::collections::HashMap;

use crate::{FlowFileBuilder, attr};

/// The attribute keys [`Fragments`] writes.
#[derive(Debug, Clone)]
struct Keys {
    identifier: String,
    index: String,
    count: String,
    original_filename: String,
}

impl Default for Keys {
    fn default() -> Self {
        Self {
            identifier: attr::FRAGMENT_ID.to_string(),
            index: attr::FRAGMENT_INDEX.to_string(),
            count: attr::FRAGMENT_COUNT.to_string(),
            original_filename: attr::SEGMENT_ORIGINAL_FILENAME.to_string(),
        }
    }
}

/// A counter for splitting one flow file into many.
///
/// Created with [`FlowFile::fragments`](crate::FlowFile::fragments). Each call
/// to [`next`](Self::next) yields a [`FlowFileBuilder`] carrying the parent's
/// attributes — with a fresh [`uuid`](attr::UUID), as
/// [`derive`](crate::FlowFile::derive) does — plus NiFi's fragment attributes,
/// which let `MergeContent` reassemble the parent in `defragment` mode:
///
/// - [`fragment.identifier`](attr::FRAGMENT_ID) — a random UUID shared by the
///   whole set.
/// - [`fragment.index`](attr::FRAGMENT_INDEX) — a one-up counter from 1.
/// - [`fragment.count`](attr::FRAGMENT_COUNT) — only after
///   [`with_count`](Self::with_count); the total is rarely known up front.
/// - [`segment.original.filename`](attr::SEGMENT_ORIGINAL_FILENAME) — the
///   parent's [`filename`](attr::FILENAME), if it had one.
///
/// Fragment attributes on the parent are dropped rather than inherited, since
/// a new split supersedes the old one.
///
/// ```
/// use nififf3::FlowFile;
///
/// let parent = FlowFile::builder()
///     .attribute("filename", "archive.tar")
///     .attribute("source", "upload")
///     .content(Vec::new());
///
/// let mut parts = parent.fragments().with_count(2);
/// let first = parts.next().attribute("filename", "a.txt").content(&b"a"[..]);
///
/// assert_eq!(first.attributes()["fragment.index"], "1");
/// assert_eq!(first.attributes()["fragment.count"], "2");
/// assert_eq!(first.attributes()["segment.original.filename"], "archive.tar");
/// assert_eq!(first.attributes()["source"], "upload"); // inherited
/// assert_eq!(first.attributes()["filename"], "a.txt"); // overridden
/// ```
#[derive(Debug, Clone)]
pub struct Fragments {
    attributes: HashMap<String, String>,
    identifier: String,
    count: Option<u64>,
    original_filename: Option<String>,
    index: u64,
    keys: Keys,
}

impl Fragments {
    pub(crate) fn new(attributes: &HashMap<String, String>) -> Self {
        let keys = Keys::default();
        let original_filename = attributes.get(attr::FILENAME).cloned();
        // A fresh split supersedes any the parent was itself part of.
        let mut attributes = attributes.clone();
        for key in [&keys.identifier, &keys.index, &keys.count] {
            attributes.remove(key.as_str());
        }
        Self {
            attributes,
            identifier: uuid::Uuid::new_v4().to_string(),
            count: None,
            original_filename,
            index: 0,
            keys,
        }
    }

    /// Use `identifier` instead of the randomly generated fragment
    /// identifier.
    #[must_use]
    pub fn with_identifier(mut self, identifier: impl Into<String>) -> Self {
        self.identifier = identifier.into();
        self
    }

    /// Record the total number of fragments, enabling the
    /// [`fragment.count`](attr::FRAGMENT_COUNT) attribute.
    #[must_use]
    pub fn with_count(mut self, count: u64) -> Self {
        self.count = Some(count);
        self
    }

    /// Use `filename` as the original filename instead of the parent's
    /// [`filename`](attr::FILENAME) attribute.
    #[must_use]
    pub fn with_original_filename(mut self, filename: impl Into<String>) -> Self {
        self.original_filename = Some(filename.into());
        self
    }

    /// Write the fragment identifier under `key` instead of
    /// [`fragment.identifier`](attr::FRAGMENT_ID).
    #[must_use]
    pub fn identifier_attribute(mut self, key: impl Into<String>) -> Self {
        self.keys.identifier = key.into();
        self
    }

    /// Write the fragment index under `key` instead of
    /// [`fragment.index`](attr::FRAGMENT_INDEX).
    #[must_use]
    pub fn index_attribute(mut self, key: impl Into<String>) -> Self {
        self.keys.index = key.into();
        self
    }

    /// Write the fragment count under `key` instead of
    /// [`fragment.count`](attr::FRAGMENT_COUNT).
    #[must_use]
    pub fn count_attribute(mut self, key: impl Into<String>) -> Self {
        self.keys.count = key.into();
        self
    }

    /// Write the original filename under `key` instead of
    /// [`segment.original.filename`](attr::SEGMENT_ORIGINAL_FILENAME).
    #[must_use]
    pub fn original_filename_attribute(mut self, key: impl Into<String>) -> Self {
        self.keys.original_filename = key.into();
        self
    }

    /// The identifier shared by every fragment in this set.
    #[must_use]
    pub fn identifier(&self) -> &str {
        &self.identifier
    }

    /// The number of fragments produced so far, which is also the
    /// [`fragment.index`](attr::FRAGMENT_INDEX) of the most recent one.
    #[must_use]
    pub fn produced(&self) -> u64 {
        self.index
    }

    /// Start the next fragment, advancing the index.
    ///
    /// Finish it by supplying content on the returned builder. Attributes set
    /// there win over the inherited and fragment ones, and
    /// [`without_attribute`](FlowFileBuilder::without_attribute) drops any of
    /// them.
    #[expect(
        clippy::should_implement_trait,
        reason = "`Iterator` would have to yield builders forever, with no way \
                  to stop; `parts.next()` is still the right name at the call site"
    )]
    pub fn next(&mut self) -> FlowFileBuilder {
        self.index += 1;
        let mut builder = FlowFileBuilder::new()
            .attributes(self.attributes.clone())
            .attribute(attr::UUID, uuid::Uuid::new_v4().to_string())
            .attribute(self.keys.identifier.as_str(), self.identifier.as_str())
            .attribute(self.keys.index.as_str(), self.index.to_string());
        if let Some(count) = self.count {
            builder = builder.attribute(self.keys.count.as_str(), count.to_string());
        }
        if let Some(filename) = &self.original_filename {
            builder = builder.attribute(self.keys.original_filename.as_str(), filename.as_str());
        }
        builder
    }
}

#[cfg(test)]
mod tests {
    use crate::FlowFile;

    fn parent() -> FlowFile<Vec<u8>> {
        FlowFile::builder()
            .attribute("filename", "archive.tar")
            .attribute("path", "/in")
            .attribute("uuid", "parent-uuid")
            .content(Vec::new())
    }

    #[test]
    fn indexes_are_one_up_and_share_an_identifier() {
        let mut parts = parent().fragments();
        let first = parts.next().content(Vec::new());
        let second = parts.next().content(Vec::new());

        assert_eq!(first.attributes()["fragment.index"], "1");
        assert_eq!(second.attributes()["fragment.index"], "2");
        assert_eq!(
            first.attributes()["fragment.identifier"],
            second.attributes()["fragment.identifier"]
        );
        assert_eq!(parts.produced(), 2);
        assert_eq!(
            parts.identifier(),
            first.attributes()["fragment.identifier"]
        );
    }

    #[test]
    fn inherits_attributes_but_not_the_parent_uuid() {
        let parent = parent();
        let child = parent.fragments().next().content(Vec::new());

        assert_eq!(child.attributes()["path"], "/in");
        assert_eq!(
            child.attributes()["segment.original.filename"],
            "archive.tar"
        );
        assert_ne!(child.attributes()["uuid"], "parent-uuid");
    }

    #[test]
    fn count_is_absent_unless_requested() {
        assert!(
            !parent()
                .fragments()
                .next()
                .content(Vec::new())
                .attributes()
                .contains_key("fragment.count")
        );
        let child = parent()
            .fragments()
            .with_count(7)
            .next()
            .content(Vec::new());
        assert_eq!(child.attributes()["fragment.count"], "7");
    }

    #[test]
    fn a_new_split_replaces_the_parents_fragment_attributes() {
        let parent = FlowFile::builder()
            .attribute("fragment.identifier", "old")
            .attribute("fragment.index", "3")
            .attribute("fragment.count", "9")
            .content(Vec::new());
        let child = parent.fragments().next().content(Vec::new());

        assert_ne!(child.attributes()["fragment.identifier"], "old");
        assert_eq!(child.attributes()["fragment.index"], "1");
        assert!(!child.attributes().contains_key("fragment.count"));
    }

    #[test]
    fn attribute_keys_are_configurable() {
        let child = parent()
            .fragments()
            .with_count(2)
            .identifier_attribute("split.id")
            .index_attribute("split.n")
            .count_attribute("split.total")
            .original_filename_attribute("split.parent")
            .with_identifier("fixed")
            .next()
            .content(Vec::new());

        assert_eq!(child.attributes()["split.id"], "fixed");
        assert_eq!(child.attributes()["split.n"], "1");
        assert_eq!(child.attributes()["split.total"], "2");
        assert_eq!(child.attributes()["split.parent"], "archive.tar");
        assert!(!child.attributes().contains_key("fragment.index"));
    }

    #[test]
    fn builder_attributes_win_over_inherited_ones() {
        let child = parent()
            .fragments()
            .next()
            .attribute("filename", "entry.txt")
            .without_attribute("path")
            .content(Vec::new());

        assert_eq!(child.attributes()["filename"], "entry.txt");
        assert!(!child.attributes().contains_key("path"));
    }
}
