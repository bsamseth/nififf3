//! Deriving many flow files from one.

#[cfg(feature = "uuid")]
use std::collections::HashMap;

#[cfg(feature = "uuid")]
use crate::{FlowFile, FlowFileBuilder};
use crate::attr;

/// The attribute keys a fragment set is numbered with.
///
/// Defaults to NiFi's own — [`fragment.identifier`](attr::FRAGMENT_ID),
/// [`fragment.index`](attr::FRAGMENT_INDEX),
/// [`fragment.count`](attr::FRAGMENT_COUNT) and
/// [`segment.original.filename`](attr::SEGMENT_ORIGINAL_FILENAME) — which is
/// what to use for anything `MergeContent` will see.
///
/// Worth naming as a value when they are *not* the defaults, because both ends
/// of a split need the same set: `Fragments::with_keys` to write them and
/// [`FlowFileBuilder::defragment_with`](crate::FlowFileBuilder::defragment_with)
/// to undo them.
///
/// ```
/// # #[cfg(feature = "uuid")] {
/// use nififf3::{FlowFile, FragmentKeys};
///
/// let keys = FragmentKeys::default()
///     .index_attribute("split.n")
///     .count_attribute("split.total");
///
/// let parent = FlowFile::builder()
///     .attribute("filename", "records.txt")
///     .content(&b"a\nb"[..]);
/// let part = parent
///     .fragments()
///     .with_keys(keys.clone())
///     .with_count(2)
///     .next_part()
///     .content(&b"a"[..]);
/// assert_eq!(part.attribute("split.n"), Some("1"));
///
/// let merged = part.derive().defragment_with(&keys).content(&b"a\nb"[..]);
/// assert_eq!(merged.attribute("split.n"), None);
/// assert_eq!(merged.attribute("filename"), Some("records.txt"));
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FragmentKeys {
    pub(crate) identifier: String,
    pub(crate) index: String,
    pub(crate) count: String,
    pub(crate) original_filename: String,
}

impl FragmentKeys {
    /// Write the fragment identifier under `key` instead of
    /// [`fragment.identifier`](attr::FRAGMENT_ID).
    #[must_use]
    pub fn identifier_attribute(mut self, key: impl Into<String>) -> Self {
        self.identifier = key.into();
        self
    }

    /// Write the fragment index under `key` instead of
    /// [`fragment.index`](attr::FRAGMENT_INDEX).
    #[must_use]
    pub fn index_attribute(mut self, key: impl Into<String>) -> Self {
        self.index = key.into();
        self
    }

    /// Write the fragment count under `key` instead of
    /// [`fragment.count`](attr::FRAGMENT_COUNT).
    #[must_use]
    pub fn count_attribute(mut self, key: impl Into<String>) -> Self {
        self.count = key.into();
        self
    }

    /// Write the original filename under `key` instead of
    /// [`segment.original.filename`](attr::SEGMENT_ORIGINAL_FILENAME).
    #[must_use]
    pub fn original_filename_attribute(mut self, key: impl Into<String>) -> Self {
        self.original_filename = key.into();
        self
    }

    /// Every key in the set, for dropping a parent's values under them.
    #[cfg(feature = "uuid")]
    pub(crate) fn all(&self) -> [&String; 4] {
        [
            &self.identifier,
            &self.index,
            &self.count,
            &self.original_filename,
        ]
    }
}

impl Default for FragmentKeys {
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
/// to [`next_part`](Self::next_part) yields a [`FlowFileBuilder`] carrying the parent's
/// attributes — with a fresh [`uuid`](attr::UUID), as
/// [`derive`](crate::FlowFile::derive) does — plus NiFi's fragment attributes,
/// which let `MergeContent` reassemble the parent in `defragment` mode:
///
/// - [`fragment.identifier`](attr::FRAGMENT_ID) — a random UUID shared by the
///   whole set.
/// - [`fragment.index`](attr::FRAGMENT_INDEX) — a one-up counter from 1.
/// - [`fragment.count`](attr::FRAGMENT_COUNT) — only after
///   [`with_count`](Self::with_count); the total is rarely known up front.
///   See *Declaring the count* below, because `MergeContent` cannot reassemble
///   a bundle that never declares one.
/// - [`segment.original.filename`](attr::SEGMENT_ORIGINAL_FILENAME) — the
///   parent's [`filename`](attr::FILENAME), if it had one.
///
/// All four are dropped from the inherited attributes rather than carried
/// over, since a new split supersedes the old one — including
/// `segment.original.filename`, so that a parent with no `filename` of its
/// own yields parts with no original filename rather than a grandparent's.
///
/// What every part inherits is otherwise the parent's attributes, and
/// [`attribute`](Self::attribute) and
/// [`without_attribute`](Self::without_attribute) adjust that set once for the
/// whole split rather than on each part.
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
/// let first = parts.next_part().attribute("filename", "a.txt").content(&b"a"[..]);
///
/// assert_eq!(first.attributes()["fragment.index"], "1");
/// assert_eq!(first.attributes()["fragment.count"], "2");
/// assert_eq!(first.attributes()["segment.original.filename"], "archive.tar");
/// assert_eq!(first.attributes()["source"], "upload"); // inherited
/// assert_eq!(first.attributes()["filename"], "a.txt"); // overridden
/// ```
///
/// # Declaring the count
///
/// A bundle has to say how big it is. NiFi's `MergeContent` fills a bin when it
/// holds as many flow files as the [`fragment.count`](attr::FRAGMENT_COUNT) of
/// one of them says, and a bundle that never declares a count is not merged at
/// all — the bin times out and every flow file in it is routed to `failure`.
/// There are two ways to declare it, and a split uses exactly one:
///
/// - The total is known before the parts are built: [`with_count`](Self::with_count),
///   and every part carries it.
/// - The total is only known once the input is exhausted — a stream, an
///   archive, a decoder: [`terminate`](Self::terminate), which ends the set
///   with an empty flow file carrying the count. Nothing has to be held back
///   or rewritten, so the parts can be streamed as they are produced.
#[cfg_attr(docsrs, doc(cfg(feature = "uuid")))]
#[cfg(feature = "uuid")]
#[derive(Debug)]
pub struct Fragments {
    attributes: HashMap<String, String>,
    identifier: String,
    count: Option<u64>,
    original_filename: Option<String>,
    index: u64,
    keys: FragmentKeys,
}

#[cfg(feature = "uuid")]
impl Fragments {
    pub(crate) fn new(attributes: &HashMap<String, String>) -> Self {
        let keys = FragmentKeys::default();
        let original_filename = attributes.get(attr::FILENAME).cloned();
        // A fresh split supersedes any the parent was itself part of, under
        // the default keys. The configured keys are not known yet — they are
        // set on the returned counter — so `part` drops those as well.
        let mut attributes = attributes.clone();
        for key in keys.all() {
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

    /// Use `keys` for all four fragment attributes.
    ///
    /// The form to reach for when the same keys have to be undone later:
    /// hold a [`FragmentKeys`] and hand it to both this and
    /// [`defragment_with`](crate::FlowFileBuilder::defragment_with).
    #[must_use]
    pub fn with_keys(mut self, keys: FragmentKeys) -> Self {
        self.keys = keys;
        self
    }

    /// Add an attribute that every part in this set carries, replacing any
    /// value inherited from the parent.
    ///
    /// The same call as [`FlowFileBuilder::attribute`], made once for the set
    /// instead of on each part — for what is true of the split rather than of
    /// one fragment: the format the parts were cut into, the run that produced
    /// them, the schema they follow.
    ///
    /// It joins the inherited attributes, so it applies to
    /// [`terminate`](Self::terminate) too, and a part that sets the same key on
    /// its own builder still wins.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let parent = FlowFile::builder()
    ///     .attribute("filename", "records.csv")
    ///     .content(&b"a\nb"[..]);
    ///
    /// let mut parts = parent
    ///     .fragments()
    ///     .attribute("mime.type", "text/csv")
    ///     .with_count(2);
    ///
    /// let first = parts.next_part().content(&b"a"[..]);
    /// let second = parts.next_part().attribute("mime.type", "text/plain").content(&b"b"[..]);
    ///
    /// assert_eq!(first.attribute("mime.type"), Some("text/csv"));
    /// assert_eq!(second.attribute("mime.type"), Some("text/plain"), "the part wins");
    /// ```
    ///
    /// The four fragment attributes are not settable this way: they are
    /// computed per part, so a value written here under one of
    /// [`keys`](Self::keys) is replaced by the one the split produces.
    #[must_use]
    pub fn attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }

    /// Add attributes from an iterator of key-value pairs to every part in
    /// this set. The plural of [`attribute`](Self::attribute).
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

    /// Drop an inherited attribute from every part in this set.
    ///
    /// The counterpart to [`attribute`](Self::attribute), for a parent
    /// attribute that does not describe the pieces it was cut into — a
    /// checksum over the whole, a record count, an original size.
    ///
    /// ```
    /// use nififf3::FlowFile;
    ///
    /// let parent = FlowFile::builder()
    ///     .attribute("filename", "records.csv")
    ///     .attribute("record.count", "2")
    ///     .content(&b"a\nb"[..]);
    ///
    /// let part = parent
    ///     .fragments()
    ///     .without_attribute("record.count")
    ///     .next_part()
    ///     .content(&b"a"[..]);
    ///
    /// assert_eq!(part.attribute("record.count"), None);
    /// assert_eq!(part.attribute("segment.original.filename"), Some("records.csv"));
    /// ```
    #[must_use]
    pub fn without_attribute(mut self, key: &str) -> Self {
        self.attributes.remove(key);
        self
    }

    /// The keys this set writes.
    #[must_use]
    pub fn keys(&self) -> &FragmentKeys {
        &self.keys
    }

    /// Write the fragment identifier under `key` instead of
    /// [`fragment.identifier`](attr::FRAGMENT_ID).
    #[must_use]
    pub fn identifier_attribute(mut self, key: impl Into<String>) -> Self {
        self.keys = self.keys.identifier_attribute(key);
        self
    }

    /// Write the fragment index under `key` instead of
    /// [`fragment.index`](attr::FRAGMENT_INDEX).
    #[must_use]
    pub fn index_attribute(mut self, key: impl Into<String>) -> Self {
        self.keys = self.keys.index_attribute(key);
        self
    }

    /// Write the fragment count under `key` instead of
    /// [`fragment.count`](attr::FRAGMENT_COUNT).
    #[must_use]
    pub fn count_attribute(mut self, key: impl Into<String>) -> Self {
        self.keys = self.keys.count_attribute(key);
        self
    }

    /// Write the original filename under `key` instead of
    /// [`segment.original.filename`](attr::SEGMENT_ORIGINAL_FILENAME).
    #[must_use]
    pub fn original_filename_attribute(mut self, key: impl Into<String>) -> Self {
        self.keys = self.keys.original_filename_attribute(key);
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
    ///
    /// # Panics
    ///
    /// If more fragments are produced than [`with_count`](Self::with_count)
    /// declared. NiFi's `MergeContent` bins on `fragment.count`, so an index
    /// past it describes a set that cannot be reassembled — a panic is louder
    /// than shipping a bundle that times out to `failure` in production.
    #[must_use]
    pub fn next_part(&mut self) -> FlowFileBuilder {
        self.index += 1;
        assert!(
            self.count.is_none_or(|count| self.index <= count),
            "fragment {} of a set declared to hold {:?}",
            self.index,
            self.count
        );
        self.part(self.index, self.count)
    }

    /// Finish the set with a terminator: an empty flow file carrying the
    /// [`fragment.count`](attr::FRAGMENT_COUNT) the parts could not know.
    ///
    /// For a split whose total is only known once the input runs out — the
    /// streaming case, where the earlier parts are on the wire long before the
    /// last one is read. NiFi's `MergeContent` needs `fragment.count` on *at
    /// least one* flow file in the bundle and fills a bin when it holds that
    /// many flow files, so a final flow file that declares the count
    /// reassembles correctly: it is one of the flow files in the bin, and
    /// contributes no content to the merge.
    ///
    /// The count therefore includes the terminator itself. After `n` parts the
    /// terminator carries `fragment.index = fragment.count = n + 1`, and this
    /// consumes the counter — a part emitted afterwards would put `n + 2` flow
    /// files in a bin declared to hold `n + 1`, which never fills and, once the
    /// bin times out, routes the whole set to `MergeContent`'s `failure`
    /// relationship.
    ///
    /// Use this *or* [`with_count`](Self::with_count), not both: when the total
    /// is known up front the parts can declare it themselves and no terminator
    /// is needed.
    ///
    /// ```
    /// use nififf3::{FlowFile, FlowFiles, FlowFilesWriter};
    ///
    /// let parent = FlowFile::builder()
    ///     .attribute("filename", "records.txt")
    ///     .content(&b"alpha\nbeta"[..]);
    ///
    /// // A producer that does not know how many parts there will be.
    /// let mut parts = parent.fragments();
    /// let mut out = Vec::new();
    /// let mut writer = FlowFilesWriter::new(&mut out);
    /// for record in parent.content().split(|byte| *byte == b'\n') {
    ///     writer.write_bytes(&parts.next_part().content(record))?;
    /// }
    /// writer.write_bytes(&parts.terminate())?;
    /// writer.finish()?;
    ///
    /// let bundle: Vec<_> = FlowFiles::new(out.as_slice()).collect::<Result<_, _>>()?;
    /// assert_eq!(bundle.len(), 3, "two parts and the terminator");
    ///
    /// // Only the terminator declares the count, and it counts itself.
    /// assert!(!bundle[0].attributes().contains_key("fragment.count"));
    /// assert_eq!(bundle[2].attributes()["fragment.count"], "3");
    /// assert_eq!(bundle[2].attributes()["fragment.index"], "3");
    /// assert_eq!(bundle[2].size(), 0);
    /// # Ok::<(), nififf3::Error>(())
    /// ```
    ///
    /// A consumer doing its own reassembly can recognize the terminator as the
    /// part whose index equals the declared count and whose content is empty.
    /// That is a convention, not a guarantee: an ordinary last part can look
    /// the same, so a producer that emits empty parts *and* needs them told
    /// apart should set an attribute of its own here — the returned flow file
    /// is an ordinary one, and
    /// [`attributes_mut`](crate::FlowFile::attributes_mut) still works.
    ///
    /// # Panics
    ///
    /// If a count was already declared and the terminator would not be the
    /// flow file that completes it.
    #[must_use]
    pub fn terminate(self) -> FlowFile<Vec<u8>> {
        let index = self.index + 1;
        assert!(
            self.count.is_none_or(|count| count == index),
            "a set declared to hold {:?} cannot be terminated as flow file {index}; \
             `with_count` already put the count on the parts",
            self.count
        );
        self.part(index, Some(index)).content(Vec::new())
    }

    /// The attributes every flow file in the set carries, at `index` and with
    /// `count` if it is to declare one.
    fn part(&self, index: u64, count: Option<u64>) -> FlowFileBuilder {
        let mut builder = FlowFileBuilder::new().attributes(self.attributes.clone());
        // The parent's values under *these* keys, which `Fragments::new` could
        // not know. Dropping them first rather than relying on the writes
        // below matters for the two that are conditional: a set that declares
        // no count, or a parent with no filename, must not leave the
        // grandparent's answer standing in for the one this split would give.
        for key in self.keys.all() {
            builder = builder.without_attribute(key);
        }
        builder = builder
            .attribute(attr::UUID, uuid::Uuid::new_v4().to_string())
            .attribute(self.keys.identifier.as_str(), self.identifier.as_str())
            .attribute(self.keys.index.as_str(), index.to_string());
        if let Some(count) = count {
            builder = builder.attribute(self.keys.count.as_str(), count.to_string());
        }
        if let Some(filename) = &self.original_filename {
            builder = builder.attribute(self.keys.original_filename.as_str(), filename.as_str());
        }
        builder
    }
}

#[cfg(all(test, feature = "uuid"))]
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
        let first = parts.next_part().content(Vec::new());
        let second = parts.next_part().content(Vec::new());

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
        let child = parent.fragments().next_part().content(Vec::new());

        assert_eq!(child.attributes()["path"], "/in");
        assert_eq!(
            child.attributes()["segment.original.filename"],
            "archive.tar"
        );
        assert_ne!(child.attributes()["uuid"], "parent-uuid");
    }

    #[test]
    fn terminate_counts_itself_as_the_last_flow_file() {
        let parent = parent();
        let mut parts = parent.fragments();
        let first = parts.next_part().content(&b"a"[..]);
        let second = parts.next_part().content(&b"b"[..]);
        let terminator = parts.terminate();

        // NiFi fills the bin when it holds `fragment.count` flow files, and
        // the terminator is one of them: two parts plus itself.
        assert_eq!(terminator.attributes()["fragment.count"], "3");
        assert_eq!(terminator.attributes()["fragment.index"], "3");
        assert_eq!(terminator.size(), 0, "it contributes nothing to the merge");

        // The count lives on the terminator alone, which is all NiFi asks for.
        for part in [&first, &second] {
            assert!(!part.attributes().contains_key("fragment.count"));
        }

        // And it is a member of the same bundle in every other respect.
        assert_eq!(
            terminator.attributes()["fragment.identifier"],
            first.attributes()["fragment.identifier"]
        );
        assert_eq!(
            terminator.attributes()["segment.original.filename"],
            "archive.tar"
        );
        assert_ne!(terminator.attributes()["uuid"], first.attributes()["uuid"]);
    }

    #[test]
    fn terminating_an_empty_split_is_a_bundle_of_one() {
        let terminator = parent().fragments().terminate();
        assert_eq!(terminator.attributes()["fragment.count"], "1");
        assert_eq!(terminator.attributes()["fragment.index"], "1");
        assert_eq!(terminator.size(), 0);
    }

    #[test]
    fn terminate_honours_custom_attribute_keys() {
        let mut parts = parent().fragments().count_attribute("split.total");
        let _ = parts.next_part();
        let terminator = parts.terminate();

        assert_eq!(terminator.attributes()["split.total"], "2");
        assert!(!terminator.attributes().contains_key("fragment.count"));
    }

    #[test]
    #[should_panic(expected = "cannot be terminated as flow file 3")]
    fn terminating_a_set_that_already_declared_its_count_is_caught() {
        // Two parts of a set declared to hold two: the terminator would be a
        // third flow file in a bin that fills at two.
        let mut parts = parent().fragments().with_count(2);
        let _ = parts.next_part();
        let _ = parts.next_part();
        let _ = parts.terminate();
    }

    /// The counted form and the terminated form describe the same bundle
    /// size, so a consumer can treat them alike.
    #[test]
    fn a_terminated_bundle_declares_the_number_of_flow_files_it_contains() {
        let parent = parent();
        let mut parts = parent.fragments();
        let mut bundle: Vec<_> = (0..4).map(|_| parts.next_part().content(Vec::new())).collect();
        bundle.push(parts.terminate());

        let declared: usize = bundle
            .iter()
            .find_map(|part| part.attributes().get("fragment.count"))
            .expect("some flow file declares the count")
            .parse()
            .unwrap();
        assert_eq!(declared, bundle.len());
    }

    /// The point of setting an attribute on the set: it reaches every part
    /// without being repeated on each, the terminator included.
    #[test]
    fn a_set_wide_attribute_reaches_every_part() {
        let parent = parent();
        let mut parts = parent
            .fragments()
            .attribute("mime.type", "text/csv")
            .attributes([("run", "42"), ("schema", "v3")]);

        let first = parts.next_part().content(Vec::new());
        let second = parts.next_part().content(Vec::new());
        let terminator = parts.terminate();

        for part in [&first, &second, &terminator] {
            assert_eq!(part.attribute("mime.type"), Some("text/csv"));
            assert_eq!(part.attribute("run"), Some("42"));
            assert_eq!(part.attribute("schema"), Some("v3"));
            // And the inherited and fragment attributes are untouched by it.
            assert_eq!(part.attribute("path"), Some("/in"));
            assert!(part.attributes().contains_key("fragment.identifier"));
        }
    }

    /// Set-wide is a default, not an override: a part that says otherwise on
    /// its own builder still wins, as it does over an inherited attribute.
    #[test]
    fn a_part_overrides_a_set_wide_attribute() {
        let parent = parent();
        let mut parts = parent.fragments().attribute("mime.type", "text/csv");

        let plain = parts.next_part().content(Vec::new());
        let special = parts
            .next_part()
            .attribute("mime.type", "text/plain")
            .content(Vec::new());
        let dropped = parts
            .next_part()
            .without_attribute("mime.type")
            .content(Vec::new());

        assert_eq!(plain.attribute("mime.type"), Some("text/csv"));
        assert_eq!(special.attribute("mime.type"), Some("text/plain"));
        assert_eq!(dropped.attribute("mime.type"), None);
    }

    #[test]
    fn a_set_can_drop_an_inherited_attribute_from_every_part() {
        let parent = FlowFile::builder()
            .attribute("filename", "records.csv")
            .attribute("record.count", "2")
            .attribute("path", "/in")
            .content(Vec::new());

        let mut parts = parent.fragments().without_attribute("record.count");
        let part = parts.next_part().content(Vec::new());
        let terminator = parts.terminate();

        for one in [&part, &terminator] {
            assert_eq!(one.attribute("record.count"), None);
            assert_eq!(one.attribute("path"), Some("/in"), "only the named one");
        }
        // Dropping `record.count` says nothing about the filename the split
        // records, which is captured before any of this.
        assert_eq!(
            part.attribute("segment.original.filename"),
            Some("records.csv")
        );
    }

    /// The fragment attributes are computed per part, so writing one under a
    /// fragment key here cannot stand: it would describe the wrong flow file
    /// on every part but at most one.
    #[test]
    fn a_set_wide_attribute_cannot_displace_the_fragment_attributes() {
        let parent = parent();
        let mut parts = parent
            .fragments()
            .attribute("fragment.index", "99")
            .attribute("fragment.identifier", "mine")
            .with_count(2);

        let first = parts.next_part().content(Vec::new());
        assert_eq!(first.attribute("fragment.index"), "1".into());
        assert_ne!(first.attribute("fragment.identifier"), Some("mine"));
        assert_eq!(first.attribute("fragment.count"), Some("2"));
    }

    /// Under custom keys the same has to hold, since it is the configured key
    /// that names a fragment attribute, not the default one.
    #[test]
    fn a_set_wide_attribute_cannot_displace_a_custom_fragment_attribute() {
        let parent = parent();
        let first = custom_keys(parent.fragments())
            .attribute("split.n", "99")
            .next_part()
            .content(Vec::new());
        assert_eq!(first.attribute("split.n"), Some("1"));
    }

    #[test]
    fn count_is_absent_unless_requested() {
        assert!(
            !parent()
                .fragments()
                .next_part()
                .content(Vec::new())
                .attributes()
                .contains_key("fragment.count")
        );
        let child = parent()
            .fragments()
            .with_count(7)
            .next_part()
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
        let child = parent.fragments().next_part().content(Vec::new());

        assert_ne!(child.attributes()["fragment.identifier"], "old");
        assert_eq!(child.attributes()["fragment.index"], "1");
        assert!(!child.attributes().contains_key("fragment.count"));
    }

    #[test]
    #[should_panic(expected = "fragment 3 of a set declared to hold Some(2)")]
    fn producing_more_fragments_than_declared_is_caught() {
        let mut parts = parent().fragments().with_count(2);
        for _ in 0..3 {
            let _ = parts.next_part();
        }
    }

    #[test]
    fn an_undeclared_count_never_runs_out() {
        let mut parts = parent().fragments();
        for _ in 0..3 {
            let _ = parts.next_part();
        }
        assert_eq!(parts.produced(), 3);
    }

    #[test]
    fn a_new_split_replaces_the_parents_original_filename() {
        // A part of an earlier split, re-split. It has no `filename` of its
        // own, so there is no original filename to record for this split.
        let parent = FlowFile::builder()
            .attribute("segment.original.filename", "grandparent.tar")
            .attribute("fragment.index", "3")
            .content(Vec::new());
        let child = parent.fragments().next_part().content(Vec::new());

        assert_eq!(child.attributes()["fragment.index"], "1");
        assert!(
            !child
                .attributes()
                .contains_key("segment.original.filename"),
            "the grandparent's filename must not outlive the split it named"
        );
    }

    /// The custom keys are configured *after* the counter is created, so the
    /// inherited values under them can only be dropped where the parts are
    /// built. A part must never inherit a fragment attribute that does not
    /// describe the split it belongs to.
    #[test]
    fn a_new_split_replaces_the_parents_custom_fragment_attributes() {
        let parent = FlowFile::builder()
            .attribute("split.id", "old")
            .attribute("split.n", "3")
            .attribute("split.total", "9")
            .attribute("split.parent", "grandparent.tar")
            .content(Vec::new());

        let child = custom_keys(parent.fragments()).next_part().content(Vec::new());

        assert_ne!(child.attributes()["split.id"], "old");
        assert_eq!(child.attributes()["split.n"], "1");
        // This split declared no count, and the parent has no `filename`, so
        // neither attribute applies to it — the parent's values must not stand
        // in for the ones this split would have written.
        assert!(
            !child.attributes().contains_key("split.total"),
            "a stale count describes a bundle that cannot be reassembled"
        );
        assert!(
            !child.attributes().contains_key("split.parent"),
            "the grandparent's filename must not outlive the split it named"
        );
    }

    /// The same, for the terminator: it goes through the same attribute path.
    #[test]
    fn a_terminator_replaces_the_parents_custom_fragment_attributes() {
        let parent = FlowFile::builder()
            .attribute("split.parent", "grandparent.tar")
            .content(Vec::new());

        let terminator = custom_keys(parent.fragments()).terminate();

        assert_eq!(terminator.attributes()["split.total"], "1");
        assert!(!terminator.attributes().contains_key("split.parent"));
    }

    /// Configuring custom keys must not stop the default ones being dropped:
    /// a part carrying a parent's `fragment.count` is just as unmergeable.
    #[test]
    fn custom_keys_still_drop_the_default_fragment_attributes() {
        let parent = FlowFile::builder()
            .attribute("fragment.identifier", "old")
            .attribute("fragment.index", "3")
            .attribute("fragment.count", "9")
            .attribute("segment.original.filename", "grandparent.tar")
            .content(Vec::new());

        let child = custom_keys(parent.fragments()).next_part().content(Vec::new());

        for key in [
            "fragment.identifier",
            "fragment.index",
            "fragment.count",
            "segment.original.filename",
        ] {
            assert!(!child.attributes().contains_key(key), "{key} should be gone");
        }
    }

    fn custom_keys(fragments: crate::Fragments) -> crate::Fragments {
        fragments
            .identifier_attribute("split.id")
            .index_attribute("split.n")
            .count_attribute("split.total")
            .original_filename_attribute("split.parent")
    }

    #[test]
    fn a_custom_original_filename_key_leaves_no_default_behind() {
        let parent = FlowFile::builder()
            .attribute("filename", "a.tar")
            .attribute("segment.original.filename", "old.tar")
            .content(Vec::new());
        let child = parent
            .fragments()
            .original_filename_attribute("split.parent")
            .next_part()
            .content(Vec::new());

        assert_eq!(child.attributes()["split.parent"], "a.tar");
        assert!(
            !child
                .attributes()
                .contains_key("segment.original.filename"),
            "one original filename per part, under the requested key"
        );
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
            .next_part()
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
            .next_part()
            .attribute("filename", "entry.txt")
            .without_attribute("path")
            .content(Vec::new());

        assert_eq!(child.attributes()["filename"], "entry.txt");
        assert!(!child.attributes().contains_key("path"));
    }
}
