use std::collections::HashMap;

use crate::{Error, Result};

/// Limits applied while parsing a flow file header, protecting against
/// malicious or corrupt input.
///
/// A crafted header can declare an enormous attribute count or attribute
/// length; without limits the parser will keep reading (and allocating) as
/// long as the input actually provides that many bytes. The plain parsing
/// functions ([`FlowFile::parse`](crate::FlowFile::parse) and friends) trust
/// their input and apply [`Limits::UNLIMITED`]; the `*_with_limits` variants
/// take explicit limits, and the axum extractors apply
/// [`recommended`](Self::recommended).
///
/// Build a set of limits by chaining the `with_max_*` methods onto whichever
/// starting point you want — [`UNLIMITED`](Self::UNLIMITED) for one built up
/// from nothing, [`recommended`](Self::recommended) for one adjusted down from
/// sensible caps. Each takes `None` as well as a value, so a limit can be
/// cleared as well as set:
///
/// ```
/// use nififf3::Limits;
///
/// let limits = Limits::recommended()
///     .with_max_attributes(64)
///     .with_max_content_len(None); // explicitly unbounded
///
/// assert_eq!(limits.max_attributes(), Some(64));
/// assert_eq!(limits.max_content_len(), None);
/// ```
///
/// # What the defaults add up to
///
/// [`max_attributes`](Self::max_attributes) and
/// [`max_attribute_len`](Self::max_attribute_len) are per-attribute, and the
/// latter applies to keys and values separately, so on their own the
/// recommended values would permit 4096 × 2 × 1 MiB — around 8 GiB of header.
/// [`max_total_attribute_len`](Self::max_total_attribute_len) is what actually
/// bounds that, at 2 MiB by default; the other two remain useful because they
/// fail earlier and say something more specific about what was wrong.
///
/// None of this bounds the *content*, which is streamed. Over HTTP that is
/// axum's `DefaultBodyLimit`'s job, and raising it to accept large content does
/// not raise the header budget with it — that is the point of the total.
///
/// Regardless of limits, an attribute buffer grows as bytes arrive rather than
/// to the length the header declares, so a header claiming a 4 GiB key over a
/// short input fails without allocating for it. The one thing sized from the
/// header alone is the attribute map, capped at 1024 entries however many the
/// header claims — so unlimited parsing of a short input stays cheap.
///
/// # Content size
///
/// [`max_content_len`](Self::max_content_len) is off by default, because the
/// content is *streamed*: parsing does not read it, and the declared size on
/// its own costs nothing. Set it when a caller will go on to buffer the
/// content — [`into_memory`](crate::FlowFile::into_memory) and friends — and
/// the declared size should be refused before that happens rather than after.
/// Over HTTP, prefer axum's
/// [`DefaultBodyLimit`](https://docs.rs/axum/latest/axum/extract/struct.DefaultBodyLimit.html),
/// which bounds the bytes actually delivered rather than the bytes claimed;
/// the two are complementary.
///
/// ```
/// use nififf3::{Error, FlowFile, Limits};
///
/// let bytes = FlowFile::builder()
///     .attribute("key", "a value longer than ten bytes")
///     .content(&b"hi"[..])
///     .to_bytes();
///
/// let limits = Limits::recommended().with_max_attribute_len(10);
/// let err = FlowFile::parse_with_limits(bytes.as_slice(), limits).unwrap_err();
/// assert!(matches!(err, Error::AttributeTooLong { .. }));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[expect(
    clippy::struct_field_names,
    reason = "each field is named for the `max_*` accessor that reads it"
)]
pub struct Limits {
    pub(crate) max_attributes: Option<usize>,
    pub(crate) max_attribute_len: Option<usize>,
    pub(crate) max_total_attribute_len: Option<usize>,
    pub(crate) max_content_len: Option<u64>,
}

impl Limits {
    /// No limits at all, matching NiFi's own unpackager.
    ///
    /// The neutral starting point: chain `with_max_*` onto this to build up a
    /// set of limits from nothing, rather than onto
    /// [`recommended`](Self::recommended), which already has some.
    pub const UNLIMITED: Self = Self {
        max_attributes: None,
        max_attribute_len: None,
        max_total_attribute_len: None,
        max_content_len: None,
    };

    /// Sensible caps for untrusted input: at most 4096 attributes, each key
    /// and value at most 1 MiB, 2 MiB of attribute bytes in total, and no cap
    /// on the declared content size.
    ///
    /// This is also [`Default::default`]. Note that it is *not* what the plain
    /// parsers apply — [`FlowFile::parse`](crate::FlowFile::parse) and friends
    /// use [`UNLIMITED`](Self::UNLIMITED), matching NiFi. "Default" here means
    /// the defaults worth starting from, not the crate's default behaviour.
    #[must_use]
    pub const fn recommended() -> Self {
        Self {
            max_attributes: Some(4096),
            max_attribute_len: Some(1 << 20),
            max_total_attribute_len: Some(2 << 20),
            max_content_len: None,
        }
    }

    /// Set or clear the maximum number of attributes.
    #[must_use]
    pub fn with_max_attributes(mut self, limit: impl Into<Option<usize>>) -> Self {
        self.max_attributes = limit.into();
        self
    }

    /// Set or clear the maximum byte length of each attribute key and value.
    #[must_use]
    pub fn with_max_attribute_len(mut self, limit: impl Into<Option<usize>>) -> Self {
        self.max_attribute_len = limit.into();
        self
    }

    /// Set or clear the maximum total byte length of all attribute keys and
    /// values in one header.
    ///
    /// This is the aggregate the per-attribute limits cannot express: 4096
    /// attributes of 1 MiB each are individually fine and collectively
    /// enormous. Checked as the header is read, so it fails part-way through
    /// rather than after the whole header has been taken in.
    ///
    /// It counts key and value bytes only. The framing around them — two to
    /// six bytes of length prefix per field — is not included, so a header is
    /// a little larger than its total; the difference is bounded by
    /// [`max_attributes`](Self::max_attributes).
    ///
    /// ```
    /// use nififf3::{Error, FlowFile, Limits};
    ///
    /// let bytes = FlowFile::builder()
    ///     .attributes((0..10).map(|i| (format!("k{i}"), "v".repeat(100))))
    ///     .content(Vec::new())
    ///     .to_bytes();
    ///
    /// // Every attribute is individually small; together they are not.
    /// let limits = Limits::UNLIMITED.with_max_total_attribute_len(256);
    /// let err = FlowFile::parse_with_limits(bytes.as_slice(), limits).unwrap_err();
    /// assert!(matches!(err, Error::HeaderTooLarge { .. }));
    /// ```
    #[must_use]
    pub fn with_max_total_attribute_len(mut self, limit: impl Into<Option<usize>>) -> Self {
        self.max_total_attribute_len = limit.into();
        self
    }

    /// Set or clear the maximum content size the header may declare, rejecting
    /// a larger one with [`Error::ContentTooLarge`](crate::Error::ContentTooLarge)
    /// before any content is read.
    ///
    /// This bounds what the header *claims*, which is what a caller about to
    /// buffer the content needs to know up front. It says nothing about how
    /// many bytes actually arrive: a header declaring one byte can still be
    /// followed by an endless stream, and only the transport can bound that.
    ///
    /// ```
    /// use nififf3::{Error, FlowFile, Limits};
    ///
    /// let bytes = FlowFile::builder().content(&b"hello"[..]).to_bytes();
    /// let limits = Limits::recommended().with_max_content_len(4);
    ///
    /// let err = FlowFile::parse_with_limits(bytes.as_slice(), limits).unwrap_err();
    /// assert!(matches!(err, Error::ContentTooLarge { size: 5, limit: 4 }));
    /// ```
    #[must_use]
    pub fn with_max_content_len(mut self, limit: impl Into<Option<u64>>) -> Self {
        self.max_content_len = limit.into();
        self
    }

    /// The maximum number of attributes, if there is one.
    #[must_use]
    pub fn max_attributes(&self) -> Option<usize> {
        self.max_attributes
    }

    /// The maximum byte length of each attribute key and value, if there is
    /// one.
    #[must_use]
    pub fn max_attribute_len(&self) -> Option<usize> {
        self.max_attribute_len
    }

    /// The maximum total byte length of all attribute keys and values, if
    /// there is one.
    #[must_use]
    pub fn max_total_attribute_len(&self) -> Option<usize> {
        self.max_total_attribute_len
    }

    /// The maximum content size the header may declare, if there is one.
    #[must_use]
    pub fn max_content_len(&self) -> Option<u64> {
        self.max_content_len
    }

    /// Check attributes and a content size that are already in hand, applying
    /// the same limits the parsers apply while reading.
    ///
    /// For flow files that did not come from this crate's parsers — built by
    /// hand, or decoded from some other representation — where the same caps
    /// should still hold. One difference follows from working on a map rather
    /// than a header: [`max_attributes`](Self::max_attributes) counts distinct
    /// keys here, where a parser counts what the header declared, duplicates
    /// included. The CLI uses it to make `--max-*` mean the same thing
    /// on the JSON path as on the binary one.
    ///
    /// ```
    /// use nififf3::{Error, FlowFile, Limits};
    ///
    /// let flow_file = FlowFile::builder()
    ///     .attribute("filename", "greeting.txt")
    ///     .content(&b"hello"[..]);
    ///
    /// let limits = Limits::UNLIMITED.with_max_content_len(2);
    /// let err = limits.check(flow_file.attributes(), flow_file.size()).unwrap_err();
    /// assert!(matches!(err, Error::ContentTooLarge { size: 5, limit: 2 }));
    /// ```
    ///
    /// # Errors
    ///
    /// The same variants the parsers produce: [`Error::TooManyAttributes`],
    /// [`Error::AttributeTooLong`], [`Error::HeaderTooLarge`] and
    /// [`Error::ContentTooLarge`].
    pub fn check(&self, attributes: &HashMap<String, String>, content_len: u64) -> Result<()> {
        if let Some(limit) = self.max_attributes
            && attributes.len() > limit
        {
            return Err(Error::TooManyAttributes {
                count: attributes.len(),
                limit,
            });
        }
        let mut total = 0usize;
        for (key, value) in attributes {
            for len in [key.len(), value.len()] {
                if let Some(limit) = self.max_attribute_len
                    && len > limit
                {
                    return Err(Error::AttributeTooLong { len, limit });
                }
                total = total.saturating_add(len);
            }
        }
        if let Some(limit) = self.max_total_attribute_len
            && total > limit
        {
            return Err(Error::HeaderTooLarge { len: total, limit });
        }
        if let Some(limit) = self.max_content_len
            && content_len > limit
        {
            return Err(Error::ContentTooLarge {
                size: content_len,
                limit,
            });
        }
        Ok(())
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self::recommended()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_can_be_set_and_cleared() {
        let limits = Limits::recommended()
            .with_max_attributes(64)
            .with_max_content_len(1024)
            .with_max_attribute_len(None);

        assert_eq!(limits.max_attributes(), Some(64));
        assert_eq!(limits.max_content_len(), Some(1024));
        assert_eq!(limits.max_attribute_len(), None, "cleared, not replaced");
        assert_eq!(limits.max_total_attribute_len(), Some(2 << 20));
    }

    #[test]
    fn unlimited_is_the_neutral_starting_point() {
        let limits = Limits::UNLIMITED.with_max_attributes(10);
        assert_eq!(limits.max_attributes(), Some(10));
        assert_eq!(limits.max_attribute_len(), None);
        assert_eq!(limits.max_total_attribute_len(), None);
        assert_eq!(limits.max_content_len(), None);
    }

    #[test]
    fn default_is_recommended() {
        assert_eq!(Limits::default(), Limits::recommended());
    }

    /// `check` and the parsers have to agree, or `--max-*` would mean one
    /// thing on the binary path and another on the JSON one.
    #[test]
    fn check_matches_what_the_parser_enforces() {
        use crate::FlowFile;

        let flow_file = FlowFile::builder()
            .attributes((0..10).map(|i| (format!("k{i}"), "v".repeat(100))))
            .content(&b"hello"[..]);
        let bytes = flow_file.to_bytes();

        for limits in [
            Limits::UNLIMITED.with_max_attributes(5),
            Limits::UNLIMITED.with_max_attribute_len(10),
            Limits::UNLIMITED.with_max_total_attribute_len(256),
            Limits::UNLIMITED.with_max_content_len(2),
            Limits::recommended(),
        ] {
            let parsed = FlowFile::from_bytes_with_limits(&bytes, limits);
            let checked = limits.check(flow_file.attributes(), flow_file.size());
            assert_eq!(
                parsed.is_err(),
                checked.is_err(),
                "{limits:?}: parser {parsed:?}, check {checked:?}"
            );
            if let (Err(parsed), Err(checked)) = (parsed, checked) {
                assert_eq!(
                    std::mem::discriminant(&parsed),
                    std::mem::discriminant(&checked),
                    "{limits:?}: {parsed:?} vs {checked:?}"
                );
            }
        }
    }
}
