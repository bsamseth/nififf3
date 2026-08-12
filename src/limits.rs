use std::collections::HashMap;

use crate::{Error, Result};

/// Limits applied while parsing a flow file header, protecting against
/// malicious or corrupt input.
///
/// A crafted header can declare an enormous attribute count, or an enormous
/// attribute length. Without limits the parser keeps reading, and allocating,
/// for as long as the input really provides that many bytes. The plain parsing
/// functions apply [`Limits::UNLIMITED`] and trust their input, as
/// [`FlowFile::parse`](crate::FlowFile::parse) and the calls beside it do. The
/// `*_with_limits` variants take explicit limits, and the axum extractors
/// apply [`recommended`](Self::recommended).
///
/// Build a set of limits by chaining the `with_max_*` methods onto a starting
/// point. Use [`UNLIMITED`](Self::UNLIMITED) to build up from nothing, and
/// [`recommended`](Self::recommended) to adjust down from sensible caps. Each
/// method takes `None` as well as a value, so you can clear a limit as well as
/// set one:
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
/// second one applies to keys and values separately. On their own, the
/// recommended values would permit 4096 × 2 × 1 MiB, which is around 8 GiB of
/// header. [`max_total_attribute_len`](Self::max_total_attribute_len) is what
/// bounds that, at 2 MiB by default. The other two are still useful, because
/// they fail earlier and say something more specific about what was wrong.
///
/// None of this bounds the content, because the content is streamed. Over HTTP,
/// bounding it is axum's `DefaultBodyLimit`'s job. Raising that limit to accept
/// large content does not raise the header budget with it, and keeping the two
/// apart is the point of the total.
///
/// Whatever the limits, no buffer is sized from a declared length. The parser
/// reserves at most 64 KiB up front. After that it reserves only as far as the
/// bytes that have actually arrived. So if a header claims a 4 GiB key and the
/// input is short, the parser allocates 64 KiB. The one other thing sized from
/// the header alone is the attribute map, and that is capped at 1024 entries
/// however many the header claims. Parsing a short input with no limits at all
/// therefore stays cheap.
///
/// Both attribute limits apply to the length a field declares, before its bytes
/// are read. So the total bounds how much the parser buffers, and not only what
/// it accepts in the end. In other words, an attribute that claims to be larger
/// than the remaining budget is rejected on its declaration, and never read
/// into memory.
///
/// # Content size
///
/// [`max_content_len`](Self::max_content_len) is off by default, because
/// parsing streams the content instead of reading it, and the declared size on
/// its own costs nothing. Set it when the caller will go on to buffer the
/// content with [`into_memory`](crate::FlowFile::into_memory) or one of the
/// calls beside it, and should learn up front that the size is unacceptable.
/// Over HTTP, reach for axum's
/// [`DefaultBodyLimit`](https://docs.rs/axum/latest/axum/extract/struct.DefaultBodyLimit.html)
/// as well. It bounds the bytes that actually arrive, where this bounds the
/// bytes the header claims, so the two work together.
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
    /// This is the neutral starting point. Chain `with_max_*` onto it to build
    /// a set of limits up from nothing.
    /// [`recommended`](Self::recommended) already has caps in place, so
    /// chaining onto that one adjusts them instead.
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
    /// This is also [`Default::default`]. The plain parsers do not apply it:
    /// [`FlowFile::parse`](crate::FlowFile::parse) and the calls beside it use
    /// [`UNLIMITED`](Self::UNLIMITED), matching NiFi. "Default" here means the
    /// defaults worth starting from, and not the crate's default behavior.
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
    /// This is the aggregate the per-attribute limits cannot express. 4096
    /// attributes of 1 MiB each are fine one at a time, and enormous together.
    /// It is checked as the header is read, so it fails part-way through
    /// rather than once the whole header is in memory.
    ///
    /// Each field is checked against what is left of the budget before its
    /// bytes are read. So this bounds how much the parser buffers, and that
    /// makes it useful on its own, with no
    /// [`max_attribute_len`](Self::max_attribute_len) beside it.
    ///
    /// It counts key and value bytes only. The framing around them is not
    /// included, meaning the two to six bytes of length prefix per field. A
    /// header is therefore a little larger than its total, and
    /// [`max_attributes`](Self::max_attributes) bounds the difference.
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
    /// This bounds what the header claims, which is what a caller about to
    /// buffer the content needs to know up front. It says nothing about how
    /// many bytes actually arrive. A header declaring one byte can still be
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
    /// Use it for a flow file that did not come from this crate's parsers, such
    /// as one you built by hand or decoded from some other representation,
    /// where the same caps should still hold. Working on a map rather than on
    /// a header makes one difference. [`max_attributes`](Self::max_attributes)
    /// counts distinct keys here, where a parser counts what the header
    /// declared, duplicates included. The CLI uses this call so that `--max-*`
    /// means the same thing on the JSON path as on the binary one.
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
