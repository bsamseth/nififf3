/// Limits applied while parsing a flow file header, protecting against
/// malicious or corrupt input.
///
/// A crafted header can declare an enormous attribute count or attribute
/// length; without limits the parser will keep reading (and allocating) as
/// long as the input actually provides that many bytes. The plain parsing
/// functions ([`FlowFile::parse`](crate::FlowFile::parse) and friends) trust
/// their input and apply [`Limits::UNLIMITED`]; the `*_with_limits` variants
/// take explicit limits, and the axum extractors apply `Limits::default()`.
///
/// Regardless of limits, the parser never allocates more than the input
/// actually provides, so unlimited parsing of a short input stays cheap.
///
/// ```
/// use nififf3::{Error, FlowFile, Limits};
///
/// let bytes = FlowFile::builder()
///     .attribute("key", "a value longer than ten bytes")
///     .content(&b"hi"[..])
///     .to_bytes();
///
/// let limits = Limits::default().max_attribute_len(10);
/// let err = FlowFile::parse_with_limits(bytes.as_slice(), &limits).unwrap_err();
/// assert!(matches!(err, Error::AttributeTooLong { .. }));
/// ```
#[derive(Debug, Clone)]
pub struct Limits {
    pub(crate) max_attributes: Option<usize>,
    pub(crate) max_attribute_len: Option<usize>,
}

impl Limits {
    /// No limits at all, matching NiFi's own unpackager.
    pub const UNLIMITED: Self = Self {
        max_attributes: None,
        max_attribute_len: None,
    };

    /// The default limits: at most 4096 attributes, each key and value at
    /// most 1 MiB.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the maximum number of attributes.
    #[must_use]
    pub fn max_attributes(mut self, limit: usize) -> Self {
        self.max_attributes = Some(limit);
        self
    }

    /// Set the maximum byte length of each attribute key and value.
    #[must_use]
    pub fn max_attribute_len(mut self, limit: usize) -> Self {
        self.max_attribute_len = Some(limit);
        self
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_attributes: Some(4096),
            max_attribute_len: Some(1 << 20),
        }
    }
}
