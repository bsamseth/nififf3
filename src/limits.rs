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
/// # What the defaults add up to
///
/// The two attribute limits are per-attribute, and apply to keys and values
/// separately, so the defaults permit 4096 × 2 × 1 MiB — around 8 GiB of
/// header. There is no aggregate cap, so what actually bounds a request is the
/// transport: over HTTP, axum's `DefaultBodyLimit`. Raise or disable that to
/// accept large *content* and the header budget rises with it, since neither
/// limit tells the two apart. Set the attribute limits to what your flows
/// really use rather than trusting the defaults to be small.
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
/// content — [`into_bytes`](crate::FlowFile::into_bytes) and friends — and the
/// declared size should be refused before that happens rather than after. Over
/// HTTP, prefer axum's
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
/// let limits = Limits::default().max_attribute_len(10);
/// let err = FlowFile::parse_with_limits(bytes.as_slice(), limits).unwrap_err();
/// assert!(matches!(err, Error::AttributeTooLong { .. }));
/// ```
#[derive(Debug, Clone, Copy)]
#[expect(
    clippy::struct_field_names,
    reason = "each field is named for the `max_*` builder method that sets it"
)]
pub struct Limits {
    pub(crate) max_attributes: Option<usize>,
    pub(crate) max_attribute_len: Option<usize>,
    pub(crate) max_content_len: Option<u64>,
}

impl Limits {
    /// No limits at all, matching NiFi's own unpackager.
    pub const UNLIMITED: Self = Self {
        max_attributes: None,
        max_attribute_len: None,
        max_content_len: None,
    };

    /// The default limits: at most 4096 attributes, each key and value at
    /// most 1 MiB, and no cap on the content size.
    ///
    /// Not a neutral starting point, despite the name — this is
    /// [`Default::default`], caps and all, so `Limits::new().max_attributes(10)`
    /// still carries the 1 MiB attribute length. Start from
    /// [`UNLIMITED`](Self::UNLIMITED) to build a set of limits up from nothing.
    ///
    /// Nor are these the limits the plain parsers apply:
    /// [`FlowFile::parse`](crate::FlowFile::parse) and friends use
    /// [`UNLIMITED`](Self::UNLIMITED), matching NiFi. `Default` here means the
    /// sensible caps for untrusted input, not the crate's default behaviour.
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

    /// Set the maximum content size the header may declare, rejecting a
    /// larger one with [`Error::ContentTooLarge`](crate::Error::ContentTooLarge)
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
    /// let limits = Limits::default().max_content_len(4);
    ///
    /// let err = FlowFile::parse_with_limits(bytes.as_slice(), limits).unwrap_err();
    /// assert!(matches!(err, Error::ContentTooLarge { size: 5, limit: 4 }));
    /// ```
    #[must_use]
    pub fn max_content_len(mut self, limit: u64) -> Self {
        self.max_content_len = Some(limit);
        self
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_attributes: Some(4096),
            max_attribute_len: Some(1 << 20),
            max_content_len: None,
        }
    }
}
