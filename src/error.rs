/// Errors produced when parsing or serializing flow files.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// An underlying I/O error, including unexpected end of input while
    /// parsing the header.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The input does not start with the `NiFiFF3` magic header.
    #[error("invalid magic header: expected \"NiFiFF3\", got {0:?}")]
    InvalidMagic([u8; 7]),

    /// An attribute key or value is not valid UTF-8.
    #[error("attribute is not valid UTF-8: {0}")]
    InvalidAttribute(#[from] std::string::FromUtf8Error),

    /// The content length does not match the size declared in the header.
    #[error("content size mismatch: header declares {expected} bytes, got {actual}")]
    SizeMismatch {
        /// The content size declared in the flow file header.
        expected: u64,
        /// The number of content bytes actually available.
        actual: u64,
    },

    /// Extra bytes remained after the declared content when parsing a buffer
    /// expected to hold exactly one flow file.
    #[error("{0} trailing bytes after flow file content")]
    TrailingData(u64),

    /// The header declares more attributes than the configured
    /// [`Limits`](crate::Limits) allow.
    #[error("attribute count {count} exceeds the limit of {limit}")]
    TooManyAttributes {
        /// The attribute count declared in the header.
        count: usize,
        /// The configured maximum.
        limit: usize,
    },

    /// An attribute key or value is longer than the configured
    /// [`Limits`](crate::Limits) allow.
    #[error("attribute length {len} exceeds the limit of {limit} bytes")]
    AttributeTooLong {
        /// The declared length of the key or value, in bytes.
        len: usize,
        /// The configured maximum.
        limit: usize,
    },
}
