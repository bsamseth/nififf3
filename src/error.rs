/// Build the error for content that ran out before its declared size.
///
/// It is an [`std::io::Error`] carrying the structured
/// [`Error::SizeMismatch`] as its payload.
pub(crate) fn truncated(expected: u64, actual: u64) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::UnexpectedEof,
        Error::SizeMismatch { expected, actual },
    )
}

/// Recover the structured error an [`std::io::Error`] carries.
///
/// This is the inverse of [`truncated`], and it is the whole of
/// `From<io::Error> for Error`. It lets an API returning [`Error`] report a
/// truncation as [`Error::SizeMismatch`], rather than burying it one level
/// down in [`Error::Io`].
///
/// Only the exact shape [`truncated`] produces is unwrapped, meaning an
/// [`UnexpectedEof`](std::io::ErrorKind::UnexpectedEof) carrying a
/// [`SizeMismatch`](Error::SizeMismatch). Any other carried [`Error`] stays
/// inside [`Error::Io`], because it came from somewhere else. Readers compose,
/// so one flow-file stream can sit inside another, and the outer one must not
/// report the inner stream's `InvalidMagic` as if it were its own.
pub(crate) fn unwrap_io(err: std::io::Error) -> Error {
    let is_truncation = err.kind() == std::io::ErrorKind::UnexpectedEof
        && matches!(
            err.get_ref()
                .and_then(|inner| inner.downcast_ref::<Error>()),
            Some(Error::SizeMismatch { .. })
        );
    if !is_truncation {
        return Error::Io(err);
    }
    match err.into_inner().and_then(|inner| inner.downcast().ok()) {
        Some(inner) => *inner,
        // Unreachable given the check above, but not worth a panic.
        None => Error::Io(std::io::ErrorKind::UnexpectedEof.into()),
    }
}

/// A write was attempted on a writer whose stream is already mid-flow-file.
pub(crate) fn poisoned() -> std::io::Error {
    Error::WriterPoisoned.into()
}

/// Errors produced when parsing or serializing flow files.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// An underlying I/O error, including unexpected end of input while
    /// parsing the header.
    ///
    /// Converting an [`std::io::Error`] into an [`Error`] goes through
    /// [`From`], which recovers a truncation as
    /// [`SizeMismatch`](Self::SizeMismatch) instead of wrapping it here.
    ///
    /// The variant is transparent, because it adds nothing to what the error
    /// it carries already says. It displays as that error, and reports that
    /// error as its source. A prefix here would be printed twice by anything
    /// that walks the chain.
    #[error(transparent)]
    Io(std::io::Error),

    /// The input does not start with the `NiFiFF3` magic header.
    #[error("invalid magic header: expected \"NiFiFF3\", got {0:?}")]
    InvalidMagic([u8; 7]),

    /// An attribute key or value is not valid UTF-8.
    ///
    /// This is not a [`From`] conversion, on purpose. With `#[from]`, every
    /// [`String::from_utf8`] failure in your own code would get a silent `?`
    /// into this variant. It would then report "attribute is not valid UTF-8"
    /// about a value that was never an attribute.
    #[error("attribute is not valid UTF-8: {0}")]
    InvalidAttribute(std::string::FromUtf8Error),

    /// The content length does not match the size declared in the header.
    ///
    /// Everything that yields this crate's [`Error`] returns this variant
    /// directly. That is [`FlowFile::from_bytes`](crate::FlowFile::from_bytes),
    /// which validates a whole buffer, along with the
    /// [`FlowFiles`](crate::FlowFiles) reader and its async twin.
    ///
    /// The operations that only move content around return
    /// [`std::io::Result`] instead. Those are `write_to`, `into_memory` and
    /// their async twins. They report the same condition as an
    /// [`std::io::Error`] of kind
    /// [`UnexpectedEof`](std::io::ErrorKind::UnexpectedEof) carrying this
    /// value. [`io::Error::get_ref`](std::io::Error::get_ref) and
    /// `downcast_ref` recover it.
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

    /// The attribute keys and values together exceed the total the configured
    /// [`Limits`](crate::Limits) allow.
    ///
    /// Raised part-way through the header, as soon as the running total passes
    /// the limit, rather than after the whole header has been read.
    #[error("attribute bytes total {len} which exceeds the limit of {limit}")]
    HeaderTooLarge {
        /// The running total of attribute key and value bytes when the limit
        /// was passed.
        len: usize,
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

    /// A write was attempted on a [`FlowFilesWriter`](crate::FlowFilesWriter)
    /// or its async twin after an earlier write failed, leaving the stream
    /// part-way through a flow file.
    ///
    /// The writers return [`std::io::Result`], so this is reported as an
    /// [`std::io::Error`] of kind
    /// [`BrokenPipe`](std::io::ErrorKind::BrokenPipe) carrying this value.
    /// `get_ref` and `downcast_ref` recover it. Use that to tell a poisoned
    /// writer from any other write failure, instead of matching on the
    /// message.
    #[error(
        "flow file writer is poisoned: an earlier write left a truncated \
         flow file in the stream"
    )]
    WriterPoisoned,

    /// The header declares more content than the configured
    /// [`Limits`](crate::Limits) allow.
    ///
    /// Raised from the declared size alone, before any content is read.
    #[error("content size {size} exceeds the limit of {limit} bytes")]
    ContentTooLarge {
        /// The content size declared in the flow file header.
        size: u64,
        /// The configured maximum.
        limit: u64,
    },
}

/// Convert an [`std::io::Error`] into an [`Error`], recovering a truncation
/// rather than wrapping it.
///
/// An [`UnexpectedEof`](std::io::ErrorKind::UnexpectedEof) carrying an
/// [`Error::SizeMismatch`] converts back to that variant, and not to
/// [`Error::Io`] around it. That shape is how `into_memory`, `write_to` and
/// the writers report content that ends early.
///
/// So `?` on an [`std::io::Result`] inside an [`Error`]-returning function
/// yields the same variant the parsing entry points would have produced for
/// the same condition:
///
/// ```
/// use nififf3::{Error, FlowFile};
///
/// fn buffer(bytes: &[u8]) -> Result<FlowFile<Vec<u8>>, Error> {
///     Ok(FlowFile::parse(bytes)?.into_memory()?) // `?` on an `io::Result`
/// }
///
/// let mut truncated = FlowFile::builder().content(&b"hello"[..]).to_bytes();
/// truncated.truncate(truncated.len() - 2);
///
/// assert!(matches!(
///     buffer(&truncated),
///     Err(Error::SizeMismatch { expected: 5, actual: 3 })
/// ));
/// ```
///
/// Every other [`std::io::Error`] becomes [`Error::Io`], including one that
/// happens to carry some other [`Error`]. That payload belongs to whatever
/// produced it, and that may be an entirely different flow-file stream further
/// down the reader stack.
impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        unwrap_io(err)
    }
}

/// Convert an [`Error`] into an [`std::io::Error`], so that `?` on a parsing
/// function works inside a function returning [`std::io::Result`].
///
/// [`Error::Io`] unwraps back to the error it carries. Every other variant
/// becomes a payload on an [`std::io::Error`], under the kind the rest of the
/// crate uses for it. That is
/// [`UnexpectedEof`](std::io::ErrorKind::UnexpectedEof) for a truncation, and
/// [`InvalidData`](std::io::ErrorKind::InvalidData) otherwise.
///
/// [`Error::SizeMismatch`] round-trips, so converting it back recovers the
/// variant. That is what lets an [`std::io::Result`] from `into_memory` or
/// from a writer carry a truncation into an [`Error`]-returning caller
/// intact. The other variants convert back to [`Error::Io`], because an
/// [`std::io::Error`] this crate did not build is not one it should take
/// apart.
///
/// ```
/// use nififf3::{Error, FlowFile};
///
/// fn read(bytes: &[u8]) -> std::io::Result<FlowFile<Vec<u8>>> {
///     Ok(FlowFile::from_bytes(bytes)?) // `?` on a `nififf3::Result`
/// }
///
/// let err = read(b"not a flow file").unwrap_err();
/// assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
/// assert!(matches!(
///     err.get_ref().and_then(|e| e.downcast_ref::<Error>()),
///     Some(Error::InvalidMagic(_))
/// ));
/// ```
impl From<Error> for std::io::Error {
    fn from(err: Error) -> Self {
        use std::io::ErrorKind;

        match err {
            Error::Io(err) => err,
            err => {
                let kind = match err {
                    Error::SizeMismatch { .. } => ErrorKind::UnexpectedEof,
                    // Not bad data: the stream itself is no longer usable.
                    Error::WriterPoisoned => ErrorKind::BrokenPipe,
                    _ => ErrorKind::InvalidData,
                };
                std::io::Error::new(kind, err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind;

    /// Every variant converts into an `io::Error` under a sensible kind, and
    /// keeps its message and its payload. So `downcast_ref` recovers the
    /// detail whichever variant it was.
    #[test]
    fn every_error_converts_into_an_io_error_that_carries_it() {
        for (err, kind) in [
            (
                Error::SizeMismatch {
                    expected: 5,
                    actual: 3,
                },
                ErrorKind::UnexpectedEof,
            ),
            (Error::InvalidMagic([0; 7]), ErrorKind::InvalidData),
            (Error::TrailingData(2), ErrorKind::InvalidData),
        ] {
            let text = err.to_string();
            let io: std::io::Error = err.into();
            assert_eq!(io.kind(), kind);
            assert_eq!(io.to_string(), text);
            assert!(
                io.get_ref()
                    .is_some_and(<dyn std::error::Error + Send + Sync>::is::<Error>)
            );
        }
    }

    /// Coming back the other way is narrower on purpose. Only a truncation is
    /// unwrapped, because only a truncation is a condition this stream can be
    /// sure it caused. See [`unwrap_io`].
    #[test]
    fn only_a_truncation_round_trips_back_into_its_variant() {
        let round_tripped = Error::from(std::io::Error::from(Error::SizeMismatch {
            expected: 5,
            actual: 3,
        }));
        assert!(
            matches!(
                round_tripped,
                Error::SizeMismatch {
                    expected: 5,
                    actual: 3
                }
            ),
            "{round_tripped:?}"
        );

        for err in [Error::InvalidMagic([0; 7]), Error::TrailingData(2)] {
            let text = err.to_string();
            let back = Error::from(std::io::Error::from(err));
            assert!(matches!(back, Error::Io(_)), "{back:?}");
            // Still legible, and the payload is still there to downcast.
            assert!(back.to_string().contains(&text));
        }
    }

    /// `Io` adds nothing to what it carries, so it must not restate it. With a
    /// prefix of its own, anything printing the chain saw the same sentence
    /// twice. Because the variant is transparent, it displays as the error it
    /// wraps.
    #[test]
    fn io_displays_as_the_error_it_carries_exactly_once() {
        fn chain(err: &Error) -> Vec<String> {
            let mut err: Option<&(dyn std::error::Error + 'static)> = Some(err);
            std::iter::from_fn(|| {
                let current = err?;
                err = current.source();
                Some(current.to_string())
            })
            .collect()
        }

        let plain = Error::Io(std::io::Error::new(ErrorKind::ConnectionReset, "gone"));
        assert_eq!(plain.to_string(), "gone", "no prefix of its own");
        assert_eq!(chain(&plain), ["gone"], "and nothing repeats it");

        // Carrying one of this crate's errors reads the same way: `io::Error`
        // displays as its payload and reports that payload's source rather
        // than the payload itself, so the message appears once here too.
        let carried = Error::Io(std::io::Error::new(
            ErrorKind::InvalidData,
            Error::InvalidMagic([0; 7]),
        ));
        assert_eq!(chain(&carried).len(), 1, "{:?}", chain(&carried));
        assert!(carried.to_string().contains("invalid magic"));

        // The payload is still a value, and `unwrap_io` reaches it the same
        // way. Transparency costs nothing structurally.
        let Error::Io(ref io) = carried else {
            panic!("not an Io error");
        };
        assert!(matches!(
            io.get_ref().and_then(|e| e.downcast_ref::<Error>()),
            Some(Error::InvalidMagic(_))
        ));
    }

    /// Poisoning is reported as a value you can match on. The writers return
    /// `io::Result`, so it travels as a payload under a kind that says the
    /// stream is unusable.
    #[test]
    fn poisoning_is_recoverable_from_the_io_error() {
        let err = poisoned();
        assert_eq!(err.kind(), ErrorKind::BrokenPipe);
        assert!(matches!(
            err.get_ref().and_then(|e| e.downcast_ref::<Error>()),
            Some(Error::WriterPoisoned)
        ));
        assert!(err.to_string().contains("poisoned"));
    }

    #[test]
    fn an_io_error_survives_a_trip_through_error() {
        let io = std::io::Error::new(ErrorKind::ConnectionReset, "gone");
        let back: std::io::Error = Error::Io(io).into();
        assert_eq!(back.kind(), ErrorKind::ConnectionReset);
        assert_eq!(back.to_string(), "gone");
    }

    /// This covers the public conversion as well as the private helper. `?` on
    /// an `io::Result` must land on the same variant the parsers produce.
    #[test]
    fn a_truncation_survives_the_public_conversion() {
        let err = Error::from(truncated(5, 3));
        assert!(
            matches!(
                err,
                Error::SizeMismatch {
                    expected: 5,
                    actual: 3
                }
            ),
            "{err:?}"
        );
    }

    /// Any other carried error belongs to whoever built it. A flow-file reader
    /// wrapping another must not adopt the inner stream's failure as its own.
    #[test]
    fn an_unrelated_carried_error_stays_io() {
        for inner in [
            std::io::Error::new(ErrorKind::InvalidData, Error::InvalidMagic([0; 7])),
            // Same payload type and the same kind a truncation uses, so only
            // the variant tells them apart.
            std::io::Error::new(ErrorKind::UnexpectedEof, Error::InvalidMagic([0; 7])),
            std::io::Error::new(ErrorKind::UnexpectedEof, Error::TrailingData(2)),
        ] {
            let err = Error::from(inner);
            assert!(matches!(err, Error::Io(_)), "{err:?}");
        }
    }

    #[test]
    fn truncated_and_unwrap_io_are_inverses() {
        let err = unwrap_io(truncated(9, 4));
        assert!(matches!(
            err,
            Error::SizeMismatch {
                expected: 9,
                actual: 4
            }
        ));
    }
}
