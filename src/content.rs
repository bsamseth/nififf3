//! The content of a flow file read straight off a stream.

use std::io::{self, Read};

/// A flow file's content, as a reader limited to its declared size.
///
/// [`FlowFile::parse`](crate::FlowFile::parse) and the readers beside it
/// produce one. It yields exactly [`size`](Self::size) bytes from the stream
/// underneath and then reports the end, so the stream is left on the byte
/// after the content, where the next flow file begins.
///
/// A stream that ends early is an error, not a short read. The error is an
/// [`std::io::Error`] of kind [`UnexpectedEof`](io::ErrorKind::UnexpectedEof)
/// carrying [`Error::SizeMismatch`](crate::Error::SizeMismatch), raised by the
/// read that reaches the end. A decoder reading this content therefore fails
/// at the truncation instead of treating it as the end of the data.
///
/// It implements [`std::io::Read`], and
/// [`tokio::io::AsyncRead`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncRead.html)
/// under the `tokio` feature, whichever the inner reader does.
#[derive(Debug)]
pub struct Content<R> {
    inner: R,
    size: u64,
    remaining: u64,
}

impl<R> Content<R> {
    /// Limit `inner` to `size` bytes.
    pub(crate) fn new(inner: R, size: u64) -> Self {
        Self {
            inner,
            size,
            remaining: size,
        }
    }

    /// The declared size of the content.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// How many bytes have not been read yet.
    pub fn remaining(&self) -> u64 {
        self.remaining
    }

    /// A reference to the stream underneath.
    pub fn get_ref(&self) -> &R {
        &self.inner
    }

    /// A mutable reference to the stream underneath.
    ///
    /// Reading from it directly moves the stream without this content
    /// noticing, so the count of what remains goes wrong.
    pub fn get_mut(&mut self) -> &mut R {
        &mut self.inner
    }

    /// Give up the content, returning the stream underneath.
    ///
    /// The stream sits wherever the content was last read to. Read the
    /// content to its end first if the stream is to be used for the flow file
    /// that follows.
    pub fn into_inner(self) -> R {
        self.inner
    }

    /// Account for `read` bytes having come out of the stream, raising the
    /// truncation error if the stream ended early.
    fn took(&mut self, read: usize) -> io::Result<()> {
        if read == 0 && self.remaining > 0 {
            return Err(crate::error::truncated(
                self.size,
                self.size - self.remaining,
            ));
        }
        self.remaining -= read as u64;
        Ok(())
    }

    /// How much of `len` may be read without running into the next flow file.
    fn allowed(&self, len: usize) -> usize {
        usize::try_from(self.remaining).map_or(len, |remaining| remaining.min(len))
    }
}

impl<R: Read> Read for Content<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.remaining == 0 || buf.is_empty() {
            return Ok(0);
        }
        let allowed = self.allowed(buf.len());
        let read = self.inner.read(&mut buf[..allowed])?;
        self.took(read)?;
        Ok(read)
    }
}

#[cfg(feature = "tokio")]
impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for Content<R> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        use tokio::io::AsyncReadExt as _;

        let this = self.get_mut();
        if this.remaining == 0 || buf.remaining() == 0 {
            return std::task::Poll::Ready(Ok(()));
        }
        // `Take` does the bounded fill of the `ReadBuf`. Doing it by hand
        // means initializing memory, which needs unsafe, and this crate
        // forbids that.
        let before = buf.filled().len();
        let mut take = (&mut this.inner).take(this.remaining);
        std::task::ready!(std::pin::Pin::new(&mut take).poll_read(cx, buf))?;
        let read = buf.filled().len() - before;
        std::task::Poll::Ready(this.took(read))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;

    fn is_truncation(err: &io::Error, expected: u64, actual: u64) -> bool {
        err.kind() == io::ErrorKind::UnexpectedEof
            && matches!(
                err.get_ref().and_then(|e| e.downcast_ref::<Error>()),
                Some(Error::SizeMismatch { expected: e, actual: a }) if *e == expected && *a == actual
            )
    }

    #[test]
    fn reads_exactly_the_declared_size_and_leaves_the_rest() {
        let mut source: &[u8] = b"hello, world";
        let mut content = Content::new(&mut source, 5);
        let mut out = String::new();
        content.read_to_string(&mut out).unwrap();
        assert_eq!(out, "hello");
        assert_eq!(content.remaining(), 0);
        assert_eq!(source, b", world");
    }

    #[test]
    fn a_stream_that_ends_early_is_an_error_not_an_end() {
        let mut content = Content::new(&b"hel"[..], 5);
        let mut out = Vec::new();
        let err = content.read_to_end(&mut out).unwrap_err();
        assert!(is_truncation(&err, 5, 3), "{err:?}");
        assert_eq!(out, b"hel", "what did arrive was delivered first");
    }

    #[test]
    fn an_empty_content_reads_nothing_and_never_errs() {
        let mut content = Content::new(&b"next flow file"[..], 0);
        let mut out = Vec::new();
        assert_eq!(content.read_to_end(&mut out).unwrap(), 0);
        assert_eq!(content.into_inner(), b"next flow file");
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn the_async_reader_matches_the_sync_one() {
        use tokio::io::AsyncReadExt;

        let mut content = Content::new(&b"hello, world"[..], 5);
        let mut out = String::new();
        AsyncReadExt::read_to_string(&mut content, &mut out)
            .await
            .unwrap();
        assert_eq!(out, "hello");
        assert_eq!(content.into_inner(), b", world");

        let mut short = Content::new(&b"hel"[..], 5);
        let mut out = Vec::new();
        let err = AsyncReadExt::read_to_end(&mut short, &mut out)
            .await
            .unwrap_err();
        assert!(is_truncation(&err, 5, 3), "{err:?}");
    }
}
