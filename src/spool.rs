//! Copying a flow file's content to a temporary file in the background.

use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, ready};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::FlowFile;

/// How much spooled content may sit between the temporary file and the reader.
const PIPE_SIZE: usize = 64 * 1024;

/// Content held in a temporary file that a background task is still filling.
///
/// [`FlowFile::spool_async`] produces one. Reading it yields the content in
/// order, waiting when the reader catches up with the copy rather than
/// reporting the end early.
///
/// If the copy fails, the error surfaces from the read that reaches the point
/// where it stopped. Dropping this stops the copy and releases the temporary
/// file.
#[derive(Debug)]
pub struct SpooledContent {
    inner: tokio::io::DuplexStream,
    failure: Arc<Mutex<Option<io::Error>>>,
    tasks: [JoinHandle<()>; 2],
}

impl AsyncRead for SpooledContent {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        ready!(Pin::new(&mut self.inner).poll_read(cx, buf))?;
        if buf.filled().len() == before {
            // The pipe ended. Either the copy finished, or it stopped on an
            // error the reader has not been told about yet.
            if let Some(err) = self.failure.lock().expect("spool lock").take() {
                return Poll::Ready(Err(err));
            }
        }
        Poll::Ready(Ok(()))
    }
}

impl Drop for SpooledContent {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl<R> FlowFile<R>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    /// Start copying the content to a temporary file in the background, and
    /// return at once.
    ///
    /// The returned flow file keeps this one's attributes and declared
    /// [`size`](FlowFile::size), and its content reads back the same bytes.
    /// Reading it can begin immediately, and the copy runs whether you read or
    /// not.
    ///
    /// Use this when the content arrives over a connection you also answer on.
    /// A producer that reads a request body and writes a response from the
    /// same task deadlocks against a client that sends its whole request
    /// before reading any of the response, and NiFi's client is one of those.
    /// Spooling separates the two: the copy keeps draining the request however
    /// long the response is blocked.
    /// [`FlowFilesResponse`](crate::FlowFilesResponse) describes the deadlock
    /// in full.
    ///
    /// Memory stays bounded whatever the content's size, because the bytes go
    /// to disk. At most 64 KiB of them sit between the file and your reader.
    ///
    /// ```
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// use nififf3::FlowFile;
    ///
    /// let bytes = FlowFile::builder()
    ///     .attribute("filename", "big.tar")
    ///     .content(&b"contents"[..])
    ///     .to_bytes();
    ///
    /// // `spool_async` outlives this call, so the source must be owned.
    /// let parsed = FlowFile::parse_async(std::io::Cursor::new(bytes)).await?;
    /// let spooled = parsed.spool_async()?;
    ///
    /// assert_eq!(spooled.attribute("filename"), Some("big.tar"));
    /// assert_eq!(spooled.size(), 8);
    /// assert_eq!(spooled.into_memory_async().await?.content().as_slice(), b"contents");
    /// # Ok::<(), nififf3::Error>(())
    /// # }).unwrap();
    /// ```
    ///
    /// # Errors
    ///
    /// If the temporary file cannot be created or opened. A failure during the
    /// copy itself is reported later, by the read that reaches it.
    ///
    /// # Panics
    ///
    /// If called outside a tokio runtime, since it spawns the copy.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            level = "debug",
            name = "spool_async",
            skip_all,
            fields(
                uuid = self.attribute(crate::attr::UUID),
                filename = self.attribute(crate::attr::FILENAME),
                size = self.size,
            )
        )
    )]
    pub fn spool_async(self) -> io::Result<FlowFile<SpooledContent>> {
        let file = tempfile::NamedTempFile::new()?;
        // Two independent handles. `try_clone` would share one file offset
        // between the copy and the reader, so each would move the other's
        // position.
        let mut sink = tokio::fs::File::from_std(file.reopen()?);
        let mut source = tokio::fs::File::from_std(file.reopen()?);

        let (size, attributes, mut content) = self.into_parts();
        let failure = Arc::new(Mutex::new(None));
        // How many bytes are in the file, and whether the copy is finished.
        let (progress, mut watch_progress) = watch::channel((0u64, false));

        let copy_failure = Arc::clone(&failure);
        // The copy outlives this call, so it needs the span carried into it.
        // Without that its events would have no flow file attached to them.
        #[cfg(feature = "tracing")]
        let span = tracing::Span::current();
        let copy = tokio::spawn(async move {
            #[cfg(feature = "tracing")]
            let _guard = span.enter();
            // `file` lives until the copy ends, so the path outlives both
            // handles being opened.
            let _file = file;
            let mut buf = vec![0u8; PIPE_SIZE];
            let mut written = 0u64;
            loop {
                let read = match content.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(read) => read,
                    Err(err) => {
                        // Nothing is watching this task. The error waits in
                        // the slot until a read reaches the point where the
                        // copy stopped, and no read may ever get there.
                        #[cfg(feature = "tracing")]
                        tracing::warn!(error = %err, written, "spool source failed");
                        *copy_failure.lock().expect("spool lock") = Some(err);
                        break;
                    }
                };
                if let Err(err) = sink.write_all(&buf[..read]).await {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(error = %err, written, "spool file write failed");
                    *copy_failure.lock().expect("spool lock") = Some(err);
                    break;
                }
                // `tokio::fs::File` buffers, so a returned `write_all` does not
                // mean the bytes are in the file. Publishing the count before
                // they are would have the reader see a short read and take it
                // for the end.
                if let Err(err) = sink.flush().await {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(error = %err, written, "spool file flush failed");
                    *copy_failure.lock().expect("spool lock") = Some(err);
                    break;
                }
                written += read as u64;
                progress.send_replace((written, false));
            }
            #[cfg(feature = "tracing")]
            tracing::debug!(spooled = written, "finished copying the content to disk");
            progress.send_replace((written, true));
        });

        let (mut pipe, inner) = tokio::io::duplex(PIPE_SIZE);
        let follow_failure = Arc::clone(&failure);
        let follow = tokio::spawn(async move {
            let mut position = 0u64;
            let mut buf = vec![0u8; PIPE_SIZE];
            loop {
                let (written, done) = *watch_progress.borrow_and_update();
                if position == written {
                    if done {
                        break;
                    }
                    if watch_progress.changed().await.is_err() {
                        break;
                    }
                    continue;
                }
                let want = usize::try_from(written - position)
                    .unwrap_or(usize::MAX)
                    .min(buf.len());
                match source.read(&mut buf[..want]).await {
                    // Below the count there are bytes to come, so wait for the
                    // next update rather than treating this as the end.
                    Ok(0) => {
                        if watch_progress.changed().await.is_err() {
                            break;
                        }
                    }
                    Ok(read) => {
                        position += read as u64;
                        if pipe.write_all(&buf[..read]).await.is_err() {
                            break; // the reader went away
                        }
                    }
                    Err(err) => {
                        *follow_failure.lock().expect("spool lock") = Some(err);
                        break;
                    }
                }
            }
            let _ = pipe.shutdown().await;
        });

        let content = SpooledContent {
            inner,
            failure,
            tasks: [copy, follow],
        };
        Ok(FlowFile::from_raw_parts(size, attributes, content))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Truncation is the point: the low byte of each step is the sample.
    #[allow(clippy::cast_possible_truncation)]
    fn noise(len: usize) -> Vec<u8> {
        let mut s = 0x9E37_79B9_7F4A_7C15u64;
        (0..len)
            .map(|_| {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                (s >> 24) as u8
            })
            .collect()
    }

    /// The spooled content has to read back exactly what went in, including
    /// content large enough to cross the pipe many times over.
    #[tokio::test]
    async fn spooling_preserves_the_flow_file() {
        for len in [0usize, 1, PIPE_SIZE - 1, PIPE_SIZE, 5 * PIPE_SIZE + 7] {
            let content = noise(len);
            let bytes = FlowFile::builder()
                .attribute("filename", "big.bin")
                .content(content.clone())
                .to_bytes();

            let parsed = FlowFile::parse_async(std::io::Cursor::new(bytes))
                .await
                .unwrap();
            let spooled = parsed.spool_async().unwrap();
            assert_eq!(spooled.size(), len as u64, "{len}");
            assert_eq!(spooled.attribute("filename"), Some("big.bin"));

            let read = spooled.into_memory_async().await.unwrap();
            assert_eq!(read.content(), &content, "{len}");
        }
    }

    /// The copy runs whether or not anything is reading, which is the whole
    /// point: a blocked reader must not stop the source being drained.
    #[tokio::test]
    async fn the_copy_proceeds_while_nothing_reads() {
        let content = noise(4 * PIPE_SIZE);
        let bytes = FlowFile::builder().content(content.clone()).to_bytes();
        let (header, body) = bytes.split_at(bytes.len() - content.len());

        // A source that reports how much of it has been taken.
        let taken = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = Counting {
            inner: std::io::Cursor::new([header, body].concat()),
            taken: Arc::clone(&taken),
        };

        let parsed = FlowFile::parse_async(counted).await.unwrap();
        let spooled = parsed.spool_async().unwrap();

        // Nothing has read a byte of `spooled`, yet the source drains anyway.
        for _ in 0..100 {
            if taken.load(std::sync::atomic::Ordering::Relaxed) == bytes.len() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            taken.load(std::sync::atomic::Ordering::Relaxed),
            bytes.len(),
            "the source should be drained without anyone reading the spool"
        );
        assert_eq!(
            spooled.into_memory_async().await.unwrap().content(),
            &content
        );
    }

    /// A source that fails part-way must report that failure, rather than
    /// looking like content that simply ended.
    #[tokio::test]
    async fn a_failing_source_surfaces_its_error() {
        let bytes = FlowFile::builder().content(vec![7u8; 4096]).to_bytes();
        let parsed = FlowFile::parse_async(Failing {
            inner: std::io::Cursor::new(bytes),
            fail_after: 200,
            read: 0,
        })
        .await
        .unwrap();

        let err = parsed
            .spool_async()
            .unwrap()
            .into_memory_async()
            .await
            .expect_err("the source failed, so the read must fail");
        assert_eq!(err.kind(), std::io::ErrorKind::ConnectionReset);
    }

    struct Counting {
        inner: std::io::Cursor<Vec<u8>>,
        taken: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl AsyncRead for Counting {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let before = buf.filled().len();
            let result = Pin::new(&mut self.inner).poll_read(cx, buf);
            let read = buf.filled().len() - before;
            self.taken
                .fetch_add(read, std::sync::atomic::Ordering::Relaxed);
            result
        }
    }

    struct Failing {
        inner: std::io::Cursor<Vec<u8>>,
        fail_after: usize,
        read: usize,
    }

    impl AsyncRead for Failing {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            if this.read >= this.fail_after {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "source gone",
                )));
            }
            // Small reads, so the failure lands part-way through the content
            // rather than after all of it has already been handed over.
            let want = buf.remaining().min(64);
            let mut chunk = vec![0u8; want];
            let mut into = ReadBuf::new(&mut chunk);
            ready!(Pin::new(&mut this.inner).poll_read(cx, &mut into))?;
            let filled = into.filled().len();
            buf.put_slice(into.filled());
            this.read += filled;
            Poll::Ready(Ok(()))
        }
    }
}
