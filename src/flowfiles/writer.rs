use std::{collections::HashMap, pin::Pin, task::Poll};

use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{FlowFile, FlowFileContentReader};

pub struct FlowFileEncoder<W> {
    writer: W,
}

impl<W: AsyncWrite + Unpin> FlowFileEncoder<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    /// Write a streaming flow file.
    ///
    /// # Errors
    /// TODO
    pub async fn write_flow_file<F, Fut>(&mut self, mut ff: FlowFile, f: F) -> anyhow::Result<()>
    where
        F: FnOnce(FlowFileBodyWriter<'_, W>, &mut FlowFileContentReader) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<()>>,
    {
        write_flowfile_header(&mut self.writer, ff.attributes(), ff.len()).await?;

        let body_writer = FlowFileBodyWriter {
            writer: &mut self.writer,
        };

        f(body_writer, ff.body()).await?;

        Ok(())
    }
}

/// Async writer for a single FlowFile body.
///
/// This borrows the underlying encoder writer for the duration
/// of writing the body bytes.
pub struct FlowFileBodyWriter<'a, W> {
    pub(crate) writer: &'a mut W,
}

// Implements AsyncWrite by forwarding to the inner writer.
impl<W: AsyncWrite + Unpin> AsyncWrite for FlowFileBodyWriter<'_, W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<tokio::io::Result<usize>> {
        Pin::new(&mut *self.writer).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<tokio::io::Result<()>> {
        Pin::new(&mut *self.writer).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<tokio::io::Result<()>> {
        Pin::new(&mut *self.writer).poll_shutdown(cx)
    }
}

pub async fn write_flowfile_header<W: AsyncWrite + Unpin>(
    writer: &mut W,
    attributes: &HashMap<String, String>,
    content_len: u64,
) -> std::io::Result<()> {
    writer.write_all(b"NiFiFF3").await?;
    write_field_length(writer, attributes.len()).await?;

    for (key, value) in attributes {
        write_string(writer, key).await?;
        write_string(writer, value).await?;
    }

    writer.write_u64(content_len).await?;

    Ok(())
}

async fn write_field_length<W: AsyncWriteExt + Unpin>(
    w: &mut W,
    len: usize,
) -> tokio::io::Result<()> {
    let Ok(len) = u32::try_from(len) else {
        return Err(tokio::io::Error::other("Field length exceeds u32::MAX"));
    };
    if let Ok(len) = u16::try_from(len) {
        return w.write_u16(len).await;
    }
    w.write_u16(u16::MAX).await?;
    w.write_u32(len).await
}

async fn write_string<W: AsyncWriteExt + Unpin>(w: &mut W, s: &str) -> tokio::io::Result<()> {
    write_field_length(w, s.len()).await?;
    w.write_all(s.as_bytes()).await
}
