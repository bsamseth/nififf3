use std::{collections::HashMap, convert::Infallible, pin::Pin, task::Poll};

use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, AsyncWriteExt};

use crate::{FlowFileContentReader, StreamedFlowFile};

pub trait Storage: AsyncRead + AsyncSeek {
    type Error;

    fn size(&self) -> impl Future<Output = Result<u64, Self::Error>> + Send;
}

impl Storage for std::io::Cursor<Vec<u8>> {
    type Error = Infallible;

    fn size(&self) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        std::future::ready(Ok(self.get_ref().len() as u64))
    }
}

impl Storage for tokio::fs::File {
    type Error = tokio::io::Error;

    async fn size(&self) -> Result<u64, Self::Error> {
        self.metadata().await.map(|m| m.len())
    }
}

pub struct OutputFlowFile<R: AsyncRead + AsyncSeek> {
    /// The full size of the flow file content, not including attributes.
    size: u64,
    /// All attributes stored in the flow file.
    attributes: HashMap<String, String>,
    /// The content of the flow file.
    content: R,
}

pub struct OutputFlowFileWithoutContent(HashMap<String, String>);

impl<R: AsyncRead + AsyncSeek> OutputFlowFile<R> {
    #[must_use]
    pub fn empty() -> OutputFlowFileWithoutContent {
        OutputFlowFileWithoutContent(HashMap::new())
    }
    #[must_use]
    pub fn with_attributes(attributes: HashMap<String, String>) -> OutputFlowFileWithoutContent {
        OutputFlowFileWithoutContent(attributes)
    }
}
impl OutputFlowFileWithoutContent {
    pub fn get_attribute(&self, key: &str) -> Option<&String> {
        self.0.get(key)
    }
    pub fn set_attribute(&mut self, key: String, value: String) -> Option<String> {
        self.0.insert(key, value)
    }
    pub fn with_attribute(&mut self, key: String, value: String) -> &mut Self {
        self.0.insert(key, value);
        self
    }

    pub async fn with_content<S: Storage>(self, content: S) -> Result<OutputFlowFile<S>, S::Error> {
        let size = content.size().await?;
        Ok(OutputFlowFile {
            size,
            attributes: self.0,
            content,
        })
    }
}

impl<S: AsyncRead + AsyncSeek> OutputFlowFile<S> {
    pub fn size(&self) -> u64 {
        self.size
    }
    pub fn get_attribute(&self, key: &str) -> Option<&String> {
        self.attributes.get(key)
    }
    pub fn set_attribute(&mut self, key: String, value: String) -> Option<String> {
        self.attributes.insert(key, value)
    }
    pub fn with_attribute(&mut self, key: String, value: String) -> &mut Self {
        self.attributes.insert(key, value);
        self
    }
    pub fn content(&mut self) -> &mut S {
        &mut self.content
    }
}

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
    pub async fn write_flow_file<F, Fut>(
        &mut self,
        mut ff: StreamedFlowFile,
        f: F,
    ) -> anyhow::Result<()>
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
