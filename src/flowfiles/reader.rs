use std::{collections::HashMap, io, pin::Pin};

use axum::body::BodyDataStream;
use futures::{Stream, TryStream, TryStreamExt};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::{bytes::Bytes, io::StreamReader};

/// A NiFi Flow File V3 with a streamed body.
///
/// The attributes from the flow file are available directly in memory,
/// while the body is streamed.
pub struct FlowFile<'a> {
    size: u64,
    attributes: HashMap<String, String>,
    contents: FlowFileContentReader<'a>,
}

impl<'a> FlowFile<'a> {
    /// The length of the body of the flow file.
    ///
    /// Note that this is not how many bytes may be left in the [`Self::body()`] reader,
    /// but rather how many bytes the reader is expected to produce in total, according
    /// to the flow file header.
    #[inline]
    #[must_use]
    pub fn len(&self) -> u64 {
        self.size
    }

    /// Return `true` if the flow file self-reports to be empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// All attributes contained in the flow file.
    #[must_use]
    pub fn attributes(&self) -> &HashMap<String, String> {
        &self.attributes
    }

    /// All attributes contained in the flow file.
    #[must_use]
    pub fn attributes_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.attributes
    }

    /// A reader of the (remaining) bytes of the body of the flow file.
    ///
    /// This contains the actual file content, and is expected to yield [`Self::len()`]
    /// bytes in total. It is guaranteed to produce no more bytes than this, but a
    /// truncated file would give EOF early.
    #[expect(mismatched_lifetime_syntaxes, reason = "Proposed fixes don't compile.")]
    pub fn body(&'a mut self) -> &'a mut FlowFileContentReader {
        &mut self.contents
    }
}

/// Trait for converting streams of data into a [`FlowFileIterator`].
///
/// See [`FlowFileIterator::from`] for which types can be used as a source.
pub trait IntoFlowFiles {
    fn into_flow_files(self) -> FlowFileIterator;
}

impl<S: Into<FlowFileIterator>> IntoFlowFiles for S {
    fn into_flow_files(self) -> FlowFileIterator {
        self.into()
    }
}

/// Boxed byte stream type
type BoxedByteStream = Pin<Box<dyn futures::Stream<Item = Result<Bytes, io::Error>> + Send>>;

/// Parser capable of yielding seccessive [`FlowFile`]s from a stream of bytes.
pub struct FlowFileIterator {
    reader: StreamReader<BoxedByteStream, Bytes>,
    bytes_till_next_or_eof: u64,
}

impl From<BodyDataStream> for FlowFileIterator {
    fn from(body: BodyDataStream) -> Self {
        let stream: BoxedByteStream = Box::pin(body.map_err(io::Error::other));
        let reader = StreamReader::new(stream);
        Self {
            reader,
            bytes_till_next_or_eof: 0,
        }
    }
}

/// Errors that can occur during parsing of [`FlowFile`]s.
#[derive(Debug, Error)]
pub enum FlowFileParsingError {
    /// The file did not contain the expected `b"NiFiFF3"` magic byte header.
    #[error("Incorrect flow file magic bytes, expected 'NiFiFF3' but got {0:?}")]
    BadMagicBytes([u8; 7]),
    /// IO error while parsing a flow file.
    ///
    /// The context indicates in which stage of parsing the error occured.
    #[error("Malformed flowfile: {context}: {io_error}")]
    Malformed {
        context: &'static str,
        io_error: tokio::io::Error,
    },
}

impl FlowFileIterator {
    /// Get the next file. Returns `None` if the stream is finished.
    ///
    /// # Errors
    /// IO errors will propagate up. Otherwise this can return an error if the flowfile
    /// is malformed.
    pub async fn next_file(&mut self) -> Result<Option<FlowFile<'_>>, FlowFileParsingError> {
        // If the last flow file wasn't fully consumed, ensure we skip the remaining length of the
        // previous file before moving on.
        if self.bytes_till_next_or_eof > 0 {
            tokio::io::copy(
                &mut (&mut self.reader).take(self.bytes_till_next_or_eof),
                &mut tokio::io::sink(),
            )
            .await
            .map_err(|io| FlowFileParsingError::Malformed {
                context: "Error while skipping till next flow file header",
                io_error: io,
            })?;
        }

        let mut buf = [0u8; 7];
        if let Err(err) = self.reader.read_exact(&mut buf).await {
            if err.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(None);
            }
            return Err(FlowFileParsingError::Malformed {
                context: "Could not read 7 bytes to check for flow file magic bytes",
                io_error: err,
            });
        }
        if &buf != b"NiFiFF3" {
            return Err(FlowFileParsingError::BadMagicBytes(buf));
        }
        let n_attributes = read_field_length(&mut self.reader).await.map_err(|io| {
            FlowFileParsingError::Malformed {
                context: "Reading number of attributes in flowfile",
                io_error: io,
            }
        })? as usize;
        let mut attributes = HashMap::with_capacity(n_attributes);
        for _ in 0..n_attributes {
            let key = read_string(&mut self.reader).await.map_err(|io| {
                FlowFileParsingError::Malformed {
                    context: "Reading key from attribute",
                    io_error: io,
                }
            })?;
            let value = read_string(&mut self.reader).await.map_err(|io| {
                FlowFileParsingError::Malformed {
                    context: "Reading value from attribute",
                    io_error: io,
                }
            })?;
            attributes.insert(key, value);
        }

        let size = self
            .reader
            .read_u64()
            .await
            .map_err(|io| FlowFileParsingError::Malformed {
                context: "Reading content length as u64",
                io_error: io,
            })?;

        self.bytes_till_next_or_eof = size;

        let file_reader = FlowFileContentReader {
            inner: (&mut self.reader).take(size),
            remaining: &mut self.bytes_till_next_or_eof,
        };

        Ok(Some(FlowFile {
            size,
            attributes,
            contents: file_reader,
        }))
    }
}

impl Stream for FlowFileIterator {
    type Item;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        todo!()
    }
}

/// Async reader for a single file
pub struct FlowFileContentReader<'a> {
    inner: tokio::io::Take<&'a mut StreamReader<BoxedByteStream, Bytes>>,
    remaining: &'a mut u64,
}

impl AsyncRead for FlowFileContentReader<'_> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        let before = buf.remaining();
        let poll_result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if poll_result.is_ready() {
            let consumed = before - buf.remaining();
            *self.remaining = self.remaining.saturating_sub(consumed as u64);
        }
        poll_result
    }
}

async fn read_field_length<R: AsyncReadExt + Unpin>(r: &mut R) -> tokio::io::Result<u32> {
    let n = r.read_u16().await?;
    if n != u16::MAX {
        return Ok(u32::from(n));
    }
    r.read_u32().await
}

async fn read_string<R: AsyncReadExt + Unpin>(r: &mut R) -> tokio::io::Result<String> {
    let n = read_field_length(r).await? as usize;
    let mut string = String::with_capacity(n);
    r.take(n as u64).read_to_string(&mut string).await?;
    Ok(string)
}
