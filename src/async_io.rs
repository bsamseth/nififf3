//! Parsing and serialization over `tokio::io::AsyncRead`/`AsyncWrite`.

use std::collections::HashMap;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::format::{MAGIC, MAX_VALUE_2_BYTES};
use crate::{Error, FlowFile, Result};

async fn read_field_len<R: AsyncRead + Unpin>(reader: &mut R) -> Result<usize> {
    let mut buf = [0u8; 2];
    reader.read_exact(&mut buf).await?;
    let len = u16::from_be_bytes(buf) as usize;
    if len < MAX_VALUE_2_BYTES {
        Ok(len)
    } else {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf).await?;
        Ok(u32::from_be_bytes(buf) as usize)
    }
}

async fn read_string<R: AsyncRead + Unpin>(reader: &mut R) -> Result<String> {
    let len = read_field_len(reader).await?;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    Ok(String::from_utf8(buf)?)
}

pub(crate) async fn parse_header<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<(HashMap<String, String>, u64)> {
    let mut magic = [0u8; 7];
    reader.read_exact(&mut magic).await?;
    if magic != MAGIC {
        return Err(Error::InvalidMagic(magic));
    }
    let count = read_field_len(reader).await?;
    let mut attributes = HashMap::with_capacity(count.min(1024));
    for _ in 0..count {
        let key = read_string(reader).await?;
        let value = read_string(reader).await?;
        attributes.insert(key, value);
    }
    let mut size = [0u8; 8];
    reader.read_exact(&mut size).await?;
    Ok((attributes, u64::from_be_bytes(size)))
}

impl<R: AsyncRead + Unpin> FlowFile<tokio::io::Take<R>> {
    /// Async version of [`FlowFile::parse`]: consumes only the header and
    /// returns the content as a reader limited to the declared size.
    ///
    /// The header is read in small increments, so wrap unbuffered sources in
    /// a [`tokio::io::BufReader`].
    pub async fn parse_async(mut reader: R) -> Result<Self> {
        let (attributes, size) = parse_header(&mut reader).await?;
        Ok(FlowFile::from_raw_parts(
            size,
            attributes,
            reader.take(size),
        ))
    }
}

impl<R: AsyncRead + Unpin> FlowFile<R> {
    /// Async version of [`FlowFile::write_to`].
    pub async fn write_to_async<W: AsyncWrite + Unpin>(&mut self, writer: &mut W) -> Result<u64> {
        writer.write_all(&self.header_bytes()).await?;
        let copied = tokio::io::copy(&mut (&mut self.content).take(self.size), writer).await?;
        if copied != self.size {
            return Err(Error::SizeMismatch {
                expected: self.size,
                actual: copied,
            });
        }
        Ok(copied)
    }

    /// Async version of [`FlowFile::into_bytes`]: reads the content to
    /// completion and validates its length against the declared size.
    pub async fn into_bytes_async(mut self) -> Result<FlowFile<Vec<u8>>> {
        let mut content = Vec::new();
        let read = (&mut self.content)
            .take(self.size)
            .read_to_end(&mut content)
            .await? as u64;
        if read != self.size {
            return Err(Error::SizeMismatch {
                expected: self.size,
                actual: read,
            });
        }
        Ok(FlowFile::from_raw_parts(
            self.size,
            self.attributes,
            content,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> FlowFile<Vec<u8>> {
        FlowFile::builder()
            .attribute("a", "b")
            .attribute("path", "x")
            .content(&b"hello"[..])
    }

    #[tokio::test]
    async fn async_roundtrip_matches_sync() {
        let expected = sample().to_bytes();

        let mut out = Vec::new();
        let copied = sample()
            .into_reader()
            .write_to_async(&mut out)
            .await
            .unwrap();
        assert_eq!(copied, 5);
        assert_eq!(out, expected);

        let parsed = FlowFile::parse_async(expected.as_slice()).await.unwrap();
        assert_eq!(parsed.size(), 5);
        let parsed = parsed.into_bytes_async().await.unwrap();
        assert_eq!(parsed.attributes()["path"], "x");
        assert_eq!(parsed.content().as_slice(), b"hello");
    }

    #[tokio::test]
    async fn async_truncated_content_is_a_size_mismatch() {
        let bytes = sample().to_bytes();
        let truncated = &bytes[..bytes.len() - 2];
        let parsed = FlowFile::parse_async(truncated).await.unwrap();
        assert!(matches!(
            parsed.into_bytes_async().await,
            Err(Error::SizeMismatch {
                expected: 5,
                actual: 3
            })
        ));
    }
}
