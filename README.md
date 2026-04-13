# nifioxide

[![crate](https://img.shields.io/crates/v/nifioxide.svg)](https://crates.io/crates/nifioxide)
[![docs](https://docs.rs/nifioxide/badge.svg)](https://docs.rs/nifioxide)

Tools for working with NiFi Flow Files from Rust, making it easier to write HTTP processors for NiFi in Rust.

## Overview

In order to deal with non-ASCII attributes, the best way to send a file from
NiFi and back is to package files as [NiFi Flow Files (v3)](https://nifi.apache.org/). 
This crate provides a streaming parser and builder for these flow files, so
that you can write your HTTP processor with these NiFi files as first class citizens.


## Features

- **Streaming parser** - Parse flow files from HTTP requests without loading entire content into memory
- **Builder API** - Create flow files from bytes, files, or readers
- **Axum integration** - Spesifically made to fit with the Axum web framework

## Installation

```toml
[dependencies]
nifioxide = "0.1"
```

## Quick Start

```rust,no_run
use axum::{routing::post, Router};
use tokio::io::AsyncReadExt;
use nifioxide::{axum::StreamedFlowFileFuture, FlowFile, FlowFileParsingError};

async fn handler(
    ff: StreamedFlowFileFuture,
) -> Result<impl axum::response::IntoResponse, FlowFileParsingError> {
    let mut ff = ff.await?;
    
    // Read attributes
    for (key, value) in ff.attributes() {
        println!("{key}: {value}");
    }
    
    // Read content
    let mut buf = Vec::new();
    ff.content_mut().read_to_end(&mut buf).await?;
    
    // Create response flow file
    let response = FlowFile::builder()
        .content_from_bytes(b"response data")
        .attributes(ff.attributes().clone())
        .build();
    
    Ok(response)
}

#[tokio::main]
async fn main() {
    let app = axum::Router::new().route("/process", axum::routing::post(handler));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:9999").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
```

## Content-Type

Requests and responses use `Content-Type: application/flowfile-v3`.

## HTTP Integration

### Extractors

```rust
// Single flow file - returns error if zero or more than one provided
async fn single(ff: nifioxide::axum::StreamedFlowFileFuture) { todo!() }

// Multiple flow files - process as a stream
async fn multiple(flow_files: nifioxide::axum::StreamedFlowFiles) { todo!() }
```

### Response Types

`FlowFile` implements `axum::response::IntoResponse`, so you can return it directly from handlers:

```rust
async fn response() -> impl axum::response::IntoResponse {
    nifioxide::FlowFile::builder()
        .content_from_bytes(b"data")
        .attributes([("filename".to_string(), "output.txt".to_string())])
        .build()
}
```

## Creating Flow Files

### From bytes (in-memory)

```rust
async fn from_bytes() {
    let ff = nifioxide::FlowFile::builder()
        .content_from_bytes(b"file content")
        .attributes([("filename".to_string(), "test.txt".to_string())])
        .build();
}
```

### From file

```rust
async fn from_file() -> tokio::io::Result<()> {
    let ff = nifioxide::FlowFile::builder()
        .content_from_file(tokio::fs::File::open("data.bin").await?)
        .await?
        .attributes([("filename".to_string(), "data.bin".to_string())])
        .build();
    Ok(())
}
```

### From reader (buffered in memory)

```rust
async fn from_reader_into_memory(
    reader: impl tokio::io::AsyncRead + Unpin
) -> tokio::io::Result<()> {
    let ff = nifioxide::FlowFile::builder()
        .content_from_reader_buffered_in_memory(reader)
        .await?
        .attributes(std::collections::HashMap::new())
        .build();
    Ok(())
}
```

### From reader (buffered in temp file)

```rust
async fn from_reader_into_memory(
    reader: impl tokio::io::AsyncRead + Unpin
) -> tokio::io::Result<()> {
    let ff = nifioxide::FlowFile::builder()
        .content_from_reader_buffered_in_tempfile(reader)
        .await?
        .attributes(std::collections::HashMap::new())
        .build();
    Ok(())
}
```

## License

This project is licensed under the [MIT license](LICENSE).

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this project by you shall be licensed as MIT, without any additional
terms or conditions.
