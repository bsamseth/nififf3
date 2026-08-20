# nififf3

Read and write NiFi's FlowFile V3 format (`application/flowfile-v3`). The
output is wire-compatible with NiFi's `FlowFilePackagerV3`, and this reads
whatever `FlowFileUnpackagerV3` reads.

A flow file is binary content together with a set of string key-value
attributes. The central type is `FlowFile<R>`, where `R` is whatever holds the
content: an in-memory `Vec<u8>`, any `std::io::Read`, or any
`tokio::io::AsyncRead`.

Answering NiFi over HTTP is the shortest way to see it:

```rust
# #[cfg(all(feature = "axum", feature = "uuid"))] {
use axum::response::IntoResponse;
use nififf3::{Error, StrictFlowFileRequest};

async fn shout(
    StrictFlowFileRequest(flow_file): StrictFlowFileRequest,
) -> Result<impl IntoResponse, Error> {
    let flow_file = flow_file.into_memory_async().await?;
    Ok(flow_file
        .derive()
        .attribute("shouted", "true")
        .content(flow_file.content().to_ascii_uppercase())
        .into_reader())
}
# }
```

The extractor parses the header and streams the body. `derive` carries the
attributes over and mints a fresh `uuid`, as NiFi does for a new flow file. The
answer is a flow file, which is a response in its own right.

## Parsing

Pick the entry point that matches where the bytes are:

- `FlowFile::from_bytes` and `from_vec` take a buffer holding exactly one flow
  file, and check the declared size against what is there.
- `FlowFile::from_reader` reads one whole flow file from a reader.
- `FlowFile::parse` reads only the header, and leaves the content as a reader
  limited to the declared size. Nothing is buffered, so the content can be any
  size.
- `FlowFiles` iterates over concatenated flow files, reading each content into
  memory. `FlowFilesReader` does the same but streams each content.

Each has an async twin under the `tokio` feature.

```rust
use nififf3::FlowFile;

let bytes = FlowFile::builder()
    .attribute("filename", "greeting.txt")
    .content(&b"Hello, NiFi!"[..])
    .to_bytes();

let flow_file = FlowFile::from_bytes(&bytes)?;
assert_eq!(flow_file["filename"], "greeting.txt");
assert_eq!(flow_file.content_str()?, "Hello, NiFi!");
# Ok::<(), Box<dyn std::error::Error>>(())
```

Indexing panics when an attribute is not set, as indexing a `HashMap` does.
`attribute` returns an `Option` for when it may be missing, and
`parse_attribute` reads one and parses it into a number.

A header from outside your own system can declare millions of attributes. Pass
`Limits` to any `*_with_limits` entry point to bound it. `Limits` documents
what the recommended caps permit, and what the parser will not do whatever you
set.

## Creating

Add attributes, then supply the content, which finishes the build:

```rust
use nififf3::FlowFile;

let flow_file = FlowFile::builder()
    .attribute("filename", "data.bin")
    .content(vec![1, 2, 3]);

assert_eq!(flow_file.size(), 3);
let bytes = flow_file.to_bytes();
```

The format stores the content length before the content, so the size has to be
known before anything can be written. `content` takes it from the bytes you
hand over, and `empty` finishes a flow file that is only attributes. When the
length is not known ahead of time, `buffered` reads the content into memory,
while `tempfile` and `spooled` put it on disk. `reader` takes a size you supply
and streams the rest.

`to_bytes` returns a `Vec<u8>`, `write_to` streams to a writer, and
`FlowFilesWriter` writes several back to back. `serialized_len` says how many
bytes any of them will produce, before they produce it.

## Splitting and merging

`derive` starts a builder carrying another flow file's attributes. `fragments`
splits one flow file into many, numbering the parts with NiFi's fragment
attributes so that `MergeContent` can put them back together, and
`defragment` undoes that at the far end. A bundle has to declare how many flow
files it holds or the merge never completes, and `Fragments` describes the two
ways to do that.

## Axum integration

`FlowFileRequest` and `StrictFlowFileRequest` extract one flow file, and
`FlowFilesRequest` extracts a batch, which is what NiFi's `PostHTTP` sends. A
flow file whose content is an `AsyncRead` is an `IntoResponse`, and
`FlowFilesResponse` streams many of them out for a handler that turns one flow
file into several.

Two things a service handling large flow files needs. Raise axum's
`DefaultBodyLimit`, which rejects anything over 2 MiB by default. And spool the
request with `FlowFile::spool_async` before answering: a handler that reads the
request and writes the response from the same task deadlocks against a client
that sends its whole request before reading any of the answer, which NiFi's
client does. `FlowFilesResponse` explains that in full, and
`examples/axum_service_large_files.rs` is a service written to avoid it.

## Examples

`examples/` holds runnable programs, each self-contained and asserting its own
output. Run one with `cargo run --example <name>`, adding `--features` as the
example's header says. `transform`, `split` and `merge` cover the three shapes
in memory, with `_async` twins. `axum_service` puts them behind HTTP, and
`axum_service_large_files` does the same for content too large to buffer.

## CLI

`cargo install nififf3 --features cli` installs a `nififf3` binary that
converts between flow files and JSON:

```console
$ echo -n hello | nififf3 create filename=greeting.txt > greeting.ff3
$ nififf3 to-json greeting.ff3
{"size":5,"attributes":{"filename":"greeting.txt"},"content":"aGVsbG8="}
```

The subcommands are `to-json`, `from-json`, `attrs`, `content` and `create`.
Each reads stdin when given no path, and processes flow files one at a time.
The `--max-*` flags apply `Limits` to every flow file, on every subcommand.

## Feature flags

The sync parsing and serialization API is always available. Only `uuid` is on
by default.

- `uuid`: `derive` and `fragments`, which mint identifiers. On by default.
  Without it, a parse-and-serialize build depends only on `thiserror`.
- `tokio`: async parsing and serialization over `AsyncRead` and `AsyncWrite`.
- `stream`: `FlowFilesAsync::into_stream`. Implies `tokio`.
- `axum`: request extractors and response types. Implies `stream`.
- `tempfile`: spool content to a temporary file, through the builder's
  `tempfile` and `spooled`, and through `FlowFile::spool_async`.
- `serde`: `Serialize` and `Deserialize` with the content base64 encoded.
- `cli`: the `nififf3` binary. Implies `serde`.

## Wire format

As written by NiFi's `FlowFilePackagerV3`:

```text
"NiFiFF3"          7-byte magic header
attribute count    field length
per attribute:     key, then value, each a field length + UTF-8 bytes
content size       8-byte big-endian integer
content            <content size> bytes
```

A field length is 2 bytes big-endian. A value of `0xFFFF` or above is written
as the marker `0xFF 0xFF` followed by the value as 4 bytes big-endian.
Attributes are written in sorted key order, so the output is deterministic.
NiFi accepts any order.

Two things to know. The extended field length encodes a `u32`, but NiFi reads
it into a Java `int`, so an attribute of 2 GiB or more is writable here and
unreadable there. And a header may repeat a key, which parses into a map, so
re-serializing emits a shorter header than the one that was read. NiFi collapses
duplicates the same way, but it does mean parsing and re-serializing is not
byte-preserving, which matters if you hash or sign the encoded form.
