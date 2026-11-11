# nififf3

Read and write NiFi's FlowFile V3 format (`application/flowfile-v3`), and
serve it over HTTP with axum. The output is wire-compatible with NiFi's
`FlowFilePackagerV3`, and this reads whatever `FlowFileUnpackagerV3` reads.

A flow file is binary content together with a set of string key-value
attributes. The central type is `FlowFile<R>`, where `R` is whatever holds the
content: an in-memory `Vec<u8>`, a `Spool` that may be on disk, any
`std::io::Read`, or any `tokio::io::AsyncRead`.

Answering NiFi over HTTP is the shortest way to see it:

```rust
# #[cfg(all(feature = "axum", feature = "uuid"))] {
use axum::response::IntoResponse;
use nififf3::{Error, FlowFile, Spool};

async fn shout(flow_file: FlowFile<Spool>) -> Result<impl IntoResponse, Error> {
    let flow_file = flow_file.into_memory_async().await?;
    Ok(flow_file
        .derive()
        .attribute("shouted", "true")
        .content(flow_file.content().to_ascii_uppercase())
        .into_reader())
}
# }
```

`FlowFile<Spool>` is the extractor. It reads the whole request before the
handler runs, into memory below a size and into a temporary file above it.
`derive` carries the attributes over and mints a fresh `uuid`, as NiFi does
for a new flow file. The answer is a flow file, which is a response in its own
right.

## Parsing

Pick the entry point that matches where the bytes are:

- `FlowFile::from_bytes` and `from_vec` take a buffer holding exactly one flow
  file, and check the declared size against what is there.
- `FlowFile::from_reader` reads one whole flow file from a reader.
- `FlowFile::parse` reads only the header, and leaves the content as a
  `Content<R>`: a reader limited to the declared size, which reports a stream
  that ends early as an error rather than as the end.
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

Indexing with `[]` panics when an attribute is not set, as indexing a
`HashMap` does. `attribute` returns an `Option` for when it may be missing,
and `parse_attribute` reads and parses in one call.

Parsing trusts its input, in the sense that nothing caps how many attributes a
header may declare. It does not trust it with memory: no buffer is ever sized
from a declared length, so a corrupt header costs an allocation proportional to
the bytes that actually arrive.

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
`tempfile` and `spooled` put it on disk, and `spool_async` picks between the
two by size. `reader` takes a size you supply and streams the rest.

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

`FlowFile<Spool>` extracts one flow file, and `Spool` extracts a whole body,
for a request holding several concatenated flow files. Both read the request in
full before the handler runs. A flow file whose content is an `AsyncRead` is an
`IntoResponse`, and `FlowFilesResponse` streams many of them out for a handler
that turns one flow file into several.

Reading the request first is what makes the service safe against NiFi's HTTP
client, which sends its whole request before it reads any of the response. A
handler that streamed its answer while still reading the request off the
socket would stall as soon as the answer outgrew the socket buffers, with each
side waiting for the other. `tests/nifi_client.rs` demonstrates that stall over
a real socket, and shows the extractor completing against the same client.

Two settings to know about. Axum's `DefaultBodyLimit` rejects anything over 2
MiB unless the router raises it. And `SpoolConfig`, added to the router as an
`Extension`, sets how large a request may be before it goes to disk, and where
on disk. `examples/axum_service_large_files.rs` is a service with both set.

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

## Feature flags

The sync parsing and serialization API is always available.

- `uuid`: `derive` and `fragments`, which mint identifiers. On by default.
- `tracing`: events and spans from inside the crate. On by default. With
  `default-features = false` a parse-and-serialize build depends only on
  `thiserror`.
- `tokio`: async parsing and serialization over `AsyncRead` and `AsyncWrite`.
- `tempfile`: spool content to disk. With `tokio` as well, `Spool` and
  `SpoolConfig`.
- `stream`: `FlowFilesAsync::into_stream`. Implies `tokio`.
- `axum`: request extractors and response types. Implies `tokio`, `tempfile`
  and `stream`.
- `serde`: `Serialize` and `Deserialize` with the content base64 encoded.
- `cli`: the `nififf3` binary. Implies `serde`.

## Tracing

Under the `tracing` feature the crate emits spans and events for the work it
does. Nothing is emitted unless your program installs a subscriber, and with
the feature off the calls are compiled out. `tracing-subscriber` with its
`EnvFilter` is what reads `RUST_LOG`, so `RUST_LOG=nififf3=debug` needs that in
your own dependencies.

Spans carry the same three fields wherever they are known, so an interleaved
log can be followed back to one flow file: `uuid`, `filename` when the flow
file has one, and `size`.

- `error` is a failure on the server's side: a spool that could not be written.
- `warn` is what your own `Result`s cannot report: a write that poisoned a
  writer, a response body that broke after the status line went out, a request
  rejected before your handler ran.
- `info` is one line per extracted request, saying whether its content went to
  memory or to disk.
- `debug` is one line per flow file parsed, written or spooled, and one per
  response body completed.
- `trace` follows the content itself, including skipping past what a reader
  left behind.

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
re-serializing emits a shorter header than the one that was read.
