# nififf3

Utilities for working with NiFi's FlowFile V3 file format
(`application/flowfile-v3`), wire-compatible with NiFi's `FlowFilePackagerV3`
and `FlowFileUnpackagerV3`.

A flow file is a binary content payload together with a set of string
key-value attributes. The central type is `FlowFile<R>`, generic over the
container of the content: an in-memory `Vec<u8>`, any `std::io::Read`, or
(behind the `tokio` feature) any `tokio::io::AsyncRead`.

## Parsing

`FlowFile::from_bytes` parses a buffer holding exactly one flow file and
validates the declared content size against the bytes present:

```rust
use nififf3::FlowFile;

// A flow file as produced by NiFi (or, here, by this crate).
let bytes = FlowFile::builder()
    .attribute("filename", "greeting.txt")
    .content(&b"Hello, NiFi!"[..])
    .to_bytes();

let flow_file = FlowFile::from_bytes(&bytes).unwrap();
assert_eq!(flow_file.size(), 12);
assert_eq!(flow_file.attributes()["filename"], "greeting.txt");
assert_eq!(flow_file.content().as_slice(), b"Hello, NiFi!");
```

`FlowFile::parse` is lazy: it consumes only the header from a reader and
returns the content as a reader limited to the declared size, so arbitrarily
large flow files can be processed without buffering them:

```rust
use nififf3::FlowFile;

let bytes = FlowFile::builder().content(&b"streamed"[..]).to_bytes();

// Only the header is read here; `flow_file.content()` is a size-limited reader.
let flow_file = FlowFile::parse(bytes.as_slice()).unwrap();
assert_eq!(flow_file.size(), 8);

// Read the content when you need it, e.g. all at once:
let flow_file = flow_file.into_bytes().unwrap();
assert_eq!(flow_file.content().as_slice(), b"streamed");
```

NiFi concatenates multiple flow files back-to-back in a single stream;
`FlowFile::parse_next` reads them one at a time and returns `None` on a clean
end of input:

```rust
use nififf3::FlowFile;

let mut bytes = FlowFile::builder().content(&b"first"[..]).to_bytes();
bytes.extend(FlowFile::builder().content(&b"second"[..]).to_bytes());

let mut reader = bytes.as_slice();
let mut contents = Vec::new();
while let Some(flow_file) = FlowFile::parse_next(&mut reader).unwrap() {
    contents.push(flow_file.into_bytes().unwrap().into_content());
}
assert_eq!(contents, [b"first".to_vec(), b"second".to_vec()]);
```

### Untrusted input

A crafted header can declare millions of attributes or multi-gigabyte
attribute values. The plain parsing functions trust their input (matching
NiFi's own unpackager, and never allocating more than the input actually
provides); for untrusted input the `*_with_limits` variants enforce caps on
the header:

```rust
use nififf3::{Error, FlowFile, Limits};

let bytes = FlowFile::builder()
    .attribute("key", "a value longer than ten bytes")
    .content(&b"hi"[..])
    .to_bytes();

// Defaults: at most 4096 attributes, 1 MiB per attribute key/value.
let limits = Limits::default().max_attribute_len(10);
let err = FlowFile::parse_with_limits(bytes.as_slice(), &limits).unwrap_err();
assert!(matches!(err, Error::AttributeTooLong { .. }));
```

## Creating flow files

Flow files are created with a builder: add attributes, then supply the
content to finish the build. In-memory content infers the size:

```rust
use nififf3::FlowFile;

let flow_file = FlowFile::builder()
    .attribute("filename", "data.bin")
    .attributes([("a", "1"), ("b", "2")])
    .content(vec![1, 2, 3]);
assert_eq!(flow_file.size(), 3);
let bytes = flow_file.to_bytes();
```

The binary format stores the content size *before* the content, so
serializing from a reader requires the size up front
(`builder().reader(read, size)`). For readers of unknown length, the builder
can spool the content to learn its size — into memory, or (behind the
`tempfile` feature) into an anonymous temporary file that is deleted on drop:

```rust
use nififf3::FlowFile;

let reader = &b"length unknown ahead of time"[..]; // any `impl Read`
let flow_file = FlowFile::builder()
    .attribute("source", "example")
    .buffered(reader)
    .unwrap();
assert_eq!(flow_file.size(), 28);
```

```rust,ignore
// Requires the `tempfile` feature. Content is spooled to disk, not memory.
let flow_file = FlowFile::builder().tempfile(reader)?;
let flow_file = FlowFile::builder().tempfile_async(reader).await?; // + `tokio`
```

Serialization targets mirror the parsing sources: `to_bytes` for `Vec<u8>`,
`write_to` for `std::io::Write`, and `write_to_async` for
`tokio::io::AsyncWrite`. To write several flow files back-to-back, use
`FlowFilesWriter` (or `FlowFilesWriterAsync`), the counterpart to the
`FlowFiles` reader.

Parsing is the only thing that produces this crate's `Error`. Everything that
just moves bytes — `write_to`, `into_bytes`, the writers, and their async
twins — returns `std::io::Result`, so handling them does not mean matching on
flow-file failures that cannot occur. Content that ends before its declared
size is `ErrorKind::UnexpectedEof` carrying an `Error::SizeMismatch`, which
`io::Error::get_ref` and `downcast_ref` recover if the detail is wanted.

## Deriving flow files from flow files

`derive` starts a builder carrying another flow file's attributes, which is
the usual way to produce a flow file *from* one you received. The `uuid`
attribute is replaced with a fresh one, since in NiFi it identifies a single
flow file — `derive_keep_uuid` copies it verbatim instead.

```rust
use nififf3::FlowFile;

let parent = FlowFile::builder()
    .attribute("filename", "report.csv")
    .attribute("source", "upload")
    .content(&b"a,b\n1,2\n"[..]);

let child = parent.derive()
    .attribute("filename", "report.header.csv")
    .without_attribute("source")
    .content(&b"a,b\n"[..]);

assert_eq!(child.attributes()["filename"], "report.header.csv");
assert!(!child.attributes().contains_key("source"));
```

When one flow file becomes *many* — an archive unpacked into a flow file per
entry, a batch split into records — `fragments` numbers the results with
NiFi's fragment attributes, so `MergeContent` can reassemble them downstream:

```rust
use nififf3::FlowFile;

let parent = FlowFile::builder()
    .attribute("filename", "pair.txt")
    .content(&b"first\nsecond"[..]);

let mut parts = parent.fragments().with_count(2);
let children: Vec<_> = parent
    .content()
    .split(|byte| *byte == b'\n')
    .map(|line| parts.next().content(line))
    .collect();

assert_eq!(children[0].attributes()["fragment.index"], "1");
assert_eq!(children[1].attributes()["fragment.index"], "2");
assert_eq!(children[0].attributes()["segment.original.filename"], "pair.txt");
```

Each part also gets its own `uuid` and a `fragment.identifier` shared across
the set. The attribute keys are configurable if you need different ones.

## Async I/O (`tokio` feature)

The async API mirrors the sync one: `parse_async` reads only the header and
exposes the content as a size-limited `AsyncRead`; `into_bytes_async` and
`write_to_async` consume it.

```rust,ignore
use nififf3::FlowFile;
use tokio::io::BufReader;

let file = BufReader::new(tokio::fs::File::open("data.ff3").await?);
let flow_file = FlowFile::parse_async(file).await?;
println!("{} bytes: {:?}", flow_file.size(), flow_file.attributes());

// Stream the content somewhere without buffering it...
let mut flow_file = flow_file;
flow_file.write_to_async(&mut tokio::io::stdout()).await?;
```

## Axum integration (`axum` feature)

Handlers can take a `FlowFileRequest` extractor, which parses the flow file
header from the request body (applying `Limits::default()`, since request
bodies are untrusted) and streams the content incrementally — arbitrarily
large flow files never need to be in memory. `StrictFlowFileRequest`
additionally rejects requests without a `application/flowfile-v3` content
type with `415 Unsupported Media Type`. Flow files with
`AsyncRead` content implement `IntoResponse` (streaming, with
`Content-Type: application/flowfile-v3`), as does the error type (as a
`400 Bad Request`).

```rust,ignore
use axum::{Router, routing::post};
use nififf3::{FlowFile, FlowFileRequest};

async fn echo(flow_file: FlowFileRequest) -> Result<impl axum::response::IntoResponse, nififf3::Error> {
    // The content streams from the request body; buffer it here for brevity.
    let flow_file = flow_file.into_bytes_async().await?;
    Ok(FlowFile::builder()
        .attribute("echoed", "true")
        .content(flow_file.into_content())
        .into_reader())
}

let app: Router = Router::new().route("/echo", post(echo));
```

### Answering with many flow files

A handler that turns one flow file into several — unpacking an archive,
splitting a batch — returns a `FlowFilesResponse`. It hands the handler a
`FlowFilesWriterAsync` and streams whatever is written to it, so neither the
number of parts nor the size of any one part has to fit in memory:

```rust,ignore
use nififf3::{Error, FlowFilesResponse, StrictFlowFileRequest};

async fn split(req: StrictFlowFileRequest) -> Result<FlowFilesResponse, Error> {
    // Validate here, while a real status code is still available.
    let parent = req.into_inner().into_bytes_async().await?;
    let mut parts = parent.fragments();

    Ok(FlowFilesResponse::new(move |mut writer| async move {
        for line in parent.content().split(|byte| *byte == b'\n') {
            // `line` is a reader, so its content is never copied into a part.
            writer.write(parts.next().reader(line, line.len() as u64)).await?;
        }
        Ok(())
    }))
}
```

The producer reports failure as a boxed error, so `?` works directly on
whatever the decoder, plain I/O or this crate hands back — no converting
between error types to satisfy the signature.

Returning the response is the commitment to a 2xx, so validate before it and
report a problem with an individual part *as a part* — a flow file whose
attributes say what went wrong — leaving the good parts to arrive. The type's
documentation covers which failures can be reported that way, and
`FlowFilesResponse::blocking` covers producers that are unavoidably
synchronous. `tests/unpack.rs` drives the whole thing against a real
`.tar.gz`.

## CLI (`cli` feature)

A CLI for converting between flow files and JSON, installed with
`cargo install nififf3 --features cli`:

```console
$ echo -n "hello" | nififf3 create filename=greeting.txt > greeting.ff3
$ nififf3 to-json greeting.ff3
{"size":5,"attributes":{"filename":"greeting.txt"},"content":"aGVsbG8="}
$ nififf3 to-json greeting.ff3 | nififf3 from-json | cmp - greeting.ff3
```

- `nififf3 to-json [path]` — convert flow files to JSON, one object per line
  per flow file (concatenated inputs are supported). The fields are `size`,
  `attributes`, and the base64-encoded `content`.
- `nififf3 from-json [path]` — the inverse: read JSON objects as produced by
  `to-json` and write flow files to stdout.
- `nififf3 attrs [path]` — print `size` and `attributes` as JSON, one object
  per line per flow file, without decoding the content.
- `nififf3 content [path]` — write the raw content of the flow files to
  stdout.
- `nififf3 create key=value ...` — create a flow file with the given
  attributes; the content is read from stdin.

Commands taking a path read from stdin when it is omitted or `-`, and
process flow files one at a time, so streams larger than memory are fine
(except for `to-json`/`from-json`, which buffer one flow file at a time to
base64 its content).

## Examples

`examples/` holds runnable programs, each self-contained and asserting its own
output. Run one with `cargo run --example <name>`, adding `--features` where
the table calls for it:

| example | features | |
| --- | --- | --- |
| `transform` / `transform_async` | — / `tokio` | one flow file in, one out |
| `split` / `split_async` | — / `tokio` | one in, many out |
| `merge` / `merge_async` | — / `tokio` | many in, one out |
| `axum_service` | `axum` | one in, one *or* many out, over HTTP |

The `split`/`merge` pair is worth reading together: `merge` reassembles what
`split` produced, using the fragment attributes the way NiFi's `MergeContent`
does in `defragment` mode.

## Feature flags

No features are enabled by default; the sync API is always available.

- `tokio` — async parsing and serialization over `AsyncRead`/`AsyncWrite`.
- `axum` — request extractor and response types (implies `tokio`).
- `tempfile` — spool content of unknown length to a temporary file.
- `serde` — `Serialize`/`Deserialize` for in-memory flow files, with the
  content base64 encoded; this is the JSON shape the CLI uses.
- `cli` — the `nififf3` binary (implies `serde`).

## Wire format

The V3 format, as written by NiFi's `FlowFilePackagerV3`:

```text
"NiFiFF3"          7-byte magic header
attribute count    field length
per attribute:
  key              field length + UTF-8 bytes
  value            field length + UTF-8 bytes
content size       8-byte big-endian integer
content            <content size> bytes
```

A *field length* is 2 bytes big-endian; values of `0xFFFF` and above are
written as the marker `0xFF 0xFF` followed by the value as 4 bytes
big-endian. Attributes are serialized in sorted key order so output is
deterministic (NiFi accepts any order).
