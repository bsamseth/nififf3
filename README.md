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
let flow_file = flow_file.into_memory().unwrap();
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
    contents.push(flow_file.into_memory().unwrap().into_content());
}
assert_eq!(contents, [b"first".to_vec(), b"second".to_vec()]);
```

### Untrusted input

A crafted header can declare millions of attributes or multi-gigabyte
attribute values. The plain parsing functions trust their input, matching
NiFi's own unpackager: an attribute buffer grows as bytes arrive rather than to
the length the header declares, so a header claiming a 4 GiB key over a short
input fails without allocating for it, and only the attribute map is sized from
the header at all (capped at 1024 entries). For untrusted input the
`*_with_limits` variants enforce caps on the header:

```rust
use nififf3::{Error, FlowFile, Limits};

let bytes = FlowFile::builder()
    .attribute("key", "a value longer than ten bytes")
    .content(&b"hi"[..])
    .to_bytes();

// Recommended: at most 4096 attributes, 1 MiB per key/value, 2 MiB of
// attribute bytes in total. `Limits::UNLIMITED` is the neutral starting point
// to build up from instead, and every `with_max_*` takes `None` to clear.
let limits = Limits::recommended().with_max_attribute_len(10);
let err = FlowFile::parse_with_limits(bytes.as_slice(), limits).unwrap_err();
assert!(matches!(err, Error::AttributeTooLong { .. }));
```

`max_content_len` caps the content size a header may declare, failing with
`Error::ContentTooLarge` before any content is read. It is off by default,
because parsing streams the content rather than buffering it — set it when
the caller will go on to `into_memory` the result and would rather learn the
size is unacceptable up front. It bounds what the header *claims*; bounding
what actually arrives is the transport's job (over HTTP, axum's
`DefaultBodyLimit` — see below).

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

// Or the middle ground: in memory up to `max_memory`, on disk past it.
let flow_file = FlowFile::builder().spooled(reader, 64 * 1024)?;
```

`into_memory` reads a reader-backed flow file's *content* into memory and
serializes nothing; `to_bytes`, below, does the opposite — it serializes a whole
flow file, header included. The pair used to be `into_bytes`/`to_bytes`, one
character apart and easily mistaken for each other.

Serialization targets mirror the parsing sources: `to_bytes` for `Vec<u8>`,
`write_to` for `std::io::Write`, and `write_to_async` for
`tokio::io::AsyncWrite`. The last two consume the flow file, since they leave
its content reader exhausted. To write several flow files back-to-back, use
`FlowFilesWriter` (or `FlowFilesWriterAsync`), the counterpart to the
`FlowFiles` reader.

A write that fails part-way leaves a truncated flow file in the stream, so the
writers refuse everything after one, the way `FlowFiles` stops reading after an
error. Appending to a stream that is mid-record would hide the problem rather
than report it: the next flow file's header would be read back as the
truncated one's content, yielding a plausible flow file with the wrong bytes.
`is_poisoned` reports the state; `into_inner` still returns the writer, for
discarding or truncating what was produced.

Finish a stream with `finish`, which flushes and hands the writer back —
writing does not flush, and neither does dropping the writer. For an
`AsyncWrite` that has an ending of its own (a compressor's trailer, a TLS
`close_notify`, a buffered writer's tail), `FlowFilesWriterAsync::shutdown` is
what emits it; skipping it truncates the output, and the flow files come back
corrupt rather than merely short. `into_inner` deliberately does neither, so
that a half-written stream can be abandoned instead of completed.

Parsing is the only thing that produces this crate's `Error`. Everything that
just moves bytes — `write_to`, `into_memory`, the writers, and their async
twins — returns `std::io::Result`, so handling them does not mean matching on
flow-file failures that cannot occur. In those, content that ends before its
declared size is `ErrorKind::UnexpectedEof` carrying an `Error::SizeMismatch`,
which `io::Error::get_ref` and `downcast_ref` recover if the detail is wanted.
Anything returning this crate's `Error` — `from_bytes`, `FlowFiles`,
`FlowFilesAsync` — reports that case as `Error::SizeMismatch` directly, and so
does `?`: converting an `io::Error` into this crate's `Error` recovers a
truncation rather than burying it under `Error::Io`, so both routes to the same
condition match the same way. Only a truncation is recovered — any other
`io::Error` stays `Error::Io`, payload and all.

The declared `size()` is what every serializer writes and every reader-based
operation consumes, and each of the constructors above keeps it in step with
the content. `map_content` swaps the container while carrying the size across,
so it is only for transforms that preserve the length. For one that *changes*
it — decompressing, re-encoding — use `map_bytes`, which derives the new size
from the content it returns, or `map_content_sized` when the result is a reader
whose length only you know. `with_size` remains for declaring a size by hand,
and is the one way left to get this wrong.

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
    .map(|line| parts.next_part().content(line))
    .collect();

assert_eq!(children[0].attributes()["fragment.index"], "1");
assert_eq!(children[1].attributes()["fragment.index"], "2");
assert_eq!(children[0].attributes()["segment.original.filename"], "pair.txt");
```

Each part also gets its own `uuid` and a `fragment.identifier` shared across
the set. The attribute keys are configurable if you need different ones.

### Declaring the count

A bundle has to say how big it is. `MergeContent` fills a bin when it holds as
many flow files as the `fragment.count` of one of them says, so a bundle that
never declares a count is not merged at all: the bin times out and every flow
file in it is routed to `failure`. It needs the count on *at least one* flow
file, not on all of them, which leaves two ways to declare it.

When the total is known before the parts are built, `with_count` puts it on
every part, as above. When it is not — an archive, a decoder, anything read to
the end — `terminate` closes the bundle with an empty flow file carrying the
count, so the parts can still be streamed as they are produced:

```rust
use nififf3::{FlowFile, FlowFiles, FlowFilesWriter};

let parent = FlowFile::builder()
    .attribute("filename", "records.txt")
    .content(&b"alpha\nbeta"[..]);

let mut parts = parent.fragments(); // no `with_count`: the total is unknown
let mut out = Vec::new();
let mut writer = FlowFilesWriter::new(&mut out);
for record in parent.content().split(|byte| *byte == b'\n') {
    writer.write_bytes(&parts.next_part().content(record))?;
}
writer.write_bytes(&parts.terminate())?; // now the total is known
writer.finish()?;

let bundle: Vec<_> = FlowFiles::new(out.as_slice()).collect::<Result<_, _>>()?;
assert_eq!(bundle.len(), 3, "two parts and the terminator");
assert_eq!(bundle[2].attributes()["fragment.count"], "3");
assert_eq!(bundle[2].size(), 0);
# Ok::<(), nififf3::Error>(())
```

The count covers every flow file in the bundle, so the terminator counts
itself: `n` parts give a terminator with `fragment.index = fragment.count =
n + 1`. It carries no content, so defragmenting concatenates it to nothing.
`terminate` consumes the counter, since a part emitted after it would leave the
bin one flow file over what it declared and it would never fill.

A consumer doing its own reassembly — rather than handing the bundle to NiFi —
can recognize the terminator as the part whose index equals the declared count
and whose content is empty, and should drop it if it puts anything *between*
parts. `examples/merge.rs` does exactly that.

`defragment` is the inverse, for the far end of a merge: it drops the fragment
attributes and restores `filename` from `segment.original.filename`, so the
reassembled flow file looks like the one the split started from.

```rust
use nififf3::FlowFile;

let parent = FlowFile::builder()
    .attribute("filename", "pair.txt")
    .content(&b"first\nsecond"[..]);
let part = parent.fragments().next_part().content(&b"first"[..]);

let merged = part.derive().defragment().content(&b"first\nsecond"[..]);
assert_eq!(merged.attributes()["filename"], "pair.txt");
assert!(!merged.attributes().contains_key("fragment.index"));
```

## Async I/O (`tokio` feature)

The async API mirrors the sync one: `parse_async` reads only the header and
exposes the content as a size-limited `AsyncRead`; `into_memory_async` and
`write_to_async` consume it.

```rust,ignore
use nififf3::FlowFile;
use tokio::io::BufReader;

let file = BufReader::new(tokio::fs::File::open("data.ff3").await?);
let flow_file = FlowFile::parse_async(file).await?;
println!("{} bytes: {:?}", flow_file.size(), flow_file.attributes());

// Stream the content somewhere without buffering it...
flow_file.write_to_async(&mut tokio::io::stdout()).await?;
```

## Axum integration (`axum` feature)

Handlers can take a `FlowFileRequest` extractor, which parses the flow file
header from the request body (applying `Limits::recommended()`, since request
bodies are untrusted) and streams the content incrementally — arbitrarily
large flow files never need to be in memory.

The body is read through axum's `DefaultBodyLimit`, as any other extractor's
is, so it is capped at 2 MiB unless the router raises or disables it:

```rust,ignore
Router::new()
    .route("/large", post(handler))
    .layer(DefaultBodyLimit::disable()); // or ::max(bytes)
```

That cap is on the bytes the client actually sends, which is the only bound
worth trusting — the size in the header is a claim, and `Limits` governs the
header alone. Exceeding either is a `413 Payload Too Large`, keeping "too big"
distinct from the `400` that a malformed flow file gets.

`StrictFlowFileRequest`
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
    let flow_file = flow_file.into_memory_async().await?;
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
    let parent = req.into_inner().into_memory_async().await?;
    let mut parts = parent.fragments();

    Ok(FlowFilesResponse::new(move |mut writer| async move {
        for line in parent.content().split(|byte| *byte == b'\n') {
            // `line` is a reader, so its content is never copied into a part.
            writer.write(parts.next_part().reader(line, line.len() as u64)).await?;
        }
        Ok(())
    }))
}
```

The producer reports failure as a boxed error, so `?` works directly on
whatever the decoder, plain I/O or this crate hands back — no converting
between error types to satisfy the signature.

A streaming producer is the case `terminate` exists for: it learns how many
parts there were only once the input is exhausted, by which time the earlier
parts are already on the wire. Ending with
`writer.write_bytes(&parts.terminate()).await?` declares the count for the
bundle without holding anything back, and is what makes the response
reassemblable by `MergeContent` at all.

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
process flow files one at a time, so streams larger than memory are fine. Two
exceptions: `to-json`/`from-json` buffer one flow file at a time to base64 its
content, and `create` reads all of stdin into memory, since it has to know the
content length before it can write the header.

Headers are trusted by default, as NiFi's own unpackager trusts them. For
input you have not vetted, `--max-attributes`, `--max-attribute-len`,
`--max-total-attribute-len` and `--max-content-len` apply the corresponding
`Limits` to every flow file. They are honoured by every subcommand, including
the two that never run a header parser — `from-json` checks each decoded flow
file and `create` checks the one it built:

```console
$ nififf3 attrs --max-attributes 4096 --max-content-len 1073741824 untrusted.ff3
```

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
- `stream` — `FlowFilesAsync::into_stream`, for composing with `StreamExt`
  (implies `tokio`).
- `axum` — request extractor and response types (implies `stream`).
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

Two limits are worth knowing. The extended field length encodes a `u32`, but
NiFi reads it into a Java `int`, so an attribute of 2 GiB or more is writable
here and unreadable there; this crate panics only at `u32::MAX`, where the
format itself runs out. And a header may repeat a key — attributes parse into a
map, so the last value wins and re-serializing emits a shorter header than it
read. NiFi collapses duplicates the same way, but it does mean parse-then-
serialize is not byte-preserving for input built to exploit it, which matters
if you hash or sign the encoded form.
