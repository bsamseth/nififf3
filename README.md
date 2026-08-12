# nififf3

Utilities for working with NiFi's FlowFile V3 file format
(`application/flowfile-v3`). The output is wire-compatible with NiFi's
`FlowFilePackagerV3` and `FlowFileUnpackagerV3`.

A flow file is a piece of binary content together with a set of string
key-value attributes. The central type is `FlowFile<R>`, where `R` is whatever
holds the content. That can be an in-memory `Vec<u8>`, any `std::io::Read`, or
any `tokio::io::AsyncRead` behind the `tokio` feature.

## Parsing

Use `FlowFile::from_bytes` when you have a buffer holding exactly one flow file.
It checks the content size declared in the header against the bytes that are
actually there:

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

`FlowFile::parse` is lazy. It reads only the header from a reader, and hands
back the content as a second reader limited to the declared size. You can
process a flow file of any size this way, because the content never has to be
in memory:

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

When you want the content in memory anyway, `FlowFile::from_reader` does those
last two steps in one call. Use it for a file on disk, or for a small request
body. If the content is truncated it reports `Error::SizeMismatch`, the same
way `from_bytes` does, instead of a bare I/O error. It reads exactly one flow
file and leaves the reader on the byte after it.

NiFi often concatenates several flow files back to back in one stream.
`FlowFile::parse_next` reads them one at a time, and returns `None` when the
input ends cleanly:

```rust
use nififf3::FlowFile;

let mut bytes = FlowFile::builder().content(&b"first"[..]).to_bytes();
bytes.extend(FlowFile::builder().content(&b"second"[..]).to_bytes());

let mut reader = bytes.as_slice();
let mut contents = Vec::new();
while let Some(flow_file) = FlowFile::parse_next(&mut reader).unwrap() {
    // The content of each flow file is the reader itself, so you have to deal
    // with it before parsing the next one. Here that means reading it.
    contents.push(flow_file.into_memory().unwrap().into_content());
}
assert_eq!(contents, [b"first".to_vec(), b"second".to_vec()]);
```

### Choosing how to read a stream

`parse_next` is the primitive, and it hands you the stream itself. The flow file
it returns has not read any content yet, because its content is the reader. The
next flow file begins where that content ends. So you have to consume every
content before parsing the next flow file. Call `into_memory` to buffer it,
`write_to` to copy it out, or `skip_content` to discard it.

Two types do that bookkeeping for you, and one of them is usually what you want:

| | content | use when |
| --- | --- | --- |
| `FlowFiles` | read into memory for you | the contents fit in memory, and you want an owned `FlowFile<Vec<u8>>` each |
| `FlowFilesReader` | streamed, positioned for you | a content may be too large to buffer |
| `FlowFile::parse_next` | streamed, positioned by you | you need the reader back between flow files, or are driving the stream yourself |

With `FlowFilesReader` you can read all of a content, part of it, or none of it.
The next call skips whatever is left:

```rust
use nififf3::{FlowFile, FlowFilesReader};

let mut bytes = FlowFile::builder().attribute("n", "1").content(&b"aaaa"[..]).to_bytes();
bytes.extend(FlowFile::builder().attribute("n", "2").content(&b"bbbb"[..]).to_bytes());

// Only the attributes are needed here, so the content is never read.
let mut flow_files = FlowFilesReader::new(bytes.as_slice());
let mut names = Vec::new();
while let Some(flow_file) = flow_files.next()? {
    names.push(flow_file.attribute("n").unwrap().to_string());
}
assert_eq!(names, ["1", "2"]);
# Ok::<(), nififf3::Error>(())
```

Only `FlowFiles` implements `Iterator`. The other two yield flow files that
borrow the stream they came from, and an `Iterator` cannot express that. The
borrow has a useful side effect: if you try to hold a flow file past the next
call, the code doesn't compile.

If you use `parse_next` directly, watch out for dropping a flow file before its
content has been read. The reader is then left inside that content, and the next
call parses the content as if it were a flow file. Usually that fails with an
error. It does not fail when the content itself starts with a valid header,
which happens when one flow file carries another. In that case you get back a
flow file that was never sent, and anything after it in the stream may be lost.
The async versions, `FlowFilesAsync` and `FlowFilesReaderAsync`, work the same
way.

### Untrusted input

A crafted header can declare millions of attributes, or attribute values of
several gigabytes. The plain parsing functions trust their input, as NiFi's own
unpackager does.

Even so, no buffer is sized from a length the header declares. The parser
reserves at most 64 KiB up front, and after that it reserves only as far as the
bytes that have actually arrived. So if a header claims a 4 GiB key and the
input is short, the parser allocates 64 KiB. The attribute map is the only other
thing sized from the header, and it is capped at 1024 entries.

For input you haven't vetted, the `*_with_limits` functions apply caps to the
header:

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

`max_content_len` caps the content size that a header may declare. It fails with
`Error::ContentTooLarge` before any content is read. It is off by default,
because parsing streams the content instead of buffering it. Set it when the
caller will go on to call `into_memory`, and should learn up front that the size
is unacceptable.

`max_content_len` bounds what the header claims. Bounding what actually arrives
is the transport's job. Over HTTP that means axum's `DefaultBodyLimit`,
described under [Axum integration](#axum-integration-axum-feature).

## Creating flow files

Build a flow file by adding attributes and then supplying the content.
Supplying the content finishes the build. In-memory content sets the size for
you:

```rust
use nififf3::FlowFile;

let flow_file = FlowFile::builder()
    .attribute("filename", "data.bin")
    .attributes([("a", "1"), ("b", "2")])
    .content(vec![1, 2, 3]);
assert_eq!(flow_file.size(), 3);
let bytes = flow_file.to_bytes();
```

`empty()` finishes a build with no content at all. Some flow files carry only
attributes. For example, the terminator of a fragment set is an empty flow file
that records how many parts there were.

### Content of unknown length

The binary format stores the content size before the content itself, so nothing
can be written until the length is known. `reader` takes your word for the
length. The others work it out by spooling the content first, and they differ
only in where they put it while they do that:

| finisher | content ends up | needs |
| --- | --- | --- |
| `content(bytes)` | in memory, size inferred | |
| `reader(read, size)` | left where it is; you supply `size` | |
| `buffered(read)` | in memory | |
| `tempfile(read)` | an anonymous temp file, deleted on drop | `tempfile` |
| `spooled(read, max)` | memory up to `max` bytes, a temp file past it | `tempfile` |

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
// Behind the `tempfile` feature: spooled to disk instead of memory.
let flow_file = FlowFile::builder().tempfile(reader)?;

// Or the middle ground: in memory up to `max_memory`, on disk past that.
let flow_file = FlowFile::builder().spooled(reader, 64 * 1024)?;

// `buffered` and `tempfile` have async twins (`tokio`).
let flow_file = FlowFile::builder().tempfile_async(reader).await?;
```

### Serializing

The targets mirror the parsing sources, one per content type:

| method | content | notes |
| --- | --- | --- |
| `to_bytes()` | `Vec<u8>` | returns the encoded flow file |
| `write_bytes_to(w)` | `Vec<u8>` | writes it to a `std::io::Write` |
| `write_to(w)` | any `Read` | streams; consumes the flow file |
| `write_to_async(w)` | any `AsyncRead` | streams; consumes the flow file |

The two streaming methods take `self`, so you can only call them once. They read
the content to the end, and a second call would write a second header and then
fail. By that point the first write has already gone out.

`serialized_len()` reports how many bytes any of them will produce. It works
from the attributes and the declared size, without serializing anything. Use it
when you need a `Content-Length` before the bytes exist. It is also the only way
to ask a reader-backed flow file how large it will be without reading it.

Two names are easy to confuse. `to_bytes` serializes a whole flow file, header
included. `into_memory` reads a reader-backed flow file's content into memory,
and serializes nothing. They were once called `to_bytes` and `into_bytes`, which
is why the second one was renamed.

### Writing several

`FlowFilesWriter` and `FlowFilesWriterAsync` write flow files back to back. They
are the counterpart to the `FlowFiles` reader:

```rust
use nififf3::{FlowFile, FlowFiles, FlowFilesWriter};

let mut writer = FlowFilesWriter::new(Vec::new());
writer.write_bytes(&FlowFile::builder().content(&b"first"[..]))?;
writer.write_bytes(&FlowFile::builder().content(&b"second"[..]))?;
let bytes = writer.finish()?; // flushes, and hands the writer back

assert_eq!(FlowFiles::new(bytes.as_slice()).count(), 2);
# Ok::<(), std::io::Error>(())
```

If a write fails, the writer is poisoned and refuses every later write. It has
left a truncated flow file behind, and `FlowFiles` stops reading after an error
for the same reason. If the writer appended instead, the next flow file's header
would be read back as the truncated flow file's content, producing a plausible
flow file with the wrong bytes. Call `is_poisoned` to check the state.
`into_inner` still returns the writer, so you can discard or truncate what was
produced.

The stream is not finished for you. Writing does not flush, and dropping the
writer does not flush either:

| call | flushes | shuts down | returns the writer |
| --- | --- | --- | --- |
| `finish()` | yes | no | yes |
| `flush()` | yes | no | no |
| `shutdown()` (async) | yes | yes | no |
| `into_inner()` | no | no | yes |

`shutdown` matters when the `AsyncWrite` underneath has an ending of its own. A
compressor, for example, writes a trailer when it shuts down. If you skip
`shutdown`, that trailer is never written and the output is truncated, so the
flow files come back corrupt rather than merely short. `into_inner` neither
flushes nor shuts down, so you can use it to abandon a half-written stream.

### Declared size and content size

Every serializer writes the declared `size()`, and every reader-based operation
consumes exactly that many bytes. All the constructors above keep the size in
step with the content. Only the transforms can pull the two apart:

```rust
use nififf3::FlowFile;

let flow_file = FlowFile::builder().content(&b"hi"[..]);

// Same bytes, different container: the size carries across.
let cursor = flow_file.clone().map_content(std::io::Cursor::new);
assert_eq!(cursor.size(), 2);

// Different bytes, so the new size is taken from the new content.
let repeated = flow_file.map_bytes(|content| content.repeat(3));
assert_eq!(repeated.size(), 6);
```

| transform | size | for |
| --- | --- | --- |
| `map_content(f)` | carried across | swapping the container, same bytes |
| `map_bytes(f)` | taken from the result | rewriting in-memory content |
| `map_content_sized(f)` | returned by `f` | a reader whose length only you know |
| `with_size(n)` | whatever you say | declaring one by hand |

Only `with_size` can be wrong, because you supply the size yourself. The other
three exist so that you rarely need it.

## Deriving flow files from flow files

`derive` starts a builder that carries another flow file's attributes. This is
the usual way to produce a flow file from one you received. In NiFi the `uuid`
attribute identifies a single flow file, so `derive` replaces it with a fresh
one. Use `derive_keep_uuid` if you want to copy it verbatim.

```rust
# #[cfg(feature = "uuid")] {
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
# }
```

Sometimes one flow file becomes many. For example, you might unpack an archive
into one flow file per entry. Each piece is called a fragment, and `fragments`
numbers them with NiFi's fragment attributes so that `MergeContent` can
reassemble them downstream:

```rust
# #[cfg(feature = "uuid")] {
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
# }
```

Each part also gets its own `uuid`, and a `fragment.identifier` shared across
the set. The attribute keys are configurable if you need different ones.

Every part inherits the parent's attributes. Calling `attribute` or
`without_attribute` on the `Fragments` value adjusts them once for the whole
split, instead of on every part. Use it for an attribute that describes the
split itself, and to drop a parent attribute that says nothing true about the
pieces. A record count is an example of the second kind:

```rust
# #[cfg(feature = "uuid")] {
use nififf3::FlowFile;

let parent = FlowFile::builder()
    .attribute("filename", "records.csv")
    .attribute("record.count", "2") // true of the whole file, of no one part
    .content(&b"a\nb"[..]);

let part = parent
    .fragments()
    .attribute("mime.type", "text/csv")
    .without_attribute("record.count")
    .next_part()
    .content(&b"a"[..]);

assert_eq!(part.attribute("mime.type"), Some("text/csv"));
assert_eq!(part.attribute("record.count"), None);
# }
```

### Declaring the count

A bundle has to say how many flow files it holds. `MergeContent` fills a bin
once it holds as many flow files as the `fragment.count` attribute says. If a
bundle never declares a count, the bin times out and every flow file in it is
routed to `failure`. The count only has to appear on one flow file in the
bundle, so there are two ways to declare it.

If you know the total before you build the parts, `with_count` puts it on every
part, as in the example above. Often you don't know it, because the input is an
archive or a decoder that you read to the end. In that case, call `terminate` to
close the bundle with an empty flow file that carries the count. The parts can
then be streamed as they are produced:

```rust
# #[cfg(feature = "uuid")] {
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
# }
# Ok::<(), nififf3::Error>(())
```

The count covers every flow file in the bundle, so the terminator counts itself.
After `n` parts, the terminator has both `fragment.index` and `fragment.count`
set to `n + 1`. It carries no content, so defragmenting concatenates nothing for
it. `terminate` consumes the counter. If you emitted another part afterwards,
the bin would hold one flow file more than it declared, and it would never fill.

If you reassemble the bundle yourself instead of handing it to NiFi, you can
recognize the terminator: it is the part whose index equals the declared count
and whose content is empty. Drop it if your reassembly puts anything between
parts, such as a separator. `examples/merge.rs` does this.

`defragment` is the inverse, for the far end of a merge. It drops the fragment
attributes and restores `filename` from `segment.original.filename`, so the
reassembled flow file looks like the one the split started from. If the split
numbered its parts with custom keys, hold those keys in a `FragmentKeys` and
pass the same value to both ends: `fragments().with_keys(keys)` on the way out,
and `defragment_with(&keys)` on the way back.

```rust
# #[cfg(feature = "uuid")] {
use nififf3::FlowFile;

let parent = FlowFile::builder()
    .attribute("filename", "pair.txt")
    .content(&b"first\nsecond"[..]);
let part = parent.fragments().next_part().content(&b"first"[..]);

let merged = part.derive().defragment().content(&b"first\nsecond"[..]);
assert_eq!(merged.attributes()["filename"], "pair.txt");
assert!(!merged.attributes().contains_key("fragment.index"));
# }
```

## Errors

Parsing is the only thing that produces this crate's `Error`. The operations
that just move bytes return `std::io::Result` instead, and that includes
`write_to`, `into_memory`, the writers, and their async versions. So when you
handle those, you don't have to match on parse failures that cannot happen.

Both kinds of operation can hit one condition: content that ends before its
declared size. Each reports it in its own way, and converting between the two
keeps the detail:

```rust
use nififf3::{Error, FlowFile};

let mut truncated = FlowFile::builder().content(&b"hello"[..]).to_bytes();
truncated.truncate(truncated.len() - 2);

// `into_memory` returns `io::Result`, so it reports an `UnexpectedEof` that
// carries the detail. Using `?` to convert into an `Error` recovers that
// detail instead of wrapping it in `Error::Io`.
fn buffer(bytes: &[u8]) -> Result<FlowFile<Vec<u8>>, Error> {
    Ok(FlowFile::parse(bytes)?.into_memory()?)
}

assert!(matches!(
    buffer(&truncated),
    Err(Error::SizeMismatch { expected: 5, actual: 3 })
));
```

Both routes to this condition therefore match the same way. `from_bytes`,
`FlowFiles`, and `FlowFilesAsync` return `Error::SizeMismatch` directly, and an
`io::Error` from a byte-moving operation converts into it. Only a truncation is
recovered like this. Any other `io::Error` stays as `Error::Io`, with its
payload intact.

## Async I/O (`tokio` feature)

The async API mirrors the sync one. `parse_async` reads only the header and
exposes the content as a size-limited `AsyncRead`. `into_memory_async` and
`write_to_async` then consume that content.

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

A handler can take a `FlowFileRequest` extractor. It parses the flow file header
from the request body and streams the content incrementally, so a large flow
file never has to be in memory. Request bodies are untrusted, so the extractor
applies `Limits::recommended()`.

The body is read through axum's `DefaultBodyLimit`, as any other extractor's
body is, so it is capped at 2 MiB unless the router raises or disables it:

```rust,ignore
Router::new()
    .route("/large", post(handler))
    .layer(DefaultBodyLimit::disable()); // or ::max(bytes)
```

That cap applies to the bytes the client actually sends. That is the bound worth
trusting, because the size in the header is only a claim, and `Limits` governs
the header alone. Exceeding either limit gives a `413 Payload Too Large`, which
keeps "too big" distinct from the `400` that a malformed flow file gets.

`StrictFlowFileRequest` also rejects a request whose content type is not
`application/flowfile-v3`, with `415 Unsupported Media Type`.

A flow file with `AsyncRead` content implements `IntoResponse`, streaming the
body with `Content-Type: application/flowfile-v3`. The error type implements it
too, as a `400 Bad Request`.

Both extractors are newtypes, like axum's own, so a handler destructures them in
its signature: `async fn handler(FlowFileRequest(flow_file): FlowFileRequest)`.

### Batches

NiFi's `PostHTTP` sends several flow files concatenated under one request.
`FlowFilesRequest` and `StrictFlowFilesRequest` read them all, yielding one at a
time. `FlowFileRequest` parses only the first:

```rust,ignore
use nififf3::FlowFilesRequest;

async fn ingest(
    FlowFilesRequest(mut flow_files): FlowFilesRequest,
) -> Result<String, nififf3::Error> {
    let mut count = 0;
    while let Some(flow_file) = flow_files.next().await {
        let flow_file = flow_file?; // a parse failure surfaces here
        count += 1;
    }
    Ok(format!("took {count}"))
}
```

Each content is buffered as it is yielded. To stream them instead, build a
`FlowFileBody` from the request body and drive `FlowFile::parse_next_async` over
it.

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

A handler that turns one flow file into several returns a `FlowFilesResponse`.
Unpacking an archive is a typical case. The response hands your closure a
`FlowFilesWriterAsync` and streams whatever you write to it, so neither the
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

The producer reports failure as a boxed error, so `?` works directly on whatever
a decoder, plain I/O, or this crate hands back. You don't have to convert
between error types to satisfy the signature.

A streaming producer is the main reason `terminate` exists. It only learns how
many parts there were once the input is exhausted, and by then the earlier parts
are already on the wire. End the closure with
`writer.write_bytes(&parts.terminate()).await?` to declare the count without
holding anything back. `MergeContent` can only reassemble the response if the
count is declared somewhere.

Returning the response commits you to a 2xx status, so validate before you
return it. Report a problem with an individual part as a part of its own: a flow
file whose attributes say what went wrong. The good parts still arrive. The
type's documentation covers which failures you can report that way.
`FlowFilesResponse::blocking` covers producers that are unavoidably synchronous,
and `tests/unpack.rs` runs the whole thing against a real `.tar.gz`.

## CLI (`cli` feature)

A CLI for converting between flow files and JSON, installed with
`cargo install nififf3 --features cli`:

```console
$ echo -n "hello" | nififf3 create filename=greeting.txt > greeting.ff3
$ nififf3 to-json greeting.ff3
{"size":5,"attributes":{"filename":"greeting.txt"},"content":"aGVsbG8="}
$ nififf3 to-json greeting.ff3 | nififf3 from-json | cmp - greeting.ff3
```

- `nififf3 to-json [path]`: convert flow files to JSON, one object per line per
  flow file. Concatenated inputs are supported. The fields are `size`,
  `attributes`, and the base64-encoded `content`.
- `nififf3 from-json [path]`: the inverse. Read JSON objects as produced by
  `to-json`, and write flow files to stdout.
- `nififf3 attrs [path]`: print `size` and `attributes` as JSON, one object per
  line per flow file, without decoding the content.
- `nififf3 content [path]`: write the raw content of the flow files to stdout.
- `nififf3 create key=value ...`: create a flow file with the given attributes.
  The content is read from stdin.

A command that takes a path reads from stdin if you omit the path or pass `-`.
Commands process flow files one at a time, so a stream larger than memory is
fine. There are two exceptions. `to-json` and `from-json` buffer one flow file
at a time in order to base64 its content. `create` reads all of stdin into
memory, because it has to know the content length before it can write the
header. In that last case `--max-content-len` bounds the read as it happens, so
an oversized stdin is stopped rather than rejected afterwards.

Headers are trusted by default, as NiFi's own unpackager trusts them. For input
you haven't vetted, use `--max-attributes`, `--max-attribute-len`,
`--max-total-attribute-len`, and `--max-content-len`. They apply the
corresponding `Limits` to every flow file. Every subcommand honors them,
including the two that never run a header parser: `from-json` checks each flow
file it decodes, and `create` checks the one it built.

```console
$ nififf3 attrs --max-attributes 4096 --max-content-len 1073741824 untrusted.ff3
```

## Examples

`examples/` holds runnable programs. Each one is self-contained and asserts its
own output. Run one with `cargo run --example <name>`, adding `--features` where
the table calls for it:

| example | features | |
| --- | --- | --- |
| `transform` / `transform_async` | none / `tokio` | one flow file in, one out |
| `split` / `split_async` | none / `tokio` | one in, many out |
| `merge` / `merge_async` | none / `tokio` | many in, one out |
| `axum_service` | `axum` | one in, one or many out, over HTTP |

Read `split` and `merge` together. `merge` reassembles what `split` produced,
using the fragment attributes the way NiFi's `MergeContent` does in `defragment`
mode.

## Feature flags

The sync parsing and serialization API is always available. Only `uuid` is on by
default.

- `uuid`: `derive` and `fragments`, which mint flow file and fragment
  identifiers. On by default, because both are central to using this crate with
  NiFi. With `default-features = false`, a parse-and-serialize build depends
  only on `thiserror`.
- `tokio`: async parsing and serialization over `AsyncRead` and `AsyncWrite`.
- `stream`: `FlowFilesAsync::into_stream`, for composing with `StreamExt`.
  Implies `tokio`.
- `axum`: request extractor and response types. Implies `stream`.
- `tempfile`: spool content of unknown length to a temporary file.
- `serde`: `Serialize` and `Deserialize` for in-memory flow files, with the
  content base64 encoded. This is the JSON shape the CLI uses. Reader-backed
  flow files serialize through `StreamingFlowFile`, which pushes the content
  through the base64 encoder instead of buffering it first.
- `cli`: the `nififf3` binary. Implies `serde`.

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

A field length is 2 bytes big-endian. A value of `0xFFFF` or above is written as
the marker `0xFF 0xFF`, followed by the value as 4 bytes big-endian. Attributes
are serialized in sorted key order, so the output is deterministic. NiFi accepts
any order.

The extended field length encodes a `u32`, but NiFi reads it into a Java `int`.
An attribute of 2 GiB or more is therefore writable here and unreadable in NiFi.
This crate itself panics only at `u32::MAX`, where the format runs out.

A header may also repeat a key. Attributes parse into a map, so the last value
wins, and re-serializing emits a shorter header than the one that was read. NiFi
collapses duplicates the same way. It does mean that parsing and then
serializing is not byte-preserving for input built to exploit it, which matters
if you hash or sign the encoded form.
