# nififf3

Read and write NiFi flow files, and build HTTP services that NiFi can call.

NiFi can hand a flow file to an outside HTTP service and take the response
back as a new flow file. The format it uses for that is FlowFile V3
(`application/flowfile-v3`). This crate reads and writes that format, and,
with the `axum` feature, turns it into request and response types for the
[axum](https://docs.rs/axum) web framework. The bytes match what NiFi's
`FlowFilePackagerV3` writes and `FlowFileUnpackagerV3` reads.

A flow file is a piece of content, which is just bytes, together with a set of
attributes, which are string key-value pairs like `filename` or `mime.type`.
In this crate it is the type `FlowFile`.

## A service in one function

Here is a complete request handler. It takes a CSV file from NiFi, checks it
line by line, and answers with a short summary.

```rust
# #[cfg(all(feature = "axum", feature = "uuid"))] {
use axum::http::StatusCode;
use nififf3::{FlowFile, attr};
use tokio::io::{AsyncBufReadExt, BufReader};

async fn summarize(flow_file: FlowFile) -> Result<FlowFile<Vec<u8>>, (StatusCode, String)> {
    if flow_file.attribute(attr::MIME_TYPE) != Some("text/csv") {
        return Err((StatusCode::UNSUPPORTED_MEDIA_TYPE, "text/csv only".into()));
    }
    let summary = flow_file.derive();
    let failed = |err: std::io::Error| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
    let mut lines = BufReader::new(flow_file.into_content()).lines();
    let (mut columns, mut rows) = (None, 0u64);
    while let Some(line) = lines.next_line().await.map_err(failed)? {
        let fields = line.split(',').count();
        match columns {
            None => columns = Some(fields),
            Some(n) if n == fields => rows += 1,
            Some(n) => {
                let message = format!("row {} has {fields} fields, expected {n}", rows + 1);
                return Err((StatusCode::BAD_REQUEST, message));
            }
        }
    }
    Ok(summary
        .attribute(attr::MIME_TYPE, "text/plain")
        .attribute(attr::RECORD_COUNT, rows.to_string())
        .content(format!("{rows} rows, {} columns\n", columns.unwrap_or(0))))
}
# }
```

In axum, a handler is an async function that runs for each request. The types
of its arguments say what it wants from the request, and its return value
becomes the response. Taking a `FlowFile` argument means: parse the request
body as a flow file and give it to me. Returning a `FlowFile` means: send this
flow file back.

Before the handler runs, the whole request has been received. Small content
is held in memory. Large content is written to a temporary file. The handler
does not need to know which, because the content reads the same way either
way. `into_content` takes the content out as a reader, and `lines` goes
through it one line at a time. A gigabyte of CSV costs this handler the memory
of one line.

`derive` starts a new flow file that carries over the attributes of the old
one and gets a fresh `uuid`, which is what NiFi does when a processor produces
a new flow file. Setting an attribute on it again replaces the carried value.
`content` finishes the new flow file with the bytes you give it. Those are
held in memory, which is fine for a summary. `derive` comes before the content
is read because it needs to look at the flow file, and `into_content` uses the
flow file up.

The error type is axum's usual pair of a status code and a message. The
handler picks statuses that say whose fault it was: a `415` when the file is
not CSV, a `400` when a row has the wrong number of fields, a `500` when the
server could not read its own temporary file. NiFi sees these as a failed
request and routes the original flow file accordingly.

## Running it

A handler goes on a router, which maps paths to handlers, and the router goes
on a server:

```rust,no_run
# #[cfg(feature = "axum")]
use axum::{Router, routing::post};
# #[cfg(feature = "axum")]
# async fn summarize() {}
# #[cfg(feature = "axum")]
#[tokio::main]
async fn main() -> std::io::Result<()> {
    let app = Router::new().route("/summarize", post(summarize));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    axum::serve(listener, app).await
}
# #[cfg(not(feature = "axum"))] fn main() {}
```

On the NiFi side, `PackageFlowFile`, or `MergeContent` with the FlowFile
Stream v3 format, wraps a flow file in this format. `InvokeHTTP` posts it to
`http://your-host:3000/summarize`, and `UnpackContent` turns the response
back into a flow file.

Two settings matter once real files start arriving:

- **Request size.** Axum refuses request bodies over 2 MiB unless told
  otherwise. `DefaultBodyLimit::max` raises that for the whole router.
- **Where content goes.** By default content up to 4 MiB stays in memory and
  anything larger goes to a temporary file in the system's temp directory.
  `SpoolConfig` changes the threshold and the directory. Add it to the router
  as an `Extension`, which is axum's way of handing a value to every handler.

```rust,no_run
# #[cfg(feature = "axum")] {
use axum::{Extension, Router, extract::DefaultBodyLimit, routing::post};
use nififf3::SpoolConfig;
# async fn summarize() {}

let app: Router = Router::new()
    .route("/summarize", post(summarize))
    .layer(DefaultBodyLimit::max(10 << 30))
    .layer(Extension(
        SpoolConfig::new()
            .with_max_in_memory(16 << 20)
            .with_directory("/var/tmp/flowfiles"),
    ));
# let _ = app;
# }
```

`examples/axum_service_large_files.rs` is a whole service set up this way,
which you can run and send files to.

## Why the request is read first

NiFi's HTTP client sends the complete request before it starts reading the
response. If a handler began streaming its response while it was still reading
the request from the network, the two would get stuck as soon as the response
grew past what the operating system buffers: NiFi would be waiting to finish
sending, and the handler would be waiting for NiFi to read. Reading the whole
request before the handler runs makes that impossible, whatever the handler
does. `tests/nifi_client.rs` shows the stall happening over a real socket,
and shows that the `FlowFile` argument avoids it.

If you have a reason to read a request while it is still arriving, and you
know your client reads responses concurrently, `FlowFileBody` gives you the
raw request body as a reader. That is the only way to get into the stall, so
treat it as an advanced option.

## Other shapes of handler

**Many flow files in.** NiFi can send several flow files in one request, one
after another. Take a `Spool` argument instead of a `FlowFile` to get the
whole body, and read flow files out of it with `FlowFilesReaderAsync`.

**Many flow files out.** A handler that splits one flow file into several
returns a `FlowFilesResponse`. You give it a function that writes flow files
to a writer, and the response streams them to NiFi as they are written.
`fragments` numbers them with the attributes NiFi's `MergeContent` processor
needs to put them back together.

**New content of unknown size.** A handler that produces content it cannot
size in advance, such as a compressed version of the input, can hand a reader
to `spool_async`. The output is kept in memory or written to a temporary file
by the same rule as the input, and the result is a `FlowFile` ready to return.

**Content in memory.** `into_memory_async` reads a flow file's content into a
`Vec<u8>`. Use it when you need all the bytes at once and know they will fit.

`examples/axum_service.rs` has a one-in-one-out and a one-in-many-out handler.
`examples/axum_service_large_files.rs` has a split, a compressing transform
built with `spool_async`, and a merge that takes many flow files in.

## Flow files without HTTP

The same types work on their own, for reading and writing flow files in
programs that are not services.

To create a flow file, add attributes and then supply the content:

```rust
use nififf3::FlowFile;

let flow_file = FlowFile::builder()
    .attribute("filename", "greeting.txt")
    .content(&b"Hello, NiFi!"[..]);

let bytes = flow_file.to_bytes();
```

`to_bytes` gives you the serialized flow file. `write_to` writes it to any
`std::io::Write`, and `write_to_async` to any `tokio::io::AsyncWrite`.

To read one back:

```rust
use nififf3::FlowFile;

# let bytes = FlowFile::builder().attribute("filename", "greeting.txt").content(&b"Hello, NiFi!"[..]).to_bytes();
let flow_file = FlowFile::from_bytes(&bytes)?;
assert_eq!(flow_file["filename"], "greeting.txt");
assert_eq!(flow_file.content_str()?, "Hello, NiFi!");
# Ok::<(), Box<dyn std::error::Error>>(())
```

Indexing with `[]` panics when an attribute is not set, like indexing a
`HashMap`. `attribute` returns an `Option` instead, and `parse_attribute`
reads and parses in one step, for attributes that hold numbers.
`set_attribute` and `remove_attribute` change attributes in place.

There are a few ways to read, depending on where the bytes are:

- `FlowFile::from_bytes` reads one flow file from a buffer that holds exactly
  one.
- `FlowFile::from_reader` reads one whole flow file from any reader.
- `FlowFile::parse` reads only the header and leaves the content to be read
  later, as a reader that knows how long it should be. If the bytes run out
  early, reading it is an error rather than a quiet end.
- `FlowFiles` goes through a reader holding several flow files one after
  another, loading each one's content. `FlowFilesReader` does the same but
  lets you stream each content instead.

Each of these has an async version with `_async` on the end, under the
`tokio` feature.

The content has to be sized before it is written, because the format puts the
length before the bytes. `content` takes the size from the bytes you give it.
When you have a reader instead, `buffered` reads it into memory, `tempfile`
writes it to disk, `spool_async` picks between the two by size, and `reader`
takes a size you already know and streams the rest.

Parsing does not limit how many attributes a header may declare, or how large
the content may be. It does protect memory: no buffer is ever allocated from a
length in the header, so a corrupt header can only cost as much memory as
bytes actually arrive.

## Examples

`examples/` holds runnable programs. Each is self-contained and checks its own
output. Run one with `cargo run --example <name>`, adding `--features` as the
example's header comment says.

- `transform`, `split` and `merge` show the three basic shapes in memory, with
  `_async` versions.
- `axum_service` puts them behind HTTP, driven in-process so it runs without
  opening a port.
- `axum_service_large_files` is a real server on a local port, set up for
  content too large to hold in memory.

## Command line

`cargo install nififf3 --features cli` installs a `nififf3` binary that
converts between flow files and JSON, for looking at files by hand:

```console
$ echo -n hello | nififf3 create filename=greeting.txt > greeting.ff3
$ nififf3 to-json greeting.ff3
{"size":5,"attributes":{"filename":"greeting.txt"},"content":"aGVsbG8="}
```

The subcommands are `to-json`, `from-json`, `attrs`, `content` and `create`.
Each reads stdin when given no path, and handles files holding several flow
files one at a time.

## Feature flags

Reading and writing flow files with `std::io` is always available. The rest
is opt-in:

- `uuid`: `derive` and `fragments`, which generate identifiers. On by default.
- `tracing`: logging through the `tracing` crate. On by default. Turning both
  defaults off leaves a build that depends only on `thiserror`.
- `tokio`: async reading and writing.
- `tempfile`: content on disk. With `tokio` as well, `Spool` and `SpoolConfig`.
- `stream`: `FlowFilesAsync::into_stream`. Implies `tokio`.
- `axum`: the request and response types. Implies `tokio`, `tempfile` and
  `stream`.
- `serde`: `Serialize` and `Deserialize`, with the content base64 encoded.
- `cli`: the `nififf3` binary. Implies `serde`.

## Logging

With the `tracing` feature, the crate reports what it does through the
`tracing` crate. Nothing is printed unless your program sets up a subscriber.
The usual one is `tracing-subscriber`, whose `EnvFilter` reads `RUST_LOG`, so
`RUST_LOG=nififf3=debug` works once that is in your dependencies.

Every message about a flow file carries its `uuid`, its `filename` when it has
one, and its `size`, so a busy log can be followed back to one file.

- `error`: something went wrong on the server's side, such as a temporary
  file that could not be written.
- `warn`: something your own `Result`s could not tell you, such as a response
  that failed after its status was already sent, or a request rejected before
  your handler ran.
- `info`: one line per request received, saying whether its content went to
  memory or to disk.
- `debug`: one line per flow file read, written or spooled, and one per
  response finished.
- `trace`: every step of the content being read.

## Wire format

For reference, this is what NiFi's `FlowFilePackagerV3` writes:

```text
"NiFiFF3"          7-byte magic header
attribute count    field length
per attribute:     key, then value, each a field length + UTF-8 bytes
content size       8-byte big-endian integer
content            <content size> bytes
```

A field length is 2 bytes, big-endian. A value of `0xFFFF` or more is written
as the marker `0xFF 0xFF` followed by the value in 4 bytes, big-endian.
Attributes are written in sorted key order, so the same flow file always
serializes to the same bytes. NiFi accepts any order.

Two details worth knowing. The 4-byte length can express values NiFi cannot
read, since NiFi reads it into a signed 32-bit integer, so an attribute of
2 GiB or more can be written here but not read there. And a header may repeat
a key, which parses into a map keeping the last value, so writing it back out
produces a shorter header than was read.
