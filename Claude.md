# Implementation Plan

## Format reference

NiFi FlowFile V3 binary format (matches `FlowFilePackagerV3`/`FlowFileUnpackagerV3`):

1. Magic header: the 7 ASCII bytes `NiFiFF3`.
2. Attribute count, as a *field length* (see below).
3. For each attribute: key then value, each written as a length-prefixed UTF-8
   string (field length + bytes).
4. Content size: 8-byte big-endian `u64`.
5. Content bytes.

A *field length* is 2 bytes big-endian, unless the value is >= 0xFFFF, in which
case it is the two bytes `0xFF 0xFF` followed by the value as 4 bytes
big-endian. Multiple flow files may be concatenated back-to-back in one stream.

## Crate layout & features

- `format` module: constants + header encode/decode against byte buffers.
- `sync` parsing/serialization: `std::io::Read`/`Write` (always available).
- `async` parsing/serialization: `tokio::io::AsyncRead`/`AsyncWrite`, behind a
  `tokio` feature.
- `axum` feature (implies `tokio`): extractor + `IntoResponse`.
- `cli` feature: `clap`-based binary (`src/bin/nififf3.rs`,
  `required-features = ["cli"]`), pulling in `serde_json` + `base64`.

## Tasks

### Core

- [x] Define `FlowFile<R>` (private fields, accessors, `into_parts`) and the
  `Error` type (`thiserror`): bad magic, invalid UTF-8 attribute, size
  mismatch, I/O.
- [x] Header encoding/decoding helpers (field lengths, strings) with unit
  tests against hand-written byte vectors.
- [x] Sync parsing: `FlowFile::parse(impl Read)` reads only the header and
  returns the content as a `Take`-limited reader (lazy); `parse_bytes` for
  `Vec<u8>`/slices validates `size` against the actual remaining length.
- [x] Sync serialization: `write_to(impl Write)` for `R: Read` content (size
  known from header) and for `Vec<u8>`.
- [x] Builder API: `FlowFile::builder().attribute(k, v)...` with
  `content(Vec<u8>)` (size inferred) and `reader(r, size)` (size required —
  the format stores the size before the content, so it must be known up
  front).
- [x] Async parsing + serialization mirroring the sync API (`tokio` feature).
- [x] Builder helpers for readers of unknown size: `buffered` (spool into
  memory) and, behind a `tempfile` feature, `tempfile` (spool into an
  anonymous temporary file), plus `_async` variants.

### CLI

- [x] `nififf3 to-json [path]`: read one *or more* concatenated flow files,
  emit one JSON object per line (`size`, `attributes`, base64 `content`).
  `-`/no path = stdin.
- [x] `nififf3 from-json [path]`: accept the stream `to-json` produces
  (one or more JSON values) and write concatenated flow files to stdout.
- [x] `nififf3 create key=value ...`: content from stdin, flow file to stdout.
- [x] CLI integration tests (round-trip through `to-json`/`from-json`).

### Axum

- [x] `FromRequest` extractor yielding `FlowFile<FlowFileBody>` where
  `FlowFileBody: AsyncRead` streams the request body (arbitrarily large
  content, never fully in memory).
- [x] `IntoResponse` for `FlowFile<R: AsyncRead>` streaming the content, with
  `Content-Type: application/flowfile-v3`.
- [x] `IntoResponse` for the error type (400 with message).
- [x] Integration test driving a handler with `tower::ServiceExt::oneshot`.

### Polish

- [x] Doc comments with examples on the public API; `cargo doc` clean.
- [x] `cargo fmt`, `cargo clippy --all-features` clean; tests pass for the
  feature matrix (default, `tokio`, `axum`, `cli`, `--all-features`).
- [x] Rewrite the README as a reference with examples, embed it as the crate
  docs root (`#![doc = include_str!]`), enable `missing_docs` warnings, and
  add usage examples throughout the item docs.

## Potential next steps (unscheduled — for review)

- [x] Axum extractor strictness: optionally reject requests whose
  `Content-Type` is not `application/flowfile-v3` (`StrictFlowFileRequest`,
  responding 415; media type parameters and case are ignored).
- [x] First-class multi-flow-file APIs: `FlowFiles` (sync `Iterator`),
  `parse_next_async`, and `FlowFilesAsync` with an async `next()` (a real
  `futures::Stream` impl was skipped — it needs hand-rolled header state
  machines or an async-stream dependency, and the `next()` loop covers the
  use case).
- [x] `SpooledTempFile` builder helper (memory up to a threshold, then disk)
  as a middle ground between `buffered` and `tempfile` (sync only —
  `SpooledTempFile` has no async I/O).
- [x] Optional `serde` support for `FlowFile<Vec<u8>>` (base64 content);
  the CLI's JSON model now lives in the library (`cli` implies `serde`).
- [ ] CLI ergonomics: streaming `to-json` (currently buffers the whole
  input), an `attrs`/`inspect` subcommand that prints attributes without
  decoding content, and a `content` subcommand extracting raw content.
- [ ] Parser hardening for untrusted input: configurable limits on attribute
  count and attribute length (a crafted header can currently request up to
  4 GiB allocations per attribute) and a request-size limit knob for the
  axum extractor.

### May be later
- [ ] Fuzzing (`cargo-fuzz`) and property-based round-trip tests for the
  parser; interop tests against files produced by a real NiFi instance.
- [ ] CI (feature-matrix build/test/clippy/fmt), and crates.io publishing
  metadata (repository/keywords/categories).


### Won't do

- [-] ~~FlowFile V1/V2 support (`application/flowfile`, `flowfile-v2`) with format auto-detection, mirroring NiFi's other packagers.~~
