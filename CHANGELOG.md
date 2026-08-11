# Changelog

## Unreleased

### Added

- `Fragments::attribute`, `attributes` and `without_attribute`: adjust the
  attributes every part of a split carries, once for the set rather than on each
  part. For what is true of the split rather than of one fragment — the format
  the parts were cut into, the run that produced them — and, in the other
  direction, for a parent attribute that does not survive being cut up. A part
  that sets the same key on its own builder still wins.

### Fixed

- `Limits::max_total_attribute_len` now bounds what the parser *buffers*, not
  only what it accepts. Both attribute limits are applied to each declared field
  length before its bytes are read, so an attribute larger than the remaining
  budget is refused on its declaration. Previously the total was checked against
  a running tally after each key-value pair, so one attribute could be read into
  memory in full before anything fired — unbounded for a caller who set this
  limit and no `max_attribute_len` beside it.

## 0.3.3

### Changed

- Published under the Unlicense.

## 0.3.2

Streaming reads that keep the stream positioned for you, and the documentation
to steer people to the right one of the three ways to read.

### Added

- `FlowFilesReader` and `FlowFilesReaderAsync`, with the `StreamedContent` and
  `StreamedContentAsync` their flow files carry: read a stream without
  buffering the content, and without making the caller responsible for the
  stream's position. `FlowFile::parse_next` requires every content to be
  consumed before the next flow file is parsed, and misparses silently when one
  is dropped unread — these skip whatever is left, so reading none of a
  content, some of it, or all of it are equally correct.
- `FlowFile::skip_content` and `skip_content_async`, the third option beside
  `into_memory` and `write_to` for code using `parse_next` directly: what to
  call when only the attributes were wanted.

### Documentation

- The three ways to read a stream — `FlowFiles` (buffers), `FlowFilesReader`
  (streams, positions for you) and `parse_next` (streams, you position it) —
  are compared side by side from the README and from each of the three, so
  whichever one you land on points at the other two.
- `parse_next` spells out what dropping a content unread does, including the
  case where the content begins with a valid header and the result is a flow
  file that was never sent rather than an error.
- "Creating flow files" is split into subsections and its comparisons turned
  into tables, and the error model moves out into a section of its own — it
  describes how the whole crate reports failure, not how to build a flow file.

## 0.3.1

### Fixed

- `nififf3 create` applied `--max-content-len` *after* reading all of stdin
  into memory, which is the opposite of a guard. It now stops one byte past the
  limit, and settles the attribute limits before reading anything.
- The response body's length check could in principle underflow, since it
  subtracted a running total covering the header from a size that does not.
  Not reachable — the header always drains — but nothing at the subtraction
  said so.

### Added

- `Limits::recommended` is `const`, matching `Limits::UNLIMITED`.
- Generative tests over the parser: arbitrary and damaged flow files must be
  rejected rather than panic, anything serialized must parse back identically,
  and both sides of the `0xFFFF` field-length boundary must round-trip.

### Documentation

- Where each limit is enforced, not just that it is. Every path now refuses
  oversized content before buffering it except `from-json`, where serde has
  decoded it by the time there is a flow file to judge; that difference is
  written down rather than left to be discovered.

## 0.3.0

A review pass over the whole crate. Several breaking changes, all small at the
call site; the bug fixes are the reason to upgrade.

### Fixed

- `Fragments` configured with custom attribute keys no longer leaves the
  parent's values under those keys on its parts. A re-split could inherit a
  stale fragment count, which describes a bundle `MergeContent` can never fill.
- A flow file response whose content reader ends early now fails the body
  instead of completing one short of its own `Content-Length`. The client saw a
  flow file declaring more content than it carried.
- `FlowFilesResponse::buffer_size(0)` hung the response; it is clamped to one
  byte.
- The axum body adapter no longer polls the request stream after it has ended,
  which a flow file declaring more content than the body carries could provoke.
- A declared size that disagrees with the content now panics in every build
  rather than only under `debug_assertions`, in `to_bytes` and in serde's
  `Serialize`. Likewise a fragment index past the declared count.
- The strict extractor abbreviates the `Content-Type` it reflects into a 415
  body, instead of echoing back however much the client sent.

### Added

- `Limits::max_total_attribute_len`, capping the attribute bytes in a header —
  2 MiB by default. The per-attribute limits could not express the aggregate.
- `Limits::check`, applying the same limits to a flow file already in hand. The
  CLI's `--max-*` flags now work on every subcommand, including `from-json` and
  `create`, which never run a header parser.
- `FlowFilesRequest` and `StrictFlowFilesRequest`: extractors for a request
  carrying several concatenated flow files, which is what NiFi's `PostHTTP`
  sends. `FlowFileBody` is constructible now too, for driving the parse
  yourself.
- `FlowFile::attribute`, `from_parts`, `from_vec`, `map_bytes`,
  `map_content_sized`, `write_bytes_to`, and `PartialEq`/`Eq`.
- `FlowFiles`/`FlowFilesAsync` gained `get_ref`, `get_mut` and `into_inner`;
  the writers gained `get_ref`.
- `FragmentKeys` is public, so `defragment_with` can undo a split that used
  custom keys.
- `StreamingFlowFile`, which serializes a reader-backed flow file through the
  base64 encoder rather than buffering the content first.
- `Error::WriterPoisoned` and `Error::HeaderTooLarge`.
- `Stream` is re-exported under the `stream` feature.
- `rust-version = "1.88"`, verified against the locked dependency set.

### Changed

- `FlowFile::into_bytes` is now `into_memory` (`into_bytes_async` →
  `into_memory_async`), which pairs it with the `into_reader` it inverts and
  stops it reading as a sibling of `to_bytes`.
- `Fragments::next` is now `next_part`.
- `FlowFilesResponse::from_vec` is now `buffered`, taking any `IntoIterator`.
- `Limits` setters are `with_max_*`, freeing `max_attributes()` and friends to
  be getters; each accepts `None` to clear a limit. `Limits::new` is gone —
  `Limits::recommended()` names the same thing, and `UNLIMITED` is the neutral
  starting point.
- `FlowFileRequest` is a newtype like `StrictFlowFileRequest`; both destructure
  in the handler signature, as axum's own extractors do.
  `StrictFlowFileRequest::into_inner` is gone in favour of the public field.
- `uuid` is behind a feature, on by default. `default-features = false` gets a
  parse-and-serialize build whose only dependency is `thiserror`, without
  `derive` or `fragments`.
- `Error::Io` is transparent, so it no longer prefixes the error it carries.
- `Error::InvalidAttribute` lost its `From<FromUtf8Error>`, which gave callers
  a silent `?` from any `String::from_utf8` failure.
- `Fragments` no longer implements `Clone`, which had let a caller keep using a
  counter `terminate` was meant to consume.

## 0.2.0

Initial published release.
