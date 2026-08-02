# Changelog

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
- `FlowFilesReader` and `FlowFilesReaderAsync`: read a stream of flow files
  without buffering their content, and without leaving the caller responsible
  for the stream's position. Where `parse_next` requires each content to be
  consumed before the next flow file is parsed — and silently misparses if one
  is dropped unread — these skip whatever is left, so reading none, some or all
  of a content are equally correct.
- `FlowFile::skip_content` and `skip_content_async`, for walking a stream when
  only the attributes are wanted — `parse_next` requires each content to be
  consumed, and this is how to consume one you do not want.
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
