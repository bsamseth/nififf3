# Changelog

## 0.3.5

A documentation release. The API and its behavior are unchanged, so upgrading
from 0.3.4 changes nothing about how your code runs.

### Documentation

- The doc comments, the code comments and this changelog are rewritten in the
  same plain style as the README. Sentences carry one idea each, the em dashes
  are gone, and each item's documentation opens with what the item does rather
  than with a comparison to a sibling method. The detail is the same
  throughout: every number, error type and trade-off that was written down
  before is still written down.
- `From<io::Error> for Error` and `From<Error> for io::Error` now document
  themselves. One doc block used to cover both conversions while sitting on
  one of them, so what you needed to know about turning an `Error` into an
  `io::Error` was rendered under the impl that does the opposite. Each
  conversion now carries its own description and its own example.

## 0.3.4

This release makes parsing and serializing faster, and adds the shorthands that
were missing from the entry points.

### Added

- `Fragments::attribute`, `attributes` and `without_attribute`: set or drop
  attributes for every part of a split at once, rather than on each part. Use
  them for something that is true of the split rather than of one fragment,
  such as the format the parts were cut into or the run that produced them. Use
  `without_attribute` for a parent attribute that says nothing true about the
  pieces, such as a record count. If a part sets the same key on its own
  builder, the part's value wins.
- `FlowFile::from_reader`, `from_reader_with_limits` and `from_reader_async`:
  read one whole flow file from a reader, content included. This is the eager
  counterpart to `parse`, and it fills the one empty cell in the entry-point
  table: in memory, from a reader. It reconciles the two error types for you,
  so a truncated content arrives as `Error::SizeMismatch` instead of wrapped in
  `Error::Io`. It reads exactly one flow file and leaves the reader on the byte
  after it, so trailing bytes are not an error the way they are for
  `from_bytes`.
- `FlowFileBuilder::empty`: finish a build with no content. A flow file that
  carries only attributes is a normal thing in NiFi, and `content(Vec::new())`
  was an awkward way to write one.
- `FlowFile::serialized_len`: how many bytes the flow file serializes to. It is
  computed from the attributes and the declared size, without serializing
  anything, and it is exact rather than an estimate. It also works on a
  reader-backed flow file that has not been read yet. That is the case where a
  `Content-Length`, a size-limited sink, or a pre-sized buffer has no other way
  to find the answer.

### Performance

Every parsing and serializing path is faster, several of them by more than
half. This entry gives no percentages on purpose. The benchmarks live in
`benches/`, and the numbers they report depend on the machine enough that
quoting one here would mislead you. Run `cargo bench` for figures that apply to
yours.

- Buffers for content and for attributes are reserved from the bytes the reader
  has actually delivered. They used to be left to `read_to_end`, which doubles
  a buffer blindly as it grows. Ordinary content and every ordinary attribute
  is now allocated once, at exactly its size. A declared length is still never
  trusted: the first reservation is capped at 64 KiB, and each one after it is
  capped at how much has already arrived.
- Content buffered by `into_memory` and the calls beside it no longer sits in
  an allocation of up to twice its size.
- The header buffer is allocated once at its exact size instead of being grown
  into, and the attributes are sorted as borrowed pairs rather than as keys
  looked up in the map a second time. That saves one hash and one probe per
  attribute. On a header with many attributes, that is most of the work of
  writing one, so this is the largest of the wins there.
- `write_bytes_to`, `FlowFilesWriter::write_bytes` and their async twins no
  longer serialize into a temporary buffer first. That temporary copied the
  whole content for nothing. The write path now copies the content once, and
  one copy is the minimum. The header and the content go out as two writes
  rather than one, so an unbuffered writer sees two syscalls.

### Fixed

- `Limits::max_total_attribute_len` now bounds how much the parser buffers, and
  not only what it accepts in the end. Both attribute limits apply to the
  length a field declares, before its bytes are read. In other words, an
  attribute that claims to be larger than the remaining budget is rejected on
  its declaration, and never read into memory. Previously the total was checked
  against a running tally after each key-value pair, so one attribute could be
  read into memory in full before anything fired. A caller who set this limit
  without `max_attribute_len` beside it therefore had no bound at all.

## 0.3.3

### Changed

- Published under the Unlicense.

## 0.3.2

This release adds streaming reads that keep the stream positioned for you, and
the documentation to steer you to the right one of the three ways to read.

### Added

- `FlowFilesReader` and `FlowFilesReaderAsync`, along with the
  `StreamedContent` and `StreamedContentAsync` their flow files carry. They
  read a stream without buffering the content, and they keep track of the
  stream's position for you. `FlowFile::parse_next` requires you to consume
  every content before parsing the next flow file, and it misparses silently if
  you drop one unread. These two skip whatever is left instead, so reading none
  of a content, some of it, or all of it are equally correct.
- `FlowFile::skip_content` and `skip_content_async`: what to call when you used
  `parse_next` directly and only wanted the attributes. They are the third
  option beside `into_memory` and `write_to`.

### Documentation

- The three ways to read a stream are compared side by side, from the README
  and from the documentation of each one. `FlowFiles` buffers each content,
  `FlowFilesReader` streams it and positions the stream for you, and
  `parse_next` streams it and leaves the positioning to you. Whichever one you
  land on points at the other two.
- `parse_next` spells out what happens if you drop a content unread. That
  includes the case where the content itself begins with a valid header, where
  you get back a flow file that was never sent rather than an error.
- "Creating flow files" is split into subsections, and its comparisons are now
  tables. The error model moved into a section of its own, because it describes
  how the whole crate reports failure rather than how to build a flow file.

## 0.3.1

### Fixed

- `nififf3 create` read all of stdin into memory and applied
  `--max-content-len` afterwards, so an oversized stdin was resident before it
  was rejected. One byte past the limit is all it takes to know the limit was
  exceeded, so that is where it stops now. It also settles the attribute limits
  before reading anything.
- The response body's length check could underflow in principle, because it
  subtracted a running total that covers the header from a size that does not.
  The header always drains, so this was never reachable, but nothing at the
  subtraction said so. The subtraction saturates now.

### Added

- `Limits::recommended` is `const`, matching `Limits::UNLIMITED`.
- Seeded generative tests over the parser. They check three things: arbitrary
  and damaged flow files are rejected rather than panicking, anything
  serialized parses back identically, and both sides of the `0xFFFF`
  field-length boundary round-trip.

### Documentation

- The documentation says where each limit is enforced, not only that it is
  enforced. Every path refuses oversized content before buffering it, except
  `from-json`. There serde has decoded the content by the time there is a flow
  file to check, and that difference is now written down instead of being left
  to be discovered.

## 0.3.0

This release is a review pass over the whole crate. It makes several breaking
changes, all of them small at the call site. The bug fixes are the reason to
upgrade.

### Fixed

- `Fragments` configured with custom attribute keys no longer leaves the
  parent's values under those keys on its parts. A re-split could inherit a
  stale fragment count, and that count describes a bundle `MergeContent` can
  never fill.
- A flow file response whose content reader ends early now fails the body,
  instead of completing short of its own `Content-Length`. The client used to
  see a flow file declaring more content than it carried.
- `FlowFilesResponse::buffer_size(0)` hung the response. It is clamped to one
  byte now.
- The axum body adapter no longer polls the request stream after it has ended.
  A flow file that declares more content than the body carries could provoke
  that.
- A declared size that disagrees with the content now panics in every build, in
  `to_bytes` and in serde's `Serialize`. Previously it only panicked under
  `debug_assertions`. A fragment index past the declared count is checked the
  same way.
- The strict extractor abbreviates the `Content-Type` it puts in a 415 body,
  instead of echoing back however much the client sent.

### Added

- `Limits::max_total_attribute_len`, which caps the attribute bytes in a
  header. It defaults to 2 MiB. The per-attribute limits could not express an
  aggregate like this.
- `Limits::check`, which applies the same limits to a flow file you already
  hold. The CLI's `--max-*` flags now work on every subcommand, including
  `from-json` and `create`. Those two never run a header parser.
- `FlowFilesRequest` and `StrictFlowFilesRequest`: extractors for a request
  carrying several concatenated flow files. That is what NiFi's `PostHTTP`
  sends. `FlowFileBody` is constructible now too, so you can drive the parsing
  yourself.
- `FlowFile::attribute`, `from_parts`, `from_vec`, `map_bytes`,
  `map_content_sized`, `write_bytes_to`, and `PartialEq`/`Eq`.
- `FlowFiles` and `FlowFilesAsync` gained `get_ref`, `get_mut` and
  `into_inner`, and the writers gained `get_ref`.
- `FragmentKeys` is public, so `defragment_with` can undo a split that used
  custom keys.
- `StreamingFlowFile`. It serializes a reader-backed flow file through the
  base64 encoder, rather than buffering the content first.
- `Error::WriterPoisoned` and `Error::HeaderTooLarge`.
- `Stream` is re-exported under the `stream` feature.
- `rust-version = "1.88"`, verified against the locked dependency set.

### Changed

- `FlowFile::into_bytes` is now `into_memory`, and `into_bytes_async` is now
  `into_memory_async`. The new name pairs it with the `into_reader` it inverts,
  and it no longer reads as a sibling of `to_bytes`.
- `Fragments::next` is now `next_part`.
- `FlowFilesResponse::from_vec` is now `buffered`, and it takes any
  `IntoIterator`.
- `Limits` setters are named `with_max_*`, which frees `max_attributes()` and
  the getters beside it to take those names. Each setter accepts `None` to
  clear a limit. `Limits::new` is gone, because `Limits::recommended()` names
  the same thing, and `UNLIMITED` is the neutral starting point to build up
  from.
- `FlowFileRequest` is a newtype, like `StrictFlowFileRequest`. Both
  destructure in the handler signature, the way axum's own extractors do.
  `StrictFlowFileRequest::into_inner` is gone, because the field is public.
- `uuid` is behind a feature that is on by default. With
  `default-features = false` you get a parse-and-serialize build whose only
  dependency is `thiserror`, and which has neither `derive` nor `fragments`.
- `Error::Io` is transparent, so it no longer prefixes the error it carries.
- `Error::InvalidAttribute` lost its `From<FromUtf8Error>`. It gave callers a
  silent `?` from any `String::from_utf8` failure, including one about
  something that was never an attribute.
- `Fragments` no longer implements `Clone`. Cloning let a caller keep using a
  counter that `terminate` was meant to consume.

## 0.2.0

Initial published release.
