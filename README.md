# NiFi Flow File 3

This crate provides helpful utilities for working with NiFi's Flow File V3 file type.

## Parsing flow files

At its core, this crate provides this type:

```
struct FlowFile<R> {
  size: u64,
  attributes: HashMap<String, String>,
  content: R,
}
```

The flow file consists:

- `size`: The length of the `content` in bytes.
- `attributes`: A set of key-value pairs of text-attributes assigned to the content.
- `content`: The actual content, generic over the container type.

The content can be `std::io::Read`, `tokio::io::AsyncRead` or `Vec<u8>`. The
type defines functions to parse out a flow file from either of the types, as
well as ways of serializing into the binary format of a flow file v3 to a
`std::io::Write/tokio::io::AsyncWrite/Vec<u8>`.

Parsing is lazy over the content, meaning the parser only reads as many bytes
as is needed to parse out the header. When the total length is known it is
validated against the `size` field.

Creating flow files can be done with a easy-to-use builder API. Since the
binary format stores the content size before the content, serializing from a
reader requires the size up front; for readers of unknown size the builder can
spool the content into memory (`buffered`) or, behind the `tempfile` feature,
into an anonymous temporary file (`tempfile`).

## CLI

A CLI tool is available for working with flow files.

- `nififf3 to-json`: Read flow files and convert them to JSON.
  - Fields in the JSON: `size`, `attributes` and `content`. The content is base64 encoded.
- `nififf3 from-json`: Turn a JSON file into a flow file.
  - The JSON must have the same structure as produced from `to-json`.
- `nififf3 create`: Create a flow file
  - Attributes given as `key=value` arguments, and content set to stdin.


Both `to-json` and `from-json` take an optional path as their argument, reading from stdin if no filename is given. The filename `-` is also interpreted as stdin.

## Axum Integration

Gated behind the `axum` feature is support for extracting a flow file from
requests, and turning flow files into responses. This support streaming, where
the content of the flow file is read incrementally, supporting arbitraryly
large flow files without needing to have them all in memory.
