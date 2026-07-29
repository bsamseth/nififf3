//! Well-known flow file attribute names used by NiFi.
//!
//! NiFi treats these as ordinary attributes, so nothing forces you to use
//! them — [`Fragments`](crate::Fragments) writes them by default but takes
//! different keys if you need them.

/// The per-flow-file unique identifier. Replaced with a fresh value by
/// [`FlowFile::derive`](crate::FlowFile::derive).
pub const UUID: &str = "uuid";

/// The name of the file the content came from.
pub const FILENAME: &str = "filename";

/// The directory the file came from, relative to the ingest root.
pub const PATH: &str = "path";

/// The absolute directory the file came from, as NiFi's `GetFile` and
/// `ListFile` set it.
pub const ABSOLUTE_PATH: &str = "absolute.path";

/// The media type of the content. NiFi's record and format-detecting
/// processors both set and read this.
pub const MIME_TYPE: &str = "mime.type";

/// The number of records in the content, as NiFi's record-oriented
/// processors set it.
pub const RECORD_COUNT: &str = "record.count";

/// Shared by every flow file produced from the same parent, so that NiFi's
/// `MergeContent` can bin them back together.
pub const FRAGMENT_ID: &str = "fragment.identifier";

/// The one-up index of a flow file within its fragment set. NiFi numbers
/// these from 1.
pub const FRAGMENT_INDEX: &str = "fragment.index";

/// The total number of flow files in the fragment set, when known.
pub const FRAGMENT_COUNT: &str = "fragment.count";

/// The [`FILENAME`] of the parent the fragment set was produced from.
pub const SEGMENT_ORIGINAL_FILENAME: &str = "segment.original.filename";
