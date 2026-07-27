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

/// The directory the file came from.
pub const PATH: &str = "path";

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
