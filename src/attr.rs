//! Well-known flow file attribute names used by NiFi.
//!
//! NiFi treats these as ordinary attributes, so nothing forces you to use
//! them. `Fragments` writes them by default, and takes different keys if you
//! need it to.
//!
//! The names come from NiFi's `CoreAttributes`, and from the processors that
//! document writing them: `GetFile`, `ListFile`, `UnpackContent`, `SplitText`,
//! `MergeContent`, `HandleHttpRequest` and `InvokeHTTP`. This is not every
//! attribute NiFi can set, and a processor is free to write whatever it
//! likes, so treat a missing constant as a gap here rather than a name you
//! cannot use.

/// The per-flow-file unique identifier. Replaced with a fresh value by
/// `FlowFile::derive`.
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

/// A numeric value indicating the flow file's priority.
pub const PRIORITY: &str = "priority";

/// Why a flow file is being discarded.
pub const DISCARD_REASON: &str = "discard.reason";

/// An identifier other than the [`UUID`] that is known to refer to this flow
/// file.
///
/// NiFi registers a provenance event when this is set to a URI.
pub const ALTERNATE_IDENTIFIER: &str = "alternate.identifier";

/// The size of the file in bytes, as NiFi's `ListFile` and `UnpackContent`
/// set it.
///
/// This describes the file the content came from. It is not the size of the
/// content itself, which is [`FlowFile::size`](crate::FlowFile::size).
pub const FILE_SIZE: &str = "file.size";

/// The owner of the file the content came from.
pub const FILE_OWNER: &str = "file.owner";

/// The group owner of the file the content came from.
pub const FILE_GROUP: &str = "file.group";

/// The read, write and execute permissions of the file the content came from.
///
/// Written as three characters for the owner, three for the group, and three
/// for other users.
pub const FILE_PERMISSIONS: &str = "file.permissions";

/// When the file the content came from was last modified.
pub const FILE_LAST_MODIFIED_TIME: &str = "file.lastModifiedTime";

/// When the file the content came from was last accessed.
pub const FILE_LAST_ACCESS_TIME: &str = "file.lastAccessTime";

/// When the file the content came from was created.
pub const FILE_CREATION_TIME: &str = "file.creationTime";

/// How many bytes of the parent this fragment holds, as NiFi's splitting
/// processors set it.
pub const FRAGMENT_SIZE: &str = "fragment.size";

/// How many lines of the parent this flow file holds, as NiFi's `SplitText`
/// sets it.
pub const TEXT_LINE_COUNT: &str = "text.line.count";

/// How many flow files went into a merge, as NiFi's `MergeContent` sets it.
pub const MERGE_COUNT: &str = "merge.count";

/// How old the bin was in milliseconds when `MergeContent` merged it.
pub const MERGE_BIN_AGE: &str = "merge.bin.age";

/// The [`UUID`] of the merged flow file, set on each flow file that went into
/// it.
pub const MERGE_UUID: &str = "merge.uuid";

/// Which threshold triggered a merge, such as `MAX_BYTES_THRESHOLD_REACHED`.
pub const MERGE_REASON: &str = "merge.reason";

/// The identifier pairing a request with its response, as NiFi's
/// `HandleHttpRequest` and `HandleHttpResponse` use it.
pub const HTTP_CONTEXT_IDENTIFIER: &str = "http.context.identifier";

/// The HTTP method the request used.
pub const HTTP_METHOD: &str = "http.method";

/// The full request URL.
pub const HTTP_REQUEST_URI: &str = "http.request.uri";

/// The query string part of the request URL.
pub const HTTP_QUERY_STRING: &str = "http.query.string";

/// The hostname of the client that made the request.
pub const HTTP_REMOTE_HOST: &str = "http.remote.host";

/// The hostname and port of the client that made the request.
pub const HTTP_REMOTE_ADDR: &str = "http.remote.addr";

/// The username of the client that made the request.
pub const HTTP_REMOTE_USER: &str = "http.remote.user";

/// The protocol the request came in over.
pub const HTTP_PROTOCOL: &str = "http.protocol";

/// The status code of a response NiFi's `InvokeHTTP` received.
pub const INVOKEHTTP_STATUS_CODE: &str = "invokehttp.status.code";

/// The status message of a response `InvokeHTTP` received.
pub const INVOKEHTTP_STATUS_MESSAGE: &str = "invokehttp.status.message";

/// The URL `InvokeHTTP` requested.
pub const INVOKEHTTP_REQUEST_URL: &str = "invokehttp.request.url";

/// The URL `InvokeHTTP` ended on, after any redirects.
pub const INVOKEHTTP_RESPONSE_URL: &str = "invokehttp.response.url";

/// How long `InvokeHTTP`'s call took, in milliseconds.
pub const INVOKEHTTP_REQUEST_DURATION: &str = "invokehttp.request.duration";

/// The transaction identifier from a response `InvokeHTTP` received.
pub const INVOKEHTTP_TX_ID: &str = "invokehttp.tx.id";

#[cfg(test)]
mod tests {
    use super::*;

    /// Every constant here was transcribed by hand from NiFi's documentation,
    /// so two of them holding the same string means one is a typo.
    #[test]
    fn the_attribute_names_are_all_distinct() {
        let names = [
            UUID,
            FILENAME,
            PATH,
            ABSOLUTE_PATH,
            MIME_TYPE,
            RECORD_COUNT,
            FRAGMENT_ID,
            FRAGMENT_INDEX,
            FRAGMENT_COUNT,
            SEGMENT_ORIGINAL_FILENAME,
            PRIORITY,
            DISCARD_REASON,
            ALTERNATE_IDENTIFIER,
            FILE_SIZE,
            FILE_OWNER,
            FILE_GROUP,
            FILE_PERMISSIONS,
            FILE_LAST_MODIFIED_TIME,
            FILE_LAST_ACCESS_TIME,
            FILE_CREATION_TIME,
            FRAGMENT_SIZE,
            TEXT_LINE_COUNT,
            MERGE_COUNT,
            MERGE_BIN_AGE,
            MERGE_UUID,
            MERGE_REASON,
            HTTP_CONTEXT_IDENTIFIER,
            HTTP_METHOD,
            HTTP_REQUEST_URI,
            HTTP_QUERY_STRING,
            HTTP_REMOTE_HOST,
            HTTP_REMOTE_ADDR,
            HTTP_REMOTE_USER,
            HTTP_PROTOCOL,
            INVOKEHTTP_STATUS_CODE,
            INVOKEHTTP_STATUS_MESSAGE,
            INVOKEHTTP_REQUEST_URL,
            INVOKEHTTP_RESPONSE_URL,
            INVOKEHTTP_REQUEST_DURATION,
            INVOKEHTTP_TX_ID,
        ];

        let unique: std::collections::HashSet<_> = names.iter().collect();
        assert_eq!(unique.len(), names.len(), "a name is repeated: {names:?}");
        assert!(names.iter().all(|name| !name.is_empty()));
    }
}
