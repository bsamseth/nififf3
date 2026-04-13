use thiserror::Error;

/// Errors that can occur during parsing of NiFi Flow Files from a streaming source.
///
/// The source will typically be an HTTP POST body, but could also be other byte streams.
#[derive(Debug, Error)]
pub enum FlowFileParsingError {
    /// The file did not contain the expected `b"NiFiFF3"` magic byte header.
    #[error("Incorrect flow file magic bytes, expected 'NiFiFF3' but got {0:?}")]
    BadMagicBytes([u8; 7]),

    /// IO error while parsing a flow file.
    ///
    /// The context indicates in which stage of parsing the error occured.
    #[error("Malformed flowfile: {context}: {io_error}")]
    Malformed {
        context: &'static str,
        io_error: tokio::io::Error,
    },

    /// Internal receive error while waiting to receive the stream reader back from a flow file reader.
    ///
    /// Such an error is never expected, and if it occurs you should consider the relevant
    /// [`crate::axum::StreamedFlowFiles`] unusable.
    #[error("Broken internal flow file parsing channel: {0}")]
    BrokenChannel(#[from] tokio::sync::oneshot::error::RecvError),

    /// Content length was less than a flow file indicated it contained.
    /// This occurs if the content length in the flow file header doesn't agree with the request
    /// content length about how many bytes are left in the stream.
    #[error(
        "Content length of {content_length} less than flow file header indicated {flow_file_required}"
    )]
    ContentLengthLengthMismatch {
        content_length: u64,
        flow_file_required: u64,
    },

    /// If a flow file was expected, but the content length stopped us from trying to parse one out.
    /// This can happen when extracting and expecting a single flow file, but the content length of
    /// the post payload was zero.
    #[error("A flow file was expected in the request payload.")]
    FlowFileExpected,

    /// If a single flow file was expected, but we received trailing data.
    /// This can happen if multiple flow files where sent, or if the flow file header size value
    /// reports a smaller size than the payload length contains.
    #[error(
        "A single flow file was expected in the request payload, but excess data was received."
    )]
    SingleFlowFileExpected,

    /// Generic I/O error.
    #[error("IO error while processing flowfile: {0}")]
    Io(#[from] tokio::io::Error),
}
