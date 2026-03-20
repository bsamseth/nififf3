use axum::response::IntoResponse;

use crate::StreamedFlowFile;

trait FlowFileHandler {
    type Error: std::error::Error;
    type Response: IntoResponse;

    fn handle_flow_file(&mut self, input: StreamedFlowFile) -> Result<Self::Response, Self::Error>;
}

struct FlowFileResponse {}
