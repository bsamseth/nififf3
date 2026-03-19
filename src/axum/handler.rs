use axum::response::IntoResponse;

use crate::FlowFile;

trait FlowFileHandler {
    type Error: std::error::Error;
    type Response: IntoResponse;

    fn handle_flow_file(&mut self, input: FlowFile) -> Result<Self::Response, Self::Error>;
}

struct FlowFileResponse {}
