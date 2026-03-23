use axum::response::IntoResponse;
use tokio::io::AsyncRead;

use crate::FlowFile;

impl<R: AsyncRead + Unpin + Send + 'static> IntoResponse for FlowFile<R> {
    fn into_response(mut self) -> axum::response::Response {
        let (w, body) = super::make_response_stream(8192);
        // This serialization only produces an error if the writer fails. And this type of writer
        // doesn't fail unless the reader end is dropped, in which case the error is expected.
        // Therefore we don't need to bother with the result that this produces.
        tokio::spawn(async move {
            let _ = self.serialize_into(w).await;
        });
        body.into_response()
    }
}
