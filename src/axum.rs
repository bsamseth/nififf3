//! Glue between this crate and [`axum`].

use axum::{http::StatusCode, response::IntoResponse};

use crate::{FlowFileIterator, FlowFileParsingError, IntoFlowFiles};

impl<S> axum::extract::FromRequest<S> for FlowFileIterator
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request(req: axum::extract::Request, _: &S) -> Result<Self, Self::Rejection> {
        if req
            .headers()
            .get("Content-Type")
            .is_none_or(|value| value.to_str().unwrap_or("") != "application/flowfile-v3")
        {
            return Err((
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "Only `Content-Type: application/flowfil-v3` is accepted.",
            ));
        }

        let stream = req.into_body().into_data_stream();
        Ok(stream.into_flow_files())
    }
}

impl IntoResponse for FlowFileParsingError {
    fn into_response(self) -> axum::response::Response {
        (StatusCode::UNPROCESSABLE_ENTITY, format!("{self}")).into_response()
    }
}
