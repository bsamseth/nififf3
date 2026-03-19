use std::pin::Pin;

use axum::response::{IntoResponse, Response as AxumResponse};
use futures::Stream;
use tower::{BoxError, Service};

use crate::FlowFileIterator;
use crate::flowfiles::{FlowFile, FlowFileParsingError, OutputFlowFile, Storage};

#[derive(Debug, Clone)]
struct FlowFileMiddleware<H> {
    handler: H,
}

trait IntoFlowFileResponse {}

impl<H> Service<FlowFileIterator> for FlowFileMiddleware<H>
where
    H: Service<FlowFile>,
    H::Error: Into<BoxError>,
    H::Response: IntoFlowFileResponse,
{
    type Response = AxumResponse;
    type Error = BoxError;
    type Future = ResponseFuture;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner_service.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, flow_files: FlowFileIterator) -> Self::Future {
        ResponseFuture { flow_files }
    }
}

#[pin_project::pin_project]
struct ResponseFuture {
    flow_files: FlowFileIterator,
}

impl Future for ResponseFuture {
    type Output = Result<AxumResponse, BoxError>;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.project() {
            ResponseFutureProj::Start(req) => {
                let flow_files: FlowFileIterator = match req.try_into() {
                    Ok(x) => x,
                    Err(err) => return std::future::ready(err.into_response()),
                };
                todo!()
            }
        }
    }
}
