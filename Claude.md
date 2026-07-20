# Implementation Plan

- [ ] Add `FlowFile` type with builder API to construct it from all supported parts.
- [ ] Add parsing logic, both sync and async.
- [ ] Add CLI interface.

Then, for the axum support:

- [ ] Define an extractor for flow file
- [ ] Define `IntoResponse` for flow files
- [ ] Define `IntoResponse` for the flow file error type
