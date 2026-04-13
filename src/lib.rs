#![expect(clippy::doc_markdown, reason = "NiFi is spelled in camelCase")]
#[doc = include_str!("../README.md")]
pub mod axum;
mod flowfiles;

pub use flowfiles::*;
