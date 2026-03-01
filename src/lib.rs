#![expect(clippy::doc_markdown, reason = "NiFi is spelled in camelCase")]
//! # nifioxide - Tools for working with NiFi files from Rust.
//!
//! This crate intends to make it easier to write HTTP processors for NiFi.
//! In order to deal with non-ASCII attributes, the best way to send a file from NiFi and back is
//! to package files as NiFi Flow Files (v3). This crate provides a streaming parser for such
//! files, making it easy to write a streaming processor.
pub mod axum;
mod flowfiles;

pub use flowfiles::*;
