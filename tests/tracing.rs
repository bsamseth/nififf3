//! What the crate emits, captured and asserted.
//!
//! Logging that is never read is easy to get wrong, so these run a real
//! subscriber over real work and check the fields that are supposed to make an
//! interleaved log followable.
#![cfg(feature = "tracing")]

use std::io::Write;
use std::sync::{Arc, Mutex};

use nififf3::{FlowFile, FlowFiles, FlowFilesWriter};
use tracing::Level;
use tracing_subscriber::fmt::MakeWriter;

/// A writer the test can read back.
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Capture {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

impl Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for Capture {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Run `work` under a subscriber at `level` and return everything it logged.
fn capture(level: Level, work: impl FnOnce()) -> String {
    let sink = Capture::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(sink.clone())
        .with_max_level(level)
        .with_ansi(false)
        .with_target(false)
        .finish();
    tracing::subscriber::with_default(subscriber, work);
    sink.text()
}

fn sample() -> Vec<u8> {
    FlowFile::builder()
        .attribute("uuid", "0f4d-known")
        .attribute("filename", "greeting.txt")
        .content(&b"hello"[..])
        .to_bytes()
}

/// The identifying fields are the point: without them an interleaved log
/// cannot be followed back to one flow file.
#[test]
fn parsing_carries_the_identity_fields() {
    let logs = capture(Level::DEBUG, || {
        let _ = FlowFile::from_bytes(&sample()).unwrap();
    });

    assert!(logs.contains("uuid=\"0f4d-known\""), "{logs}");
    assert!(logs.contains("filename=\"greeting.txt\""), "{logs}");
    assert!(logs.contains("size=5"), "{logs}");
    assert!(logs.contains("parsed flow file"), "{logs}");
}

/// A flow file with no filename should not log an empty one.
#[test]
fn a_missing_filename_is_left_out_rather_than_logged_empty() {
    let bytes = FlowFile::builder().content(&b"hi"[..]).to_bytes();
    let logs = capture(Level::DEBUG, || {
        let _ = FlowFile::from_bytes(&bytes).unwrap();
    });

    assert!(logs.contains("size=2"), "{logs}");
    assert!(!logs.contains("filename"), "{logs}");
}

/// Nothing below the chosen level should appear, or `RUST_LOG` is not the
/// control it looks like.
#[test]
fn the_level_actually_filters() {
    let bytes = sample();

    let quiet = capture(Level::INFO, || {
        let _ = FlowFile::from_bytes(&bytes).unwrap();
    });
    assert!(quiet.is_empty(), "debug work logged at info: {quiet}");

    let loud = capture(Level::TRACE, || {
        let mut reader = bytes.as_slice();
        while let Some(flow_file) = FlowFile::parse_next(&mut reader).unwrap() {
            flow_file.skip_content().unwrap();
        }
    });
    assert!(
        loud.contains("stream ended on a flow file boundary"),
        "{loud}"
    );
    assert!(loud.contains("skip_content"), "{loud}");
}

/// A failed write poisons the writer and every later write is refused. The
/// first error reaches the caller; what follows would otherwise be silent.
#[test]
fn a_poisoned_writer_says_so() {
    struct Broken;
    impl Write for Broken {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("disk gone"))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let logs = capture(Level::WARN, || {
        let mut writer = FlowFilesWriter::new(Broken);
        let _ = writer.write_bytes(&FlowFile::builder().content(&b"hi"[..]));
    });

    assert!(logs.contains("poisoning the writer"), "{logs}");
    assert!(logs.contains("disk gone"), "{logs}");
}

/// Reading a stream should account for every flow file in it.
#[test]
fn each_flow_file_in_a_stream_is_logged_once() {
    let mut bytes = sample();
    bytes.extend(sample());

    let logs = capture(Level::DEBUG, || {
        let count = FlowFiles::new(bytes.as_slice()).count();
        assert_eq!(count, 2);
    });

    let parsed = logs.matches("parsed header").count();
    assert_eq!(parsed, 2, "one per flow file, got {parsed}: {logs}");
}

/// A wrong content type is answered with a 415 before the handler runs, so
/// nothing in the handler can report it.
#[cfg(all(feature = "axum", feature = "uuid"))]
#[test]
fn a_rejected_content_type_is_logged() {
    use axum::Router;
    use axum::body::Body;
    use axum::http::Request;
    use axum::routing::post;
    use tower::ServiceExt;

    let logs = {
        let sink = Capture::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(sink.clone())
            .with_max_level(Level::WARN)
            .with_ansi(false)
            .with_target(false)
            .finish();
        let app = Router::new().route(
            "/in",
            post(|_: nififf3::StrictFlowFileRequest| async { "unreachable" }),
        );
        let request = Request::builder()
            .method("POST")
            .uri("/in")
            .header("content-type", "text/plain")
            .body(Body::from(sample()))
            .unwrap();
        let response = tracing::subscriber::with_default(subscriber, || {
            futures_executor_block_on(app.oneshot(request))
        });
        assert_eq!(response.unwrap().status(), 415);
        sink.text()
    };

    assert!(logs.contains("content type is not"), "{logs}");
    assert!(logs.contains("text/plain"), "{logs}");
}

/// `with_default` sets the subscriber for this thread, so the whole exchange
/// has to run inside it rather than on a runtime started outside.
#[cfg(all(feature = "axum", feature = "uuid"))]
fn futures_executor_block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}
