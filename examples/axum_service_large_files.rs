//! A service that handles large flow files: split, transform and merge.
//!
//! This is meant to be copied. Everything above the harness banner is what a
//! pipeline handling inputs and outputs too large to buffer needs, and the
//! comments say why each piece is there rather than what it does.
//! `axum_service.rs` is the smaller version of the same three shapes, for
//! content that does fit in memory.
//!
//! # Where each part's size comes from
//!
//! One rule shapes every handler. The V3 header carries the content length,
//! so a part's size has to be known before any of its bytes can go out. The
//! three handlers differ only in where that size comes from:
//!
//! - `POST /split` cuts the input into fixed-size chunks, so every size is
//!   known in advance. Each part streams straight out of the spool.
//! - `POST /transform` gzips the input, and compression reports its length
//!   only by finishing. The output is spooled, and the response streams from
//!   there.
//! - `POST /merge` concatenates flow files, so the length is the sum of
//!   theirs. The spool is read twice: once to add up the sizes, once to
//!   stream.
//!
//! # What the wiring has to do
//!
//! The extractors read each request in full before the handler runs, into
//! memory or to disk by size, so no handler here ever reads from the socket
//! while it writes to it. That is what keeps a client which sends before it
//! reads, as NiFi's does, from deadlocking the service.
//!
//! `DefaultBodyLimit` is raised to what the service is willing to accept,
//! because axum's default of 2 MiB fails most flow files. `SpoolConfig` says
//! how large a request may be before it goes to disk.
//!
//! Validation happens before a `FlowFilesResponse` is returned, because
//! returning one commits the status to 200. After that a problem can only be
//! reported inside the body.
//!
//!     cargo run --features axum --example axum_service_large_files

use async_compression::tokio::bufread::GzipEncoder;
use axum::extract::DefaultBodyLimit;
use axum::http::{StatusCode, header};
use axum::routing::post;
use axum::{Extension, Router, response::IntoResponse};
use nififf3::{Error, FlowFile, FlowFilesReaderAsync, FlowFilesResponse, Spool, SpoolConfig, attr};
use tokio::io::{AsyncReadExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

/// The largest request the service will accept.
const MAX_UPLOAD: usize = 64 << 20;

/// Requests over this size are spooled to disk rather than held in memory.
const MAX_IN_MEMORY: usize = 1 << 20;

/// How much content goes in each part `/split` produces.
const CHUNK: u64 = 1 << 20;

/// The size of the flow file the example sends in.
const INPUT: usize = 4 << 20;

/// One in, many out, with nothing held.
///
/// The chunk size decides every part's length up front, so each part can be a
/// reader over the next stretch of the input. `write` streams exactly the
/// declared number of bytes, so a part is never resident however large it is.
async fn split(parent: FlowFile<Spool>) -> Result<FlowFilesResponse, (StatusCode, String)> {
    // Check what can be checked here, while a status code is still available.
    // Returning the response below commits this request to 200, and after that
    // a problem can only be reported as a flow file inside the body.
    let Some(filename) = parent.attribute(attr::FILENAME).map(str::to_owned) else {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("missing the {} attribute", attr::FILENAME),
        ));
    };

    let remaining = parent.size();
    let mut parts = parent.fragments();
    let mut content = parent.into_content();

    Ok(FlowFilesResponse::new(move |mut writer| async move {
        let mut remaining = remaining;
        let mut index = 0;
        while remaining > 0 {
            let size = CHUNK.min(remaining);
            // A reader over just this chunk. Nothing is copied here.
            let part = parts
                .next_part()
                .attribute(attr::FILENAME, format!("{filename}.chunk-{index}"))
                .reader((&mut content).take(size), size);
            writer.write(part).await?;
            remaining -= size;
            index += 1;
        }
        // How many chunks there were is only known now, so the count travels
        // on a terminator rather than on the parts.
        writer.write_bytes(&parts.terminate()).await?;
        Ok(())
    }))
}

/// One in, one out, where the transform changes the length.
///
/// Compression only reports its length by running to the end, so there is no
/// size to declare until it has. `spool_async` reads the encoder to completion
/// and takes the size from what it read, holding the output in memory or on
/// disk by the same rule the extractor uses.
async fn transform(parent: FlowFile<Spool>) -> Result<impl IntoResponse, Error> {
    let builder = parent
        .derive()
        .attribute(attr::MIME_TYPE, "application/gzip")
        .attribute("transformed", "gzip");
    let encoder = GzipEncoder::new(BufReader::new(parent.into_content()));
    Ok(builder.spool_async(encoder).await?)
}

/// Many in, one out.
///
/// The merged length is the sum of the inputs', and a flow file's size is only
/// known once its header has been read. The spool is read twice: the first
/// pass adds up the sizes and takes the attributes from the first part, and
/// the second streams the contents out one after another.
async fn merge(mut body: Spool) -> Result<impl IntoResponse, Error> {
    let mut merged = None;
    let mut size = 0;
    {
        let mut parts = FlowFilesReaderAsync::new(&mut body);
        while let Some(part) = parts.next().await? {
            // `defragment` drops the fragment attributes and puts `filename`
            // back, so the result looks like the flow file the split started
            // from. Every part carries the parent's attributes, so the first
            // one is enough.
            if merged.is_none() {
                merged = Some(part.derive_keep_uuid().defragment());
            }
            size += part.size();
            // The content is not read here. The reader skips whatever is left
            // when the next part is asked for.
        }
    }
    body.rewind().await?;

    // The second pass feeds the response as it goes, so the merged content is
    // never resident either. The spool is a plain value, so the task copying
    // from it waits on nothing but the client.
    let (mut sink, source) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        let mut parts = FlowFilesReaderAsync::new(body);
        while let Ok(Some(mut part)) = parts.next().await {
            if tokio::io::copy(part.content_mut(), &mut sink)
                .await
                .is_err()
            {
                break; // the client went away
            }
        }
    });

    Ok(merged
        .unwrap_or_else(FlowFile::builder)
        .reader(source, size))
}

/// The router, with the layers a large-file endpoint needs.
fn app() -> Router {
    Router::new()
        .route("/split", post(split))
        .route("/transform", post(transform))
        .route("/merge", post(merge))
        // The default is 2 MiB, which every request here would fail. Raise it
        // to what the service is willing to accept, rather than disabling it.
        .layer(DefaultBodyLimit::max(MAX_UPLOAD))
        // Requests up to a MiB stay in memory; the rest go to disk.
        .layer(Extension(
            SpoolConfig::new().with_max_in_memory(MAX_IN_MEMORY),
        ))
}

/// What `main` looks like in production.
///
/// Graceful shutdown matters more than usual here: a request may be minutes
/// from finishing, and dropping the response mid-stream leaves the client with
/// a truncated flow file rather than an error it can act on.
async fn serve(listener: TcpListener, shutdown: oneshot::Receiver<()>) -> std::io::Result<()> {
    axum::serve(listener, app())
        .with_graceful_shutdown(async {
            shutdown.await.ok();
        })
        .await
}

// --------------------------------------------------------------------------
// Harness below, not part of the template.
// --------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A real server on a real socket, so the example exercises the wiring
    // above rather than calling the handlers directly.
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (stop, shutdown) = oneshot::channel();
    let server = tokio::spawn(serve(listener, shutdown));
    let client = reqwest::Client::new();
    println!("serving on http://{addr}");

    // Compressible, so the gzip round trip has something to show.
    let content: Vec<u8> = std::iter::repeat_n(b"large-flow-file-", INPUT / 16)
        .flatten()
        .copied()
        .collect();
    let incoming = FlowFile::builder()
        .attribute("filename", "large.bin")
        .attribute("source", "example")
        .content(content.clone())
        .to_bytes();
    println!("in: one flow file, {} bytes of content", content.len());

    // --- one in, many out --------------------------------------------------
    let (status, bundle) = send(&client, addr, "/split", incoming.clone()).await?;
    assert_eq!(status, StatusCode::OK, "split");

    let mut flow_files = nififf3::FlowFilesAsync::new(bundle.as_slice());
    let (mut chunks, mut chunk_bytes, mut declared) = (0, 0u64, None);
    while let Some(part) = flow_files.next().await {
        let part = part?;
        // The terminator is the one carrying the count, and it has no content.
        if let Some(count) = part.parse_attribute::<usize>(attr::FRAGMENT_COUNT)? {
            declared = Some(count);
        } else {
            chunks += 1;
            chunk_bytes += part.size();
        }
    }
    println!("POST /split -> {chunks} chunks, {chunk_bytes} bytes, count={declared:?}");
    assert_eq!(chunks, 4, "4 MiB in 1 MiB chunks");
    assert_eq!(chunk_bytes, content.len() as u64, "no content lost");
    assert_eq!(declared, Some(chunks + 1), "the chunks plus the terminator");

    // --- one in, one out, length decided by the transform ------------------
    let (status, body) = send(&client, addr, "/transform", incoming).await?;
    assert_eq!(status, StatusCode::OK, "transform");
    let compressed = FlowFile::from_bytes(&body)?;
    println!(
        "POST /transform -> {} bytes gzipped, mime.type={}",
        compressed.size(),
        compressed[attr::MIME_TYPE],
    );
    assert!(compressed.size() < content.len() as u64, "it compressed");
    assert_eq!(compressed[attr::FILENAME], "large.bin");

    // --- many in, one out --------------------------------------------------
    // The bundle `/split` produced goes straight back in.
    let (status, body) = send(&client, addr, "/merge", bundle).await?;
    assert_eq!(status, StatusCode::OK, "merge");
    let rejoined = FlowFile::from_bytes(&body)?;
    println!(
        "POST /merge -> {} bytes, filename={}",
        rejoined.size(),
        rejoined[attr::FILENAME],
    );
    assert_eq!(rejoined.content(), &content, "the split is undone exactly");
    assert!(
        !rejoined.attributes().contains_key(attr::FRAGMENT_INDEX),
        "defragment drops the fragment attributes"
    );

    // --- the failures that still get a status code -------------------------
    let (status, _) = send(&client, addr, "/split", b"not a flow file".to_vec()).await?;
    println!("POST /split (bad body) -> {status}");
    assert_eq!(
        status, 400,
        "the extractor rejects it before the handler runs"
    );

    let nameless = FlowFile::builder().content(&b"anonymous"[..]).to_bytes();
    let (status, message) = send(&client, addr, "/split", nameless).await?;
    println!(
        "POST /split (no filename) -> {status} {}",
        String::from_utf8_lossy(&message)
    );
    assert_eq!(
        status, 400,
        "the handler validates before it commits to 200"
    );

    stop.send(()).ok();
    server.await??;
    Ok(())
}

/// Send one flow file and return the status and the whole response body.
async fn send(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    path: &str,
    body: Vec<u8>,
) -> Result<(StatusCode, Vec<u8>), Box<dyn std::error::Error>> {
    let response = client
        .post(format!("http://{addr}{path}"))
        .header(header::CONTENT_TYPE, nififf3::MEDIA_TYPE)
        .body(body)
        .send()
        .await?;
    let status = response.status();
    Ok((status, response.bytes().await?.to_vec()))
}
