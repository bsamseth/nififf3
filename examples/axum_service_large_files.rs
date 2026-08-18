//! A service that streams large flow files: split, transform and merge.
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
//!   known in advance. Nothing is ever held: the input streams into the parts.
//! - `POST /transform` gzips the input, and compression reports its length
//!   only by finishing. The output goes to a temporary file, and the response
//!   streams from there.
//! - `POST /merge` concatenates flow files, so the length is the sum of
//!   theirs. That is not known until every header has been read, and reading a
//!   header means consuming the content before it. The request is spooled and
//!   read twice: once to add up the sizes, once to stream.
//!
//! # What the wiring has to do
//!
//! Every handler spools its request with `spool_async`, so that reading the
//! request never waits on the response. Without it, a client that sends its
//! whole request before reading any of the answer deadlocks against a handler
//! that answers as it reads. NiFi's client is one of those.
//! `FlowFilesResponse` describes the deadlock and
//! `tests/response_deadlock.rs` reproduces it.
//!
//! `DefaultBodyLimit` is raised rather than disabled. It caps the bytes a
//! client actually sends, which is the only bound that means anything on a
//! public endpoint. Disabling it lets any client hold as much of your disk as
//! it likes. The header is bounded separately: both extractors apply
//! `Limits::recommended`, so a crafted header is rejected before its
//! attributes are read.
//!
//! Validation happens before a `FlowFilesResponse` is returned, because
//! returning one commits the status to 200. After that a problem can only be
//! reported inside the body.
//!
//!     cargo run --features axum,tempfile --example axum_service_large_files

use async_compression::tokio::bufread::GzipEncoder;
use axum::body::Body;
use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Router, response::IntoResponse};
use nififf3::{
    Error, FlowFile, FlowFileBody, FlowFilesAsync, FlowFilesReaderAsync, FlowFilesResponse,
    StrictFlowFileRequest, attr,
};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

/// The largest request the service will accept.
const MAX_UPLOAD: usize = 64 << 20;

/// How much content goes in each part `/split` produces.
const CHUNK: u64 = 1 << 20;

/// The size of the flow file the example sends in.
const INPUT: usize = 4 << 20;

/// One in, many out, with nothing held.
///
/// The chunk size decides every part's length up front, so each part can be a
/// reader over the next stretch of the input. `write` streams exactly the
/// declared number of bytes, so a part is never resident however large it is.
async fn split(
    StrictFlowFileRequest(parent): StrictFlowFileRequest,
) -> Result<FlowFilesResponse, (StatusCode, String)> {
    // Check what can be checked here, while a status code is still available.
    // Returning the response below commits this request to 200, and after that
    // a problem can only be reported as a flow file inside the body.
    let Some(filename) = parent.attribute(attr::FILENAME).map(str::to_owned) else {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("missing the {} attribute", attr::FILENAME),
        ));
    };

    let parent = parent
        .spool_async()
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
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
/// size to declare until it has. `tempfile_async` reads the encoder to
/// completion into an anonymous temporary file and takes the size from what it
/// wrote. Memory stays flat; the bytes live on disk until the response streams
/// them out.
async fn transform(
    StrictFlowFileRequest(parent): StrictFlowFileRequest,
) -> Result<impl IntoResponse, Error> {
    let parent = parent.spool_async()?;
    let builder = parent
        .derive()
        .attribute("mime.type", "application/gzip")
        .attribute("transformed", "gzip");
    let encoder = GzipEncoder::new(BufReader::new(parent.into_content()));
    Ok(builder.tempfile_async(encoder).await?)
}

/// Many in, one out.
///
/// The merged length is the sum of the inputs', and a flow file's size is only
/// known once its header has been read. Reading the next header means
/// consuming the content before it, so one pass cannot both learn the total
/// and still have the content to send. The request is spooled to a file and
/// read twice instead: the first pass adds up the sizes and takes the
/// attributes from the first part, the second streams the contents out.
async fn merge(body: Body) -> Result<impl IntoResponse, Error> {
    // The route disables `DefaultBodyLimit`, so nothing is lost by reading the
    // body directly. Behind a limit, go through `RequestExt::with_limited_body`.
    let spool = FlowFile::builder()
        .tempfile_async(FlowFileBody::from_body(body))
        .await?;
    let mut file = spool.into_content();

    let mut merged = None;
    let mut size = 0;
    {
        let mut parts = FlowFilesReaderAsync::new(&mut file);
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
    file.rewind().await?;

    // The second pass feeds the response as it goes, so the merged content is
    // never resident either.
    let (mut sink, source) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        let mut parts = FlowFilesReaderAsync::new(file);
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A real server on a real socket, so the example exercises the wiring
    // above rather than calling the handlers directly.
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (stop, shutdown) = oneshot::channel();
    let server = tokio::spawn(serve(listener, shutdown));
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
    let (status, bundle) = send(addr, "/split", incoming.clone()).await?;
    assert_eq!(status, 200, "split");

    let mut flow_files = FlowFilesAsync::new(bundle.as_slice());
    let (mut chunks, mut chunk_bytes, mut declared) = (0, 0u64, None);
    while let Some(part) = flow_files.next().await {
        let part = part?;
        // The terminator is the one carrying the count, and it has no content.
        if let Some(count) = part.attributes().get("fragment.count") {
            declared = Some(count.parse::<usize>()?);
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
    let (status, body) = send(addr, "/transform", incoming).await?;
    assert_eq!(status, 200, "transform");
    let compressed = FlowFile::from_bytes(&body)?;
    println!(
        "POST /transform -> {} bytes gzipped, mime.type={}",
        compressed.size(),
        compressed.attributes()["mime.type"],
    );
    assert!(compressed.size() < content.len() as u64, "it compressed");
    assert_eq!(compressed.attributes()["filename"], "large.bin");

    // --- many in, one out --------------------------------------------------
    // The bundle `/split` produced goes straight back in.
    let (status, body) = send(addr, "/merge", bundle).await?;
    assert_eq!(status, 200, "merge");
    let rejoined = FlowFile::from_bytes(&body)?;
    println!(
        "POST /merge -> {} bytes, filename={}",
        rejoined.size(),
        rejoined.attributes()["filename"],
    );
    assert_eq!(rejoined.content(), &content, "the split is undone exactly");
    assert!(
        !rejoined.attributes().contains_key("fragment.index"),
        "defragment drops the fragment attributes"
    );

    // --- the failures that still get a status code -------------------------
    let (status, _) = send(addr, "/split", b"not a flow file".to_vec()).await?;
    println!("POST /split (bad body) -> {status}");
    assert_eq!(
        status, 400,
        "the extractor rejects it before the handler runs"
    );

    let nameless = FlowFile::builder().content(&b"anonymous"[..]).to_bytes();
    let (status, message) = send(addr, "/split", nameless).await?;
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

// --------------------------------------------------------------------------
// Harness below. This is a stand-in for a client, not part of the template.
// --------------------------------------------------------------------------

/// Send one flow file request and return the status and the decoded body.
///
/// Named `send` so that it does not collide with `axum::routing::post`.
///
/// It drains the response while the request is still going out. A client that
/// does not is what `spool_async` protects the handlers against.
async fn send(
    addr: std::net::SocketAddr,
    uri: &str,
    body: Vec<u8>,
) -> Result<(u16, Vec<u8>), Box<dyn std::error::Error>> {
    let stream = TcpStream::connect(addr).await?;
    let (mut rx, mut tx) = tokio::io::split(stream);
    let reading = tokio::spawn(async move {
        let mut raw = Vec::new();
        rx.read_to_end(&mut raw).await.map(|_| raw)
    });

    let head = format!(
        "POST {uri} HTTP/1.1\r\nHost: localhost\r\nContent-Type: {}\r\n\
         Connection: close\r\nContent-Length: {}\r\n\r\n",
        nififf3::MEDIA_TYPE,
        body.len()
    );
    tx.write_all(head.as_bytes()).await?;
    tx.write_all(&body).await?;
    tx.flush().await?;
    let raw = reading.await??;

    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or("no header terminator in the response")?;
    let head = String::from_utf8_lossy(&raw[..split]).to_ascii_lowercase();
    let status = head
        .split_whitespace()
        .nth(1)
        .ok_or("no status code")?
        .parse()?;
    let body = &raw[split + 4..];
    // A streamed response is chunked; one with a known length is not.
    Ok((
        status,
        if head.contains("transfer-encoding: chunked") {
            dechunk(body)?
        } else {
            body.to_vec()
        },
    ))
}

/// Undo `Transfer-Encoding: chunked`.
fn dechunk(mut body: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut out = Vec::new();
    loop {
        let eol = body
            .windows(2)
            .position(|w| w == b"\r\n")
            .ok_or("truncated chunk header")?;
        let size = usize::from_str_radix(std::str::from_utf8(&body[..eol])?.trim(), 16)?;
        if size == 0 {
            return Ok(out);
        }
        out.extend_from_slice(&body[eol + 2..eol + 2 + size]);
        body = &body[eol + 2 + size + 2..];
    }
}
