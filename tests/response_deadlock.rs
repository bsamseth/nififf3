//! Reproduction for a streaming `FlowFilesResponse` that stalls against a
//! client which writes its whole request before reading the response.
//!
//! A stall is detected by lack of progress, not by total time. Small socket
//! buffers make the deadlock reachable with modest payloads, and they also
//! make the transfer slow, so a wall-clock timeout cannot tell "stuck" from
//! "slow".
#![cfg(all(feature = "axum", feature = "uuid"))]
// A reproduction harness, not library code: the outcome fields exist to be
// printed through `Debug`, and the byte generator truncates on purpose.
#![allow(dead_code, clippy::cast_possible_truncation)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_compression::tokio::write::GzipEncoder;
use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::routing::post;
use nififf3::{Error, FlowFile, FlowFilesResponse, StrictFlowFileRequest};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpSocket};
use tokio_stream::StreamExt;
use tokio_tar::{Archive, Builder, EntryType, Header};

/// How long without a single byte moving in either direction counts as stuck.
const STALL_AFTER: Duration = Duration::from_secs(15);

#[derive(Clone, Copy, Debug)]
enum Handler {
    /// Gzip each entry into memory, then write it. Size unknown up front.
    GzipBuffered,
    /// As above, but drain the request into memory before responding.
    GzipDrainMemory,
    /// Stream each entry through with its size known up front.
    StreamKnownSize,
    /// As above, but spool the request to a temporary file first.
    StreamSpooled,
    /// As above, but drain and produce concurrently through the spool.
    StreamSpooledConcurrent,
}

#[derive(Debug)]
enum Outcome {
    Completed { response_bytes: u64 },
    Stalled { sent: u64, received: u64 },
}

impl Outcome {
    fn completed(&self) -> bool {
        matches!(self, Outcome::Completed { .. })
    }
}

fn noise(len: usize, seed: u64) -> Vec<u8> {
    let mut s = seed | 1;
    (0..len)
        .map(|_| {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            (s >> 24) as u8
        })
        .collect()
}

async fn tar_of(entries: &[(&str, Vec<u8>)]) -> Vec<u8> {
    let mut b = Builder::new(Vec::new());
    for (name, data) in entries {
        let mut h = Header::new_gnu();
        h.set_size(data.len() as u64);
        h.set_entry_type(EntryType::Regular);
        h.set_mode(0o644);
        b.append_data(&mut h, name, data.as_slice()).await.unwrap();
    }
    b.finish().await.unwrap();
    b.into_inner().await.unwrap()
}

/// One flow file per archive entry, each gzipped.
///
/// The V3 header carries the content length, so every compressed byte has to
/// exist before the part can be written. The producer therefore alternates a
/// long read phase with a long write phase instead of interleaving them.
#[allow(clippy::unused_async, reason = "axum handlers must be async")]
async fn unpack_gzip(
    StrictFlowFileRequest(parent): StrictFlowFileRequest,
) -> Result<FlowFilesResponse, Error> {
    let mut parts = parent.fragments();
    let mut archive = Archive::new(BufReader::new(parent.into_content()));

    Ok(FlowFilesResponse::new(move |mut writer| async move {
        let mut entries = archive.entries()?;
        while let Some(entry) = entries.next().await {
            let mut entry = entry?;
            let name = entry.path()?.display().to_string();
            let mut enc = GzipEncoder::new(Vec::new());
            tokio::io::copy(&mut entry, &mut enc).await?;
            enc.shutdown().await?;
            eprintln!(
                "    [server] {name}: gzipped to {} B, writing",
                enc.get_ref().len()
            );
            writer
                .write_bytes(
                    &parts
                        .next_part()
                        .attribute("filename", name)
                        .content(enc.into_inner()),
                )
                .await?;
            eprintln!("    [server] part written");
        }
        writer.write_bytes(&parts.terminate()).await?;
        eprintln!("    [server] terminator written, producer done");
        Ok(())
    }))
}

/// The same pipeline, but the request body is drained before the response
/// begins. The client can then finish sending and move on to reading, so
/// response backpressure has nothing left to deadlock against.
async fn unpack_gzip_drained(
    StrictFlowFileRequest(parent): StrictFlowFileRequest,
) -> Result<FlowFilesResponse, Error> {
    let parent = parent.into_memory_async().await?;
    eprintln!("    [server] request drained before responding");
    let mut parts = parent.fragments();
    let mut archive = Archive::new(BufReader::new(std::io::Cursor::new(parent.into_content())));

    Ok(FlowFilesResponse::new(move |mut writer| async move {
        let mut entries = archive.entries()?;
        while let Some(entry) = entries.next().await {
            let mut entry = entry?;
            let name = entry.path()?.display().to_string();
            let mut enc = GzipEncoder::new(Vec::new());
            tokio::io::copy(&mut entry, &mut enc).await?;
            enc.shutdown().await?;
            writer
                .write_bytes(
                    &parts
                        .next_part()
                        .attribute("filename", name)
                        .content(enc.into_inner()),
                )
                .await?;
        }
        writer.write_bytes(&parts.terminate()).await?;
        eprintln!("    [server] terminator written, producer done");
        Ok(())
    }))
}

/// The streaming shape, fed by [`FlowFile::spool_async`]. Responds
/// immediately and cannot deadlock.
#[allow(clippy::unused_async, reason = "axum handlers must be async")]
async fn unpack_stream_spooled_concurrent(
    StrictFlowFileRequest(parent): StrictFlowFileRequest,
) -> Result<FlowFilesResponse, Error> {
    let parent = parent.spool_async()?;
    let mut parts = parent.fragments();
    let mut archive = Archive::new(BufReader::new(parent.into_content()));
    let t0 = std::time::Instant::now();

    Ok(FlowFilesResponse::new(move |mut writer| async move {
        let mut entries = archive.entries()?;
        let mut first = true;
        while let Some(entry) = entries.next().await {
            let entry = entry?;
            if first {
                first = false;
                eprintln!(
                    "    [server] first part ready after {} ms",
                    t0.elapsed().as_millis()
                );
            }
            let size = entry.header().entry_size()?;
            let name = entry.path()?.display().to_string();
            writer
                .write(
                    parts
                        .next_part()
                        .attribute("filename", name)
                        .reader(entry, size),
                )
                .await?;
        }
        writer.write_bytes(&parts.terminate()).await?;
        eprintln!("    [server] producer done");
        Ok(())
    }))
}

/// The shape that knows each part's size up front: no compression, no
/// buffering, the entry streamed straight through. Read and write interleave
/// inside `write`'s copy loop rather than alternating in long phases.
#[allow(clippy::unused_async, reason = "axum handlers must be async")]
async fn unpack_stream(
    StrictFlowFileRequest(parent): StrictFlowFileRequest,
) -> Result<FlowFilesResponse, Error> {
    let mut parts = parent.fragments();
    let mut archive = Archive::new(BufReader::new(parent.into_content()));

    Ok(FlowFilesResponse::new(move |mut writer| async move {
        let mut entries = archive.entries()?;
        while let Some(entry) = entries.next().await {
            let entry = entry?;
            let size = entry.header().entry_size()?;
            let name = entry.path()?.display().to_string();
            writer
                .write(
                    parts
                        .next_part()
                        .attribute("filename", name)
                        .reader(entry, size),
                )
                .await?;
        }
        writer.write_bytes(&parts.terminate()).await?;
        eprintln!("    [server] producer done");
        Ok(())
    }))
}

/// The streaming shape again, but the request is spooled to a temporary file
/// first. Memory stays bounded, and the client is free to finish sending.
async fn unpack_stream_spooled(
    StrictFlowFileRequest(parent): StrictFlowFileRequest,
) -> Result<FlowFilesResponse, Error> {
    let t0 = std::time::Instant::now();
    let builder = parent.derive_keep_uuid();
    let spooled = builder.tempfile_async(parent.into_content()).await?;
    eprintln!("    [server] request spooled to disk before responding");

    let mut parts = spooled.fragments();
    let mut archive = Archive::new(BufReader::new(spooled.into_content()));

    Ok(FlowFilesResponse::new(move |mut writer| async move {
        let mut entries = archive.entries()?;
        let mut first = true;
        while let Some(entry) = entries.next().await {
            let entry = entry?;
            if first {
                first = false;
                eprintln!(
                    "    [server] first part ready after {} ms",
                    t0.elapsed().as_millis()
                );
            }
            let size = entry.header().entry_size()?;
            let name = entry.path()?.display().to_string();
            writer
                .write(
                    parts
                        .next_part()
                        .attribute("filename", name)
                        .reader(entry, size),
                )
                .await?;
        }
        writer.write_bytes(&parts.terminate()).await?;
        eprintln!("    [server] producer done");
        Ok(())
    }))
}

/// `read_while_writing` is the whole variable: `false` is the NiFi shape
/// (write the request in full, then read), `true` is the curl shape.
#[allow(clippy::too_many_lines, reason = "one harness, read top to bottom")]
async fn run(
    buffer_size: Option<usize>,
    read_while_writing: bool,
    handler: Handler,
    body: Vec<u8>,
) -> Outcome {
    let app = Router::new()
        .route(
            "/unpack",
            post(move |req: StrictFlowFileRequest| async move {
                let made = match handler {
                    Handler::GzipBuffered => unpack_gzip(req).await,
                    Handler::GzipDrainMemory => unpack_gzip_drained(req).await,
                    Handler::StreamKnownSize => unpack_stream(req).await,
                    Handler::StreamSpooled => unpack_stream_spooled(req).await,
                    Handler::StreamSpooledConcurrent => unpack_stream_spooled_concurrent(req).await,
                };
                match (made, buffer_size) {
                    (Ok(r), Some(n)) => Ok(r.buffer_size(n)),
                    (other, _) => other,
                }
            }),
        )
        .layer(DefaultBodyLimit::disable());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let sock = TcpSocket::new_v4().unwrap();
    sock.set_recv_buffer_size(64 * 1024).unwrap();
    sock.set_send_buffer_size(64 * 1024).unwrap();
    let stream = sock.connect(addr).await.unwrap();
    let (mut rd, mut wr) = tokio::io::split(stream);

    let sent = Arc::new(AtomicU64::new(0));
    let received = Arc::new(AtomicU64::new(0));

    let head = format!(
        "POST /unpack HTTP/1.1\r\nHost: x\r\nContent-Type: application/flowfile-v3\r\n\
         Connection: close\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );

    let recv_counter = received.clone();
    let started = std::time::Instant::now();
    let drain = async move {
        let mut buf = vec![0u8; 64 * 1024];
        let mut first = true;
        loop {
            match rd.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if first {
                        first = false;
                        eprintln!(
                            "    [client] first response byte after {} ms",
                            started.elapsed().as_millis()
                        );
                    }
                    recv_counter.fetch_add(n as u64, Ordering::Relaxed);
                }
            }
        }
    };
    // Either start draining now, or hold it until the request is fully sent.
    // Boxed so the eager and the deferred arm have the same type.
    let drain = Box::pin(drain);
    let (reader, deferred) = if read_while_writing {
        (Some(tokio::spawn(drain)), None)
    } else {
        (None, Some(drain))
    };

    let send_counter = sent.clone();
    let work = async move {
        wr.write_all(head.as_bytes()).await.ok()?;
        let throttle: u64 = std::env::var("REPRO_THROTTLE_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        for chunk in body.chunks(256 * 1024) {
            wr.write_all(chunk).await.ok()?;
            send_counter.fetch_add(chunk.len() as u64, Ordering::Relaxed);
            if throttle > 0 {
                tokio::time::sleep(Duration::from_millis(throttle)).await;
            }
        }
        wr.flush().await.ok()?;
        eprintln!("    [client] request fully sent");
        match (reader, deferred) {
            (Some(handle), _) => handle.await.ok()?,
            (None, Some(drain)) => drain.await,
            (None, None) => unreachable!(),
        }
        Some(())
    };
    tokio::pin!(work);

    // Watchdog: stuck means no byte moved in either direction for STALL_AFTER.
    let mut last = (0u64, 0u64);
    let mut idle = Duration::ZERO;
    loop {
        tokio::select! {
            done = &mut work => {
                let response_bytes = received.load(Ordering::Relaxed);
                return match done {
                    Some(()) => Outcome::Completed { response_bytes },
                    None => Outcome::Stalled { sent: sent.load(Ordering::Relaxed), received: response_bytes },
                };
            }
            () = tokio::time::sleep(Duration::from_secs(1)) => {
                let now = (sent.load(Ordering::Relaxed), received.load(Ordering::Relaxed));
                if now == last {
                    idle += Duration::from_secs(1);
                    if idle >= STALL_AFTER {
                        return Outcome::Stalled { sent: now.0, received: now.1 };
                    }
                } else {
                    idle = Duration::ZERO;
                    last = now;
                }
            }
        }
    }
}

async fn payload() -> Vec<u8> {
    // A large entry first, then more archive behind it, so the producer is
    // still writing part one while the client still has bytes to send.
    let tar = tar_of(&[
        ("big.bin", noise(24 << 20, 1)),
        ("tail.bin", noise(24 << 20, 2)),
    ])
    .await;
    FlowFile::builder()
        .attribute("filename", "archive.tar")
        .content(tar)
        .to_bytes()
}

/// Demonstrates the bug, so it fails on purpose. Run it with
/// `cargo test --all-features --test stall_repro -- --ignored --nocapture`.
#[ignore = "reproduces the stall: fails by design"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nifi_shape_default_buffer() {
    let out = run(None, false, Handler::GzipBuffered, payload().await).await;
    println!("NiFi shape, 64 KiB buffer  -> {out:?}");
    assert!(out.completed(), "STALLED");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nifi_shape_huge_buffer() {
    let out = run(Some(1 << 30), false, Handler::GzipBuffered, payload().await).await;
    println!("NiFi shape, 1 GiB buffer   -> {out:?}");
    assert!(out.completed(), "STALLED");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn curl_shape_default_buffer() {
    let out = run(None, true, Handler::GzipBuffered, payload().await).await;
    println!("curl shape, 64 KiB buffer  -> {out:?}");
    assert!(out.completed(), "STALLED");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nifi_shape_drain_request_first() {
    let out = run(None, false, Handler::GzipDrainMemory, payload().await).await;
    println!("NiFi shape, drained first  -> {out:?}");
    assert!(out.completed(), "STALLED");
}

/// The user's real shape: sizes known up front, nothing buffered.
#[ignore = "reproduces the stall: fails by design"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nifi_shape_streaming_known_size() {
    let out = run(None, false, Handler::StreamKnownSize, payload().await).await;
    println!("NiFi shape, streaming known size -> {out:?}");
    assert!(out.completed(), "STALLED");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nifi_shape_streaming_spooled_to_disk() {
    let out = run(None, false, Handler::StreamSpooled, payload().await).await;
    println!("NiFi shape, spooled to disk      -> {out:?}");
    assert!(out.completed(), "STALLED");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nifi_shape_streaming_spooled_concurrently() {
    let out = run(
        None,
        false,
        Handler::StreamSpooledConcurrent,
        payload().await,
    )
    .await;
    println!("NiFi shape, concurrent spool     -> {out:?}");
    assert!(out.completed(), "STALLED");
}
