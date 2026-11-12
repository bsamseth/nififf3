//! The crate against a client that behaves like NiFi's: it writes the whole
//! request before it reads any of the response.
//!
//! Over a real socket with small buffers, so that the exchange can actually
//! deadlock. A stall is detected by lack of progress, not by total time,
//! because small socket buffers also make the transfer slow.
#![cfg(all(feature = "axum", feature = "uuid"))]
#![allow(dead_code, clippy::cast_possible_truncation)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::RequestExt as _;
use axum::Router;
use axum::extract::{DefaultBodyLimit, Request};
use axum::routing::post;
use nififf3::{Error, FlowFile, FlowFileBody, FlowFilesResponse};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpSocket};
use tokio_stream::StreamExt;
use tokio_tar::{Archive, Builder, EntryType, Header};

/// How long without a single byte moving in either direction counts as stuck.
const STALL_AFTER: Duration = Duration::from_secs(10);

#[derive(Debug)]
enum Outcome {
    Completed { response_bytes: u64 },
    Stalled { sent: u64, received: u64 },
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

/// One flow file per archive entry, streamed straight through. The parent is
/// spooled by the extractor, so the response never waits on the socket.
async fn split(parent: FlowFile) -> Result<FlowFilesResponse, Error> {
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
        Ok(())
    }))
}

/// The flow file straight back out.
async fn echo(parent: FlowFile) -> FlowFile {
    parent
}

/// The same split, but reading the request off the socket while writing the
/// response. This is the shape the extractor exists to prevent.
async fn split_from_the_socket(request: Request) -> Result<FlowFilesResponse, Error> {
    let body = FlowFileBody::new(request.with_limited_body().into_body());
    let parent = FlowFile::parse_async(body).await?;
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
        Ok(())
    }))
}

fn app() -> Router {
    Router::new()
        .route("/split", post(split))
        .route("/echo", post(echo))
        .route("/split-from-the-socket", post(split_from_the_socket))
        .layer(DefaultBodyLimit::disable())
}

async fn serve() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app()).await.unwrap() });
    addr
}

/// A socket with small buffers, so the deadlock is reachable with a modest
/// payload.
async fn connect(addr: std::net::SocketAddr) -> tokio::net::TcpStream {
    let sock = TcpSocket::new_v4().unwrap();
    sock.set_recv_buffer_size(64 * 1024).unwrap();
    sock.set_send_buffer_size(64 * 1024).unwrap();
    sock.connect(addr).await.unwrap()
}

fn request_head(path: &str, body_len: usize) -> String {
    format!(
        "POST {path} HTTP/1.1\r\nHost: x\r\nContent-Type: application/flowfile-v3\r\n\
         Connection: close\r\nContent-Length: {body_len}\r\n\r\n"
    )
}

/// `read_while_writing` is the whole variable: `false` is the NiFi shape,
/// `true` is the curl shape.
async fn run(path: &str, read_while_writing: bool, body: Vec<u8>) -> Outcome {
    let addr = serve().await;
    let stream = connect(addr).await;
    let (mut rd, mut wr) = tokio::io::split(stream);

    let sent = Arc::new(AtomicU64::new(0));
    let received = Arc::new(AtomicU64::new(0));
    let head = request_head(path, body.len());

    let recv_counter = Arc::clone(&received);
    let drain = Box::pin(async move {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            match rd.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    recv_counter.fetch_add(n as u64, Ordering::Relaxed);
                }
            }
        }
    });
    let (reader, deferred) = if read_while_writing {
        (Some(tokio::spawn(drain)), None)
    } else {
        (None, Some(drain))
    };

    let send_counter = Arc::clone(&sent);
    let work = async move {
        wr.write_all(head.as_bytes()).await.ok()?;
        for chunk in body.chunks(256 * 1024) {
            wr.write_all(chunk).await.ok()?;
            send_counter.fetch_add(chunk.len() as u64, Ordering::Relaxed);
        }
        wr.flush().await.ok()?;
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

/// A request large enough that the client cannot park what is left of it in
/// the socket buffers: a few MiB on an ordinary kernel. 48 MiB leaves room to
/// spare, and a large first entry keeps the server writing while the client
/// still has bytes to send.
async fn payload() -> Vec<u8> {
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

/// The extractor spools the request, so streaming the response completes
/// against a client that will not read until it has sent everything.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_streaming_split_completes_against_a_nifi_shaped_client() {
    let out = run("/split", false, payload().await).await;
    assert!(
        matches!(out, Outcome::Completed { response_bytes } if response_bytes > 48 << 20),
        "{out:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_echo_completes_against_a_nifi_shaped_client() {
    let body = payload().await;
    let out = run("/echo", false, body.clone()).await;
    assert!(
        matches!(out, Outcome::Completed { response_bytes } if response_bytes > body.len() as u64),
        "{out:?}"
    );
}

/// The hazard itself, pinned: a handler that reads the request off the socket
/// while streaming its response stalls against the NiFi-shaped client. If
/// this ever completes, the extractor's design rationale needs revisiting.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streaming_from_the_socket_stalls_against_a_nifi_shaped_client() {
    let out = run("/split-from-the-socket", false, payload().await).await;
    assert!(
        matches!(out, Outcome::Stalled { received: 0, .. }),
        "expected the documented deadlock, got {out:?}"
    );
}

/// And the same handler is fine against a client that reads while it writes,
/// which is why the problem looks like one particular client's fault.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streaming_from_the_socket_completes_against_a_curl_shaped_client() {
    let out = run("/split-from-the-socket", true, payload().await).await;
    assert!(matches!(out, Outcome::Completed { .. }), "{out:?}");
}

/// Many NiFi-shaped clients at once, each large enough to go to disk, all
/// echoed back intact. What a busy processor sees.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_concurrent_nifi_shaped_requests_all_complete() {
    const CLIENTS: u64 = 32;
    let addr = serve().await;

    let mut clients = tokio::task::JoinSet::new();
    for seed in 0..CLIENTS {
        clients.spawn(async move {
            let request = FlowFile::builder()
                .attribute("client", seed.to_string())
                .content(noise(6 << 20, seed + 1))
                .to_bytes();
            let mut stream = connect(addr).await;
            stream
                .write_all(request_head("/echo", request.len()).as_bytes())
                .await
                .unwrap();
            stream.write_all(&request).await.unwrap();
            stream.flush().await.unwrap();

            let mut response = Vec::new();
            stream.read_to_end(&mut response).await.unwrap();
            let body_at = response
                .windows(4)
                .position(|w| w == b"\r\n\r\n")
                .expect("a complete response head")
                + 4;
            assert!(response.starts_with(b"HTTP/1.1 200"), "client {seed}");
            assert_eq!(&response[body_at..], request.as_slice(), "client {seed}");
        });
    }

    tokio::time::timeout(Duration::from_secs(120), async {
        while let Some(client) = clients.join_next().await {
            client.unwrap();
        }
    })
    .await
    .expect("every client finished");
}
