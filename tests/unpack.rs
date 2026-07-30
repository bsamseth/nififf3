//! The 1-to-many case end to end: one flow file containing a `.tar.gz` in,
//! one flow file per archive entry out.
//!
//! This is the shape [`FlowFilesResponse`] exists for, so it is exercised
//! against real decoders (`async-compression` + `astral-tokio-tar`) rather
//! than a stand-in.

#![cfg(feature = "axum")]

use std::io;

use async_compression::tokio::bufread::GzipDecoder;
use async_compression::tokio::write::GzipEncoder;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use axum::routing::post;
use axum::{Router, response::IntoResponse};
use http_body_util::BodyExt;
use nififf3::{Error, FlowFile, FlowFilesAsync, FlowFilesResponse, StrictFlowFileRequest};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio_stream::StreamExt;
use tokio_tar::{Archive, Builder, EntryType, Header};
use tower::ServiceExt;

/// The attribute a failed entry is reported under; the handler's choice, not
/// the crate's.
const ERROR_ATTRIBUTE: &str = "unpack.error";

async fn tar_bytes(files: &[(&str, &[u8])]) -> Vec<u8> {
    let mut builder = Builder::new(Vec::new());
    for (name, data) in files {
        let mut header = Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_entry_type(EntryType::Regular);
        header.set_mode(0o644);
        builder.append_data(&mut header, name, *data).await.unwrap();
    }
    builder.finish().await.unwrap();
    builder.into_inner().await.unwrap()
}

async fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = GzipEncoder::new(Vec::new());
    encoder.write_all(bytes).await.unwrap();
    encoder.shutdown().await.unwrap();
    encoder.into_inner()
}

async fn tar_gz(files: &[(&str, &[u8])]) -> Vec<u8> {
    gzip(&tar_bytes(files).await).await
}

fn archive_of(parent: FlowFile<impl AsyncRead + Unpin>) -> Archive<impl AsyncRead + Unpin> {
    Archive::new(GzipDecoder::new(BufReader::new(parent.into_content())))
}

/// Unpack an archive into one flow file per entry, streaming each entry's
/// content straight through.
///
/// Everything that can fail the request as a whole is checked before the
/// `FlowFilesResponse` is returned. Past that point the status is 200, and a
/// *structural* problem with the archive is reported as an attribute on its
/// own flow file.
///
/// Note what this handler cannot recover from: `write` commits an entry's
/// declared size before its bytes are read, so an entry whose content runs
/// short can only abort the response. See [`unpack_lenient`] for the
/// alternative.
async fn unpack(req: StrictFlowFileRequest) -> Result<FlowFilesResponse, Error> {
    let parent = req.into_inner();
    let mut parts = parent.fragments();
    let mut archive = archive_of(parent);

    Ok(FlowFilesResponse::new(move |mut writer| async move {
        let mut entries = archive.entries()?;
        while let Some(entry) = entries.next().await {
            match entry {
                Ok(entry) => {
                    let size = entry.header().entry_size()?;
                    let name = entry.path()?.display().to_string();
                    writer
                        .write(parts.next().attribute("filename", name).reader(entry, size))
                        .await?;
                }
                Err(err) => {
                    writer
                        .write_bytes(
                            &parts
                                .next()
                                .attribute(ERROR_ATTRIBUTE, err.to_string())
                                .without_attribute("filename")
                                .content(Vec::new()),
                        )
                        .await?;
                    // The archive position is no longer trustworthy after a
                    // decode error, and `Entries` would keep reporting it, so
                    // stop rather than spin.
                    break;
                }
            }
        }
        // How many entries there were is only known now, so the count goes on
        // a terminator rather than on the parts. Without it `MergeContent`
        // could never fill the bin and would fail the whole bundle.
        writer.write_bytes(&parts.terminate()).await?;
        Ok(())
    }))
}

/// The same unpacking, but buffering each entry before writing it.
///
/// Reading an entry to the end first means a truncated entry is discovered
/// while nothing has been committed for it yet, so it can be reported as an
/// error flow file instead of poisoning the response. The cost is holding one
/// entry in memory at a time — the trade the streaming [`unpack`] avoids.
async fn unpack_lenient(req: StrictFlowFileRequest) -> Result<FlowFilesResponse, Error> {
    let parent = req.into_inner();
    let mut parts = parent.fragments();
    let mut archive = archive_of(parent);

    Ok(FlowFilesResponse::new(move |mut writer| async move {
        let mut entries = archive.entries()?;
        while let Some(entry) = entries.next().await {
            let (part, failure) = match entry {
                Ok(mut entry) => {
                    let size = entry.header().entry_size()?;
                    let name = entry.path()?.display().to_string();
                    let part = parts.next().attribute("filename", name);
                    let mut content = Vec::new();
                    match entry.read_to_end(&mut content).await {
                        Ok(read) if read as u64 == size => (part.content(content), None),
                        Ok(read) => (
                            part.content(Vec::new()),
                            Some(format!("truncated: {read} of {size} bytes")),
                        ),
                        Err(err) => (part.content(Vec::new()), Some(err.to_string())),
                    }
                }
                Err(err) => (
                    parts
                        .next()
                        .without_attribute("filename")
                        .content(Vec::new()),
                    Some(err.to_string()),
                ),
            };

            match failure {
                None => writer.write_bytes(&part).await?,
                Some(message) => {
                    let mut part = part;
                    part.attributes_mut()
                        .insert(ERROR_ATTRIBUTE.to_string(), message);
                    writer.write_bytes(&part).await?;
                    break;
                }
            };
        }
        // As in `unpack`: the bundle declares its own size on the way out, so
        // what did arrive is still mergeable — the failure report included.
        writer.write_bytes(&parts.terminate()).await?;
        Ok(())
    }))
}

fn app() -> Router {
    Router::new()
        .route("/unpack", post(unpack))
        .route("/unpack-lenient", post(unpack_lenient))
}

fn request(body: Vec<u8>) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/unpack")
        .header(header::CONTENT_TYPE, nififf3::MEDIA_TYPE)
        .body(Body::from(body))
        .unwrap()
}

/// Pack `archive` into a flow file the way a NiFi client would.
fn parent_flow_file(archive: Vec<u8>) -> Vec<u8> {
    FlowFile::builder()
        .attribute("filename", "archive.tar.gz")
        .attribute("source", "upload")
        .attribute("uuid", "parent-uuid")
        .content(archive)
        .to_bytes()
}

async fn collect(response: axum::response::Response) -> Vec<FlowFile<Vec<u8>>> {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let mut flow_files = FlowFilesAsync::new(bytes.as_ref());
    let mut collected = Vec::new();
    while let Some(flow_file) = flow_files.next().await {
        collected.push(flow_file.unwrap());
    }
    collected
}

/// The entry parts of a response, checking that the bundle declares its own
/// size the way `MergeContent` needs before dropping the terminator.
///
/// The handlers do not know how many entries an archive holds until they have
/// walked it, so the count arrives last, on a flow file that counts itself.
async fn parts_of(response: axum::response::Response) -> Vec<FlowFile<Vec<u8>>> {
    let mut bundle = collect(response).await;

    let terminator = bundle.pop().expect("a terminated bundle is never empty");
    assert_eq!(terminator.size(), 0, "the terminator carries no content");
    assert_eq!(
        terminator.attributes()[nififf3::attr::FRAGMENT_COUNT],
        (bundle.len() + 1).to_string(),
        "the count covers every flow file in the bundle, terminator included"
    );
    assert_eq!(
        terminator.attributes()[nififf3::attr::FRAGMENT_INDEX],
        (bundle.len() + 1).to_string(),
        "and it is the last of them"
    );
    for part in &bundle {
        assert!(
            !part
                .attributes()
                .contains_key(nififf3::attr::FRAGMENT_COUNT),
            "one flow file declares the count; NiFi asks for no more"
        );
    }

    bundle
}

#[tokio::test]
async fn unpacks_an_archive_into_one_flow_file_per_entry() {
    let archive = tar_gz(&[
        ("a.txt", b"first"),
        ("nested/b.txt", b"second"),
        ("c.bin", &[0u8; 5000]),
    ])
    .await;

    let response = app()
        .oneshot(request(parent_flow_file(archive)))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        nififf3::MEDIA_TYPE
    );
    // Streamed, so the length is not known when the headers go out.
    assert!(!response.headers().contains_key(header::CONTENT_LENGTH));

    let parts = parts_of(response).await;
    assert_eq!(parts.len(), 3);

    let names: Vec<_> = parts
        .iter()
        .map(|part| part.attributes()["filename"].as_str())
        .collect();
    assert_eq!(names, ["a.txt", "nested/b.txt", "c.bin"]);

    assert_eq!(parts[0].content().as_slice(), b"first");
    assert_eq!(parts[1].content().as_slice(), b"second");
    assert_eq!(parts[2].size(), 5000);
}

#[tokio::test]
async fn parts_carry_inherited_and_fragment_attributes() {
    let archive = tar_gz(&[("a.txt", b"first"), ("b.txt", b"second")]).await;
    let parts = collect(
        app()
            .oneshot(request(parent_flow_file(archive)))
            .await
            .unwrap(),
    )
    .await;

    for (offset, part) in parts.iter().enumerate() {
        let attributes = part.attributes();
        assert_eq!(attributes["source"], "upload", "inherited from the parent");
        assert_eq!(
            attributes["segment.original.filename"], "archive.tar.gz",
            "the parent's filename, not the entry's"
        );
        assert_eq!(attributes["fragment.index"], (offset + 1).to_string());
        assert_ne!(attributes["uuid"], "parent-uuid", "each part is its own");
    }

    assert_eq!(
        parts[0].attributes()["fragment.identifier"],
        parts[1].attributes()["fragment.identifier"],
        "one identifier for the whole set"
    );
    assert_ne!(
        parts[0].attributes()["uuid"],
        parts[1].attributes()["uuid"],
        "but a distinct uuid per part"
    );
    // The count is not known before the entries have been walked.
    assert!(!parts[0].attributes().contains_key("fragment.count"));
}

/// A `.tar.gz` whose gzip framing is intact but whose *second* entry is cut
/// off part-way through its content. Truncating the compressed stream instead
/// would only lose the trailer, leaving both entries decodable.
async fn truncated_archive() -> Vec<u8> {
    let tar = tar_bytes(&[("a.txt", b"first"), ("b.txt", &[b'x'; 600])]).await;
    // 512 header + 512 padded data for `a.txt`, then `b.txt`'s header, then
    // 300 of its 600 content bytes.
    gzip(&tar[..1536 + 300]).await
}

#[tokio::test]
async fn a_truncated_entry_is_reported_as_a_part_when_the_handler_buffers() {
    let request = Request::builder()
        .method("POST")
        .uri("/unpack-lenient")
        .header(header::CONTENT_TYPE, nififf3::MEDIA_TYPE)
        .body(Body::from(parent_flow_file(truncated_archive().await)))
        .unwrap();

    let response = app().oneshot(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "the flow file itself was valid, so the request succeeded"
    );

    // The bundle is still terminated, so what did arrive is mergeable: the
    // good entry and the failure report bin together and complete.
    let parts = parts_of(response).await;
    let (ok, broken): (Vec<_>, Vec<_>) = parts
        .iter()
        .partition(|part| !part.attributes().contains_key(ERROR_ATTRIBUTE));

    assert_eq!(ok.len(), 1, "the intact entry came through");
    assert_eq!(ok[0].attributes()["filename"], "a.txt");
    assert_eq!(ok[0].content().as_slice(), b"first");

    assert_eq!(broken.len(), 1, "the failure arrived as a flow file");
    assert_eq!(broken[0].attributes()["fragment.index"], "2");
    assert!(broken[0].content().is_empty());
    assert!(!broken[0].attributes()[ERROR_ATTRIBUTE].is_empty());
}

#[tokio::test]
async fn a_truncated_entry_aborts_the_body_when_the_handler_streams() {
    let response = app()
        .oneshot(request(parent_flow_file(truncated_archive().await)))
        .await
        .unwrap();

    // The status was already committed, so the failure can only show up as a
    // body that never terminates cleanly.
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.into_body().collect().await.is_err(),
        "a part whose size was already declared cannot be walked back"
    );
}

#[tokio::test]
async fn a_body_that_is_not_a_flow_file_is_rejected_before_any_unpacking() {
    let response = app().oneshot(request(b"not a flow file".to_vec())).await;
    assert_eq!(response.unwrap().status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_missing_content_type_is_rejected_before_any_unpacking() {
    let request = Request::builder()
        .method("POST")
        .uri("/unpack")
        .body(Body::from(parent_flow_file(tar_gz(&[]).await)))
        .unwrap();

    let response = app().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
}

#[tokio::test]
async fn an_archive_with_no_entries_is_a_bundle_with_no_parts() {
    let response = app()
        .oneshot(request(parent_flow_file(tar_gz(&[]).await)))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    // The terminator is still there, declaring a bundle of one: an empty
    // archive is a split that produced nothing, not a truncated response.
    assert!(parts_of(response).await.is_empty());
}

#[tokio::test]
async fn from_vec_sets_a_content_length() {
    let parent = FlowFile::builder()
        .attribute("filename", "pair")
        .content(Vec::new());
    let mut parts = parent.fragments().with_count(2);
    let response = FlowFilesResponse::from_vec(vec![
        parts.next().content(&b"first"[..]),
        parts.next().content(&b"second"[..]),
    ])
    .into_response();

    let length: usize = response.headers()[header::CONTENT_LENGTH]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();

    let collected = collect(response).await;
    assert_eq!(collected.len(), 2);
    assert_eq!(collected[0].attributes()["fragment.count"], "2");
    assert!(length > 0);
}

/// A zero-sized buffer would leave the producer's first write parked forever
/// on a duplex that can never accept a byte, hanging the response with no error
/// to show for it. The timeout is the assertion: a regression fails the test
/// instead of wedging the suite.
#[tokio::test]
async fn a_zero_buffer_size_still_delivers_the_body() {
    let response = FlowFilesResponse::new(|mut writer| async move {
        writer
            .write_bytes(&FlowFile::builder().content(&b"delivered"[..]))
            .await?;
        Ok(())
    })
    .buffer_size(0)
    .into_response();

    let body = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        response.into_body().collect(),
    )
    .await
    .expect("a zero buffer size must not hang the response")
    .unwrap()
    .to_bytes();

    let mut parts = FlowFilesAsync::new(body.as_ref());
    let part = parts.next().await.unwrap().unwrap();
    assert_eq!(part.content().as_slice(), b"delivered");
}

#[tokio::test]
async fn a_producer_error_truncates_the_body_rather_than_ending_it_cleanly() {
    let response = FlowFilesResponse::new(|mut writer| async move {
        writer
            .write_bytes(&FlowFile::builder().content(&b"delivered"[..]))
            .await?;
        // Any error will do; this one has nothing to do with flow files.
        Err(io::Error::other("the source went away").into())
    })
    .into_response();

    assert_eq!(response.status(), StatusCode::OK);
    // The bytes written before the failure are still sent, but the body ends
    // in an error instead of a clean EOF.
    assert!(response.into_body().collect().await.is_err());
}

/// A producer mixes errors from its decoder, from plain I/O and from this
/// crate. None of them should need converting to a common type by hand.
#[tokio::test]
async fn a_producer_may_mix_error_types_without_converting_them() {
    fn parse_port(text: &str) -> Result<u16, std::num::ParseIntError> {
        text.parse()
    }

    let response = FlowFilesResponse::new(|mut writer| async move {
        let port = parse_port("8443")?; // ParseIntError
        std::str::from_utf8(b"ok")?; // Utf8Error
        io::Result::Ok(())?; // io::Error
        let parent = FlowFile::from_bytes(&parent_flow_file(Vec::new()))?; // nififf3::Error

        writer
            .write_bytes(
                &parent
                    .derive()
                    .attribute("port", port.to_string())
                    .content(Vec::new()),
            )
            .await?; // nififf3::Error
        Ok(())
    })
    .into_response();

    let parts = collect(response).await;
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].attributes()["port"], "8443");
}

#[tokio::test]
async fn blocking_producers_stream_the_same_way() {
    let response = FlowFilesResponse::blocking(|mut writer| {
        for i in 0..3 {
            writer.write_bytes(
                &FlowFile::builder()
                    .attribute("n", i.to_string())
                    .content(vec![b'x'; i]),
            )?;
        }
        Ok(())
    })
    .into_response();

    let parts = collect(response).await;
    assert_eq!(parts.len(), 3);
    assert_eq!(parts[2].attributes()["n"], "2");
    assert_eq!(parts[2].content().as_slice(), b"xx");
}
