//! Many flow files in, one flow file out — asynchronously.
//!
//! The `tokio` mirror of `merge.rs`. `FlowFilesAsync` reads a concatenated
//! stream one flow file at a time, so the parts arrive as they come off the
//! wire rather than all at once.
//!
//!     cargo run --features tokio --example merge_async

use nififf3::{FlowFile, FlowFilesAsync, attr};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input = fragmented();

    let mut parts: Vec<FlowFile<Vec<u8>>> = Vec::new();
    let mut flow_files = FlowFilesAsync::new(input.as_slice());
    while let Some(part) = flow_files.next().await {
        parts.push(part?);
    }
    println!(
        "in:  {} flow files, {} bytes total",
        parts.len(),
        input.len()
    );

    let identifier = parts[0].attributes()[attr::FRAGMENT_ID].clone();
    assert!(
        parts
            .iter()
            .all(|part| part.attributes()[attr::FRAGMENT_ID] == identifier),
        "parts from different splits must not be merged"
    );

    parts.sort_by_key(|part| {
        part.attributes()[attr::FRAGMENT_INDEX]
            .parse::<u64>()
            .unwrap_or(0)
    });

    let mut content = Vec::new();
    for (offset, part) in parts.iter().enumerate() {
        if offset > 0 {
            content.push(b'\n');
        }
        content.extend_from_slice(part.content());
    }

    let original_filename = parts[0].attributes()[attr::SEGMENT_ORIGINAL_FILENAME].clone();
    let merged = parts[0]
        .derive()
        .attribute(attr::FILENAME, original_filename)
        .without_attribute(attr::FRAGMENT_ID)
        .without_attribute(attr::FRAGMENT_INDEX)
        .without_attribute(attr::FRAGMENT_COUNT)
        .without_attribute(attr::SEGMENT_ORIGINAL_FILENAME)
        .content(content);

    let mut out = Vec::new();
    merged.into_reader().write_to_async(&mut out).await?;

    let parsed = FlowFile::from_bytes(&out)?;
    println!(
        "out: {} bytes, filename={}",
        parsed.size(),
        parsed.attributes()["filename"],
    );
    println!("     {:?}", String::from_utf8_lossy(parsed.content()));

    assert_eq!(parsed.content().as_slice(), b"alpha\nbeta\ngamma");
    assert!(!parsed.attributes().contains_key(attr::FRAGMENT_INDEX));
    Ok(())
}

fn fragmented() -> Vec<u8> {
    let parent = FlowFile::builder()
        .attribute("filename", "records.txt")
        .attribute("source", "example")
        .content(Vec::new());

    let mut parts = parent.fragments().with_count(3);
    let flow_files = [
        parts.next().content(&b"alpha"[..]),
        parts.next().content(&b"beta"[..]),
        parts.next().content(&b"gamma"[..]),
    ];

    let mut bytes = Vec::new();
    for offset in [2, 0, 1] {
        bytes.extend_from_slice(&flow_files[offset].to_bytes());
    }
    bytes
}
