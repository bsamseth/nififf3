//! Many flow files in, one flow file out — synchronously.
//!
//! The inverse of `split.rs`. NiFi's `MergeContent` reassembles a split in
//! `defragment` mode by binning on `fragment.identifier`, ordering by
//! `fragment.index`, and checking the bin against `fragment.count`; this does
//! the same, then restores the original filename and drops the fragment
//! attributes so the result looks like the flow file the split started from.
//!
//!     cargo run --example merge

use nififf3::{FlowFile, FlowFiles, attr};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input = fragmented();

    let mut parts: Vec<FlowFile<Vec<u8>>> =
        FlowFiles::new(input.as_slice()).collect::<Result<_, _>>()?;
    println!(
        "in:  {} flow files, {} bytes total",
        parts.len(),
        input.len()
    );

    // One bin: every part must belong to the same split.
    let identifier = parts[0].attributes()[attr::FRAGMENT_ID].clone();
    assert!(
        parts
            .iter()
            .all(|part| part.attributes()[attr::FRAGMENT_ID] == identifier),
        "parts from different splits must not be merged"
    );

    parts.sort_by_key(index_of);

    // At least one part must declare the total, and the bin must be complete.
    let expected: usize = parts
        .iter()
        .find_map(|part| part.attributes().get(attr::FRAGMENT_COUNT))
        .ok_or("no part declares fragment.count")?
        .parse()?;
    assert_eq!(parts.len(), expected, "incomplete fragment set");

    let mut content = Vec::new();
    for (offset, part) in parts.iter().enumerate() {
        if offset > 0 {
            content.push(b'\n');
        }
        content.extend_from_slice(part.content());
    }

    // Rebuild from the first part: its inherited attributes are the parent's.
    let original_filename = parts[0].attributes()[attr::SEGMENT_ORIGINAL_FILENAME].clone();
    let merged = parts[0]
        .derive()
        .attribute(attr::FILENAME, original_filename)
        .without_attribute(attr::FRAGMENT_ID)
        .without_attribute(attr::FRAGMENT_INDEX)
        .without_attribute(attr::FRAGMENT_COUNT)
        .without_attribute(attr::SEGMENT_ORIGINAL_FILENAME)
        .content(content);

    println!(
        "out: {} bytes, filename={}",
        merged.size(),
        merged.attributes()["filename"],
    );
    println!("     {:?}", String::from_utf8_lossy(merged.content()));

    assert_eq!(merged.content().as_slice(), b"alpha\nbeta\ngamma");
    assert_eq!(merged.attributes()["filename"], "records.txt");
    assert_eq!(merged.attributes()["source"], "example", "inherited");
    assert!(!merged.attributes().contains_key(attr::FRAGMENT_INDEX));
    Ok(())
}

fn index_of(part: &FlowFile<Vec<u8>>) -> u64 {
    part.attributes()[attr::FRAGMENT_INDEX].parse().unwrap_or(0)
}

/// The output of `split.rs`, with the parts deliberately out of order to show
/// that `fragment.index` is what puts them back.
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
