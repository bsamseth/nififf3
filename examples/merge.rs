//! Many flow files in, one flow file out — synchronously.
//!
//! The inverse of `split.rs`. NiFi's `MergeContent` reassembles a split in
//! `defragment` mode by binning on `fragment.identifier`, ordering by
//! `fragment.index`, and checking the bin against `fragment.count`; this does
//! the same, then restores the original filename and drops the fragment
//! attributes so the result looks like the flow file the split started from.
//!
//! It takes both forms a split can arrive in: `split.rs` knows the total up
//! front and puts `fragment.count` on every part, while `split_async.rs` does
//! not and ends the bundle with a terminator carrying the count. Only one flow
//! file has to declare it either way, so the completeness check is the same.
//! The terminator needs one extra step here that NiFi does not need: it is
//! empty, so concatenating it is harmless, but this example joins parts with a
//! newline and would otherwise leave a trailing one.
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

    // At least one flow file must declare the total, and the bin must be
    // complete. The count is over flow files in the bundle, so a terminator
    // counts towards it — which is exactly why it can declare it at all.
    let expected: usize = parts
        .iter()
        .find_map(|part| part.attributes().get(attr::FRAGMENT_COUNT))
        .ok_or("no part declares fragment.count")?
        .parse()?;
    assert_eq!(parts.len(), expected, "incomplete fragment set");

    // Drop a terminator now that it has served its purpose. It carries no
    // content, so NiFi would concatenate it to nothing and never notice; a
    // consumer that puts something *between* parts has to.
    parts.retain(|part| !is_terminator(part, expected));

    let mut content = Vec::new();
    for (offset, part) in parts.iter().enumerate() {
        if offset > 0 {
            content.push(b'\n');
        }
        content.extend_from_slice(part.content());
    }

    // Rebuild from the first part: its inherited attributes are the parent's.
    // `defragment` drops the fragment attributes and puts the original
    // filename back, undoing exactly what `fragments` added in `split.rs`.
    let merged = parts[0].derive().defragment().content(content);

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

/// Whether `part` is the empty flow file `Fragments::terminate` adds to carry
/// the count: the last in the bundle, with nothing in it.
///
/// A convention rather than a guarantee — a split whose real last part happens
/// to be empty looks identical. A producer that emits empty parts and needs
/// them told apart should mark its terminator with an attribute of its own.
fn is_terminator(part: &FlowFile<Vec<u8>>, count: usize) -> bool {
    part.size() == 0 && index_of(part) == count as u64
}

/// The output of `split_async.rs` — a bundle whose count arrives on a
/// terminator, since that is the form that needs the extra handling. Swapping
/// `fragments()` for `fragments().with_count(3)` and dropping the terminator
/// gives `split.rs`'s output, which merges through the same code.
///
/// The parts are deliberately out of order, to show that `fragment.index` is
/// what puts them back.
fn fragmented() -> Vec<u8> {
    let parent = FlowFile::builder()
        .attribute("filename", "records.txt")
        .attribute("source", "example")
        .content(Vec::new());

    let mut parts = parent.fragments();
    let flow_files = [
        parts.next_part().content(&b"alpha"[..]),
        parts.next_part().content(&b"beta"[..]),
        parts.next_part().content(&b"gamma"[..]),
        parts.terminate(),
    ];

    let mut bytes = Vec::new();
    for offset in [2, 0, 1, 3] {
        bytes.extend_from_slice(&flow_files[offset].to_bytes());
    }
    bytes
}
