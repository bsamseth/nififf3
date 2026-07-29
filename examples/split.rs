//! One flow file in, many flow files out — synchronously.
//!
//! Splits a record-per-line payload into a flow file per record, numbering
//! them with NiFi's fragment attributes so `merge.rs` (or NiFi's
//! `MergeContent`) can put them back together.
//!
//!     cargo run --example split

use nififf3::{FlowFile, FlowFiles, FlowFilesWriter};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let parent = FlowFile::from_bytes(&incoming())?;
    println!(
        "in:  {} bytes, filename={}",
        parent.size(),
        parent.attributes()["filename"],
    );

    let records: Vec<&[u8]> = parent.content().split(|byte| *byte == b'\n').collect();

    // The count is optional — supply it only when it is known up front, as it
    // is here. Each part inherits the parent's attributes, gets its own
    // `uuid`, and is numbered from 1.
    let mut parts = parent.fragments().with_count(records.len() as u64);

    let mut out = Vec::new();
    let mut writer = FlowFilesWriter::new(&mut out);
    for (offset, record) in records.iter().enumerate() {
        writer.write_bytes(
            &parts
                .next()
                .attribute("filename", format!("record-{offset}.txt"))
                .content(*record),
        )?;
    }
    println!(
        "out: {} flow files, {} bytes total",
        writer.count(),
        out.len()
    );

    // Read them back the way a downstream consumer would.
    for part in FlowFiles::new(out.as_slice()) {
        let part = part?;
        println!(
            "     [{}/{}] {} = {:?}",
            part.attributes()["fragment.index"],
            part.attributes()["fragment.count"],
            part.attributes()["filename"],
            String::from_utf8_lossy(part.content()),
        );
        assert_eq!(
            part.attributes()["segment.original.filename"],
            "records.txt"
        );
        assert_eq!(part.attributes()["source"], "example", "inherited");
    }
    Ok(())
}

fn incoming() -> Vec<u8> {
    FlowFile::builder()
        .attribute("filename", "records.txt")
        .attribute("source", "example")
        .content(&b"alpha\nbeta\ngamma"[..])
        .to_bytes()
}
