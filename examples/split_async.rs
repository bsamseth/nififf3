//! One flow file in, many flow files out — asynchronously.
//!
//! The `tokio` mirror of `split.rs`, but writing each part from a *reader*
//! rather than from memory. `write` streams exactly the declared number of
//! bytes, so a part may be arbitrarily large without ever being buffered —
//! which is what makes unpacking an archive over HTTP practical.
//!
//!     cargo run --features tokio --example split_async

use nififf3::{FlowFile, FlowFilesAsync, FlowFilesWriterAsync};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let parent = FlowFile::from_bytes(&incoming())?;
    println!(
        "in:  {} bytes, filename={}",
        parent.size(),
        parent.attributes()["filename"],
    );

    let records: Vec<&[u8]> = parent.content().split(|byte| *byte == b'\n').collect();
    let mut parts = parent.fragments().with_count(records.len() as u64);

    let mut out = Vec::new();
    let mut writer = FlowFilesWriterAsync::new(&mut out);
    for (offset, record) in records.iter().enumerate() {
        // `record` is an `AsyncRead`; its bytes go straight to the writer.
        let part = parts
            .next()
            .attribute("filename", format!("record-{offset}.txt"))
            .reader(*record, record.len() as u64);
        writer.write(part).await?;
    }
    // Finish the stream rather than just dropping the writer. A `Vec` needs
    // nothing, but an `AsyncWrite` that encodes — a compressor, a TLS session
    // — emits its ending only on `shutdown`, and losing it corrupts the lot.
    let count = writer.count();
    writer.finish().await?;
    println!("out: {count} flow files, {} bytes total", out.len());

    let mut flow_files = FlowFilesAsync::new(out.as_slice());
    while let Some(part) = flow_files.next().await {
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
