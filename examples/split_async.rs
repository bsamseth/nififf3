//! One flow file in, many flow files out, asynchronously.
//!
//! This is the `tokio` mirror of `split.rs`, and it writes each part from a
//! reader rather than from memory. `write` streams exactly the declared number
//! of bytes, so a part can be any size without ever being buffered. That is
//! what makes unpacking an archive over HTTP practical.
//!
//! It also shows the other way to declare a bundle's size. `split.rs` counts
//! the records first and calls `with_count`. Here the parts are written as
//! they are found, so the total is not known until the input runs out. The set
//! ends with `terminate()`, an empty flow file carrying the count. That flow
//! file is one of the bundle, so it counts itself. NiFi's `MergeContent` needs
//! the count on at least one flow file and fills its bin when it holds that
//! many, so this reassembles like any other split. See `merge.rs`, which takes
//! both forms.
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

    // No `collect`, so the number of records is not known up front. This is
    // the streaming shape, where each part is on the wire before the next one
    // is read.
    let mut parts = parent.fragments();

    let mut out = Vec::new();
    let mut writer = FlowFilesWriterAsync::new(&mut out);
    for (offset, record) in parent.content().split(|byte| *byte == b'\n').enumerate() {
        // `record` is an `AsyncRead`; its bytes go straight to the writer.
        let part = parts
            .next_part()
            .attribute("filename", format!("record-{offset}.txt"))
            .reader(record, record.len() as u64);
        writer.write(part).await?;
    }

    // Now the total is known. The terminator declares it for the whole
    // bundle, counting itself: three records means a count of four.
    writer.write_bytes(&parts.terminate()).await?;
    // Finish the stream rather than just dropping the writer. A `Vec` needs
    // nothing, but an `AsyncWrite` that encodes, such as a compressor or a TLS
    // session, emits its ending only on `shutdown`. Losing that ending
    // corrupts everything written.
    let count = writer.count();
    writer.finish().await?;
    println!("out: {count} flow files, {} bytes total", out.len());

    // Read them back the way a downstream consumer would. Only the last flow
    // file declares the count, which is all `MergeContent` asks for.
    let mut flow_files = FlowFilesAsync::new(out.as_slice());
    let mut declared = None;
    let mut seen = 0;
    while let Some(part) = flow_files.next().await {
        let part = part?;
        let index = &part["fragment.index"];
        match part.parse_attribute::<usize>("fragment.count")? {
            Some(count) => {
                declared = Some(count);
                println!("     [{index}] terminator, count={count}");
            }
            None => println!(
                "     [{index}] {} = {:?}",
                part["filename"],
                String::from_utf8_lossy(part.content()),
            ),
        }
        assert_eq!(
            part.attributes()["segment.original.filename"],
            "records.txt"
        );
        seen += 1;
    }

    // What the bundle promised is what arrived, the terminator included.
    assert_eq!(declared, Some(seen));
    assert_eq!(seen, 4, "three records and the terminator");
    Ok(())
}

fn incoming() -> Vec<u8> {
    FlowFile::builder()
        .attribute("filename", "records.txt")
        .attribute("source", "example")
        .content(&b"alpha\nbeta\ngamma"[..])
        .to_bytes()
}
