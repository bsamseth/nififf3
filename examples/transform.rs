//! One flow file in, one flow file out — synchronously.
//!
//! The everyday case: receive a flow file, rewrite its content, and pass it on
//! with the attributes intact.
//!
//!     cargo run --example transform

use std::io::Cursor;

use nififf3::FlowFile;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input = incoming();

    // `parse` consumes only the header; the content is left as a reader
    // limited to the declared size, so a large flow file need not be in memory.
    let flow_file = FlowFile::parse(Cursor::new(&input))?;
    println!(
        "in:  {} bytes, filename={}",
        flow_file.size(),
        flow_file.attributes()["filename"],
    );

    // Here we do want the content, so read it in and check it was complete.
    let flow_file = flow_file.into_memory()?;

    // `derive` carries the attributes over and mints a fresh `uuid`, since in
    // NiFi that attribute identifies one flow file.
    let shouted = flow_file
        .derive()
        .attribute("transformed", "uppercase")
        .content(flow_file.content().to_ascii_uppercase());

    let mut out = Vec::new();
    shouted.into_reader().write_to(&mut out)?;

    let parsed = FlowFile::from_bytes(&out)?;
    println!(
        "out: {} bytes, filename={}, transformed={}",
        parsed.size(),
        parsed.attributes()["filename"],
        parsed.attributes()["transformed"],
    );
    println!("     {}", String::from_utf8_lossy(parsed.content()));

    assert_eq!(parsed.content().as_slice(), b"HELLO, NIFI!");
    assert_eq!(parsed.attributes()["source"], "example", "inherited");
    assert_ne!(
        parsed.attributes()["uuid"],
        flow_file.attributes()["uuid"],
        "a derived flow file is a new one"
    );
    Ok(())
}

/// A flow file as it would arrive from NiFi.
fn incoming() -> Vec<u8> {
    FlowFile::builder()
        .attribute("filename", "greeting.txt")
        .attribute("source", "example")
        .attribute("uuid", "11111111-1111-1111-1111-111111111111")
        .content(&b"Hello, NiFi!"[..])
        .to_bytes()
}
