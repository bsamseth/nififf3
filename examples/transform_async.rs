//! One flow file in, one flow file out — asynchronously.
//!
//! The `tokio` mirror of `transform.rs`: the same API with `_async` spellings,
//! reading and writing over `AsyncRead`/`AsyncWrite`.
//!
//!     cargo run --features tokio --example transform_async

use nififf3::FlowFile;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input = incoming();

    // Only the header is read here; the content stays a size-limited
    // `AsyncRead`, so this works the same against a socket or a file.
    let flow_file = FlowFile::parse_async(input.as_slice()).await?;
    println!(
        "in:  {} bytes, filename={}",
        flow_file.size(),
        flow_file.attributes()["filename"],
    );

    let flow_file = flow_file.into_memory_async().await?;

    let shouted = flow_file
        .derive()
        .attribute("transformed", "uppercase")
        .content(flow_file.content().to_ascii_uppercase());

    let mut out = Vec::new();
    shouted.into_reader().write_to_async(&mut out).await?;

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
    Ok(())
}

fn incoming() -> Vec<u8> {
    FlowFile::builder()
        .attribute("filename", "greeting.txt")
        .attribute("source", "example")
        .attribute("uuid", "11111111-1111-1111-1111-111111111111")
        .content(&b"Hello, NiFi!"[..])
        .to_bytes()
}
