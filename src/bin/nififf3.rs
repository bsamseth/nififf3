use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use clap::{Parser, Subcommand};
use nififf3::FlowFile;

#[derive(Parser)]
#[command(name = "nififf3", version, about = "Work with NiFi FlowFile V3 files")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Convert flow files to JSON, one object per line per flow file.
    ///
    /// Each object has the fields `size`, `attributes` and `content`, with
    /// the content base64 encoded.
    ToJson {
        /// Input file; `-` or omitted reads from stdin.
        path: Option<PathBuf>,
    },
    /// Convert JSON (as produced by `to-json`) into flow files on stdout.
    FromJson {
        /// Input file; `-` or omitted reads from stdin.
        path: Option<PathBuf>,
    },
    /// Create a flow file on stdout; the content is read from stdin.
    Create {
        /// Attributes as key=value pairs.
        #[arg(value_name = "KEY=VALUE")]
        attributes: Vec<String>,
    },
}

#[derive(serde::Serialize, serde::Deserialize)]
struct JsonFlowFile {
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<u64>,
    #[serde(default)]
    attributes: BTreeMap<String, String>,
    content: String,
}

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn main() -> ExitCode {
    if let Err(err) = run() {
        eprintln!("error: {err}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

fn run() -> Result<()> {
    match Cli::parse().command {
        Command::ToJson { path } => to_json(&read_input(path.as_deref())?),
        Command::FromJson { path } => from_json(&read_input(path.as_deref())?),
        Command::Create { attributes } => create(&attributes),
    }
}

fn read_input(path: Option<&Path>) -> io::Result<Vec<u8>> {
    match path {
        Some(path) if path != Path::new("-") => std::fs::read(path),
        _ => {
            let mut buf = Vec::new();
            io::stdin().lock().read_to_end(&mut buf)?;
            Ok(buf)
        }
    }
}

fn to_json(mut input: &[u8]) -> Result<()> {
    let mut stdout = io::stdout().lock();
    while let Some(flow_file) = FlowFile::parse_next(&mut input)? {
        let (size, attributes, content) = flow_file.into_bytes()?.into_parts();
        let json = JsonFlowFile {
            size: Some(size),
            attributes: attributes.into_iter().collect(),
            content: BASE64.encode(content),
        };
        serde_json::to_writer(&mut stdout, &json)?;
        writeln!(stdout)?;
    }
    Ok(())
}

fn from_json(input: &[u8]) -> Result<()> {
    let mut stdout = io::stdout().lock();
    for item in serde_json::Deserializer::from_slice(input).into_iter::<JsonFlowFile>() {
        let item = item?;
        let content = BASE64.decode(item.content.as_bytes())?;
        if let Some(size) = item.size
            && size != content.len() as u64
        {
            return Err(format!(
                "size field ({size}) does not match decoded content length ({})",
                content.len()
            )
            .into());
        }
        let flow_file = FlowFile::builder()
            .attributes(item.attributes)
            .content(content);
        stdout.write_all(&flow_file.to_bytes())?;
    }
    Ok(())
}

fn create(attribute_args: &[String]) -> Result<()> {
    let mut builder = FlowFile::builder();
    for arg in attribute_args {
        let (key, value) = arg
            .split_once('=')
            .ok_or_else(|| format!("invalid attribute {arg:?}: expected KEY=VALUE"))?;
        builder = builder.attribute(key, value);
    }
    let mut content = Vec::new();
    io::stdin().lock().read_to_end(&mut content)?;
    io::stdout()
        .lock()
        .write_all(&builder.content(content).to_bytes())?;
    Ok(())
}
