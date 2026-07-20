use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

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
        serde_json::to_writer(&mut stdout, &flow_file.into_bytes()?)?;
        writeln!(stdout)?;
    }
    Ok(())
}

fn from_json(input: &[u8]) -> Result<()> {
    let mut stdout = io::stdout().lock();
    for flow_file in serde_json::Deserializer::from_slice(input).into_iter::<FlowFile<Vec<u8>>>() {
        stdout.write_all(&flow_file?.to_bytes())?;
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
