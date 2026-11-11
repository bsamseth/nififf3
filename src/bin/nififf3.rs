use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use nififf3::{FlowFile, FlowFilesReader};

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
    /// Print size and attributes as JSON, without decoding the content.
    Attrs {
        /// Input file; `-` or omitted reads from stdin.
        path: Option<PathBuf>,
    },
    /// Write the raw content of flow files to stdout.
    Content {
        /// Input file; `-` or omitted reads from stdin.
        path: Option<PathBuf>,
    },
    /// Create a flow file on stdout. The content is read from stdin.
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
    let cli = Cli::parse();
    match cli.command {
        Command::ToJson { path } => to_json(open_input(path.as_deref())?),
        Command::FromJson { path } => from_json(open_input(path.as_deref())?),
        Command::Attrs { path } => attrs(open_input(path.as_deref())?),
        Command::Content { path } => content(open_input(path.as_deref())?),
        Command::Create { attributes } => create(&attributes),
    }
}

/// Open the input for streaming. Flow files are processed one at a time,
/// rather than by reading the whole input up front.
fn open_input(path: Option<&Path>) -> io::Result<Box<dyn BufRead>> {
    match path {
        Some(path) if path != Path::new("-") => Ok(Box::new(BufReader::new(File::open(path)?))),
        _ => Ok(Box::new(io::stdin().lock())),
    }
}

fn to_json(reader: Box<dyn BufRead>) -> Result<()> {
    let mut stdout = io::stdout().lock();
    let mut flow_files = FlowFilesReader::new(reader);
    while let Some(flow_file) = flow_files.next()? {
        serde_json::to_writer(&mut stdout, &flow_file.into_memory()?)?;
        writeln!(stdout)?;
    }
    Ok(())
}

fn from_json(reader: Box<dyn BufRead>) -> Result<()> {
    let mut stdout = io::stdout().lock();
    for flow_file in serde_json::Deserializer::from_reader(reader).into_iter::<FlowFile<Vec<u8>>>()
    {
        stdout.write_all(&flow_file?.to_bytes())?;
    }
    Ok(())
}

fn attrs(reader: Box<dyn BufRead>) -> Result<()> {
    let mut stdout = io::stdout().lock();
    let mut flow_files = FlowFilesReader::new(reader);
    // The content is never read; the reader skips it on the way to the next
    // flow file, and reports a stream that ends inside it.
    while let Some(flow_file) = flow_files.next()? {
        let attributes: BTreeMap<_, _> = flow_file.attributes().iter().collect();
        let line = serde_json::json!({
            "size": flow_file.size(),
            "attributes": attributes,
        });
        serde_json::to_writer(&mut stdout, &line)?;
        writeln!(stdout)?;
    }
    Ok(())
}

fn content(reader: Box<dyn BufRead>) -> Result<()> {
    let mut stdout = io::stdout().lock();
    let mut flow_files = FlowFilesReader::new(reader);
    while let Some(mut flow_file) = flow_files.next()? {
        // The content reports a stream that ends early as an error, so a
        // plain copy is a verified copy.
        io::copy(flow_file.content_mut(), &mut stdout)?;
    }
    Ok(())
}

fn create(attribute_args: &[String]) -> Result<()> {
    let mut attributes = HashMap::new();
    for arg in attribute_args {
        let (key, value) = arg
            .split_once('=')
            .ok_or_else(|| format!("invalid attribute {arg:?}: expected KEY=VALUE"))?;
        attributes.insert(key.to_string(), value.to_string());
    }
    let mut content = Vec::new();
    io::stdin().lock().read_to_end(&mut content)?;
    let flow_file = FlowFile::builder().attributes(attributes).content(content);
    io::stdout().lock().write_all(&flow_file.to_bytes())?;
    Ok(())
}
