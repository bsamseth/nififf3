use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};
use nififf3::{Error, FlowFile, Limits};

#[derive(Parser)]
#[command(name = "nififf3", version, about = "Work with NiFi FlowFile V3 files")]
struct Cli {
    #[command(subcommand)]
    command: Command,

    #[command(flatten)]
    limits: LimitArgs,
}

/// Caps on a flow file's attributes and declared content size. Unset means no
/// cap, matching NiFi's own unpackager; set them when handling input you do not
/// trust.
///
/// Every subcommand honours them, including the two that never run a header
/// parser: `from-json` checks each decoded flow file, and `create` checks the
/// one it built.
#[derive(Args)]
#[expect(
    clippy::struct_field_names,
    reason = "clap derives the `--max-*` flag names from these fields"
)]
struct LimitArgs {
    /// Reject headers declaring more than this many attributes.
    #[arg(long, global = true, value_name = "N")]
    max_attributes: Option<usize>,

    /// Reject attribute keys or values longer than this, in bytes.
    #[arg(long, global = true, value_name = "BYTES")]
    max_attribute_len: Option<usize>,

    /// Reject headers whose attribute keys and values total more than this,
    /// in bytes.
    #[arg(long, global = true, value_name = "BYTES")]
    max_total_attribute_len: Option<usize>,

    /// Reject headers declaring more content than this, in bytes.
    #[arg(long, global = true, value_name = "BYTES")]
    max_content_len: Option<u64>,
}

impl From<LimitArgs> for Limits {
    fn from(args: LimitArgs) -> Self {
        let mut limits = Limits::UNLIMITED;
        if let Some(n) = args.max_attributes {
            limits = limits.with_max_attributes(n);
        }
        if let Some(n) = args.max_attribute_len {
            limits = limits.with_max_attribute_len(n);
        }
        if let Some(n) = args.max_total_attribute_len {
            limits = limits.with_max_total_attribute_len(n);
        }
        if let Some(n) = args.max_content_len {
            limits = limits.with_max_content_len(n);
        }
        limits
    }
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
    let cli = Cli::parse();
    let limits = Limits::from(cli.limits);
    match cli.command {
        Command::ToJson { path } => to_json(open_input(path.as_deref())?, limits),
        Command::FromJson { path } => from_json(open_input(path.as_deref())?, limits),
        Command::Attrs { path } => attrs(open_input(path.as_deref())?, limits),
        Command::Content { path } => content(open_input(path.as_deref())?, limits),
        Command::Create { attributes } => create(&attributes, limits),
    }
}

/// Open the input for streaming; flow files are processed one at a time
/// rather than reading the whole input up front.
fn open_input(path: Option<&Path>) -> io::Result<Box<dyn BufRead>> {
    match path {
        Some(path) if path != Path::new("-") => Ok(Box::new(BufReader::new(File::open(path)?))),
        _ => Ok(Box::new(io::stdin().lock())),
    }
}

fn to_json(mut reader: Box<dyn BufRead>, limits: Limits) -> Result<()> {
    let mut stdout = io::stdout().lock();
    while let Some(flow_file) = FlowFile::parse_next_with_limits(&mut reader, limits)? {
        serde_json::to_writer(&mut stdout, &flow_file.into_memory()?)?;
        writeln!(stdout)?;
    }
    Ok(())
}

fn from_json(reader: Box<dyn BufRead>, limits: Limits) -> Result<()> {
    let mut stdout = io::stdout().lock();
    for flow_file in serde_json::Deserializer::from_reader(reader).into_iter::<FlowFile<Vec<u8>>>()
    {
        // The JSON path never goes through a parser, so the limits have to be
        // applied to the decoded flow file instead — otherwise `--max-*` would
        // mean something on one input format and nothing on the other.
        let flow_file = flow_file?;
        limits.check(flow_file.attributes(), flow_file.size())?;
        stdout.write_all(&flow_file.to_bytes())?;
    }
    Ok(())
}

fn attrs(mut reader: Box<dyn BufRead>, limits: Limits) -> Result<()> {
    let mut stdout = io::stdout().lock();
    while let Some(flow_file) = FlowFile::parse_next_with_limits(&mut reader, limits)? {
        let attributes: BTreeMap<_, _> = flow_file.attributes().iter().collect();
        let line = serde_json::json!({
            "size": flow_file.size(),
            "attributes": attributes,
        });
        serde_json::to_writer(&mut stdout, &line)?;
        writeln!(stdout)?;
        copy_content(flow_file, &mut io::sink())?;
    }
    Ok(())
}

fn content(mut reader: Box<dyn BufRead>, limits: Limits) -> Result<()> {
    let mut stdout = io::stdout().lock();
    while let Some(flow_file) = FlowFile::parse_next_with_limits(&mut reader, limits)? {
        copy_content(flow_file, &mut stdout)?;
    }
    Ok(())
}

/// Stream the content to `writer`, verifying that it was complete.
fn copy_content<R: Read, W: Write>(flow_file: FlowFile<R>, writer: &mut W) -> Result<()> {
    let expected = flow_file.size();
    let copied = io::copy(&mut flow_file.into_content(), writer)?;
    if copied != expected {
        return Err(Error::SizeMismatch {
            expected,
            actual: copied,
        }
        .into());
    }
    Ok(())
}

fn create(attribute_args: &[String], limits: Limits) -> Result<()> {
    let mut builder = FlowFile::builder();
    for arg in attribute_args {
        let (key, value) = arg
            .split_once('=')
            .ok_or_else(|| format!("invalid attribute {arg:?}: expected KEY=VALUE"))?;
        builder = builder.attribute(key, value);
    }
    let flow_file = builder.buffered(io::stdin().lock())?;
    // A flow file this command wrote should be one it would also accept.
    limits.check(flow_file.attributes(), flow_file.size())?;
    io::stdout().lock().write_all(&flow_file.to_bytes())?;
    Ok(())
}
