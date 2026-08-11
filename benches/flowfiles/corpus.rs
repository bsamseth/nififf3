//! The inputs the benchmarks run against: generated from a fixed seed, cached
//! on disk, and never checked in.
//!
//! Benchmarks want inputs big enough to measure and identical between runs,
//! which rules out both committing them and building them fresh each time. So
//! they are generated from a fixed-seed generator — the same one
//! `tests/robustness.rs` uses — into `benches/corpus/`, which is gitignored,
//! and reused on every later run.
//!
//! Cache validity is keyed on [`VERSION`] through the file name. Changing a
//! generator means bumping it, so stale files stop being found rather than
//! being reused under a shape they no longer have.

use std::fs;
use std::path::PathBuf;

use nififf3::{FlowFile, FlowFilesWriter};

/// Bump when any generator below changes shape.
const VERSION: u32 = 1;

/// A small linear congruential generator: not good randomness, but the same
/// sequence every run, which is what a benchmark needs.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 ^ (self.0 >> 33)
    }

    /// A value in `low..=high`.
    pub fn between(&mut self, low: usize, high: usize) -> usize {
        let span = (high - low + 1) as u64;
        low + usize::try_from(self.next_u64() % span).expect("a value below a usize fits")
    }

    pub fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| (self.next_u64() & 0xff) as u8).collect()
    }

    /// A short string over an alphabet with multi-byte characters in it, so
    /// the byte lengths the format writes are not the character counts.
    pub fn text(&mut self, len: usize) -> String {
        const ALPHABET: [&str; 8] = ["a", "Z", "0", ".", "_", "é", "→", "🙂"];
        (0..len)
            .map(|_| ALPHABET[self.between(0, ALPHABET.len() - 1)])
            .collect()
    }
}

/// One shape of input, and how to build it.
pub struct Corpus {
    /// Short name, used in benchmark ids and in the cache file name.
    pub name: &'static str,
    /// What the shape is for, in one line.
    pub about: &'static str,
    build: fn() -> Vec<u8>,
}

/// The shapes worth measuring, chosen so each puts a different part of the
/// crate on the critical path.
pub const CORPORA: &[Corpus] = &[
    Corpus {
        name: "many_small",
        about: "20k flow files of ~256 B: per-flow-file overhead dominates",
        build: many_small,
    },
    Corpus {
        name: "wide_attrs",
        about: "200 flow files of 400 attributes: header work dominates",
        build: wide_attrs,
    },
    Corpus {
        name: "few_large",
        about: "8 flow files of 4 MiB: content movement dominates",
        build: few_large,
    },
];

/// The bytes for one shape, generating and caching them on first use.
///
/// # Panics
///
/// If the cache file cannot be read or written. A benchmark with no input has
/// nothing to say, so there is nothing useful to do but stop.
pub fn load(corpus: &Corpus) -> Vec<u8> {
    let path = cache_path(corpus.name);
    if let Ok(bytes) = fs::read(&path) {
        return bytes;
    }
    let bytes = (corpus.build)();
    let dir = path.parent().expect("the cache path has a parent");
    fs::create_dir_all(dir).expect("create the corpus cache directory");
    fs::write(&path, &bytes).expect("write the corpus cache file");
    // Only on the run that generates it, so it says what the numbers about to
    // appear were measured over without repeating itself on every later run.
    eprintln!(
        "generated {} ({} KiB) — {}",
        corpus.name,
        bytes.len() / 1024,
        corpus.about,
    );
    bytes
}

fn cache_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("benches/corpus")
        .join(format!("{name}.v{VERSION}.ff"))
}

/// Attributes in the shape NiFi actually sends: a handful of well-known keys
/// with short values.
fn typical_attributes(rng: &mut Rng, index: usize) -> Vec<(String, String)> {
    let uuid = format!("{:016x}-{:016x}", rng.next_u64(), rng.next_u64());
    let records = rng.between(1, 9999).to_string();
    let source_len = rng.between(4, 16);
    vec![
        ("filename".to_string(), format!("record-{index}.dat")),
        ("path".to_string(), "./ingest".to_string()),
        ("uuid".to_string(), uuid),
        (
            "mime.type".to_string(),
            "application/octet-stream".to_string(),
        ),
        ("record.count".to_string(), records),
        ("source".to_string(), rng.text(source_len)),
    ]
}

fn write_all(flow_files: impl IntoIterator<Item = FlowFile<Vec<u8>>>) -> Vec<u8> {
    let mut writer = FlowFilesWriter::new(Vec::new());
    for flow_file in flow_files {
        writer
            .write_bytes(&flow_file)
            .expect("writing to a Vec cannot fail");
    }
    writer.finish().expect("flushing a Vec cannot fail")
}

fn many_small() -> Vec<u8> {
    let mut rng = Rng::new(0x0511_0001);
    write_all((0..20_000).map(|i| {
        let attributes = typical_attributes(&mut rng, i);
        let content_len = rng.between(64, 512);
        FlowFile::builder()
            .attributes(attributes)
            .content(rng.bytes(content_len))
    }))
}

fn wide_attrs() -> Vec<u8> {
    let mut rng = Rng::new(0x3ade_0002);
    write_all((0..200).map(|i| {
        let mut attributes = typical_attributes(&mut rng, i);
        attributes.extend((0..400).map(|k| {
            let key_len = rng.between(2, 6);
            let key = format!("attr.{k}.{}", rng.text(key_len));
            let value_len = rng.between(8, 64);
            (key, rng.text(value_len))
        }));
        FlowFile::builder()
            .attributes(attributes)
            .content(rng.bytes(128))
    }))
}

fn few_large() -> Vec<u8> {
    let mut rng = Rng::new(0x1a4_6e0003);
    write_all((0..8).map(|i| {
        let attributes = typical_attributes(&mut rng, i);
        FlowFile::builder()
            .attributes(attributes)
            .content(rng.bytes(4 << 20))
    }))
}
