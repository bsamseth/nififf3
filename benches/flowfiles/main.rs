//! Benchmarks for the parsing and serialization paths.
//!
//! Run everything with `cargo bench`, or one group with
//! `cargo bench -- read/` and friends. The inputs come from [`corpus`], which
//! generates them from a fixed seed and caches them on disk, so numbers are
//! comparable between runs on the same machine.
//!
//! Each group is measured against three shapes of input, listed in
//! [`corpus::CORPORA`], because they stress different things: per-flow-file
//! overhead, header work, and moving content. A change that helps one can
//! easily do nothing for the others, and showing that is what these are for.
//!
//! Throughput is reported over the serialized size of the input in every case,
//! including the write groups, so a given corpus's numbers can be compared
//! across groups.
//!
//! # Reading the numbers
//!
//! Destinations are [`Consuming`] rather than `io::sink`. See there for why a
//! sink turns a real 9% win into a reported 99.9% one.
//!
//! `few_large` moves 32 MiB per iteration and is dominated by page faults and
//! the allocator, so it swings by tens of percent between runs on a machine
//! that is doing anything else. `many_small` and `wide_attrs` reproduce to
//! within a few points. Take a `few_large` result seriously only when it holds
//! across runs, and check `uptime` before believing any of them.

mod corpus;

use std::hint::black_box;
use std::io::{self, Read, Write};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use nififf3::{FlowFile, FlowFiles, FlowFilesReader, FlowFilesWriter};

/// One corpus, in the forms the benchmarks want it.
struct Input {
    name: &'static str,
    /// The whole corpus as one stream of concatenated flow files.
    stream: Vec<u8>,
    /// The same flow files, parsed.
    parsed: Vec<FlowFile<Vec<u8>>>,
    /// The same flow files, each serialized on its own, which is what the
    /// single-flow-file entry points take. Shrunk to an exact fit, so that the
    /// capacity of the input is not itself something a change can alter: how
    /// `to_bytes` sizes its buffer is under measurement elsewhere, and must not
    /// leak into what `from_bytes` and `from_vec` are handed.
    each: Vec<Vec<u8>>,
}

fn inputs() -> Vec<Input> {
    corpus::CORPORA
        .iter()
        .map(|shape| {
            let stream = corpus::load(shape);
            let parsed: Vec<_> = FlowFiles::new(stream.as_slice())
                .collect::<Result<_, _>>()
                .expect("the generated corpus parses");
            let each = parsed.iter().map(serialized_exactly).collect();
            Input {
                name: shape.name,
                stream,
                parsed,
                each,
            }
        })
        .collect()
}

/// Bytes, for a throughput figure. Corpora are far below `u64::MAX`.
fn bytes(len: usize) -> Throughput {
    Throughput::Bytes(len as u64)
}

/// Serialize a flow file into a buffer of exactly its own length.
///
/// Every input built by `to_bytes` has to go through this, because how
/// `to_bytes` sizes its buffer is itself under measurement: left alone, a
/// change to it hands the parser inputs with different capacity, and for a
/// corpus of twenty thousand small buffers the difference in how much address
/// space they span moves the parse figure by twenty points. That is the
/// allocator answering a question nobody asked.
fn serialized_exactly(flow_file: &FlowFile<Vec<u8>>) -> Vec<u8> {
    let mut bytes = flow_file.to_bytes();
    bytes.shrink_to_fit();
    bytes
}

/// A destination that costs what a destination costs: every byte handed to it
/// is copied exactly once, and nothing is kept.
///
/// `io::sink` will not do. It discards the bytes rather than copying them, so
/// a change that removes a copy the crate was making for itself shows up
/// against it as removing essentially all the work. That is true of the crate
/// and false of the program, because the copy into a real destination is still
/// to come. It turns a genuine 9% end-to-end win into a reported 99.9%.
///
/// This keeps a single scratch buffer and reuses it, so after the first write
/// there is no allocation and no growth. What remains is the crate's work plus
/// one `memcpy`, and a real writer cannot do less than that.
#[derive(Default)]
struct Consuming(Vec<u8>);

impl Write for Consuming {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.clear();
        self.0.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Reading a whole stream, by each of the three ways the crate offers.
fn read(c: &mut Criterion) {
    let inputs = inputs();

    let mut group = c.benchmark_group("read/stream_buffered");
    for input in &inputs {
        group.throughput(bytes(input.stream.len()));
        group.bench_function(input.name, |b| {
            b.iter(|| {
                for flow_file in FlowFiles::new(input.stream.as_slice()) {
                    black_box(flow_file.expect("the corpus parses"));
                }
            });
        });
    }
    group.finish();

    // The attributes-only case: headers are parsed, content is skipped rather
    // than buffered, so this is the floor for walking a stream.
    let mut group = c.benchmark_group("read/stream_headers_only");
    for input in &inputs {
        group.throughput(bytes(input.stream.len()));
        group.bench_function(input.name, |b| {
            b.iter(|| {
                let mut reader = FlowFilesReader::new(input.stream.as_slice());
                while let Some(flow_file) = reader.next().expect("the corpus parses") {
                    black_box(flow_file.attributes().len());
                }
            });
        });
    }
    group.finish();

    // Streaming the content out as it arrives, which is what the streaming
    // reader is actually for.
    let mut group = c.benchmark_group("read/stream_out");
    for input in &inputs {
        group.throughput(bytes(input.stream.len()));
        group.bench_function(input.name, |b| {
            let mut out = Consuming::default();
            b.iter(|| {
                let mut reader = FlowFilesReader::new(input.stream.as_slice());
                while let Some(mut flow_file) = reader.next().expect("the corpus parses") {
                    io::copy(flow_file.content_mut(), &mut out).expect("cannot fail");
                }
            });
        });
    }
    group.finish();
}

/// The single-flow-file entry points, which differ in who owns the bytes.
fn read_one(c: &mut Criterion) {
    let inputs = inputs();

    let mut group = c.benchmark_group("read/one_from_bytes");
    for input in &inputs {
        group.throughput(bytes(input.stream.len()));
        group.bench_function(input.name, |b| {
            b.iter(|| {
                for one in &input.each {
                    black_box(FlowFile::from_bytes(one).expect("the corpus parses"));
                }
            });
        });
    }
    group.finish();

    // The owning form: the header is parsed and the content left in the
    // allocation it arrived in, so this pays a clone of the input instead of
    // a copy of the content.
    let mut group = c.benchmark_group("read/one_from_vec");
    for input in &inputs {
        group.throughput(bytes(input.stream.len()));
        group.bench_function(input.name, |b| {
            b.iter(|| {
                for one in &input.each {
                    black_box(FlowFile::from_vec(one.clone()).expect("the corpus parses"));
                }
            });
        });
    }
    group.finish();
}

/// Serializing, by each of the ways the crate offers.
fn write(c: &mut Criterion) {
    let inputs = inputs();

    let mut group = c.benchmark_group("write/to_bytes");
    for input in &inputs {
        group.throughput(bytes(input.stream.len()));
        group.bench_function(input.name, |b| {
            b.iter(|| {
                for flow_file in &input.parsed {
                    black_box(flow_file.to_bytes());
                }
            });
        });
    }
    group.finish();

    // Straight to a writer, which need not build the serialized form at all.
    // The destination copies but does not grow (see `Consuming`), so this is
    // the crate's work plus the one copy no writer can avoid.
    let mut group = c.benchmark_group("write/to_writer");
    for input in &inputs {
        group.throughput(bytes(input.stream.len()));
        group.bench_function(input.name, |b| {
            let mut out = Consuming::default();
            b.iter(|| {
                for flow_file in &input.parsed {
                    flow_file.write_bytes_to(&mut out).expect("cannot fail");
                }
            });
        });
    }
    group.finish();

    // The same, into a `Vec` that has to grow: what a caller building a
    // response body in memory actually pays.
    let mut group = c.benchmark_group("write/stream_to_vec");
    for input in &inputs {
        group.throughput(bytes(input.stream.len()));
        group.bench_function(input.name, |b| {
            b.iter(|| {
                let mut writer = FlowFilesWriter::new(Vec::with_capacity(input.stream.len()));
                for flow_file in &input.parsed {
                    writer.write_bytes(flow_file).expect("a Vec cannot fail");
                }
                black_box(writer.finish().expect("a Vec cannot fail"));
            });
        });
    }
    group.finish();
}

/// The header on its own, away from any content: the part every one of the
/// paths above shares.
fn header(c: &mut Criterion) {
    let inputs = inputs();

    // Serializing a header means sorting the attributes and encoding them.
    let mut group = c.benchmark_group("header/encode");
    for input in &inputs {
        let headers: Vec<_> = input
            .parsed
            .iter()
            .map(|flow_file| {
                FlowFile::from_parts(0, flow_file.attributes().clone(), Vec::new())
            })
            .collect();
        let total: usize = headers.iter().map(|h| h.to_bytes().len()).sum();
        group.throughput(bytes(total));
        group.bench_function(input.name, |b| {
            b.iter(|| {
                for flow_file in &headers {
                    black_box(flow_file.to_bytes());
                }
            });
        });
    }
    group.finish();

    // And parsing one: field lengths, UTF-8 decoding, and the map.
    let mut group = c.benchmark_group("header/parse");
    for input in &inputs {
        let headers: Vec<Vec<u8>> = input
            .parsed
            .iter()
            .map(|flow_file| {
                serialized_exactly(&FlowFile::from_parts(
                    0,
                    flow_file.attributes().clone(),
                    Vec::new(),
                ))
            })
            .collect();
        let total: usize = headers.iter().map(Vec::len).sum();
        group.throughput(bytes(total));
        group.bench_function(input.name, |b| {
            b.iter(|| {
                for one in &headers {
                    black_box(FlowFile::from_bytes(one).expect("a header parses"));
                }
            });
        });
    }
    group.finish();
}

/// Content movement with the header out of the way: one 4 MiB flow file
/// through each of the reader-based operations.
fn content(c: &mut Criterion) {
    let one = serialized_exactly(
        &FlowFile::builder()
            .attribute("filename", "big.dat")
            .content(vec![0x5a; 4 << 20]),
    );

    let mut group = c.benchmark_group("content/4MiB");
    group.throughput(bytes(one.len()));
    group.sample_size(30);

    group.bench_function("into_memory", |b| {
        b.iter(|| {
            let flow_file = FlowFile::parse(one.as_slice()).expect("parses");
            black_box(flow_file.into_memory().expect("complete"));
        });
    });
    group.bench_function("skip_content", |b| {
        b.iter(|| {
            let flow_file = FlowFile::parse(one.as_slice()).expect("parses");
            black_box(flow_file.skip_content().expect("complete"));
        });
    });
    group.bench_function("write_to", |b| {
        let mut out = Consuming::default();
        b.iter(|| {
            let flow_file = FlowFile::parse(one.as_slice()).expect("parses");
            flow_file.write_to(&mut out).expect("cannot fail");
        });
    });
    group.bench_function("write_bytes_to", |b| {
        let parsed = FlowFile::from_bytes(&one).expect("parses");
        let mut out = Consuming::default();
        b.iter(|| {
            parsed.write_bytes_to(&mut out).expect("cannot fail");
        });
    });
    group.finish();
}

/// What the crate costs on top of moving the same bytes with `std` alone.
///
/// This is a scale for the numbers above rather than a target to beat, because
/// the parser has real work to do that these do not. If buffering a stream of
/// flow files is far off `read_to_end` over the same bytes, the gap is the
/// crate's, and worth knowing the size of.
fn baseline(c: &mut Criterion) {
    let inputs = inputs();

    let mut group = c.benchmark_group("baseline/std");
    for input in &inputs {
        group.throughput(bytes(input.stream.len()));
        group.bench_function(format!("{}/read_to_end", input.name), |b| {
            b.iter(|| {
                let mut buf = Vec::new();
                input
                    .stream
                    .as_slice()
                    .read_to_end(&mut buf)
                    .expect("a slice cannot fail");
                black_box(buf);
            });
        });
        group.bench_function(format!("{}/write_all", input.name), |b| {
            b.iter(|| {
                let mut out = Vec::with_capacity(input.stream.len());
                out.write_all(&input.stream).expect("a Vec cannot fail");
                black_box(out);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, read, read_one, write, header, content, baseline);
criterion_main!(benches);
