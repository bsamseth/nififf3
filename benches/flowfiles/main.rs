//! Benchmarks for the parsing and serialization paths.
//!
//! Run everything with `cargo bench`, or one group with
//! `cargo bench -- read/` and friends. The inputs come from [`corpus`], which
//! generates them from a fixed seed and caches them on disk, so numbers are
//! comparable between runs on the same machine.
//!
//! Each group is measured against three shapes of input — see
//! [`corpus::CORPORA`] — because they stress different things: per-flow-file
//! overhead, header work, and moving content. A change that helps one can
//! easily do nothing for the others, which is exactly what these are here to
//! show.
//!
//! Throughput is reported over the *serialized* size of the input in every
//! case, including the write groups, so a given corpus's numbers can be
//! compared across groups.

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
    /// The same flow files, each serialized on its own — what the
    /// single-flow-file entry points take.
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
            let each = parsed.iter().map(FlowFile::to_bytes).collect();
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
    let mut group = c.benchmark_group("read/stream_to_sink");
    for input in &inputs {
        group.throughput(bytes(input.stream.len()));
        group.bench_function(input.name, |b| {
            b.iter(|| {
                let mut reader = FlowFilesReader::new(input.stream.as_slice());
                while let Some(mut flow_file) = reader.next().expect("the corpus parses") {
                    io::copy(flow_file.content_mut(), &mut io::sink()).expect("sink cannot fail");
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
    let mut group = c.benchmark_group("write/to_sink");
    for input in &inputs {
        group.throughput(bytes(input.stream.len()));
        group.bench_function(input.name, |b| {
            b.iter(|| {
                let mut sink = io::sink();
                for flow_file in &input.parsed {
                    flow_file
                        .write_bytes_to(&mut sink)
                        .expect("sink cannot fail");
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
                FlowFile::from_parts(0, flow_file.attributes().clone(), Vec::new()).to_bytes()
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
    let one = FlowFile::builder()
        .attribute("filename", "big.dat")
        .content(vec![0x5a; 4 << 20])
        .to_bytes();

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
        b.iter(|| {
            let flow_file = FlowFile::parse(one.as_slice()).expect("parses");
            flow_file.write_to(&mut io::sink()).expect("sink cannot fail");
        });
    });
    group.bench_function("write_bytes_to", |b| {
        let parsed = FlowFile::from_bytes(&one).expect("parses");
        b.iter(|| {
            parsed
                .write_bytes_to(&mut io::sink())
                .expect("sink cannot fail");
        });
    });
    group.finish();
}

/// What the crate costs on top of moving the same bytes with `std` alone.
///
/// Not a target to beat — the parser has real work to do that these do not —
/// but a scale for the numbers above: if buffering a stream of flow files is
/// far off `read_to_end` over the same bytes, the gap is the crate's, and
/// worth knowing the size of.
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
