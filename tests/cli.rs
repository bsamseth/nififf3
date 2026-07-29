#![cfg(feature = "cli")]

use assert_cmd::Command;
use nififf3::FlowFile;

fn sample_bytes() -> Vec<u8> {
    FlowFile::builder()
        .attribute("filename", "greeting.txt")
        .content(&b"hello"[..])
        .to_bytes()
}

fn nififf3() -> Command {
    Command::cargo_bin("nififf3").unwrap()
}

#[test]
fn to_json_from_json_roundtrip() {
    let flow_file = sample_bytes();

    let json = nififf3()
        .arg("to-json")
        .write_stdin(flow_file.clone())
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let line: serde_json::Value = serde_json::from_slice(&json).unwrap();
    assert_eq!(line["size"], 5);
    assert_eq!(line["attributes"]["filename"], "greeting.txt");
    assert_eq!(line["content"], "aGVsbG8=");

    let back = nififf3()
        .arg("from-json")
        .write_stdin(json)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    assert_eq!(back, flow_file);
}

#[test]
fn to_json_handles_concatenated_flow_files() {
    let mut input = sample_bytes();
    input.extend_from_slice(&sample_bytes());

    let stdout = nififf3()
        .arg("to-json")
        .write_stdin(input)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let lines: Vec<_> = stdout
        .split(|&b| b == b'\n')
        .filter(|l| !l.is_empty())
        .collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0], lines[1]);
}

#[test]
fn to_json_reads_from_file_argument() {
    let dir = std::env::temp_dir().join("nififf3-cli-test");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("sample.ff3");
    std::fs::write(&path, sample_bytes()).unwrap();

    nififf3()
        .arg("to-json")
        .arg(&path)
        .assert()
        .success()
        .stdout(predicates::str::contains("greeting.txt"));
}

#[test]
fn create_builds_a_flow_file() {
    let stdout = nififf3()
        .args(["create", "k=v", "other=thing"])
        .write_stdin("hello")
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let flow_file = FlowFile::from_bytes(&stdout).unwrap();
    assert_eq!(flow_file.attributes()["k"], "v");
    assert_eq!(flow_file.attributes()["other"], "thing");
    assert_eq!(flow_file.content().as_slice(), b"hello");
}

#[test]
fn attrs_prints_metadata_without_content() {
    let mut input = sample_bytes();
    input.extend_from_slice(&sample_bytes());

    let stdout = nififf3()
        .arg("attrs")
        .write_stdin(input)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let lines: Vec<serde_json::Value> = stdout
        .split(|&b| b == b'\n')
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_slice(l).unwrap())
        .collect();
    assert_eq!(lines.len(), 2);
    for line in lines {
        assert_eq!(line["size"], 5);
        assert_eq!(line["attributes"]["filename"], "greeting.txt");
        assert!(line.get("content").is_none());
    }
}

#[test]
fn content_extracts_raw_content() {
    let mut input = sample_bytes();
    input.extend_from_slice(&sample_bytes());

    nififf3()
        .arg("content")
        .write_stdin(input)
        .assert()
        .success()
        .stdout("hellohello");
}

#[test]
fn content_detects_truncated_input() {
    let bytes = sample_bytes();
    nififf3()
        .arg("content")
        .write_stdin(bytes[..bytes.len() - 2].to_vec())
        .assert()
        .failure()
        .stderr(predicates::str::contains("size mismatch"));
}

#[test]
fn create_rejects_malformed_attributes() {
    nififf3()
        .args(["create", "not-a-pair"])
        .write_stdin("")
        .assert()
        .failure()
        .stderr(predicates::str::contains("expected KEY=VALUE"));
}

#[test]
fn from_json_rejects_size_mismatch() {
    nififf3()
        .arg("from-json")
        .write_stdin(r#"{"size": 3, "attributes": {}, "content": "aGVsbG8="}"#)
        .assert()
        .failure()
        .stderr(predicates::str::contains("does not match"));
}

#[test]
fn limit_flags_reject_oversized_headers() {
    let many = FlowFile::builder()
        .attributes((0..50).map(|i| (format!("k{i}"), "v")))
        .content(Vec::new())
        .to_bytes();

    nififf3()
        .args(["attrs", "--max-attributes", "10"])
        .write_stdin(many.clone())
        .assert()
        .failure()
        .stderr(predicates::str::contains("attribute count"));

    nififf3()
        .args(["content", "--max-content-len", "2"])
        .write_stdin(sample_bytes())
        .assert()
        .failure()
        .stderr(predicates::str::contains("content size"));

    // Unset means no cap, as before.
    nififf3()
        .arg("attrs")
        .write_stdin(many)
        .assert()
        .success();
}

#[test]
fn to_json_rejects_garbage() {
    nififf3()
        .arg("to-json")
        .write_stdin("not a flow file")
        .assert()
        .failure()
        .stderr(predicates::str::contains("invalid magic"));
}
