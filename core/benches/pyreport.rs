use std::collections::HashMap;

use codecov_rs::{
    parsers::pyreport::{chunks, report_json},
    report::test::{TestReport, TestReportBuilder},
};
use divan::Bencher;
use winnow::Parser as _;

// #[global_allocator]
// static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

#[divan::bench]
fn simple_report_json() {
    let reports: &[&[u8]] = &[
        br#"{"files": {"src/report.rs": [0, {}, [], null]}, "sessions": {"0": {"j": "codecov-rs CI"}}}"#,
        br#"{"files": {"src/report.rs": [0, {}, [], null], "src/report/models.rs": [1, {}, [], null]}, "sessions": {"0": {"j": "codecov-rs CI"}, "1": {"j": "codecov-rs CI 2"}}}"#,
        br#"{"files": {}, "sessions": {"0": {"j": "codecov-rs CI"}, "1": {"j": "codecov-rs CI 2"}}}"#,
        br#"{"files": {"src/report.rs": [0, {}, [], null], "src/report/models.rs": [1, {}, [], null]}, "sessions": {}}"#,
        br#"{"files": {}, "sessions": {}}"#,
    ];

    for input in reports {
        parse_report_json(input);
    }
}

#[divan::bench]
fn complex_report_json(bencher: Bencher) {
    // this is a ~11M `report_json`
    let report = load_fixture(
        "pyreport/large/worker-c71ddfd4cb1753c7a540e5248c2beaa079fc3341-report_json.json",
    );

    bencher.bench(|| parse_report_json(&report));
}

fn parse_report_json(input: &[u8]) -> report_json::ParsedReportJson {
    let mut report_builder = TestReportBuilder::default();
    report_json::parse_report_json(input, &mut report_builder).unwrap()
}

#[divan::bench]
fn simple_chunks() {
    let chunks = &[
        // Header and one chunk with an empty line
        "{}\n<<<<< end_of_header >>>>>\n{}\n",
        // No header, one chunk with a populated line and an  empty line
        "{}\n[1, null, [[0, 1]]]\n",
        // No header, two chunks, the second having just one empty line
        "{}\n[1, null, [[0, 1]]]\n\n<<<<< end_of_chunk >>>>>\n{}\n",
        // Header, two chunks, the second having multiple data lines and an empty line
        "{}\n<<<<< end_of_header >>>>>\n{}\n[1, null, [[0, 1]]]\n\n<<<<< end_of_chunk >>>>>\n{}\n[1, null, [[0, 1]]]\n[1, null, [[0, 1]]]\n",
    ];

    let files = HashMap::from([(0, 0), (1, 1), (2, 2)]);
    let sessions = HashMap::from([(0, 0), (1, 1), (2, 2)]);

    for input in chunks {
        parse_chunks_file(input, files.clone(), sessions.clone())
    }
}

// just 1 iteration, as this is currently ~4 seconds on my machine
#[divan::bench(sample_count = 1)]
fn complex_chunks(bencher: Bencher) {
    // this is a ~96M `chunks` file
    let chunks =
        load_fixture("pyreport/large/worker-c71ddfd4cb1753c7a540e5248c2beaa079fc3341-chunks.txt");
    let chunks = std::str::from_utf8(&chunks).unwrap();

    // parsing the chunks depends on having loaded the `report_json`
    let report = load_fixture(
        "pyreport/large/worker-c71ddfd4cb1753c7a540e5248c2beaa079fc3341-report_json.json",
    );
    let report_json::ParsedReportJson { files, sessions } = parse_report_json(&report);

    bencher.bench(|| parse_chunks_file(chunks, files.clone(), sessions.clone()));
}

fn parse_chunks_file(input: &str, files: HashMap<usize, i64>, sessions: HashMap<usize, i64>) {
    let report_builder = TestReportBuilder::default();

    let chunks_ctx = chunks::ParseCtx::new(report_builder, files, sessions);
    let mut chunks_stream = chunks::ReportOutputStream::<&str, TestReport, TestReportBuilder> {
        input,
        state: chunks_ctx,
    };

    chunks::parse_chunks_file
        .parse_next(&mut chunks_stream)
        .unwrap();
}

#[track_caller]
fn load_fixture(path: &str) -> Vec<u8> {
    let path = format!("./fixtures/{path}");
    let contents = std::fs::read(path).unwrap();

    if contents.starts_with(b"version https://git-lfs.github.com/spec/v1") {
        panic!("Fixture has not been pulled from Git LFS");
    }

    contents
}
