use std::collections::HashMap;

use codecov_rs::{
    parsers::pyreport::{chunks, report_json},
    test_utils::test_report::{TestReport, TestReportBuilder},
};
use criterion::{criterion_group, criterion_main, Criterion};
use test_utils::fixtures::{read_fixture, FixtureFormat::Pyreport, FixtureSize::Large};
use winnow::Parser as _;

criterion_group!(
    benches,
    simple_report_json,
    complex_report_json,
    simple_chunks,
    complex_chunks,
);
criterion_main!(benches);

fn simple_report_json(c: &mut Criterion) {
    let reports: &[&[u8]] = &[
        br#"{"files": {"src/report.rs": [0, {}, [], null]}, "sessions": {"0": {"j": "codecov-rs CI"}}}"#,
        br#"{"files": {"src/report.rs": [0, {}, [], null], "src/report/models.rs": [1, {}, [], null]}, "sessions": {"0": {"j": "codecov-rs CI"}, "1": {"j": "codecov-rs CI 2"}}}"#,
        br#"{"files": {}, "sessions": {"0": {"j": "codecov-rs CI"}, "1": {"j": "codecov-rs CI 2"}}}"#,
        br#"{"files": {"src/report.rs": [0, {}, [], null], "src/report/models.rs": [1, {}, [], null]}, "sessions": {}}"#,
        br#"{"files": {}, "sessions": {}}"#,
    ];

    c.bench_function("simple_report_json", |b| {
        b.iter(|| {
            for input in reports {
                parse_report_json(input);
            }
        })
    });
}

fn complex_report_json(c: &mut Criterion) {
    // this is a ~11M `report_json`
    let report = read_fixture(
        Pyreport,
        Large,
        "worker-c71ddfd4cb1753c7a540e5248c2beaa079fc3341-report_json.json",
    )
    .unwrap();

    c.bench_function("complex_report_json", |b| {
        b.iter(|| parse_report_json(&report))
    });
}

fn parse_report_json(input: &[u8]) -> report_json::ParsedReportJson {
    let mut report_builder = TestReportBuilder::default();
    report_json::parse_report_json(input, &mut report_builder).unwrap()
}

fn simple_chunks(c: &mut Criterion) {
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

    c.bench_function("simple_chunks", |b| {
        b.iter(|| {
            for input in chunks {
                parse_chunks_file(input, files.clone(), sessions.clone())
            }
        })
    });
}

// just 1 iteration, as this is currently ~4 seconds on my machine
fn complex_chunks(c: &mut Criterion) {
    // this is a ~96M `chunks` file
    let chunks = read_fixture(
        Pyreport,
        Large,
        "worker-c71ddfd4cb1753c7a540e5248c2beaa079fc3341-chunks.txt",
    )
    .unwrap();
    let chunks = std::str::from_utf8(&chunks).unwrap();

    // parsing the chunks depends on having loaded the `report_json`
    let report = read_fixture(
        Pyreport,
        Large,
        "worker-c71ddfd4cb1753c7a540e5248c2beaa079fc3341-report_json.json",
    )
    .unwrap();
    let report_json::ParsedReportJson { files, sessions } = parse_report_json(&report);

    c.bench_function("complex_chunks", |b| {
        b.iter(|| parse_chunks_file(chunks, files.clone(), sessions.clone()))
    });
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
