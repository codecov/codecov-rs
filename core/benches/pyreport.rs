use codecov_rs::{parsers::pyreport::report_json, report::test::TestReportBuilder};
use divan::Bencher;

fn main() {
    divan::main();
}

#[divan::bench]
fn simple_report() {
    let reports = &[
        &br#"{"files": {"src/report.rs": [0, {}, [], null]}, "sessions": {"0": {"j": "codecov-rs CI"}}}"#[..],
        &br#"{"files": {"src/report.rs": [0, {}, [], null], "src/report/models.rs": [1, {}, [], null]}, "sessions": {"0": {"j": "codecov-rs CI"}, "1": {"j": "codecov-rs CI 2"}}}"#[..],
        &br#"{"files": {}, "sessions": {"0": {"j": "codecov-rs CI"}, "1": {"j": "codecov-rs CI 2"}}}"#[..],
        &br#"{"files": {"src/report.rs": [0, {}, [], null], "src/report/models.rs": [1, {}, [], null]}, "sessions": {}}"#[..],
        &br#"{"files": {}, "sessions": {}}"#[..],
    ];

    for input in reports {
        run_parsing(input);
    }
}

// parsing this is quite slow
#[divan::bench]
fn complex_report(bencher: Bencher) {
    // this is a ~11M `report_json`
    let path =
        "./fixtures/pyreport/large/worker-c71ddfd4cb1753c7a540e5248c2beaa079fc3341-report_json.json";
    let Ok(report) = std::fs::read(path) else {
        println!("Failed to read test report");
        return;
    };

    if report.starts_with(b"version https://git-lfs.github.com/spec/v1\n") {
        println!("Sample report has not been pulled from Git LFS");
        return;
    }

    bencher.bench(|| run_parsing(&report));
}

fn run_parsing(input: &[u8]) {
    let mut report_builder = TestReportBuilder::default();
    report_json::parse_report_json(input, &mut report_builder).unwrap();
}
