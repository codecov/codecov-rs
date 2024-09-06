use std::{collections::HashMap, fs::File, io::Seek, path::PathBuf};

use codecov_rs::{
    parsers::pyreport::{
        self, chunks,
        report_json::{self, ParsedReportJson},
    },
    report::{
        models, pyreport::ToPyreport, Report, ReportBuilder, SqliteReport, SqliteReportBuilder,
    },
};
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde_json::json;
use tempfile::TempDir;
use test_utils::fixtures::{
    open_fixture, read_fixture, FixtureFormat::Pyreport, FixtureSize::Small,
};
use winnow::Parser;

type ChunksStream<'a> = chunks::ReportOutputStream<&'a str, SqliteReport, SqliteReportBuilder>;

struct Ctx {
    temp_dir: TempDir,
    db_file: PathBuf,
}

fn setup() -> Ctx {
    let temp_dir = TempDir::new().ok().unwrap();
    let db_file = temp_dir.path().to_owned().join("db.sqlite");

    Ctx { temp_dir, db_file }
}

#[test]
fn test_parse_report_json() {
    let input = read_fixture(Pyreport, Small, "codecov-rs-reports-json-d2a9ba1.txt").unwrap();

    let rng_seed = 5;
    let mut rng = StdRng::seed_from_u64(rng_seed);

    let expected_files = vec![
        models::SourceFile::new("src/report.rs"),
        models::SourceFile::new("src/report/models.rs"),
        models::SourceFile::new("src/report/schema.rs"),
    ];

    let expected_session = models::RawUpload {
        id: rng.gen(),
        timestamp: Some(1704827412),
        raw_upload_url: Some("v4/raw/2024-01-09/BD18D96000B80FA280C411B0081460E1/d2a9ba133c9b30468d97e7fad1462728571ad699/065067fe-7677-4bd8-93b2-0a8d0b879f78/340c0c0b-a955-46a0-9de9-3a9b5f2e81e2.txt".to_string()),
        flags: Some(json!([])),
        provider: None,
        build: None,
        name: None,
        job_name: Some("codecov-rs CI".to_string()),
        ci_run_url: Some("https://github.com/codecov/codecov-rs/actions/runs/7465738121".to_string()),
        state: None,
        env: None,
        session_type: Some("uploaded".to_string()),
        session_extras: Some(json!({})),
    };

    let expected_json_files = HashMap::from([
        (0, expected_files[0].id),
        (1, expected_files[1].id),
        (2, expected_files[2].id),
    ]);

    let expected_json_sessions = HashMap::from([(0, expected_session.id)]);

    let test_ctx = setup();
    let mut report_builder =
        SqliteReportBuilder::new_with_seed(test_ctx.db_file, rng_seed).unwrap();

    let ParsedReportJson {
        files: actual_files,
        sessions: actual_sessions,
    } = report_json::parse_report_json(&input, &mut report_builder).expect("Failed to parse");
    assert_eq!(actual_files, expected_json_files);
    assert_eq!(actual_sessions, expected_json_sessions);

    let report = report_builder.build().unwrap();

    let files = report.list_files().unwrap();
    assert_eq!(files, expected_files);

    let contexts = report.list_contexts().unwrap();
    assert!(contexts.is_empty());

    let uploads = report.list_raw_uploads().unwrap();
    assert_eq!(uploads, vec![expected_session]);
}

#[test]
fn test_parse_chunks_file() {
    let input = read_fixture(Pyreport, Small, "codecov-rs-chunks-d2a9ba1.txt").unwrap();
    let input = std::str::from_utf8(&input).unwrap();
    let test_ctx = setup();

    let mut report_builder = SqliteReportBuilder::new(test_ctx.db_file).unwrap();

    // Pretend `parse_report_json` has already run
    let mut report_json_files = HashMap::new();
    for (i, file) in [
        "src/report.rs",
        "src/report/models.rs",
        "src/report/schema.rs",
    ]
    .iter()
    .enumerate()
    {
        let file = report_builder.insert_file(file).unwrap();
        report_json_files.insert(i, file.id);
    }

    // Pretend `parse_report_json` has already run
    let mut report_json_sessions = HashMap::new();
    let session = report_builder
        .insert_raw_upload(Default::default())
        .unwrap();
    report_json_sessions.insert(0, session.id);

    // Set up to call the chunks parser
    let chunks_parse_ctx = chunks::ParseCtx::new(
        report_builder,
        report_json_files.clone(),
        report_json_sessions.clone(),
    );

    let mut buf = ChunksStream {
        input,
        state: chunks_parse_ctx,
    };

    chunks::parse_chunks_file
        .parse_next(&mut buf)
        .expect("Failed to parse");

    // Helper function for creating our expected values
    let mut coverage_sample_id_iterator = 0..;
    let mut make_sample =
        |source_file_id: i64, line_no: i64, hits: i64| -> models::CoverageSample {
            models::CoverageSample {
                local_sample_id: coverage_sample_id_iterator.next().unwrap(),
                raw_upload_id: session.id,
                source_file_id,
                line_no,
                coverage_type: models::CoverageType::Line,
                hits: Some(hits),
                hit_branches: None,
                total_branches: None,
            }
        };
    // (start_line, end_line, hits)
    let covered_lines: [Vec<(i64, i64, i64)>; 3] = [
        vec![
            (17, 25, 3),
            (39, 43, 2),
            (45, 49, 1),
            (51, 53, 1),
            (55, 59, 1),
            (61, 78, 1),
        ],
        vec![
            (5, 5, 0),
            (12, 12, 0),
            (22, 22, 0),
            (33, 33, 0),
            (45, 45, 1),
        ],
        vec![
            (3, 3, 0),
            (10, 16, 0),
            (18, 27, 0),
            (29, 39, 0),
            (41, 48, 0),
            (50, 50, 0),
            (51, 52, 5),
            (53, 54, 6),
            (55, 56, 5),
        ],
    ];
    let mut expected_coverage_samples = Vec::new();
    for (i, file) in covered_lines.iter().enumerate() {
        for (start_line, end_line, hits) in file {
            for line_no in *start_line..=*end_line {
                expected_coverage_samples.push(make_sample(report_json_files[&i], line_no, *hits));
            }
        }
    }

    let report = buf.state.db.report_builder.build().unwrap();
    let actual_coverage_samples = report
        .list_coverage_samples()
        .expect("Failed to list coverage samples");
    assert_eq!(
        actual_coverage_samples.len(),
        expected_coverage_samples.len()
    );
    for i in 0..actual_coverage_samples.len() {
        expected_coverage_samples[i].local_sample_id = actual_coverage_samples[i].local_sample_id;
        assert_eq!(actual_coverage_samples[i], expected_coverage_samples[i]);
    }
}

#[test]
fn test_parse_pyreport() {
    let report_json_file =
        open_fixture(Pyreport, Small, "codecov-rs-reports-json-d2a9ba1.txt").unwrap();
    let chunks_file = open_fixture(Pyreport, Small, "codecov-rs-chunks-d2a9ba1.txt").unwrap();
    let test_ctx = setup();

    let rng_seed = 5;
    let mut rng = StdRng::seed_from_u64(rng_seed);

    let report = pyreport::parse_pyreport_with_seed(
        &report_json_file,
        &chunks_file,
        test_ctx.db_file,
        rng_seed,
    )
    .expect("Failed to parse pyreport");

    let expected_session = models::RawUpload {
        id: rng.gen(),
        timestamp: Some(1704827412),
        raw_upload_url: Some("v4/raw/2024-01-09/BD18D96000B80FA280C411B0081460E1/d2a9ba133c9b30468d97e7fad1462728571ad699/065067fe-7677-4bd8-93b2-0a8d0b879f78/340c0c0b-a955-46a0-9de9-3a9b5f2e81e2.txt".to_string()),
        flags: Some(json!([])),
        provider: None,
        build: None,
        name: None,
        job_name: Some("codecov-rs CI".to_string()),
        ci_run_url: Some("https://github.com/codecov/codecov-rs/actions/runs/7465738121".to_string()),
        state: None,
        env: None,
        session_type: Some("uploaded".to_string()),
        session_extras: Some(json!({})),
    };

    let expected_files = [
        models::SourceFile::new("src/report.rs"),
        models::SourceFile::new("src/report/models.rs"),
        models::SourceFile::new("src/report/schema.rs"),
    ];

    // Helper function for creating our expected values
    let mut coverage_sample_id_iterator = 0..;
    let mut make_sample =
        |source_file_id: i64, line_no: i64, hits: i64| -> models::CoverageSample {
            models::CoverageSample {
                local_sample_id: coverage_sample_id_iterator.next().unwrap(),
                raw_upload_id: expected_session.id,
                source_file_id,
                line_no,
                coverage_type: models::CoverageType::Line,
                hits: Some(hits),
                hit_branches: None,
                total_branches: None,
            }
        };

    // (start_line, end_line, hits)
    let covered_lines: [Vec<(i64, i64, i64)>; 3] = [
        vec![
            (17, 25, 3),
            (39, 43, 2),
            (45, 49, 1),
            (51, 53, 1),
            (55, 59, 1),
            (61, 78, 1),
        ],
        vec![
            (5, 5, 0),
            (12, 12, 0),
            (22, 22, 0),
            (33, 33, 0),
            (45, 45, 1),
        ],
        vec![
            (3, 3, 0),
            (10, 16, 0),
            (18, 27, 0),
            (29, 39, 0),
            (41, 48, 0),
            (50, 50, 0),
            (51, 52, 5),
            (53, 54, 6),
            (55, 56, 5),
        ],
    ];
    let mut expected_coverage_samples = Vec::new();
    for (i, file) in covered_lines.iter().enumerate() {
        for (start_line, end_line, hits) in file {
            for line_no in *start_line..=*end_line {
                expected_coverage_samples.push(make_sample(expected_files[i].id, line_no, *hits));
            }
        }
    }

    let actual_coverage_samples = report
        .list_coverage_samples()
        .expect("Failed to list coverage samples");
    assert_eq!(actual_coverage_samples, expected_coverage_samples);

    let contexts = report.list_contexts().unwrap();
    assert!(contexts.is_empty());

    let uploads = report.list_raw_uploads().unwrap();
    assert_eq!(uploads, vec![expected_session]);
}

#[test]
fn test_sql_to_pyreport_to_sql_totals_match() {
    let report_json_input_file =
        open_fixture(Pyreport, Small, "codecov-rs-reports-json-d2a9ba1.txt").unwrap();
    let chunks_input_file = open_fixture(Pyreport, Small, "codecov-rs-chunks-d2a9ba1.txt").unwrap();
    let test_ctx = setup();

    let report = pyreport::parse_pyreport(
        &report_json_input_file,
        &chunks_input_file,
        test_ctx.db_file,
    )
    .expect("Failed to parse pyreport");

    let report_json_output_path = test_ctx.temp_dir.path().join("report_json.json");
    let chunks_output_path = test_ctx.temp_dir.path().join("chunks.txt");
    let mut report_json_output_file = File::options()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(report_json_output_path)
        .unwrap();
    let mut chunks_output_file = File::options()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(chunks_output_path)
        .unwrap();

    report
        .to_pyreport(&mut report_json_output_file, &mut chunks_output_file)
        .expect("Failed to write to output files");

    let original_totals = report.totals().unwrap();

    report_json_output_file.rewind().unwrap();
    chunks_output_file.rewind().unwrap();

    let roundtrip_db_path = test_ctx.temp_dir.path().join("roundtrip.sqlite");
    let report = pyreport::parse_pyreport(
        &report_json_output_file,
        &chunks_output_file,
        roundtrip_db_path,
    )
    .expect("Failed to parse roundtrip report");
    let roundtrip_totals = report.totals().unwrap();

    assert_eq!(original_totals, roundtrip_totals);
}
