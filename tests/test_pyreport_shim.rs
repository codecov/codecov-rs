use std::{
    collections::HashMap,
    fs::File,
    io::Seek,
    path::{Path, PathBuf},
};

use codecov_rs::{
    parsers::{
        common::ReportBuilderCtx,
        pyreport,
        pyreport::{chunks, report_json},
    },
    report::{
        models, pyreport::ToPyreport, Report, ReportBuilder, SqliteReport, SqliteReportBuilder,
    },
};
use tempfile::TempDir;
use winnow::Parser;

mod common;

type ReportJsonStream<'a> =
    report_json::ReportOutputStream<&'a str, SqliteReport, SqliteReportBuilder>;
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

fn hash_id(key: &str) -> i64 {
    seahash::hash(key.as_bytes()) as i64
}

#[test]
fn test_parse_report_json() {
    let input = common::read_sample_file(Path::new("codecov-rs-reports-json-d2a9ba1.txt"));

    let test_ctx = setup();
    let parse_ctx = ReportBuilderCtx::new(SqliteReportBuilder::new(test_ctx.db_file).unwrap());
    let mut buf = ReportJsonStream {
        input: &input,
        state: parse_ctx,
    };

    let expected_files = vec![
        models::SourceFile {
            id: hash_id("src/report.rs"),
            path: "src/report.rs".to_string(),
        },
        models::SourceFile {
            id: hash_id("src/report/models.rs"),
            path: "src/report/models.rs".to_string(),
        },
        models::SourceFile {
            id: hash_id("src/report/schema.rs"),
            path: "src/report/schema.rs".to_string(),
        },
    ];

    let expected_name = "[1704827412] codecov-rs CI: https://github.com/codecov/codecov-rs/actions/runs/7465738121 (0)";
    let expected_sessions = vec![models::Context {
        id: hash_id(expected_name),
        context_type: models::ContextType::Upload,
        name: expected_name.to_string(),
    }];

    let expected_json_files = HashMap::from([
        (0, expected_files[0].id),
        (1, expected_files[1].id),
        (2, expected_files[2].id),
    ]);

    let expected_json_sessions = HashMap::from([(0, expected_sessions[0].id)]);

    let (actual_files, actual_sessions) = report_json::parse_report_json
        .parse_next(&mut buf)
        .expect("Failed to parse");
    assert_eq!(actual_files, expected_json_files);
    assert_eq!(actual_sessions, expected_json_sessions);

    let report = buf.state.report_builder.build().unwrap();

    let files = report.list_files().unwrap();
    assert_eq!(files, expected_files);

    let contexts = report.list_contexts().unwrap();
    assert_eq!(contexts.len(), 1);
    assert_eq!(contexts[0].context_type, models::ContextType::Upload);
    assert_eq!(contexts[0].name, expected_name);
}

#[test]
fn test_parse_chunks_file() {
    let input = common::read_sample_file(Path::new("codecov-rs-chunks-d2a9ba1.txt"));
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
        let file = report_builder.insert_file(file.to_string()).unwrap();
        report_json_files.insert(i, file.id);
    }

    // Pretend `parse_report_json` has already run
    let mut report_json_sessions = HashMap::new();
    let session = report_builder
        .insert_context(models::ContextType::Upload, "codecov-rs CI")
        .unwrap();
    report_json_sessions.insert(0, session.id);

    // Set up to call the chunks parser
    let chunks_parse_ctx = chunks::ParseCtx::new(
        report_builder,
        report_json_files.clone(),
        report_json_sessions.clone(),
    );

    let mut buf = ChunksStream {
        input: &input,
        state: chunks_parse_ctx,
    };

    chunks::parse_chunks_file
        .parse_next(&mut buf)
        .expect("Failed to parse");

    // Helper function for creating our expected values
    fn make_sample(source_file_id: i64, line_no: i64, hits: i64) -> models::CoverageSample {
        models::CoverageSample {
            id: uuid::Uuid::nil(), // Ignored
            source_file_id,
            line_no,
            coverage_type: models::CoverageType::Line,
            hits: Some(hits),
            hit_branches: None,
            total_branches: None,
        }
    }
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
    let actual_contexts = report.list_contexts().expect("Failed to list contexts");
    assert_eq!(
        actual_coverage_samples.len(),
        expected_coverage_samples.len()
    );
    for i in 0..actual_coverage_samples.len() {
        expected_coverage_samples[i].id = actual_coverage_samples[i].id;
        assert_eq!(actual_coverage_samples[i], expected_coverage_samples[i]);

        assert_eq!(
            report
                .list_contexts_for_sample(&actual_coverage_samples[i])
                .unwrap(),
            actual_contexts
        );
    }
}

#[test]
fn test_parse_pyreport() {
    let report_json_file =
        File::open(common::sample_data_path().join("codecov-rs-reports-json-d2a9ba1.txt"))
            .expect("Failed to open report json file");
    let chunks_file = File::open(common::sample_data_path().join("codecov-rs-chunks-d2a9ba1.txt"))
        .expect("Failed to open chunks file");
    let test_ctx = setup();

    let report = pyreport::parse_pyreport(&report_json_file, &chunks_file, test_ctx.db_file)
        .expect("Failed to parse pyreport");

    let expected_files = vec![
        models::SourceFile {
            id: hash_id("src/report.rs"),
            path: "src/report.rs".to_string(),
        },
        models::SourceFile {
            id: hash_id("src/report/models.rs"),
            path: "src/report/models.rs".to_string(),
        },
        models::SourceFile {
            id: hash_id("src/report/schema.rs"),
            path: "src/report/schema.rs".to_string(),
        },
    ];

    let expected_name = "[1704827412] codecov-rs CI: https://github.com/codecov/codecov-rs/actions/runs/7465738121 (0)";
    let expected_sessions = vec![models::Context {
        id: hash_id(expected_name),
        context_type: models::ContextType::Upload,
        name: expected_name.to_string(),
    }];

    // Helper function for creating our expected values
    fn make_sample(source_file_id: i64, line_no: i64, hits: i64) -> models::CoverageSample {
        models::CoverageSample {
            id: uuid::Uuid::nil(), // Ignored
            source_file_id,
            line_no,
            coverage_type: models::CoverageType::Line,
            hits: Some(hits),
            hit_branches: None,
            total_branches: None,
        }
    }
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
    let actual_contexts = report.list_contexts().expect("Failed to list contexts");
    assert_eq!(actual_contexts, expected_sessions);
    assert_eq!(
        actual_coverage_samples.len(),
        expected_coverage_samples.len()
    );
    for i in 0..actual_coverage_samples.len() {
        expected_coverage_samples[i].id = actual_coverage_samples[i].id;
        assert_eq!(actual_coverage_samples[i], expected_coverage_samples[i]);

        assert_eq!(
            report
                .list_contexts_for_sample(&actual_coverage_samples[i])
                .unwrap(),
            actual_contexts
        );
    }
}

#[test]
fn test_sql_to_pyreport_to_sql_totals_match() {
    let report_json_input_file =
        File::open(common::sample_data_path().join("codecov-rs-reports-json-d2a9ba1.txt"))
            .expect("Failed to open report json file");
    let chunks_input_file =
        File::open(common::sample_data_path().join("codecov-rs-chunks-d2a9ba1.txt"))
            .expect("Failed to open chunks file");
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

    let _ = report_json_output_file.rewind().unwrap();
    let _ = chunks_output_file.rewind().unwrap();

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
