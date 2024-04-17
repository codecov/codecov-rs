use std::{collections::HashMap, path::Path};

use codecov_rs::{
    parsers::{pyreport_shim::report_json, ReportBuilderCtx},
    report::{models, Report, ReportBuilder, SqliteReport, SqliteReportBuilder},
};
use tempfile::TempDir;
use winnow::Parser;

mod common;

type SqliteStream<'a> = report_json::ReportOutputStream<&'a str, SqliteReport, SqliteReportBuilder>;

struct Ctx {
    _temp_dir: TempDir,
    parse_ctx: ReportBuilderCtx<SqliteReport, SqliteReportBuilder>,
}

fn setup() -> Ctx {
    let temp_dir = TempDir::new().ok().unwrap();
    let db_file = temp_dir.path().to_owned().join("db.sqlite");

    let report_builder = SqliteReportBuilder::new(db_file);
    let parse_ctx = ReportBuilderCtx::new(report_builder);

    Ctx {
        _temp_dir: temp_dir,
        parse_ctx,
    }
}

fn hash_id(key: &str) -> i64 {
    seahash::hash(key.as_bytes()) as i64
}

#[test]
fn test_parse_report_json() {
    let input = common::read_sample_file(Path::new("codecov-rs-reports-json-d2a9ba1.txt"));

    let ctx = setup();
    let mut buf = SqliteStream {
        input: &input,
        state: ctx.parse_ctx,
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

    let expected_sessions = vec![models::Context {
        id: hash_id("codecov-rs CI"),
        context_type: models::ContextType::Upload,
        name: "codecov-rs CI".to_string(),
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

    let report = buf.state.report_builder.build();

    let files = report.list_files().unwrap();
    assert_eq!(files, expected_files);

    let contexts = report.list_contexts().unwrap();
    assert_eq!(contexts.len(), 1);
    assert_eq!(contexts[0].context_type, models::ContextType::Upload);
    assert_eq!(contexts[0].name, "codecov-rs CI".to_string());
}
