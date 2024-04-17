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

#[test]
fn test_parse_report_json() {
    let input = common::read_sample_file(Path::new("codecov-rs-reports-json-d2a9ba1.txt"));

    let ctx = setup();
    let mut buf = SqliteStream {
        input: &input,
        state: ctx.parse_ctx,
    };

    let expected_files = HashMap::from([(0, 1), (1, 2), (2, 3)]);
    let expected_sessions = HashMap::from([(0, 1)]);

    assert_eq!(
        report_json::parse_report_json.parse_next(&mut buf),
        Ok((expected_files, expected_sessions))
    );

    let report = buf.state.report_builder.build();

    let files = report.list_files().unwrap();
    assert_eq!(
        files,
        vec![
            models::SourceFile {
                id: Some(1),
                path: "src/report.rs".to_string(),
            },
            models::SourceFile {
                id: Some(2),
                path: "src/report/models.rs".to_string(),
            },
            models::SourceFile {
                id: Some(3),
                path: "src/report/schema.rs".to_string(),
            },
        ]
    );

    let contexts = report.list_contexts().unwrap();
    assert_eq!(
        contexts,
        vec![models::Context {
            id: Some(1),
            context_type: models::ContextType::Upload,
            name: "codecov-rs CI".to_string(),
        },]
    );
}
