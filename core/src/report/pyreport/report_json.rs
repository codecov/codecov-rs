use std::io::Write;

use serde_json::json;

use crate::{
    error::Result,
    parsers::json::JsonVal,
    report::{models, sqlite::json_value_from_sql, SqliteReport},
};

/// Coverage percentages are written with 5 decimal places of precision unless
/// they are 0 or 100.
fn calculate_coverage_pct(hits: i64, lines: i64) -> String {
    match (hits, lines) {
        (0, _) => 0.to_string(),
        (h, l) if h == l => 100.to_string(),
        (h, l) => format!("{:.5}", h as f64 / l as f64 * 100.0),
    }
}

/// Build the "files" object inside of a report JSON and write it to
/// `output_file`. The caller is responsible for the enclosing `{}`s or
/// succeeding comma; this function just writes the key/value pair like so:
///
/// ```notrust
/// "files": {
///     "src/report/report.rs": [
///         ...
///     ],
///     "src/report/models.rs": [
///         ...
///     ]
/// }
/// ```
///
/// See [`crate::report::pyreport`] for more details about the content and
/// structure of a report JSON.
fn sql_to_files_dict(report: &SqliteReport, output: &mut impl Write) -> Result<()> {
    let mut stmt = report
        .conn
        .prepare_cached(include_str!("queries/files_to_report_json.sql"))?;
    let mut rows = stmt.query([])?;

    /// Each row returned by `queries/files_to_report_json.sql` represents a
    /// `models::SourceFile` from a `SqliteReport` alongside some aggregated
    /// coverage metrics for that file. This helper function returns the
    /// key/value pair that will be written into the files object for a row,
    /// where the key is the file's path and the value is its data.
    fn build_file_from_row(row: &rusqlite::Row) -> Result<(String, JsonVal)> {
        let chunk_index = row.get::<usize, i64>(0)?;
        let new_path = row.get(2)?;
        let lines = row.get::<usize, i64>(3)?;
        let hits = row.get::<usize, i64>(4)?;
        let misses = row.get::<usize, i64>(5)?;
        let partials = row.get::<usize, i64>(6)?;
        let branches = row.get::<usize, i64>(7)?;
        let methods = row.get::<usize, i64>(8)?;
        let hit_complexity_paths = row.get::<usize, i64>(9)?;
        let total_complexity = row.get::<usize, i64>(10)?;

        let coverage_pct = calculate_coverage_pct(hits, lines);
        let totals = json!([
            0, // file_count
            lines,
            hits,
            misses,
            partials,
            coverage_pct,
            branches,
            methods,
            0, // messages
            0, // sessions
            hit_complexity_paths,
            total_complexity,
            0, // diff
        ]);

        Ok((
            new_path,
            json!([
                chunk_index,
                totals,
                JsonVal::Null, /* session_totals */
                JsonVal::Null  /* diff_totals */
            ]),
        ))
    }

    // Write the "files" key to the output file and build its value by iterating
    // over our query results. It's the caller's responsibility to write
    // surroundings {}s or ,s as needed.
    write!(output, "\"files\": {{")?;
    let mut first_file = true;
    while let Some(row) = rows.next()? {
        let (file_path, file) = build_file_from_row(row)?;
        // No preceding , for the first file we write
        let delimiter = if first_file { "" } else { "," };
        write!(output, "{delimiter}\"{file_path}\": {file}")?;
        first_file = false;
    }
    write!(output, "}}")?;
    Ok(())
}

/// Build the "sessions" object inside of a report JSON and write it to
/// `output_file`. The caller is responsible for the enclosing `{}`s or
/// succeeding comma; this function just writes the key/value pair like so:
///
/// ```notrust
/// "sessions": {
///     "0": {
///         ...
///     },
///     "1": {
///         ...
///     }
/// }
/// ```
///
/// See [`crate::report::pyreport`] for more details about the content and
/// structure of a report JSON.
fn sql_to_sessions_dict(report: &SqliteReport, output: &mut impl Write) -> Result<()> {
    let mut stmt = report
        .conn
        .prepare_cached(include_str!("queries/sessions_to_report_json.sql"))?;
    let mut rows = stmt.query([])?;

    /// Each row returned by `queries/sessions_to_report_json.sql` represents a
    /// "session" in pyreport parlance, or a `models::RawUpload` in a
    /// `SQLiteReport`. This helper function returns the key/value pair that
    /// will be written into the sessions object for a row, where the key is
    /// the session ID and the value is the data for that session.
    fn build_session_from_row(row: &rusqlite::Row) -> Result<(String, JsonVal)> {
        let session_id = row.get::<usize, String>(0)?;
        let file_count = row.get::<usize, i64>(2)?;
        let lines = row.get::<usize, i64>(3)?;
        let hits = row.get::<usize, i64>(4)?;
        let misses = row.get::<usize, i64>(5)?;
        let partials = row.get::<usize, i64>(6)?;
        let branches = row.get::<usize, i64>(7)?;
        let methods = row.get::<usize, i64>(8)?;
        let hit_complexity_paths = row.get::<usize, i64>(9)?;
        let total_complexity = row.get::<usize, i64>(10)?;

        let coverage_pct = calculate_coverage_pct(hits, lines);
        let totals = json!([
            file_count,
            lines,
            hits,
            misses,
            partials,
            coverage_pct,
            branches,
            methods,
            0, // messages
            0, // sessions
            hit_complexity_paths,
            total_complexity,
            0, // diff
        ]);

        let flags = if let Some(flags) = row.get(13)? {
            Some(json_value_from_sql(flags, 13)?)
        } else {
            None
        };

        let session_extras = if let Some(session_extras) = row.get(22)? {
            Some(json_value_from_sql(session_extras, 22)?)
        } else {
            None
        };

        let raw_upload = models::RawUpload {
            timestamp: row.get(11)?,
            raw_upload_url: row.get::<usize, Option<String>>(12)?,
            flags,
            provider: row.get(14)?,
            build: row.get(15)?,
            name: row.get(16)?,
            job_name: row.get(17)?,
            ci_run_url: row.get(18)?,
            state: row.get(19)?,
            env: row.get(20)?,
            session_type: row.get(21)?,
            session_extras,
            ..Default::default()
        };
        Ok((
            session_id,
            json!({
                "t": totals,
                "d": raw_upload.timestamp,
                "a": raw_upload.raw_upload_url,
                "f": raw_upload.flags,
                "c": raw_upload.provider,
                "n": raw_upload.build,
                "N": raw_upload.name,
                "j": raw_upload.job_name,
                "u": raw_upload.ci_run_url,
                "p": raw_upload.state,
                "e": raw_upload.env,
                "st": raw_upload.session_type,
                "se": raw_upload.session_extras,
            }),
        ))
    }

    // Write the "sessions" key to the output file and build its value by iterating
    // over our query results. It's the caller's responsibility to write
    // surroundings {}s or ,s as needed.
    write!(output, "\"sessions\": {{")?;
    let mut first_session = true;
    while let Some(row) = rows.next()? {
        let (session_id, session) = build_session_from_row(row)?;
        // No preceding , for the first session we write
        let delimiter = if first_session { "" } else { "," };
        write!(output, "{delimiter}\"{session_id}\": {session}")?;
        first_session = false;
    }
    write!(output, "}}")?;
    Ok(())
}

/// Builds a report JSON from a [`SqliteReport`] and writes it to `output_file`.
/// See [`crate::report::pyreport`] for more details about the content and
/// structure of a report JSON.
pub fn sql_to_report_json(report: &SqliteReport, output: &mut impl Write) -> Result<()> {
    write!(output, "{{")?;
    sql_to_files_dict(report, output)?;
    write!(output, ",")?;
    sql_to_sessions_dict(report, output)?;
    write!(output, "}}")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use serde_json::json;
    use tempfile::TempDir;

    use super::*;
    use crate::report::{
        sqlite::{Insertable, SqliteReportBuilder},
        ReportBuilder,
    };

    struct Ctx {
        temp_dir: TempDir,
    }

    fn setup() -> Ctx {
        Ctx {
            temp_dir: TempDir::new().ok().unwrap(),
        }
    }

    fn build_sample_report(path: PathBuf) -> Result<SqliteReport> {
        let mut builder = SqliteReportBuilder::new(path)?;
        let file_1 = builder.insert_file("src/report/report.rs")?;
        let file_2 = builder.insert_file("src/report/models.rs")?;

        let upload_1 = models::RawUpload {
            id: 5,
            timestamp: Some(123),
            raw_upload_url: Some("upload 1 url".to_string()),
            flags: Some(json!(["flag on upload 1"])),
            provider: Some("provider upload 1".to_string()),
            build: Some("build upload 1".to_string()),
            name: Some("name upload 1".to_string()),
            job_name: Some("job name upload 1".to_string()),
            ci_run_url: Some("ci run url upload 1".to_string()),
            state: Some("state upload 1".to_string()),
            env: Some("env upload 1".to_string()),
            session_type: Some("type upload 1".to_string()),
            session_extras: Some(json!({"k1": "v1"})),
            ..Default::default()
        };
        // Insert directly, not through report builder, because we don't want a random
        // ID
        upload_1.insert(&builder.conn)?;

        let upload_2 = models::RawUpload {
            id: 10,
            timestamp: Some(456),
            raw_upload_url: Some("upload 2 url".to_string()),
            flags: Some(json!(["flag on upload 2"])),
            provider: Some("provider upload 2".to_string()),
            build: Some("build upload 2".to_string()),
            name: Some("name upload 2".to_string()),
            job_name: Some("job name upload 2".to_string()),
            ci_run_url: Some("ci run url upload 2".to_string()),
            state: Some("state upload 2".to_string()),
            env: Some("env upload 2".to_string()),
            session_type: Some("type upload 2".to_string()),
            session_extras: Some(json!({"k2": "v2"})),
            ..Default::default()
        };
        // Insert directly, not through report builder, because we don't want a random
        // ID
        upload_2.insert(&builder.conn)?;

        let line_1 = builder.insert_coverage_sample(models::CoverageSample {
            raw_upload_id: upload_1.id,
            source_file_id: file_1.id,
            line_no: 1,
            coverage_type: models::CoverageType::Line,
            hits: Some(3),
            ..Default::default()
        })?;

        let line_2 = builder.insert_coverage_sample(models::CoverageSample {
            raw_upload_id: upload_1.id,
            source_file_id: file_2.id,
            line_no: 1,
            coverage_type: models::CoverageType::Line,
            hits: Some(4),
            ..Default::default()
        })?;
        let _line_3 = builder.insert_coverage_sample(models::CoverageSample {
            raw_upload_id: upload_2.id,
            source_file_id: file_2.id,
            line_no: 3,
            coverage_type: models::CoverageType::Line,
            hits: Some(0),
            ..Default::default()
        })?;

        let branch_sample_1 = builder.insert_coverage_sample(models::CoverageSample {
            raw_upload_id: upload_1.id,
            source_file_id: file_1.id,
            line_no: 3,
            coverage_type: models::CoverageType::Branch,
            hit_branches: Some(2),
            total_branches: Some(2),
            ..Default::default()
        })?;
        let _ = builder.insert_branches_data(models::BranchesData {
            raw_upload_id: upload_1.id,
            source_file_id: branch_sample_1.source_file_id,
            local_sample_id: branch_sample_1.local_sample_id,
            hits: 1,
            branch_format: models::BranchFormat::Condition,
            branch: "0:jump".to_string(),
            ..Default::default()
        })?;
        let _ = builder.insert_branches_data(models::BranchesData {
            raw_upload_id: upload_1.id,
            source_file_id: branch_sample_1.source_file_id,
            local_sample_id: branch_sample_1.local_sample_id,
            hits: 1,
            branch_format: models::BranchFormat::Condition,
            branch: "1".to_string(),
            ..Default::default()
        })?;

        let branch_sample_2 = builder.insert_coverage_sample(models::CoverageSample {
            raw_upload_id: upload_1.id,
            source_file_id: file_2.id,
            line_no: 6,
            coverage_type: models::CoverageType::Branch,
            hit_branches: Some(2),
            total_branches: Some(4),
            ..Default::default()
        })?;
        let _ = builder.insert_branches_data(models::BranchesData {
            raw_upload_id: upload_1.id,
            source_file_id: branch_sample_2.source_file_id,
            local_sample_id: branch_sample_2.local_sample_id,
            hits: 1,
            branch_format: models::BranchFormat::Condition,
            branch: "0:jump".to_string(),
            ..Default::default()
        })?;
        let _ = builder.insert_branches_data(models::BranchesData {
            raw_upload_id: upload_1.id,
            source_file_id: branch_sample_2.source_file_id,
            local_sample_id: branch_sample_2.local_sample_id,
            hits: 1,
            branch_format: models::BranchFormat::Condition,
            branch: "1".to_string(),
            ..Default::default()
        })?;
        let _ = builder.insert_branches_data(models::BranchesData {
            raw_upload_id: upload_1.id,
            source_file_id: branch_sample_2.source_file_id,
            local_sample_id: branch_sample_2.local_sample_id,
            hits: 0,
            branch_format: models::BranchFormat::Condition,
            branch: "2".to_string(),
            ..Default::default()
        })?;
        let _ = builder.insert_branches_data(models::BranchesData {
            raw_upload_id: upload_1.id,
            source_file_id: branch_sample_2.source_file_id,
            local_sample_id: branch_sample_2.local_sample_id,
            hits: 0,
            branch_format: models::BranchFormat::Condition,
            branch: "3".to_string(),
            ..Default::default()
        })?;

        let method_sample_1 = builder.insert_coverage_sample(models::CoverageSample {
            raw_upload_id: upload_1.id,
            source_file_id: file_1.id,
            line_no: 2,
            coverage_type: models::CoverageType::Method,
            hits: Some(2),
            ..Default::default()
        })?;
        let _ = builder.insert_method_data(models::MethodData {
            raw_upload_id: upload_1.id,
            source_file_id: method_sample_1.source_file_id,
            local_sample_id: method_sample_1.local_sample_id,
            line_no: Some(method_sample_1.line_no),
            hit_branches: Some(2),
            total_branches: Some(4),
            hit_complexity_paths: Some(2),
            total_complexity: Some(4),
            ..Default::default()
        })?;

        let method_sample_2 = builder.insert_coverage_sample(models::CoverageSample {
            raw_upload_id: upload_1.id,
            source_file_id: file_2.id,
            line_no: 2,
            coverage_type: models::CoverageType::Method,
            hits: Some(5),
            ..Default::default()
        })?;
        let _ = builder.insert_method_data(models::MethodData {
            raw_upload_id: upload_1.id,
            source_file_id: method_sample_2.source_file_id,
            local_sample_id: method_sample_2.local_sample_id,
            line_no: Some(method_sample_2.line_no),
            hit_branches: Some(2),
            total_branches: Some(4),
            ..Default::default()
        })?;

        let method_sample_3 = builder.insert_coverage_sample(models::CoverageSample {
            raw_upload_id: upload_2.id,
            source_file_id: file_2.id,
            line_no: 5,
            coverage_type: models::CoverageType::Method,
            hits: Some(0),
            ..Default::default()
        })?;
        let _ = builder.insert_method_data(models::MethodData {
            raw_upload_id: upload_2.id,
            source_file_id: method_sample_3.source_file_id,
            local_sample_id: method_sample_3.local_sample_id,
            line_no: Some(method_sample_3.line_no),
            hit_complexity_paths: Some(2),
            total_complexity: Some(4),
            ..Default::default()
        })?;

        let line_with_partial_1 = builder.insert_coverage_sample(models::CoverageSample {
            raw_upload_id: upload_1.id,
            source_file_id: file_1.id,
            line_no: 8,
            coverage_type: models::CoverageType::Line,
            hits: Some(3),
            ..Default::default()
        })?;
        let _ = builder.insert_span_data(models::SpanData {
            raw_upload_id: upload_1.id,
            source_file_id: line_with_partial_1.source_file_id,
            local_sample_id: Some(line_with_partial_1.local_sample_id),
            start_line: Some(line_with_partial_1.line_no),
            start_col: Some(3),
            end_line: Some(line_with_partial_1.line_no),
            end_col: None,
            hits: 3,
            ..Default::default()
        })?;

        let label_1 = builder.insert_context("test-case")?;
        let _ = builder.associate_context(models::ContextAssoc {
            context_id: label_1.id,
            raw_upload_id: upload_1.id,
            local_sample_id: Some(line_1.local_sample_id),
            ..Default::default()
        })?;
        let _ = builder.associate_context(models::ContextAssoc {
            context_id: label_1.id,
            raw_upload_id: upload_1.id,
            local_sample_id: Some(line_2.local_sample_id),
            ..Default::default()
        })?;

        builder.build()
    }

    #[test]
    fn test_calculate_coverage_pct() {
        assert_eq!(calculate_coverage_pct(0, 16), "0".to_string());
        assert_eq!(calculate_coverage_pct(4, 16), "25.00000".to_string());
        assert_eq!(calculate_coverage_pct(16, 16), "100".to_string());
        assert_eq!(calculate_coverage_pct(1, 3), "33.33333".to_string());
        assert_eq!(calculate_coverage_pct(1, 8), "12.50000".to_string());

        // Should not occur in normal usage, just documenting the behavior
        assert_eq!(calculate_coverage_pct(-1, 8), "-12.50000".to_string());
        assert_eq!(calculate_coverage_pct(9, 8), "112.50000".to_string());
    }

    #[test]
    fn test_sql_to_files_dict() {
        let ctx = setup();
        let report = build_sample_report(ctx.temp_dir.path().join("db.sqlite")).unwrap();

        let mut files_output = Vec::new();
        files_output.push(b'{');
        sql_to_files_dict(&report, &mut files_output).unwrap();
        files_output.push(b'}');

        let files_dict: JsonVal = serde_json::from_slice(&files_output).unwrap();

        let expected = json!({
            "files": {
                "src/report/models.rs": [
                    0,
                    [
                        0,          // file count
                        5,          // line count
                        2,          // hits
                        2,          // misses
                        1,          // partials
                        "40.00000", // coverage %
                        1,          // branch count
                        2,          // method count
                        0,          // messages
                        0,          // session count
                        2,          // hit complexity paths
                        4,          // total complexity
                        0           // diff
                    ],
                    null,
                    null
                ],
                "src/report/report.rs": [
                    1,
                    [
                        0,      // file count
                        4,      // line count
                        4,      // hits
                        0,      // misses
                        0,      // partials
                        "100",  // coverage %
                        1,      // branch count
                        1,      // method count
                        0,      // messages
                        0,      // sessions
                        2,      // hit complexity paths
                        4,      // total complexity
                        0       // diff
                    ],
                    null,
                    null
                ],
            }
        });

        assert_eq!(files_dict, expected);
    }

    #[test]
    fn test_sql_to_sessions_dict() {
        let ctx = setup();
        let report = build_sample_report(ctx.temp_dir.path().join("db.sqlite")).unwrap();

        let mut sessions_output = Vec::new();
        sessions_output.push(b'{');
        sql_to_sessions_dict(&report, &mut sessions_output).unwrap();
        sessions_output.push(b'}');

        let sessions_dict: JsonVal = serde_json::from_slice(&sessions_output).unwrap();

        let expected = json!({
            "sessions": {
                "0": {
                    "t": [
                        2,              // file count
                        7,              // line count
                        6,              // hits
                        0,              // misses
                        1,              // partials
                        "85.71429",     // coverage %
                        2,              // branch count
                        2,              // method count
                        0,              // messages
                        0,              // sessions
                        2,              // hit_complexity_paths
                        4,              // total_complexity
                        0               // diff
                    ],
                    "d": 123,
                    "a": "upload 1 url",
                    "f": ["flag on upload 1"],
                    "c": "provider upload 1",
                    "n": "build upload 1",
                    "N": "name upload 1",
                    "j": "job name upload 1",
                    "u": "ci run url upload 1",
                    "p": "state upload 1",
                    "e": "env upload 1",
                    "st": "type upload 1",
                    "se": {"k1": "v1"},
                },
                "1": {
                    "t": [
                        1,      // file count
                        2,      // line count
                        0,      // hits
                        2,      // misses
                        0,      // partials
                        "0",    // coverage %
                        0,      // branch count
                        1,      // method count
                        0,      // messages
                        0,      // sessions
                        2,      // hit_complexity_paths
                        4,      // total_complexity
                        0       // diff
                    ],
                    "d": 456,
                    "a": "upload 2 url",
                    "f": ["flag on upload 2"],
                    "c": "provider upload 2",
                    "n": "build upload 2",
                    "N": "name upload 2",
                    "j": "job name upload 2",
                    "u": "ci run url upload 2",
                    "p": "state upload 2",
                    "e": "env upload 2",
                    "st": "type upload 2",
                    "se": {"k2": "v2"},
                }
            }
        });

        assert_eq!(sessions_dict, expected);
    }

    #[test]
    fn test_sql_to_report_json() {
        let ctx = setup();
        let report = build_sample_report(ctx.temp_dir.path().join("db.sqlite")).unwrap();

        let mut report_output = Vec::new();
        sql_to_report_json(&report, &mut report_output).unwrap();
        let report_json: JsonVal = serde_json::from_slice(&report_output).unwrap();

        // All of the totals are the same as in previous test cases so they have been
        // collapsed/uncommented for brevity
        let expected = json!({
            "files": {
                "src/report/models.rs": [
                    0,
                    [0, 5, 2, 2, 1, "40.00000", 1, 2, 0, 0, 2, 4, 0],
                    null,
                    null
                ],
                "src/report/report.rs": [
                    1,
                    [0, 4, 4, 0, 0, "100", 1, 1, 0, 0, 2, 4, 0],
                    null,
                    null
                ],
            },
            "sessions": {
                "0": {
                    "t": [2, 7, 6, 0, 1, "85.71429", 2, 2, 0, 0, 2, 4, 0],
                    "d": 123,
                    "a": "upload 1 url",
                    "f": ["flag on upload 1"],
                    "c": "provider upload 1",
                    "n": "build upload 1",
                    "N": "name upload 1",
                    "j": "job name upload 1",
                    "u": "ci run url upload 1",
                    "p": "state upload 1",
                    "e": "env upload 1",
                    "st": "type upload 1",
                    "se": {"k1": "v1"},
                },
                "1": {
                    "t": [1, 2, 0, 2, 0, "0", 0, 1, 0, 0, 2, 4, 0],
                    "d": 456,
                    "a": "upload 2 url",
                    "f": ["flag on upload 2"],
                    "c": "provider upload 2",
                    "n": "build upload 2",
                    "N": "name upload 2",
                    "j": "job name upload 2",
                    "u": "ci run url upload 2",
                    "p": "state upload 2",
                    "e": "env upload 2",
                    "st": "type upload 2",
                    "se": {"k2": "v2"},
                }
            }
        });

        assert_eq!(report_json, expected);

        let empty_report = SqliteReport::new(ctx.temp_dir.path().join("empty.db")).unwrap();

        let mut report_output = Vec::new();
        sql_to_report_json(&empty_report, &mut report_output).unwrap();
        let report_json: JsonVal = serde_json::from_slice(&report_output).unwrap();

        let expected = json!({"files": {}, "sessions": {}});
        assert_eq!(report_json, expected);
    }
}
