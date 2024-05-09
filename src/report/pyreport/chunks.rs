use std::{fs::File, io::Write};

use serde_json::{json, json_internal};

use super::{CHUNKS_FILE_END_OF_CHUNK, CHUNKS_FILE_HEADER_TERMINATOR};
use crate::{
    error::{CodecovError, Result},
    parsers::json::{JsonNumber, JsonVal},
    report::{models, sqlite::json_value_from_sql, SqliteReport},
};

/// To save space, trailing nulls are removed from arrays in `ReportLine`s.
///
/// Examples:
/// ```notrust
/// // These two lines are equivalent
/// [0, null, [[0, 1, null, null, null]], null, null, null]
/// [0, null, [[0, 1]]]
///
/// // If a later field is present, as in the following example, no nulls may be removed
/// [0, "m", [[0, 1, null, null, [0, 1]]], null, null, [[0, 1, null, ["label"]]]]
/// ```
fn array_without_trailing_nulls(mut json_array: JsonVal) -> JsonVal {
    let JsonVal::Array(vals) = &mut json_array else {
        return json_array;
    };

    for (i, val) in vals.iter().enumerate().rev() {
        if val != &JsonVal::Null {
            return JsonVal::Array(vals[0..=i].into());
        }
    }
    // If we get out of the for loop, there were no non-null elements
    json!([])
}

/// The chunks file header contains a "labels index" mapping of a numeric ID to
/// a string label name. `queries/chunks_file_header.sql` builds the whole index
/// into a JSON object and we just have to deserialize it.
fn query_chunks_file_header(report: &SqliteReport) -> Result<JsonVal> {
    let mut stmt = report
        .conn
        .prepare_cached(include_str!("queries/chunks_file_header.sql"))?;
    Ok(stmt.query_row([], |row| row.get(0).and_then(|s| json_value_from_sql(s, 0)))?)
}

/// This function is called each time we encounter a row for a new line. It
/// writes out the current line so we can start building the new one. It takes
/// in the line number of the last line that we wrote out so that, if there is a
/// gap between that number and the number of our current line, it can fill that
/// gap with empty lines. That way, line 17 will always be the 17th line in the
/// current chunk, even if there is no data for lines 11-16.
fn maybe_write_current_line(
    current_line: Option<(i64, JsonVal)>,
    output_file: &mut File,
    last_populated_line: i64,
) -> Result<i64> {
    if let Some((line_no, line_values)) = current_line {
        // If last_populated_line is 6 and we're dealing with 7, this loop does not have
        // to write any newlines. If last_populated_line is 0 (its starting value) and
        // we're dealing with 1 (the first line), same story.
        for _ in last_populated_line..line_no - 1 {
            let _ = output_file.write("\n".as_bytes())?;
        }

        // Every line is preceded by, but not followed by, a newline. When starting a
        // new chunk, the cursor will be at the end of the header object on the
        // line before data is supposed to start.
        let _ = output_file
            .write(format!("\n{}", array_without_trailing_nulls(line_values)).as_bytes())?;
        Ok(line_no)
    } else {
        // We don't have a new line to print. Return the old value for
        // `last_populated_line`
        Ok(last_populated_line)
    }
}

/// The coverage field in a report line can be an integer, representing a hit
/// count, or a string representation of a fraction where the numerator is the
/// number of branches that were covered and the denominator is the total number
/// of possible branches.
fn format_coverage(
    hits: &Option<i64>,
    hit_branches: &Option<i64>,
    total_branches: &Option<i64>,
) -> Result<JsonVal> {
    match (hits, hit_branches, total_branches) {
        (Some(hits), _, _) => Ok(JsonVal::Number(JsonNumber::from(*hits))),
        (_, Some(hit_branches), Some(total_branches)) => Ok(JsonVal::String(format!(
            "{}/{}",
            hit_branches, total_branches
        ))),
        _ => Err(CodecovError::PyreportConversionError(
            "incomplete coverage data".to_string(),
        )),
    }
}

/// Method coverage has type `"m"` and branch coverage has type `"b"`. Line
/// coverage is serialized as `null`, which unfortunately is also what gets
/// serialized when coverage type is omitted entirely.
fn format_coverage_type(coverage_type: &models::CoverageType) -> JsonVal {
    match coverage_type {
        models::CoverageType::Line => JsonVal::Null,
        models::CoverageType::Branch => JsonVal::String("b".to_string()),
        models::CoverageType::Method => JsonVal::String("m".to_string()),
    }
}

/// Complexity is written as either a single integer, representing the total
/// cyclomatic complexity of a method, or a list containing exactly two
/// integers, the first being the number of "complexity paths" hit and the
/// second being the total complexity.
fn format_complexity(
    hit_complexity_paths: &Option<i64>,
    total_complexity: &Option<i64>,
) -> JsonVal {
    match (hit_complexity_paths, total_complexity) {
        (Some(hit_paths), Some(total)) => json!([hit_paths, total]),
        (None, Some(total)) => json!(total),
        (Some(hit_paths), None) => json!(hit_paths),
        (None, None) => JsonVal::Null,
    }
}

/// The data for a single report line in a chunk is spread across multiple rows
/// in the results of `queries/samples_to_chunks.rs`. However, every row
/// contains a copy of certain aggregate metrics for a line. This helper
/// function is given the first row for each line and returns the JSON
/// array that will be written for that line, but only those whole-line fields
/// are filled in. The rest of the array will be filled out by processing the
/// rest of the columns/rows returned for this line.
fn build_report_line_from_row(row: &rusqlite::Row) -> Result<(i64, JsonVal)> {
    let line_no = row.get::<usize, i64>(1)?;
    let coverage_type = row.get::<usize, models::CoverageType>(2)?;
    let hits = row.get::<usize, Option<i64>>(3)?;
    let hit_branches = row.get::<usize, Option<i64>>(4)?;
    let total_branches = row.get::<usize, Option<i64>>(5)?;
    let hit_complexity_paths = row.get::<usize, Option<i64>>(6)?;
    let total_complexity = row.get::<usize, Option<i64>>(7)?;

    let coverage = format_coverage(&hits, &hit_branches, &total_branches)?;
    let coverage_type_json = format_coverage_type(&coverage_type);
    let complexity = format_complexity(&hit_complexity_paths, &total_complexity);
    Ok((
        line_no,
        json!([coverage, coverage_type_json, [], null, complexity, null]),
    ))
}

/// Each report line in a chunk includes a list of measurements for that line
/// taken during different sessions. Each row in the results of
/// `queries/samples_to_chunks.sql` contains those per-session measurements and
/// this helper function returns the JSON value that will be written for them.
fn build_line_session_from_row(row: &rusqlite::Row) -> Result<JsonVal> {
    let session_index = row.get::<usize, i64>(8)?;
    let hits = row.get(10)?;
    let hit_branches = row.get(11)?;
    let total_branches = row.get(12)?;
    let hit_complexity_paths = row.get(13)?;
    let total_complexity = row.get(14)?;

    let coverage = format_coverage(&hits, &hit_branches, &total_branches)?;
    let complexity = format_complexity(&hit_complexity_paths, &total_complexity);

    let mut line_session_values = vec![
        JsonVal::Number(JsonNumber::from(session_index)),
        coverage,
        JsonVal::Null, // missing_branches, may be filled in later
        JsonVal::Null, // partials, may be filled in later
        complexity,
    ];

    // both of these are json
    if let Some(missing_branches) = row.get(15)? {
        line_session_values[2] = json_value_from_sql(missing_branches, 15)?;
    }

    if let Some(partials) = row.get(16)? {
        line_session_values[3] = json_value_from_sql(partials, 16)?;
    }

    // This probably does unnecessary copies
    Ok(array_without_trailing_nulls(JsonVal::Array(
        line_session_values,
    )))
}

/// The primary source for a report line's per-session metrics is its `sessions`
/// field, but some of that information is also copied into its `datapoints`
/// field. A "datapoint" _also_ may contain a list of "labels" that apply to the
/// line/session the datapoint is for. This helper function dutifully copies the
/// redundant information along with that list of labels into a JSON value
/// that will be written as part of the `datapoints` field, or returns `None` if
/// there are no labels.
fn build_datapoint_from_row(row: &rusqlite::Row) -> Result<Option<JsonVal>> {
    let session_index = row.get::<usize, i64>(8)?;
    let labels_raw = row.get::<usize, Option<String>>(17)?;
    if let Some(labels_raw) = labels_raw {
        let coverage_type = row.get::<usize, models::CoverageType>(2)?;
        let hits = row.get::<usize, Option<i64>>(10)?;
        let hit_branches = row.get::<usize, Option<i64>>(11)?;
        let total_branches = row.get::<usize, Option<i64>>(12)?;

        let coverage = format_coverage(&hits, &hit_branches, &total_branches)?;
        let coverage_type_json = format_coverage_type(&coverage_type);
        Ok(Some(json!([
            session_index,
            coverage,
            coverage_type_json,
            json_value_from_sql(labels_raw, 17)?
        ])))
    } else {
        Ok(None)
    }
}

/// Builds a chunks file from a [`SqliteReport`] and writes it to `output_file`.
/// See [`crate::report::pyreport`] for more details about the content and
/// structure of a chunks file.
pub fn sql_to_chunks(report: &SqliteReport, output_file: &mut File) -> Result<()> {
    let chunks_file_header = query_chunks_file_header(report)?;
    let _ = output_file
        .write(format!("{}{}", chunks_file_header, CHUNKS_FILE_HEADER_TERMINATOR).as_bytes())?;

    // TODO: query from chunk_indices rather than samples in case there are chunks
    // with no samples?
    let mut stmt = report
        .conn
        .prepare_cached(include_str!("queries/samples_to_chunks.sql"))?;
    let mut rows = stmt.query([])?;

    let mut current_chunk: Option<i64> = None;
    let mut last_populated_line = 0;

    // Each row in our query results corresponds to a single session, and a line can
    // have several sessions. We build up the current line over many rows, and
    // when we get to a row for a new line, we write the current line and then
    // start building the new one.
    let mut current_report_line: Option<(i64, JsonVal)> = None;

    while let Some(row) = rows.next()? {
        let chunk_index = row.get::<usize, i64>(0)?;
        let line_no = row.get::<usize, i64>(1)?;

        let is_new_chunk = Some(chunk_index) != current_chunk;
        let is_new_line = if let Some((current_line, _)) = &current_report_line {
            *current_line != line_no
        } else {
            false
        };
        if is_new_chunk || is_new_line {
            last_populated_line =
                maybe_write_current_line(current_report_line, output_file, last_populated_line)?;
            current_report_line = Some(build_report_line_from_row(row)?);

            if is_new_chunk {
                // Each chunk has a header which may contain a list of sessions that have
                // measurements for lines in that chunk.
                let present_sessions = row.get(9).and_then(|s| json_value_from_sql(s, 9))?;

                // The first chunk should not be preceded by the `END_OF_CHUNK` header but all
                // others should be.
                let delimiter = if current_chunk.is_none() {
                    ""
                } else {
                    CHUNKS_FILE_END_OF_CHUNK
                };
                let _ = output_file.write(
                    format!(
                        "{}{}",
                        delimiter,
                        json!({"present_sessions": present_sessions})
                    )
                    .as_bytes(),
                )?;
                current_chunk = Some(chunk_index);
                last_populated_line = 0;
            }
        }

        let Some((_, JsonVal::Array(report_line_values))) = &mut current_report_line else {
            return Err(CodecovError::PyreportConversionError(
                "report line is null".to_string(),
            ));
        };
        let Some(JsonVal::Array(line_sessions)) = report_line_values.get_mut(2) else {
            return Err(CodecovError::PyreportConversionError(
                "report line is missing line sessions".to_string(),
            ));
        };
        let session = build_line_session_from_row(row)?;
        line_sessions.push(session);

        // If there are any datapoints for this line session, create/append to the
        // report line's `datapoints` field. Otherwise this should remain null and be
        // stripped.
        if let Some(datapoint) = build_datapoint_from_row(row)? {
            if report_line_values.get(5) == Some(&JsonVal::Null) {
                report_line_values[5] = json!([datapoint]);
            } else if let Some(JsonVal::Array(datapoints)) = report_line_values.get_mut(5) {
                datapoints.push(datapoint);
            }
        }
    }
    // The loop writes each line when it gets to the first row from the next line.
    // There are no rows following the last line, so we have to manually write
    // it here.
    maybe_write_current_line(current_report_line, output_file, last_populated_line)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Seek},
        path::PathBuf,
    };

    use serde_json::{json, json_internal};
    use tempfile::TempDir;

    use super::*;
    use crate::report::{sqlite::SqliteReportBuilder, ReportBuilder};

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
        let file_1 = builder.insert_file("src/report/report.rs".to_string())?;
        let file_2 = builder.insert_file("src/report/models.rs".to_string())?;

        let line_1 = builder.insert_coverage_sample(models::CoverageSample {
            source_file_id: file_1.id,
            line_no: 1,
            coverage_type: models::CoverageType::Line,
            hits: Some(3),
            ..Default::default()
        })?;
        let line_2 = builder.insert_coverage_sample(models::CoverageSample {
            source_file_id: file_2.id,
            line_no: 1,
            coverage_type: models::CoverageType::Line,
            hits: Some(4),
            ..Default::default()
        })?;
        let line_3 = builder.insert_coverage_sample(models::CoverageSample {
            source_file_id: file_2.id,
            line_no: 3,
            coverage_type: models::CoverageType::Line,
            hits: Some(0),
            ..Default::default()
        })?;

        let branch_sample_1 = builder.insert_coverage_sample(models::CoverageSample {
            source_file_id: file_1.id,
            line_no: 3,
            coverage_type: models::CoverageType::Branch,
            hit_branches: Some(2),
            total_branches: Some(2),
            ..Default::default()
        })?;
        let _ = builder.insert_branches_data(models::BranchesData {
            source_file_id: branch_sample_1.source_file_id,
            sample_id: branch_sample_1.id,
            hits: 1,
            branch_format: models::BranchFormat::Condition,
            branch: "0:jump".to_string(),
            ..Default::default()
        })?;
        let _ = builder.insert_branches_data(models::BranchesData {
            source_file_id: branch_sample_1.source_file_id,
            sample_id: branch_sample_1.id,
            hits: 1,
            branch_format: models::BranchFormat::Condition,
            branch: "1".to_string(),
            ..Default::default()
        })?;

        let branch_sample_2 = builder.insert_coverage_sample(models::CoverageSample {
            source_file_id: file_2.id,
            line_no: 6,
            coverage_type: models::CoverageType::Branch,
            hit_branches: Some(2),
            total_branches: Some(4),
            ..Default::default()
        })?;
        let _ = builder.insert_branches_data(models::BranchesData {
            source_file_id: branch_sample_2.source_file_id,
            sample_id: branch_sample_2.id,
            hits: 1,
            branch_format: models::BranchFormat::Condition,
            branch: "0:jump".to_string(),
            ..Default::default()
        })?;
        let _ = builder.insert_branches_data(models::BranchesData {
            source_file_id: branch_sample_2.source_file_id,
            sample_id: branch_sample_2.id,
            hits: 1,
            branch_format: models::BranchFormat::Condition,
            branch: "1".to_string(),
            ..Default::default()
        })?;
        let _ = builder.insert_branches_data(models::BranchesData {
            source_file_id: branch_sample_2.source_file_id,
            sample_id: branch_sample_2.id,
            hits: 0,
            branch_format: models::BranchFormat::Condition,
            branch: "2".to_string(),
            ..Default::default()
        })?;
        let _ = builder.insert_branches_data(models::BranchesData {
            source_file_id: branch_sample_2.source_file_id,
            sample_id: branch_sample_2.id,
            hits: 0,
            branch_format: models::BranchFormat::Condition,
            branch: "3".to_string(),
            ..Default::default()
        })?;

        let method_sample_1 = builder.insert_coverage_sample(models::CoverageSample {
            source_file_id: file_1.id,
            line_no: 2,
            coverage_type: models::CoverageType::Method,
            hits: Some(2),
            ..Default::default()
        })?;
        let _ = builder.insert_method_data(models::MethodData {
            source_file_id: method_sample_1.source_file_id,
            sample_id: Some(method_sample_1.id),
            line_no: Some(method_sample_1.line_no),
            hit_branches: Some(2),
            total_branches: Some(4),
            hit_complexity_paths: Some(2),
            total_complexity: Some(4),
            ..Default::default()
        })?;

        let method_sample_2 = builder.insert_coverage_sample(models::CoverageSample {
            source_file_id: file_2.id,
            line_no: 2,
            coverage_type: models::CoverageType::Method,
            hits: Some(5),
            ..Default::default()
        })?;
        let _ = builder.insert_method_data(models::MethodData {
            source_file_id: method_sample_2.source_file_id,
            sample_id: Some(method_sample_2.id),
            line_no: Some(method_sample_2.line_no),
            hit_branches: Some(2),
            total_branches: Some(4),
            ..Default::default()
        })?;

        let method_sample_3 = builder.insert_coverage_sample(models::CoverageSample {
            source_file_id: file_2.id,
            line_no: 5,
            coverage_type: models::CoverageType::Method,
            hits: Some(0),
            ..Default::default()
        })?;
        let _ = builder.insert_method_data(models::MethodData {
            source_file_id: method_sample_3.source_file_id,
            sample_id: Some(method_sample_3.id),
            line_no: Some(method_sample_3.line_no),
            hit_complexity_paths: Some(2),
            total_complexity: Some(4),
            ..Default::default()
        })?;

        let line_with_partial_1 = builder.insert_coverage_sample(models::CoverageSample {
            source_file_id: file_1.id,
            line_no: 8,
            coverage_type: models::CoverageType::Line,
            hits: Some(3),
            ..Default::default()
        })?;
        let _ = builder.insert_span_data(models::SpanData {
            source_file_id: line_with_partial_1.source_file_id,
            sample_id: Some(line_with_partial_1.id),
            start_line: Some(line_with_partial_1.line_no),
            start_col: Some(3),
            end_line: Some(line_with_partial_1.line_no),
            end_col: None,
            hits: 3,
            ..Default::default()
        })?;

        let upload_1 = builder.insert_context(models::ContextType::Upload, "codecov-rs CI")?;
        let _ = builder.associate_context(models::ContextAssoc {
            context_id: upload_1.id,
            sample_id: Some(line_1.id),
            ..Default::default()
        })?;
        let _ = builder.associate_context(models::ContextAssoc {
            context_id: upload_1.id,
            sample_id: Some(line_2.id),
            ..Default::default()
        })?;
        let _ = builder.associate_context(models::ContextAssoc {
            context_id: upload_1.id,
            sample_id: Some(branch_sample_1.id),
            ..Default::default()
        })?;
        let _ = builder.associate_context(models::ContextAssoc {
            context_id: upload_1.id,
            sample_id: Some(branch_sample_2.id),
            ..Default::default()
        })?;
        let _ = builder.associate_context(models::ContextAssoc {
            context_id: upload_1.id,
            sample_id: Some(method_sample_1.id),
            ..Default::default()
        })?;
        let _ = builder.associate_context(models::ContextAssoc {
            context_id: upload_1.id,
            sample_id: Some(method_sample_2.id),
            ..Default::default()
        })?;
        let _ = builder.associate_context(models::ContextAssoc {
            context_id: upload_1.id,
            sample_id: Some(line_with_partial_1.id),
            ..Default::default()
        })?;

        let upload_2 = builder.insert_context(models::ContextType::Upload, "codecov-rs CI 2")?;
        let _ = builder.associate_context(models::ContextAssoc {
            context_id: upload_2.id,
            sample_id: Some(line_3.id),
            ..Default::default()
        })?;
        let _ = builder.associate_context(models::ContextAssoc {
            context_id: upload_2.id,
            sample_id: Some(method_sample_3.id),
            ..Default::default()
        })?;

        let label_1 = builder.insert_context(models::ContextType::TestCase, "test-case")?;
        let _ = builder.associate_context(models::ContextAssoc {
            context_id: label_1.id,
            sample_id: Some(line_1.id),
            ..Default::default()
        })?;
        let _ = builder.associate_context(models::ContextAssoc {
            context_id: label_1.id,
            sample_id: Some(line_2.id),
            ..Default::default()
        })?;
        let label_2 = builder.insert_context(models::ContextType::TestCase, "test-case 2")?;
        let _ = builder.associate_context(models::ContextAssoc {
            context_id: label_2.id,
            sample_id: Some(line_1.id),
            ..Default::default()
        })?;
        let _ = builder.associate_context(models::ContextAssoc {
            context_id: label_2.id,
            sample_id: Some(line_2.id),
            ..Default::default()
        })?;
        let _ = builder.associate_context(models::ContextAssoc {
            context_id: label_2.id,
            sample_id: Some(method_sample_1.id),
            ..Default::default()
        })?;

        let _ = builder.insert_upload_details(models::UploadDetails {
            context_id: upload_1.id,
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
        })?;
        let _ = builder.insert_upload_details(models::UploadDetails {
            context_id: upload_2.id,
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
        })?;

        Ok(builder.build())
    }

    #[test]
    fn test_array_without_trailing_nulls() {
        assert_eq!(
            array_without_trailing_nulls(json!([1, null, [[0, 1]]])),
            json!([1, null, [[0, 1]]])
        );
        assert_eq!(
            array_without_trailing_nulls(json!([1, null, [[0, 1]], null, null])),
            json!([1, null, [[0, 1]]])
        );
        assert_eq!(array_without_trailing_nulls(json!([])), json!([]));
        assert_eq!(array_without_trailing_nulls(json!([null])), json!([]));
    }

    #[test]
    fn test_build_datapoint_from_row() {
        let ctx = setup();
        let db_path = ctx.temp_dir.path().join("test.db");
        let report = SqliteReport::new(db_path).unwrap();

        let test_cases: &[(&[&dyn rusqlite::ToSql], Option<JsonVal>)] = &[
            (
                rusqlite::params![
                    models::CoverageType::Line,
                    0,
                    Some(3),
                    None::<Option<i64>>,
                    None::<Option<i64>>,
                    // If no labels are present, don't build a datapoint
                    None::<Option<&str>>,
                ],
                None,
            ),
            (
                rusqlite::params![
                    models::CoverageType::Line,
                    0,
                    Some(3),
                    None::<Option<i64>>,
                    None::<Option<i64>>,
                    // If labels are present, build a datapoint
                    "[\"label1\", \"label2\"]",
                ],
                Some(json!([0, 3, null, ["label1", "label2"]])),
            ),
            (
                rusqlite::params![
                    models::CoverageType::Branch,
                    0,
                    None::<Option<i64>>,
                    Some(2),
                    Some(4),
                    // If labels are present, build a datapoint
                    "[\"label1\", \"label2\"]",
                ],
                Some(json!([0, "2/4", "b", ["label1", "label2"]])),
            ),
            (
                rusqlite::params![
                    models::CoverageType::Method,
                    0,
                    Some(3),
                    None::<Option<i64>>,
                    None::<Option<i64>>,
                    // If labels are present, build a datapoint
                    "[\"label1\", \"label2\"]",
                ],
                Some(json!([0, 3, "m", ["label1", "label2"]])),
            ),
        ];
        let query = "select 0, 1, ?1, 3, 4, 5, 6, 7, ?2, 9, ?3, ?4, ?5, 13, 14, 15, 16, ?6";
        for test_case in test_cases {
            assert_eq!(
                report
                    .conn
                    .query_row_and_then(query, test_case.0, |row| { build_datapoint_from_row(row) })
                    .unwrap(),
                test_case.1
            );
        }
    }

    #[test]
    fn test_build_line_session_from_row() {
        let ctx = setup();
        let db_path = ctx.temp_dir.path().join("test.db");
        let report = SqliteReport::new(db_path).unwrap();

        let test_cases: &[(&[&dyn rusqlite::ToSql], JsonVal)] = &[
            (
                rusqlite::params![
                    0,
                    None::<Option<i64>>,
                    Some(2),
                    Some(4),
                    None::<Option<i64>>,
                    None::<Option<i64>>,
                    None::<Option<&str>>,
                    None::<Option<&str>>,
                ],
                json!([0, "2/4"]),
            ),
            (
                rusqlite::params![
                    0,
                    Some(3),
                    None::<Option<i64>>,
                    None::<Option<i64>>,
                    None::<Option<i64>>,
                    None::<Option<i64>>,
                    None::<Option<&str>>,
                    None::<Option<&str>>,
                ],
                json!([0, 3]),
            ),
            (
                rusqlite::params![
                    0,
                    Some(3),
                    None::<Option<i64>>,
                    None::<Option<i64>>,
                    Some(2),
                    Some(4),
                    None::<Option<&str>>,
                    None::<Option<&str>>,
                ],
                json!([0, 3, null, null, [2, 4]]),
            ),
            (
                rusqlite::params![
                    0,
                    Some(3),
                    None::<Option<i64>>,
                    None::<Option<i64>>,
                    None::<Option<i64>>,
                    None::<Option<i64>>,
                    "[\"0:jump\", \"1\"]",
                    None::<Option<&str>>,
                ],
                json!([0, 3, ["0:jump", "1"]]),
            ),
            (
                rusqlite::params![
                    0,
                    Some(3),
                    None::<Option<i64>>,
                    None::<Option<i64>>,
                    None::<Option<i64>>,
                    None::<Option<i64>>,
                    None::<Option<&str>>,
                    "[[0, 3, 3], [4, 5, 0]]"
                ],
                json!([0, 3, null, [[0, 3, 3], [4, 5, 0]]]),
            ),
        ];
        let query = "select 0, 1, 2, 3, 4, 5, 6, 7, ?1, 9, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 17";
        for test_case in test_cases {
            assert_eq!(
                report
                    .conn
                    .query_row_and_then(query, test_case.0, |row| {
                        build_line_session_from_row(row)
                    })
                    .unwrap(),
                test_case.1
            );
        }
    }

    #[test]
    fn test_build_report_line_from_row() {
        let ctx = setup();
        let db_path = ctx.temp_dir.path().join("test.db");
        let report = SqliteReport::new(db_path).unwrap();

        let test_cases: &[(&[&dyn rusqlite::ToSql], (i64, JsonVal))] = &[
            (
                rusqlite::params![
                    1,
                    models::CoverageType::Line,
                    Some(3),
                    None::<Option<i64>>,
                    None::<Option<i64>>,
                    None::<Option<i64>>,
                    None::<Option<i64>>,
                ],
                (1, json!([3, null, [], null, null, null])),
            ),
            (
                rusqlite::params![
                    2,
                    models::CoverageType::Method,
                    Some(3),
                    None::<Option<i64>>,
                    None::<Option<i64>>,
                    Some(2),
                    Some(4),
                ],
                (2, json!([3, "m", [], null, [2, 4], null])),
            ),
            (
                rusqlite::params![
                    3,
                    models::CoverageType::Branch,
                    None::<Option<i64>>,
                    Some(2),
                    Some(4),
                    None::<Option<i64>>,
                    None::<Option<i64>>,
                ],
                (3, json!(["2/4", "b", [], null, null, null])),
            ),
        ];
        let query = "select 0, ?1, ?2, ?3, ?4, ?5, ?6, ?7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17";
        for test_case in test_cases {
            assert_eq!(
                report
                    .conn
                    .query_row_and_then(query, test_case.0, |row| {
                        build_report_line_from_row(row)
                    })
                    .unwrap(),
                test_case.1
            );
        }
    }

    #[test]
    fn test_format_complexity() {
        assert_eq!(format_complexity(&Some(2), &Some(4)), json!([2, 4]));
        assert_eq!(format_complexity(&Some(2), &None), json!(2));
        assert_eq!(format_complexity(&None, &Some(4)), json!(4));
        assert_eq!(format_complexity(&None, &None), json!(null));
    }

    #[test]
    fn test_format_coverage() {
        // Good inputs
        assert_eq!(format_coverage(&Some(3), &None, &None).unwrap(), json!(3));
        assert_eq!(
            format_coverage(&None, &Some(2), &Some(4)).unwrap(),
            json!("2/4")
        );

        // Malformed
        assert!(
            format_coverage(&None, &Some(2), &None).is_err_and(|e| match e {
                CodecovError::PyreportConversionError(s) =>
                    s == "incomplete coverage data".to_string(),
                _ => false,
            })
        );
        assert!(
            format_coverage(&None, &None, &Some(4)).is_err_and(|e| match e {
                CodecovError::PyreportConversionError(s) =>
                    s == "incomplete coverage data".to_string(),
                _ => false,
            })
        );
        assert!(
            format_coverage(&None, &None, &None).is_err_and(|e| match e {
                CodecovError::PyreportConversionError(s) =>
                    s == "incomplete coverage data".to_string(),
                _ => false,
            })
        );
    }

    #[test]
    fn test_format_coverage_type() {
        assert_eq!(
            format_coverage_type(&models::CoverageType::Line),
            json!(null)
        );
        assert_eq!(
            format_coverage_type(&models::CoverageType::Branch),
            json!("b")
        );
        assert_eq!(
            format_coverage_type(&models::CoverageType::Method),
            json!("m")
        );
    }

    #[test]
    fn test_maybe_write_current_line() {
        let ctx = setup();

        let test_cases = [
            (None, 1, 1, ""),
            (None, 2, 2, ""),
            (Some((3, json!(["foo"]))), 1, 3, "\n\n[\"foo\"]"),
            (Some((7, json!(["foo"]))), 5, 7, "\n\n[\"foo\"]"),
            (Some((7, json!(["foo"]))), 7, 7, "\n[\"foo\"]"),
            // Shouldn't happen, just documenting behavior
            (Some((7, json!(["foo"]))), 8, 7, "\n[\"foo\"]"),
        ];
        for test_case in test_cases {
            let mut dummy_file = File::options()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(ctx.temp_dir.path().join("dummy.txt"))
                .unwrap();

            let last_populated_line =
                maybe_write_current_line(test_case.0, &mut dummy_file, test_case.1).unwrap();
            assert_eq!(last_populated_line, test_case.2);

            let _ = dummy_file.rewind();
            let mut file_str = String::new();
            let _ = dummy_file.read_to_string(&mut file_str);
            assert_eq!(file_str, test_case.3);
        }
    }

    #[test]
    fn test_query_chunks_file_header() {
        let ctx = setup();
        let db_path = ctx.temp_dir.path().join("test.db");
        let report = build_sample_report(db_path).unwrap();

        assert_eq!(
            query_chunks_file_header(&report).unwrap(),
            json!({"labels_index": {"1": "test-case", "2": "test-case 2"}})
        );

        let empty_report = SqliteReport::new(ctx.temp_dir.path().join("empty.db")).unwrap();
        assert_eq!(query_chunks_file_header(&empty_report).unwrap(), json!({}),);
    }

    #[test]
    fn test_sql_to_chunks() {
        let ctx = setup();
        let report = build_sample_report(ctx.temp_dir.path().join("db.sqlite")).unwrap();
        let chunks_path = ctx.temp_dir.path().join("chunks.txt");
        let mut chunks_file = File::options()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&chunks_path)
            .unwrap();

        sql_to_chunks(&report, &mut chunks_file).unwrap();

        let mut chunks = String::new();
        let _ = chunks_file.rewind().unwrap();
        let _ = chunks_file.read_to_string(&mut chunks).unwrap();

        let chunks_header = json!({"labels_index": {"1": "test-case", "2": "test-case 2"}});
        // line_1 variable in build_sample_report()
        let file_1_header = json!({"present_sessions": [1]});
        let file_1_line_1 = json!([
            3,
            null,
            [[1, 3]],
            null,
            null,
            [[1, 3, null, ["test-case", "test-case 2"]]]
        ]);
        // method_sample_1 variable in build_sample_report()
        let file_1_line_2 = json!([
            2,
            "m",
            [[1, 2, null, null, [2, 4]]],
            null,
            [2, 4],
            [[1, 2, "m", ["test-case 2"]]]
        ]);
        // branch_sample_1 variable in build_sample_report()
        let file_1_line_3 = json!(["2/2", "b", [[1, "2/2"]]]);
        // line_with_partial_1 variable in build_sample_report()
        let file_1_line_8 = json!([3, null, [[1, 3, null, [[3, null, 3]]]]]);

        let file_2_header = json!({"present_sessions": [0, 1]});
        // line_2 variable in build_sample_report()
        let file_2_line_1 = json!([
            4,
            null,
            [[1, 4]],
            null,
            null,
            [[1, 4, null, ["test-case", "test-case 2"]]]
        ]);
        // method_sample_2 variable in build_sample_report()
        let file_2_line_2 = json!([5, "m", [[1, 5]]]);
        // line_3 variable in build_sample_report()
        let file_2_line_3 = json!([0, null, [[0, 0]],]);
        // method_sample_3 variable in build_sample_report()
        let file_2_line_5 = json!([0, "m", [[0, 0, null, null, [2, 4]]], null, [2, 4]]);
        // branch_sample_2 variable in build_sample_report()
        let file_2_line_6 = json!(["2/4", "b", [[1, "2/4", ["2", "3"]]],]);

        let expected = format!(
            "{chunks_header}
<<<<< end_of_header >>>>>
{file_2_header}
{file_2_line_1}
{file_2_line_2}
{file_2_line_3}

{file_2_line_5}
{file_2_line_6}
<<<<< end_of_chunk >>>>>
{file_1_header}
{file_1_line_1}
{file_1_line_2}
{file_1_line_3}




{file_1_line_8}"
        );

        // Leaving this here because it makes debugging easier if this breaks later
        for (i, (l, r)) in std::iter::zip(chunks.lines(), expected.lines()).enumerate() {
            println!("actual {}  : {}", i, l);
            println!("expected {}: {}", i, r);
            println!("actual == expected: {}", l == r);
            println!();
        }

        assert_eq!(chunks, expected);
    }
}
