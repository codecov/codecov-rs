use std::{fs::File, io::Write};

use serde_json::{json, json_internal};

use super::{CHUNKS_FILE_END_OF_CHUNK, CHUNKS_FILE_HEADER_TERMINATOR};
use crate::{
    error::{CodecovError, Result},
    parsers::json::{JsonNumber, JsonVal},
    report::{models, sqlite::json_value_from_sql, SqliteReport},
};

fn array_without_trailing_nulls(mut json_array: JsonVal) -> JsonVal {
    if let JsonVal::Array(vals) = &mut json_array {
        for (i, val) in vals.iter().enumerate().rev() {
            if val != &JsonVal::Null {
                return JsonVal::Array(vals[0..=i].into());
            }
        }
    }
    json_array
}

fn query_chunks_file_header(report: &SqliteReport) -> Result<JsonVal> {
    let mut stmt = report
        .conn
        .prepare_cached(include_str!("queries/chunks_file_header.sql"))?;
    Ok(stmt.query_row([], |row| row.get(0).and_then(|s| json_value_from_sql(s, 0)))?)
}

fn maybe_write_current_line(
    current_line: Option<(i64, JsonVal)>,
    output_file: &mut File,
    last_populated_line: i64,
) -> Result<i64> {
    if let Some((line_no, line_values)) = current_line {
        for _ in last_populated_line..line_no - 1 {
            let _ = output_file.write("\n".as_bytes())?;
        }
        let _ = output_file
            .write(format!("\n{}", array_without_trailing_nulls(line_values)).as_bytes())?;
        Ok(line_no)
    } else {
        // We don't have a new line to print. Return the old value for
        // `last_populated_line`
        Ok(last_populated_line)
    }
}

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

fn format_coverage_type(coverage_type: &models::CoverageType) -> JsonVal {
    match coverage_type {
        models::CoverageType::Line => JsonVal::Null,
        models::CoverageType::Branch => JsonVal::String("b".to_string()),
        models::CoverageType::Method => JsonVal::String("m".to_string()),
    }
}

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
        JsonVal::Null,
        JsonVal::Null,
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

fn build_datapoint_from_row(row: &rusqlite::Row) -> Result<Option<JsonVal>> {
    let session_index = row.get::<usize, i64>(8)?;
    let labels_raw = row.get::<usize, Option<String>>(16)?;
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
                let present_sessions = row.get(9).and_then(|s| json_value_from_sql(s, 9))?;
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
