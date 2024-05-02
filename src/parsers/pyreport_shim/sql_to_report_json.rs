use std::{fs::File, io::Write};

use serde_json::{json, json_internal};

use crate::{
    error::{CodecovError, Result},
    parsers::json::JsonVal,
    report::{models, models::json_value_from_sql, SqliteReport},
};

fn calculate_coverage_pct(hits: i64, lines: i64) -> String {
    match (hits, lines) {
        (0, _) => 0.to_string(),
        (h, l) if h == l => 100.to_string(),
        (h, l) => format!("{:.5}", h as f64 / l as f64 * 100.0),
    }
}

fn sql_to_files_dict(report: &SqliteReport, output_file: &mut File) -> Result<()> {
    let mut stmt = report
        .conn
        .prepare_cached(include_str!("queries/files_to_report_json.sql"))?;
    let mut rows = stmt.query([])?;

    fn maybe_write_current_file(
        current_file: &Option<(String, JsonVal)>,
        output_file: &mut File,
        first_file: bool,
    ) -> Result<()> {
        if let Some((current_path, current_file)) = &current_file {
            let delimiter = if first_file { "" } else { "," };
            let _ = output_file
                .write(format!("{}\"{}\": {}", delimiter, current_path, current_file).as_bytes())?;
        }
        Ok(())
    }

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
            json!([chunk_index, totals, {"meta": {"session_count": 0}}, JsonVal::Null]),
        ))
    }

    fn build_session_totals_from_row(row: &rusqlite::Row) -> Result<(String, JsonVal)> {
        let session_id = row.get(11)?;
        let lines = row.get::<usize, i64>(12)?;
        let hits = row.get(13)?;
        let misses = row.get::<usize, i64>(14)?;
        let partials = row.get::<usize, i64>(15)?;

        let coverage_pct = calculate_coverage_pct(hits, lines);
        Ok((
            session_id,
            json!([
                0, // file_count
                lines,
                hits,
                misses,
                partials,
                coverage_pct,
            ]),
        ))
    }

    // Write the "files" key to the output file and build its value by iterating
    // over our query results. It's the caller's responsibility to write
    // surroundings {}s or ,s as needed.
    let _ = output_file.write("\"files\": {".as_bytes())?;

    // Each row in our query results corresponds to a single session, and a file can
    // have several sessions. We build up the current file over many rows, and
    // when we get to a row for a new file, we write the current file and then
    // start building the new one.
    let mut current_file: Option<(String, JsonVal)> = None;
    let mut first_file = true;
    while let Some(row) = rows.next()? {
        let chunk_index = row.get(0)?;
        let is_new_file = if let Some((_, JsonVal::Array(v))) = &current_file {
            v[0].as_u64() != chunk_index
        } else {
            true
        };
        if is_new_file {
            maybe_write_current_file(&current_file, output_file, first_file)?;
            first_file = current_file.is_none();
            current_file = Some(build_file_from_row(row)?);
        }

        let (session_id, session_totals) = build_session_totals_from_row(row)?;

        let Some((_, JsonVal::Array(file_values))) = &mut current_file else {
            return Err(CodecovError::PyreportConversionError(
                "current file is null".to_string(),
            ));
        };

        let Some(JsonVal::Object(session_map)) = file_values.get_mut(2) else {
            return Err(CodecovError::PyreportConversionError(
                "current file is missing session map".to_string(),
            ));
        };

        session_map.insert(session_id, session_totals);

        let meta = session_map
            .get_mut("meta")
            .unwrap()
            .as_object_mut()
            .unwrap();
        let session_count = meta.get("session_count").unwrap().as_i64().unwrap();
        meta.insert("session_count".into(), JsonVal::from(session_count + 1));
    }
    // The loop writes each file when it gets to the first row from the next file.
    // There are no rows following the last file, so we have to manually write
    // it here.
    maybe_write_current_file(&current_file, output_file, first_file)?;

    let _ = output_file.write("}".as_bytes())?;
    Ok(())
}

fn sql_to_sessions_dict(report: &SqliteReport, output_file: &mut File) -> Result<()> {
    let mut stmt = report
        .conn
        .prepare_cached(include_str!("queries/sessions_to_report_json.sql"))?;
    let mut rows = stmt.query([])?;

    // Each row represents a session and this helper function returns the key/value
    // pair that will be written into the sessions dict for that session.
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

        let upload_details = models::UploadDetails {
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
                "d": upload_details.timestamp,
                "a": upload_details.raw_upload_url,
                "f": upload_details.flags,
                "c": upload_details.provider,
                "n": upload_details.build,
                "N": upload_details.name,
                "j": upload_details.job_name,
                "u": upload_details.ci_run_url,
                "p": upload_details.state,
                "e": upload_details.env,
                "st": upload_details.session_type,
                "se": upload_details.session_extras,
            }),
        ))
    }

    // Write the "sessions" key to the output file and build its value by iterating
    // over our query results. It's the caller's responsibility to write
    // surroundings {}s or ,s as needed.
    let _ = output_file.write("\"sessions\": {".as_bytes())?;
    let mut first_session = true;
    while let Some(row) = rows.next()? {
        let (session_id, session) = build_session_from_row(row)?;
        // No preceding , for the first session we write
        let delimiter = if first_session { "" } else { "," };
        let _ = output_file
            .write(format!("{}\"{}\": {}", delimiter, session_id, session).as_bytes())?;
        first_session = false;
    }

    let _ = output_file.write("}".as_bytes())?;
    Ok(())
}

pub fn sql_to_report_json(report: &SqliteReport, output_file: &mut File) -> Result<()> {
    let _ = output_file.write("{".as_bytes())?;
    sql_to_files_dict(report, output_file)?;
    let _ = output_file.write(",".as_bytes())?;
    sql_to_sessions_dict(report, output_file)?;
    let _ = output_file.write("}".as_bytes())?;

    Ok(())
}
