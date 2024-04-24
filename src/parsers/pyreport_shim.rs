use std::{fs::File, path::PathBuf};

use memmap2::Mmap;

use crate::{
    parsers::ReportBuilderCtx,
    report::{ReportBuilder, SqliteReport, SqliteReportBuilder},
};

pub mod report_json;

pub mod chunks;

/// Parses the two parts of our Python report class and reshapes the data into a
/// `SqliteReport`.
///
/// Reports in our Python codebase are serialized in two parts:
/// - Report JSON, which describes the files and sessions in the report
/// - Chunks file, which describes line-by-line coverage data for each file
///
/// The parser for the report JSON inserts a
/// [`crate::report::models::SourceFile`] for each file
/// and a [`crate::report::models::Context`] for each session. It returns two
/// hashmaps: one which maps each file's "chunk index" to the database PK for
/// the `SourceFile` that was inserted for it, and one which maps each session's
/// "session_id" to the database PK for the `Context` that was inserted for it.
///
/// The parser for the chunks file inserts a
/// [`crate::report::models::CoverageSample`] (and possibly other records) for
/// each coverage measurement contained in the chunks file. It uses the
/// results of the report JSON parser to figure out the appropriate FKs to
/// associate a measurement with its `SourceFile` and `Context`(s).
///
/// TODO: Make this unit testable (currently relying on integration tests)
pub fn parse_pyreport(
    report_json_file: &File,
    chunks_file: &File,
    out_path: PathBuf,
) -> Result<SqliteReport, std::io::Error> {
    let report_builder = SqliteReportBuilder::new(out_path);

    // Memory-map the input file so we don't have to read the whole thing into RAM
    let mmap_handle = unsafe { Mmap::map(report_json_file)? };
    let buf = unsafe { std::str::from_utf8_unchecked(&mmap_handle[..]) };
    let mut stream = report_json::ReportOutputStream::<&str, SqliteReport, SqliteReportBuilder> {
        input: buf,
        state: ReportBuilderCtx::new(report_builder),
    };
    // TODO handle error
    let (files, sessions) =
        report_json::parse_report_json(&mut stream).expect("Failed to parse report JSON");

    // Replace our mmap handle so the first one can be unmapped
    let mmap_handle = unsafe { Mmap::map(chunks_file)? };
    let buf = unsafe { std::str::from_utf8_unchecked(&mmap_handle[..]) };

    // Move `report_builder` from the report JSON's parse context to this one
    let chunks_ctx = chunks::ParseCtx::new(stream.state.report_builder, files, sessions);
    let mut chunks_stream = chunks::ReportOutputStream::<&str, SqliteReport, SqliteReportBuilder> {
        input: buf,
        state: chunks_ctx,
    };
    // TODO handle error
    chunks::parse_chunks_file(&mut chunks_stream).expect("Failed to parse chunks file");

    // Build and return the `SqliteReport`
    Ok(chunks_stream.state.db.report_builder.build())
}
