use std::fs::File;

use memmap2::Mmap;

use crate::{error::Result, report::SqliteReportBuilder};

pub mod chunks;
pub mod report_json;

mod utils;

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
    report_builder: &mut SqliteReportBuilder,
) -> Result<()> {
    let mut report_builder_tx = report_builder.transaction()?;

    // Memory-map the input file so we don't have to read the whole thing into RAM
    let report_json_file = unsafe { Mmap::map(report_json_file)? };
    let report_json = report_json::parse_report_json(&report_json_file, &mut report_builder_tx)?;

    // Replace our mmap handle so the first one can be unmapped
    let chunks_file = unsafe { Mmap::map(chunks_file)? };

    chunks::parse_chunks_file(&chunks_file, report_json, report_builder_tx)?;

    Ok(())
}
