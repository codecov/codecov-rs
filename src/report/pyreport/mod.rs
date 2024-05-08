/*!
 * Types and functions to interoperate with our Python report format.
 *
 * Reports in our Python codebase are serialized in two parts:
 * - Report JSON, which describes the files and sessions in the report
 * - Chunks file, which describes line-by-line coverage data for each file
 *
 * Parsers that will build a [`SQLiteReport`] from these parts live in
 * [`crate::parsers::pyreport`] but code that will convert a
 * [`SQLiteReport`] back into a Pyreport lives here.
 */

use std::fs::File;

use super::SqliteReport;
use crate::error::Result;

mod chunks;
mod report_json;

pub(crate) const CHUNKS_FILE_HEADER_TERMINATOR: &str = "\n<<<<< end_of_header >>>>>\n";
pub(crate) const CHUNKS_FILE_END_OF_CHUNK: &str = "\n<<<<< end_of_chunk >>>>>\n";

pub trait ToPyreport {
    /// Format and write the contents of a [`SqliteReport`] to
    /// `report_json_file` and `chunks_file`.
    fn to_pyreport(&self, report_json_file: &mut File, chunks_file: &mut File) -> Result<()>;
}

impl ToPyreport for SqliteReport {
    fn to_pyreport(&self, report_json_file: &mut File, chunks_file: &mut File) -> Result<()> {
        report_json::sql_to_report_json(self, report_json_file)?;
        chunks::sql_to_chunks(self, chunks_file)?;
        Ok(())
    }
}
