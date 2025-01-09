use std::fs::File;
use std::path::PathBuf;

use codecov_rs::{parsers, report};
use pyo3::prelude::*;

use crate::error::PyCodecovError;

mod error;

#[pyclass]
pub struct SqliteReportBuilder(report::SqliteReportBuilder);

#[pymethods]
impl SqliteReportBuilder {
    pub fn filepath(&self) -> PyResult<&PathBuf> {
        Ok(&self.0.filename)
    }

    #[staticmethod]
    #[pyo3(signature = (report_json_filepath, chunks_filepath, out_path))]
    pub fn from_pyreport(
        report_json_filepath: &str,
        chunks_filepath: &str,
        out_path: &str,
    ) -> PyResult<SqliteReportBuilder> {
        let mut report_builder =
            report::SqliteReportBuilder::open(out_path.into()).map_err(PyCodecovError::from)?;

        let report_json_file = File::open(report_json_filepath)?;
        let chunks_file = File::open(chunks_filepath)?;
        parsers::pyreport::parse_pyreport(&report_json_file, &chunks_file, &mut report_builder)
            .map_err(PyCodecovError::from)?;
        Ok(SqliteReportBuilder(report_builder))
    }
}

#[pymodule]
fn _bindings(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<SqliteReportBuilder>()?;
    Ok(())
}
