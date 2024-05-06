#[cfg(test)]
use mockall::automock;

pub mod models;

mod sqlite_report;
pub use sqlite_report::*;

use crate::error::Result;

/// Aggregated coverage metrics for lines, branches, and sessions in a report
/// (or filtered subset).
#[derive(PartialEq, Debug)]
pub struct CoverageTotals {
    /// The number of lines that were hit in this report/subset.
    hit_lines: u64,

    /// The total number of lines tracked in this report/subset.
    total_lines: u64,

    /// The number of branch paths that were hit in this report/subset.
    hit_branches: u64,

    /// The number of possible branch paths tracked in this report/subset.
    total_branches: u64,

    /// The number of branch roots tracked in this report/subset.
    total_branch_roots: u64,

    /// The number of methods that were hit in this report/subset.
    hit_methods: u64,

    /// The number of methods tracked in this report/subset.
    total_methods: u64,

    /// The number of possible cyclomathic paths hit in this report/subset.
    hit_complexity_paths: u64,

    /// The total cyclomatic complexity of code tracked in this report/subset.
    total_complexity: u64,
}

impl<'a> std::convert::TryFrom<&'a rusqlite::Row<'a>> for CoverageTotals {
    type Error = rusqlite::Error;

    fn try_from(row: &'a ::rusqlite::Row) -> Result<Self, Self::Error> {
        Ok(Self {
            hit_lines: row.get(row.as_ref().column_index("hit_lines")?)?,
            total_lines: row.get(row.as_ref().column_index("total_lines")?)?,
            hit_branches: row.get(row.as_ref().column_index("hit_branches")?)?,
            total_branches: row.get(row.as_ref().column_index("total_branches")?)?,
            total_branch_roots: row.get(row.as_ref().column_index("total_branch_roots")?)?,
            hit_methods: row.get(row.as_ref().column_index("hit_methods")?)?,
            total_methods: row.get(row.as_ref().column_index("total_methods")?)?,
            hit_complexity_paths: row.get(row.as_ref().column_index("hit_complexity_paths")?)?,
            total_complexity: row.get(row.as_ref().column_index("total_complexity")?)?,
        })
    }
}

#[derive(PartialEq, Debug)]
pub struct ReportTotals {
    /// Number of files with data in this aggregation.
    pub files: u64,

    /// Number of uploads with data in this aggregation.
    pub uploads: u64,

    /// Number of test cases with data in this aggregation.
    pub test_cases: u64,

    /// Aggregated coverage data.
    pub coverage: CoverageTotals,
}

impl<'a> std::convert::TryFrom<&'a rusqlite::Row<'a>> for ReportTotals {
    type Error = rusqlite::Error;

    fn try_from(row: &'a ::rusqlite::Row) -> Result<Self, Self::Error> {
        Ok(Self {
            files: row.get(row.as_ref().column_index("file_count")?)?,
            uploads: row.get(row.as_ref().column_index("upload_count")?)?,
            test_cases: row.get(row.as_ref().column_index("test_case_count")?)?,
            coverage: row.try_into()?,
        })
    }
}

#[cfg_attr(test, automock)]
pub trait Report {
    fn list_files(&self) -> Result<Vec<models::SourceFile>>;
    fn list_contexts(&self) -> Result<Vec<models::Context>>;
    fn list_coverage_samples(&self) -> Result<Vec<models::CoverageSample>>;
    fn list_contexts_for_sample(
        &self,
        sample: &models::CoverageSample,
    ) -> Result<Vec<models::Context>>;
    fn list_samples_for_file(
        &self,
        file: &models::SourceFile,
    ) -> Result<Vec<models::CoverageSample>>;
    fn get_details_for_upload(&self, upload: &models::Context) -> Result<models::UploadDetails>;

    fn merge(&mut self, other: &Self) -> Result<()>;

    fn totals(&self) -> Result<ReportTotals>;
}

#[cfg_attr(test, automock)]
pub trait ReportBuilder<R: Report> {
    fn insert_file(&mut self, path: String) -> Result<models::SourceFile>;
    fn insert_context(
        &mut self,
        context_type: models::ContextType,
        name: &str,
    ) -> Result<models::Context>;

    fn insert_coverage_sample(
        &mut self,
        sample: models::CoverageSample,
    ) -> Result<models::CoverageSample>;

    fn insert_branches_data(
        &mut self,
        branch: models::BranchesData,
    ) -> Result<models::BranchesData>;

    fn insert_method_data(&mut self, method: models::MethodData) -> Result<models::MethodData>;

    fn insert_span_data(&mut self, span: models::SpanData) -> Result<models::SpanData>;

    fn associate_context<'a>(
        &mut self,
        assoc: models::ContextAssoc,
    ) -> Result<models::ContextAssoc>;

    fn insert_upload_details(
        &mut self,
        upload_details: models::UploadDetails,
    ) -> Result<models::UploadDetails>;

    fn build(self) -> R;
}
