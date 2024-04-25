#[cfg(test)]
use mockall::automock;

pub mod models;

mod sqlite_report;
use rusqlite::Result;
pub use sqlite_report::*;
use uuid::Uuid;

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

    fn merge(&mut self, other: &Self) -> Result<()>;
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
        source_file_id: i64,
        line_no: i64,
        coverage_type: models::CoverageType,
        hits: Option<i64>,
        hit_branches: Option<i64>,
        total_branches: Option<i64>,
    ) -> Result<models::CoverageSample>;

    fn insert_branches_data(
        &mut self,
        source_file_id: i64,
        sample_id: Uuid,
        hits: i64,
        branch_format: models::BranchFormat,
        branch: String,
    ) -> Result<models::BranchesData>;

    fn insert_method_data(
        &mut self,
        source_file_id: i64,
        sample_id: Option<Uuid>,
        line_no: Option<i64>,
        hit_branches: Option<i64>,
        total_branches: Option<i64>,
        hit_complexity_paths: Option<i64>,
        total_complexity: Option<i64>,
    ) -> Result<models::MethodData>;

    fn insert_span_data(
        &mut self,
        source_file_id: i64,
        sample_id: Option<Uuid>,
        hits: i64,
        start_line: Option<i64>,
        start_col: Option<i64>,
        end_line: Option<i64>,
        end_col: Option<i64>,
    ) -> Result<models::SpanData>;

    fn associate_context<'a>(
        &mut self,
        context_id: i64,
        sample: Option<&'a models::CoverageSample>,
        branches_data: Option<&'a models::BranchesData>,
        method_data: Option<&'a models::MethodData>,
        span_data: Option<&'a models::SpanData>,
    ) -> Result<models::ContextAssoc>;

    fn build(self) -> R;
}
