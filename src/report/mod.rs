#[cfg(test)]
use mockall::automock;

pub mod models;

mod sqlite_report;
pub use sqlite_report::*;

use crate::error::Result;

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
