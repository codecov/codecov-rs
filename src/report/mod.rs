#[cfg(test)]
use mockall::automock;

pub mod models;

pub mod sqlite;
pub use sqlite::{SqliteReport, SqliteReportBuilder, SqliteReportBuilderTx};

#[cfg(feature = "pyreport")]
pub mod pyreport;

use crate::error::Result;

/// An interface for coverage data.
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

    /// Merges another report into this one. Does not modify the other report.
    fn merge(&mut self, other: &Self) -> Result<()>;

    /// Computes aggregated metrics for the data in the report.
    fn totals(&self) -> Result<models::ReportTotals>;
}

/// An interface for creating a new coverage report.
#[cfg_attr(test, automock)]
pub trait ReportBuilder<R: Report> {
    /// Create a [`models::SourceFile`] record and return it.
    fn insert_file(&mut self, path: String) -> Result<models::SourceFile>;

    /// Create a [`models::Context`] record and return it.
    fn insert_context(
        &mut self,
        context_type: models::ContextType,
        name: &str,
    ) -> Result<models::Context>;

    /// Create a [`models::CoverageSample`] record and return it. The passed-in
    /// model's `id` field is ignored and overwritten with a new UUIDv4.
    fn insert_coverage_sample(
        &mut self,
        sample: models::CoverageSample,
    ) -> Result<models::CoverageSample>;

    /// Create a [`models::BranchesData`] record and return it. The passed-in
    /// model's `id` field is ignored and overwritten with a new UUIDv4.
    fn insert_branches_data(
        &mut self,
        branch: models::BranchesData,
    ) -> Result<models::BranchesData>;

    /// Create a [`models::MethodData`] record and return it. The passed-in
    /// model's `id` field is ignored and overwritten with a new UUIDv4.
    fn insert_method_data(&mut self, method: models::MethodData) -> Result<models::MethodData>;

    /// Create a [`models::SpanData`] record and return it. The passed-in
    /// model's `id` field is ignored and overwritten with a new UUIDv4.
    fn insert_span_data(&mut self, span: models::SpanData) -> Result<models::SpanData>;

    /// Create a [`models::ContextAssoc`] record that associates a
    /// [`models::Context`] with another model. Return the `ContextAssoc`
    /// model.
    fn associate_context<'a>(
        &mut self,
        assoc: models::ContextAssoc,
    ) -> Result<models::ContextAssoc>;

    /// Create a [`models::UploadDetails`] record and return it.
    fn insert_upload_details(
        &mut self,
        upload_details: models::UploadDetails,
    ) -> Result<models::UploadDetails>;

    /// Consume `self` and return a [`Report`].
    fn build(self) -> Result<R>;
}
