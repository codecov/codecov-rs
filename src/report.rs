#[cfg(test)]
use mockall::automock;

pub mod models;

mod sqlite_report;
use rusqlite::Result;
pub use sqlite_report::*;

#[cfg_attr(test, automock)]
pub trait Report {
    fn list_files(&self) -> Result<Vec<models::SourceFile>>;
    fn list_contexts(&self) -> Result<Vec<models::Context>>;
}

#[cfg_attr(test, automock)]
pub trait ReportBuilder<R: Report> {
    fn insert_file(&mut self, file: models::SourceFile) -> Result<models::SourceFile>;
    fn insert_context(&mut self, context: models::Context) -> Result<models::Context>;

    fn insert_line(
        &mut self,
        line: models::LineStatus,
        context: &models::Context,
    ) -> Result<models::LineStatus>;
    fn insert_branch(
        &mut self,
        branch: models::BranchStatus,
        context: &models::Context,
    ) -> Result<models::BranchStatus>;

    fn build(self) -> R;
}
