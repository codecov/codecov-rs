use std::path::PathBuf;

use rusqlite::Connection;
use uuid::Uuid;

use super::{open_database, SqliteReport};
use crate::{
    error::Result,
    report::{models, ReportBuilder},
};

pub struct SqliteReportBuilder {
    pub filename: PathBuf,
    pub conn: Connection,
}

impl SqliteReportBuilder {
    pub fn new(filename: PathBuf) -> Result<SqliteReportBuilder> {
        let conn = open_database(&filename)?;
        Ok(SqliteReportBuilder { filename, conn })
    }
}

impl ReportBuilder<SqliteReport> for SqliteReportBuilder {
    fn insert_file(&mut self, path: String) -> Result<models::SourceFile> {
        let mut stmt = self.conn.prepare_cached(
            "INSERT INTO source_file (id, path) VALUES (?1, ?2) RETURNING id, path",
        )?;

        Ok(
            stmt.query_row((seahash::hash(path.as_bytes()) as i64, path), |row| {
                row.try_into()
            })?,
        )
    }

    fn insert_context(
        &mut self,
        context_type: models::ContextType,
        name: &str,
    ) -> Result<models::Context> {
        let mut stmt = self.conn.prepare_cached("INSERT INTO context (id, context_type, name) VALUES (?1, ?2, ?3) RETURNING id, context_type, name")?;
        Ok(stmt.query_row(
            (
                seahash::hash(name.as_bytes()) as i64,
                context_type.to_string(),
                name,
            ),
            |row| row.try_into(),
        )?)
    }

    fn insert_coverage_sample(
        &mut self,
        mut sample: models::CoverageSample,
    ) -> Result<models::CoverageSample> {
        let mut stmt = self.conn.prepare_cached("INSERT INTO coverage_sample (id, source_file_id, line_no, coverage_type, hits, hit_branches, total_branches) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)")?;
        sample.id = Uuid::new_v4();
        let _ = stmt.execute((
            sample.id,
            sample.source_file_id,
            sample.line_no,
            sample.coverage_type,
            sample.hits,
            sample.hit_branches,
            sample.total_branches,
        ))?;

        Ok(sample)
    }

    fn insert_branches_data(
        &mut self,
        mut branch: models::BranchesData,
    ) -> Result<models::BranchesData> {
        let mut stmt = self.conn.prepare_cached("INSERT INTO branches_data (id, source_file_id, sample_id, hits, branch_format, branch) VALUES (?1, ?2, ?3, ?4, ?5, ?6)")?;

        branch.id = Uuid::new_v4();
        let _ = stmt.execute((
            branch.id,
            branch.source_file_id,
            branch.sample_id,
            branch.hits,
            branch.branch_format,
            &branch.branch,
        ))?;
        Ok(branch)
    }

    fn insert_method_data(&mut self, mut method: models::MethodData) -> Result<models::MethodData> {
        let mut stmt = self.conn.prepare_cached("INSERT INTO method_data (id, source_file_id, sample_id, line_no, hit_branches, total_branches, hit_complexity_paths, total_complexity) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)")?;
        method.id = Uuid::new_v4();

        let _ = stmt.execute((
            method.id,
            method.source_file_id,
            method.sample_id,
            method.line_no,
            method.hit_branches,
            method.total_branches,
            method.hit_complexity_paths,
            method.total_complexity,
        ))?;
        Ok(method)
    }

    fn insert_span_data(&mut self, mut span: models::SpanData) -> Result<models::SpanData> {
        let mut stmt = self.conn.prepare_cached("INSERT INTO span_data (id, source_file_id, sample_id, hits, start_line, start_col, end_line, end_col) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)")?;
        span.id = Uuid::new_v4();

        let _ = stmt.execute((
            span.id,
            span.source_file_id,
            span.sample_id,
            span.hits,
            span.start_line,
            span.start_col,
            span.end_line,
            span.end_col,
        ))?;
        Ok(span)
    }

    fn associate_context<'a>(
        &mut self,
        assoc: models::ContextAssoc,
    ) -> Result<models::ContextAssoc> {
        let mut stmt = self.conn.prepare_cached("INSERT INTO context_assoc (context_id, sample_id, branch_id, method_id, span_id) VALUES (?1, ?2, ?3, ?4, ?5)")?;

        let _ = stmt.execute((
            assoc.context_id,
            assoc.sample_id,
            assoc.branch_id,
            assoc.method_id,
            assoc.span_id,
        ))?;
        Ok(assoc)
    }

    fn insert_upload_details(
        &mut self,
        upload_details: models::UploadDetails,
    ) -> Result<models::UploadDetails> {
        let mut stmt = self.conn.prepare_cached("INSERT INTO upload_details (context_id, timestamp, raw_upload_url, flags, provider, build, name, job_name, ci_run_url, state, env, session_type, session_extras) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)")?;
        let _ = stmt.execute((
            &upload_details.context_id,
            &upload_details.timestamp,
            &upload_details.raw_upload_url,
            &upload_details.flags.as_ref().map(|v| v.to_string()),
            &upload_details.provider,
            &upload_details.build,
            &upload_details.name,
            &upload_details.job_name,
            &upload_details.ci_run_url,
            &upload_details.state,
            &upload_details.env,
            &upload_details.session_type,
            &upload_details
                .session_extras
                .as_ref()
                .map(|v| v.to_string()),
        ))?;

        Ok(upload_details)
    }

    fn build(self) -> SqliteReport {
        SqliteReport {
            filename: self.filename,
            conn: self.conn,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use rusqlite_migration::SchemaVersion;
    use serde_json::{json, json_internal};
    use tempfile::TempDir;

    use super::*;
    use crate::report::Report;

    struct Ctx {
        temp_dir: TempDir,
    }

    fn setup() -> Ctx {
        Ctx {
            temp_dir: TempDir::new().ok().unwrap(),
        }
    }

    fn hash_id(key: &str) -> i64 {
        seahash::hash(key.as_bytes()) as i64
    }

    #[test]
    fn test_new_report_builder_runs_migrations() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        assert!(!db_file.exists());

        let report_builder = SqliteReportBuilder::new(db_file).unwrap();
        assert_eq!(
            super::super::MIGRATIONS.current_version(&report_builder.conn),
            Ok(SchemaVersion::Inside(NonZeroUsize::new(1).unwrap()))
        );
    }

    #[test]
    fn test_insert_file() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let expected_file = models::SourceFile {
            id: hash_id("src/report.rs"),
            path: "src/report.rs".to_string(),
        };
        let actual_file = report_builder
            .insert_file(expected_file.path.clone())
            .unwrap();
        assert_eq!(actual_file, expected_file);
    }

    #[test]
    fn test_insert_context() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let expected_context = models::Context {
            id: hash_id("foo"),
            context_type: models::ContextType::Upload,
            name: "foo".to_string(),
        };
        let actual_context = report_builder
            .insert_context(expected_context.context_type, &expected_context.name)
            .unwrap();
        assert_eq!(actual_context, expected_context);
    }

    #[test]
    fn test_insert_coverage_sample() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let file = report_builder
            .insert_file("src/report.rs".to_string())
            .unwrap();

        let mut expected_sample = models::CoverageSample {
            id: Uuid::nil(), // Ignored
            source_file_id: file.id,
            line_no: 1,
            coverage_type: models::CoverageType::Line,
            hits: Some(3),
            hit_branches: Some(2),
            total_branches: Some(4),
        };
        let actual_sample = report_builder
            .insert_coverage_sample(expected_sample.clone())
            .unwrap();
        assert_ne!(expected_sample.id, actual_sample.id);
        expected_sample.id = actual_sample.id.clone();
        assert_eq!(actual_sample, expected_sample);
    }

    #[test]
    fn test_insert_branches_data() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let file = report_builder
            .insert_file("src/report.rs".to_string())
            .unwrap();

        let coverage_sample = report_builder
            .insert_coverage_sample(models::CoverageSample {
                source_file_id: file.id,
                line_no: 1,
                coverage_type: models::CoverageType::Branch,
                hit_branches: Some(2),
                total_branches: Some(4),
                ..Default::default()
            })
            .unwrap();

        let mut expected_branch = models::BranchesData {
            id: Uuid::nil(), // Ignored
            source_file_id: file.id,
            sample_id: coverage_sample.id,
            hits: 0,
            branch_format: models::BranchFormat::Condition,
            branch: "0:jump".to_string(),
        };
        let actual_branch = report_builder
            .insert_branches_data(expected_branch.clone())
            .unwrap();
        assert_ne!(expected_branch.id, actual_branch.id);
        expected_branch.id = actual_branch.id;
        assert_eq!(actual_branch, expected_branch);
    }

    #[test]
    fn test_insert_method_data() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let file = report_builder
            .insert_file("src/report.rs".to_string())
            .unwrap();

        let coverage_sample = report_builder
            .insert_coverage_sample(models::CoverageSample {
                source_file_id: file.id,
                line_no: 1, // line_no
                coverage_type: models::CoverageType::Branch,
                hit_branches: Some(2),   // hit_branches
                total_branches: Some(4), // total_branches
                ..Default::default()
            })
            .unwrap();

        let mut expected_method = models::MethodData {
            id: Uuid::nil(), // Ignored
            source_file_id: file.id,
            sample_id: Some(coverage_sample.id),
            line_no: Some(1),
            hit_branches: Some(1),
            total_branches: Some(2),
            hit_complexity_paths: Some(1),
            total_complexity: Some(2),
        };

        let actual_method = report_builder
            .insert_method_data(expected_method.clone())
            .unwrap();
        expected_method.id = actual_method.id;
        assert_eq!(actual_method, expected_method);
    }

    #[test]
    fn test_insert_span_data() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let file = report_builder
            .insert_file("src/report.rs".to_string())
            .unwrap();

        let coverage_sample = report_builder
            .insert_coverage_sample(models::CoverageSample {
                source_file_id: file.id,
                line_no: 1, // line_no
                coverage_type: models::CoverageType::Branch,
                hit_branches: Some(2),   // hit_branches
                total_branches: Some(4), // total_branches
                ..Default::default()
            })
            .unwrap();

        let mut expected_span = models::SpanData {
            id: Uuid::nil(), // Ignored
            source_file_id: file.id,
            sample_id: Some(coverage_sample.id),
            hits: 2,
            start_line: Some(1),
            start_col: Some(0),
            end_line: Some(30),
            end_col: Some(60),
        };
        let actual_span = report_builder
            .insert_span_data(expected_span.clone())
            .unwrap();
        expected_span.id = actual_span.id;
        assert_eq!(actual_span, expected_span);
    }

    #[test]
    fn test_insert_context_assoc() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let file = report_builder
            .insert_file("src/report.rs".to_string())
            .unwrap();

        let coverage_sample = report_builder
            .insert_coverage_sample(models::CoverageSample {
                source_file_id: file.id,
                line_no: 1, // line_no
                coverage_type: models::CoverageType::Branch,
                hit_branches: Some(2),   // hit_branches
                total_branches: Some(4), // total_branches
                ..Default::default()
            })
            .unwrap();

        let branch = report_builder
            .insert_branches_data(models::BranchesData {
                source_file_id: file.id,
                sample_id: coverage_sample.id,
                hits: 0, // hits
                branch_format: models::BranchFormat::Condition,
                branch: "0:jump".to_string(),
                ..Default::default()
            })
            .unwrap();

        let method = report_builder
            .insert_method_data(models::MethodData {
                source_file_id: file.id,
                sample_id: Some(coverage_sample.id),
                line_no: Some(1),
                hit_branches: Some(1),
                total_branches: Some(2),
                hit_complexity_paths: Some(1),
                total_complexity: Some(2),
                ..Default::default()
            })
            .unwrap();

        let span = report_builder
            .insert_span_data(models::SpanData {
                source_file_id: file.id,
                sample_id: Some(coverage_sample.id),
                hits: 1,             // hits
                start_line: Some(1), // start_line
                start_col: Some(0),  // start_col
                end_line: Some(30),  // end_line
                end_col: Some(60),   // end_col
                ..Default::default()
            })
            .unwrap();

        let context = report_builder
            .insert_context(models::ContextType::Upload, &"upload".to_string())
            .unwrap();

        let expected_assoc = models::ContextAssoc {
            context_id: context.id,
            sample_id: Some(coverage_sample.id),
            branch_id: Some(branch.id),
            method_id: Some(method.id),
            span_id: Some(span.id),
        };
        let actual_assoc = report_builder
            .associate_context(models::ContextAssoc {
                context_id: context.id,
                sample_id: Some(coverage_sample.id),
                branch_id: Some(branch.id),
                method_id: Some(method.id),
                span_id: Some(span.id),
            })
            .unwrap();
        assert_eq!(actual_assoc, expected_assoc);
    }

    #[test]
    fn test_insert_upload_details() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let upload = report_builder
            .insert_context(models::ContextType::Upload, "codecov-rs CI")
            .unwrap();
        let inserted_details = models::UploadDetails {
            context_id: upload.id,
            timestamp: Some(123),
            raw_upload_url: Some("https://example.com".to_string()),
            flags: Some(json!(["abc".to_string(), "def".to_string()])),
            provider: Some("provider".to_string()),
            build: Some("build".to_string()),
            name: Some("name".to_string()),
            job_name: Some("job name".to_string()),
            ci_run_url: Some("https://example.com".to_string()),
            state: Some("state".to_string()),
            env: Some("env".to_string()),
            session_type: Some("uploaded".to_string()),
            session_extras: Some(json!({})),
        };
        let inserted_details = report_builder
            .insert_upload_details(inserted_details)
            .unwrap();

        let other_upload = report_builder
            .insert_context(models::ContextType::Upload, "codecov-rs CI 2")
            .unwrap();

        let report = report_builder.build();
        let fetched_details = report.get_details_for_upload(&upload).unwrap();
        assert_eq!(fetched_details, inserted_details);

        let other_details_result = report.get_details_for_upload(&other_upload);
        assert!(other_details_result.is_err());
        match other_details_result {
            Err(crate::error::CodecovError::SqliteError(rusqlite::Error::QueryReturnedNoRows)) => {}
            _ => assert!(false),
        }
    }
}