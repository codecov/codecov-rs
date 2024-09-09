use std::{fmt, path::PathBuf};

use rusqlite::{Connection, OptionalExtension};

use super::open_database;
use crate::{
    error::Result,
    report::{models, Report},
};

pub struct SqliteReport {
    pub filename: PathBuf,
    pub conn: Connection,
}

impl fmt::Debug for SqliteReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteReport").finish_non_exhaustive()
    }
}

impl SqliteReport {
    pub fn new(filename: PathBuf) -> Result<SqliteReport> {
        let conn = open_database(&filename)?;
        Ok(SqliteReport { filename, conn })
    }
}

impl Report for SqliteReport {
    // TODO: implement for real, just using for integration tests
    fn list_files(&self) -> Result<Vec<models::SourceFile>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT id, path FROM source_file")?;
        let files = stmt
            .query_map([], |row| row.try_into())?
            .collect::<rusqlite::Result<Vec<models::SourceFile>>>()?;
        Ok(files)
    }

    // TODO: implement for real, just using for integration tests
    fn list_contexts(&self) -> Result<Vec<models::Context>> {
        let mut stmt = self.conn.prepare_cached("SELECT id, name FROM context")?;
        let contexts = stmt
            .query_map([], |row| row.try_into())?
            .collect::<rusqlite::Result<Vec<models::Context>>>()?;
        Ok(contexts)
    }

    // TODO implement for real, just using for integration tests
    fn list_coverage_samples(&self) -> Result<Vec<models::CoverageSample>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT raw_upload_id, local_sample_id, source_file_id, line_no, coverage_type, hits, hit_branches, total_branches FROM coverage_sample ORDER BY 2, 3")?;
        let samples = stmt
            .query_map([], |row| row.try_into())?
            .collect::<rusqlite::Result<Vec<models::CoverageSample>>>()?;
        Ok(samples)
    }

    fn list_branches_for_sample(
        &self,
        sample: &models::CoverageSample,
    ) -> Result<Vec<models::BranchesData>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT branches_data.local_branch_id, branches_data.raw_upload_id, branches_data.source_file_id, branches_data.local_sample_id, branches_data.branch, branches_data.branch_format, branches_data.hits FROM branches_data WHERE branches_data.local_sample_id = ?1")?;
        let branches = stmt
            .query_map([sample.local_sample_id], |row| row.try_into())?
            .collect::<rusqlite::Result<Vec<models::BranchesData>>>()?;
        Ok(branches)
    }

    fn get_method_for_sample(
        &self,
        sample: &models::CoverageSample,
    ) -> Result<Option<models::MethodData>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT method_data.local_method_id, method_data.raw_upload_id, method_data.source_file_id, method_data.local_sample_id, method_data.line_no, method_data.hit_branches, method_data.total_branches, method_data.hit_complexity_paths, method_data.total_complexity FROM method_data WHERE method_data.local_sample_id = ?1")?;

        Ok(stmt
            .query_row([sample.local_sample_id], |row| row.try_into())
            .optional()?)
    }

    fn list_spans_for_sample(
        &self,
        sample: &models::CoverageSample,
    ) -> Result<Vec<models::SpanData>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT span_data.local_span_id, span_data.raw_upload_id, span_data.source_file_id, span_data.local_sample_id, span_data.hits, span_data.start_line, span_data.start_col, span_data.end_line, span_data.end_col FROM span_data WHERE span_data.local_sample_id = ?1")?;
        let span = stmt
            .query_map([sample.local_sample_id], |row| row.try_into())?
            .collect::<rusqlite::Result<Vec<models::SpanData>>>()?;
        Ok(span)
    }

    // TODO implement for real, just using for integration tests
    fn list_contexts_for_sample(
        &self,
        sample: &models::CoverageSample,
    ) -> Result<Vec<models::Context>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT context.id, context.name FROM context INNER JOIN context_assoc ON context.id = context_assoc.context_id WHERE context_assoc.local_sample_id = ?1")?;
        let contexts = stmt
            .query_map([sample.local_sample_id], |row| row.try_into())?
            .collect::<rusqlite::Result<Vec<models::Context>>>()?;
        Ok(contexts)
    }

    // TODO implement for real, just using for integration tests
    fn list_samples_for_file(
        &self,
        file: &models::SourceFile,
    ) -> Result<Vec<models::CoverageSample>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT sample.local_sample_id, sample.raw_upload_id, sample.source_file_id, sample.line_no, sample.coverage_type, sample.hits, sample.hit_branches, sample.total_branches FROM coverage_sample sample INNER JOIN source_file ON sample.source_file_id = source_file.id WHERE source_file_id=?1")?;
        let samples = stmt
            .query_map([file.id], |row| row.try_into())?
            .collect::<rusqlite::Result<Vec<models::CoverageSample>>>()?;
        Ok(samples)
    }

    fn list_raw_uploads(&self) -> Result<Vec<models::RawUpload>> {
        let mut stmt = self.conn.prepare_cached("SELECT id, timestamp, raw_upload_url, flags, provider, build, name, job_name, ci_run_url, state, env, session_type, session_extras FROM raw_upload")?;
        let uploads = stmt
            .query_map([], |row| row.try_into())?
            .collect::<rusqlite::Result<Vec<models::RawUpload>>>()?;
        Ok(uploads)
    }

    /// Merge `other` into `self` without modifying `other`.
    ///
    /// TODO: Probably put this in a commit
    fn merge(&mut self, other: &SqliteReport) -> Result<()> {
        //        let tx = self.conn.transaction()?;
        let _ = self
            .conn
            .execute("ATTACH DATABASE ?1 AS other", [other.conn.path()])?;

        let merge_stmts = [
            // The same `source_file` and `context` records may appear in multiple databases. They
            // use a hash of their "names" as their PK so any instance of them will
            // come up with the same PK. We can `INSERT OR IGNORE` to effectively union the tables
            "INSERT OR IGNORE INTO source_file SELECT * FROM other.source_file",
            "INSERT OR IGNORE INTO raw_upload SELECT * FROM other.raw_upload",
            "INSERT OR IGNORE INTO context SELECT * FROM other.context",
            // For everything else, we use a joint primary key that should be globally unique and
            // can simply concatenate the tables
            "INSERT INTO coverage_sample SELECT * FROM other.coverage_sample",
            "INSERT INTO branches_data SELECT * FROM other.branches_data",
            "INSERT INTO method_data SELECT * FROM other.method_data",
            "INSERT INTO span_data SELECT * FROM other.span_data",
            "INSERT INTO context_assoc SELECT * FROM other.context_assoc",
        ];
        for stmt in merge_stmts {
            let _ = self.conn.prepare_cached(stmt)?.execute([])?;
        }

        self.conn.execute_batch("DETACH DATABASE other")?;

        Ok(())
    }

    fn totals(&self) -> Result<models::ReportTotals> {
        let mut stmt = self
            .conn
            .prepare_cached(include_str!("queries/totals.sql"))?;

        Ok(stmt.query_row([], |row| row.try_into())?)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use rusqlite_migration::SchemaVersion;
    use tempfile::TempDir;

    use super::{super::SqliteReportBuilder, *};
    use crate::report::ReportBuilder;

    struct Ctx {
        temp_dir: TempDir,
    }

    fn setup() -> Ctx {
        Ctx {
            temp_dir: TempDir::new().ok().unwrap(),
        }
    }

    #[test]
    fn test_new_report_runs_migrations() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        assert!(!db_file.exists());

        let report = SqliteReport::new(db_file).unwrap();
        assert_eq!(
            super::super::MIGRATIONS.current_version(&report.conn),
            Ok(SchemaVersion::Inside(NonZeroUsize::new(1).unwrap()))
        );
    }

    #[test]
    fn test_merge() {
        let ctx = setup();
        let db_file_left = ctx.temp_dir.path().join("left.sqlite");
        let db_file_right = ctx.temp_dir.path().join("right.sqlite");

        let mut left_report_builder = SqliteReportBuilder::new(db_file_left).unwrap();
        let file_1 = left_report_builder.insert_file("src/report.rs").unwrap();
        let file_2 = left_report_builder
            .insert_file("src/report/models.rs")
            .unwrap();
        let upload_1 = left_report_builder
            .insert_raw_upload(Default::default())
            .unwrap();
        let test_case_1 = left_report_builder.insert_context("test case 1").unwrap();
        let line_1 = left_report_builder
            .insert_coverage_sample(models::CoverageSample {
                source_file_id: file_1.id,
                raw_upload_id: upload_1.id,
                line_no: 1,
                coverage_type: models::CoverageType::Line,
                ..Default::default()
            })
            .unwrap();
        let line_2 = left_report_builder
            .insert_coverage_sample(models::CoverageSample {
                raw_upload_id: upload_1.id,
                source_file_id: file_2.id,
                line_no: 1,
                coverage_type: models::CoverageType::Branch,
                hit_branches: Some(1),
                total_branches: Some(2),
                ..Default::default()
            })
            .unwrap();
        let line_3 = left_report_builder
            .insert_coverage_sample(models::CoverageSample {
                raw_upload_id: upload_1.id,
                source_file_id: file_2.id,
                line_no: 2,
                coverage_type: models::CoverageType::Method,
                hits: Some(2),
                ..Default::default()
            })
            .unwrap();
        for line in [&line_1, &line_2, &line_3] {
            let _ = left_report_builder.associate_context(models::ContextAssoc {
                context_id: test_case_1.id,
                raw_upload_id: upload_1.id,
                local_sample_id: Some(line.local_sample_id),
                ..Default::default()
            });
        }

        let mut right_report_builder = SqliteReportBuilder::new(db_file_right).unwrap();
        let file_2 = right_report_builder
            .insert_file("src/report/models.rs")
            .unwrap();
        let file_3 = right_report_builder
            .insert_file("src/report/schema.rs")
            .unwrap();
        let upload_2 = right_report_builder
            .insert_raw_upload(Default::default())
            .unwrap();
        let test_case_2 = right_report_builder.insert_context("test case 2").unwrap();
        let line_4 = right_report_builder
            .insert_coverage_sample(models::CoverageSample {
                raw_upload_id: upload_2.id,
                source_file_id: file_2.id,
                line_no: 3,
                coverage_type: models::CoverageType::Line,
                hits: Some(1),
                ..Default::default()
            })
            .unwrap();
        let line_5 = right_report_builder
            .insert_coverage_sample(models::CoverageSample {
                raw_upload_id: upload_2.id,
                source_file_id: file_3.id,
                line_no: 1,
                coverage_type: models::CoverageType::Branch,
                hit_branches: Some(1),
                total_branches: Some(2),
                ..Default::default()
            })
            .unwrap();
        let _ = right_report_builder.insert_branches_data(models::BranchesData {
            raw_upload_id: upload_2.id,
            source_file_id: file_2.id,
            local_sample_id: line_5.local_sample_id,
            hits: 0,
            branch_format: models::BranchFormat::Condition,
            branch: "1".to_string(),
            ..Default::default()
        });
        let line_6 = right_report_builder
            .insert_coverage_sample(models::CoverageSample {
                raw_upload_id: upload_2.id,
                source_file_id: file_3.id,
                line_no: 2,
                coverage_type: models::CoverageType::Method,
                hits: Some(2),
                ..Default::default()
            })
            .unwrap();
        let _ = right_report_builder.insert_method_data(models::MethodData {
            raw_upload_id: upload_2.id,
            source_file_id: file_2.id,
            local_sample_id: line_6.local_sample_id,
            line_no: Some(2),
            hit_complexity_paths: Some(1),
            total_complexity: Some(2),
            ..Default::default()
        });
        for line in [&line_4, &line_5, &line_6] {
            let _ = right_report_builder.associate_context(models::ContextAssoc {
                context_id: test_case_2.id,
                local_sample_id: Some(line.local_sample_id),
                ..Default::default()
            });
        }

        let mut left = left_report_builder.build().unwrap();
        let right = right_report_builder.build().unwrap();
        left.merge(&right).unwrap();

        // NOTE: the assertions here are sensitive to the sort order:
        assert_eq!(
            left.list_files().unwrap(),
            &[file_1.clone(), file_2.clone(), file_3.clone()]
        );
        assert_eq!(left.list_contexts().unwrap(), &[test_case_2, test_case_1]);
        assert_eq!(
            left.list_coverage_samples().unwrap(),
            &[
                line_1.clone(),
                line_4.clone(),
                line_2.clone(),
                line_5.clone(),
                line_3.clone(),
                line_6.clone()
            ]
        );
        assert_eq!(left.list_samples_for_file(&file_1).unwrap(), &[line_1]);
        assert_eq!(
            left.list_samples_for_file(&file_2).unwrap(),
            &[line_2, line_3, line_4]
        );
        assert_eq!(
            left.list_samples_for_file(&file_3).unwrap(),
            &[line_5, line_6]
        );
    }

    #[test]
    fn test_totals() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        assert!(!db_file.exists());
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let file_1 = report_builder.insert_file("src/report.rs").unwrap();
        let file_2 = report_builder.insert_file("src/report/models.rs").unwrap();
        let upload_1 = report_builder
            .insert_raw_upload(Default::default())
            .unwrap();
        let test_case_1 = report_builder.insert_context("test_totals").unwrap();
        let line_1 = report_builder
            .insert_coverage_sample(models::CoverageSample {
                raw_upload_id: upload_1.id,
                source_file_id: file_1.id,
                line_no: 1,
                coverage_type: models::CoverageType::Line,
                ..Default::default()
            })
            .unwrap();
        let line_2 = report_builder
            .insert_coverage_sample(models::CoverageSample {
                source_file_id: file_2.id,
                raw_upload_id: upload_1.id,
                line_no: 1,
                coverage_type: models::CoverageType::Branch,
                hit_branches: Some(1),
                total_branches: Some(2),
                ..Default::default()
            })
            .unwrap();
        let line_3 = report_builder
            .insert_coverage_sample(models::CoverageSample {
                raw_upload_id: upload_1.id,
                source_file_id: file_2.id,
                line_no: 2,
                coverage_type: models::CoverageType::Method,
                hits: Some(2),
                ..Default::default()
            })
            .unwrap();
        let _ = report_builder.insert_method_data(models::MethodData {
            raw_upload_id: upload_1.id,
            source_file_id: file_2.id,
            local_sample_id: line_3.local_sample_id,
            line_no: Some(2),
            hit_complexity_paths: Some(2),
            total_complexity: Some(4),
            ..Default::default()
        });
        for line in [&line_1, &line_2, &line_3] {
            let _ = report_builder.associate_context(models::ContextAssoc {
                raw_upload_id: upload_1.id,
                context_id: test_case_1.id,
                local_sample_id: Some(line.local_sample_id),
                ..Default::default()
            });
        }

        let report = report_builder.build().unwrap();

        let expected_totals = models::ReportTotals {
            files: 2,
            uploads: 1,
            test_cases: 1,
            coverage: models::CoverageTotals {
                hit_lines: 0,
                total_lines: 1,
                hit_branches: 1,
                total_branches: 2,
                total_branch_roots: 1,
                hit_methods: 1,
                total_methods: 1,
                hit_complexity_paths: 2,
                total_complexity: 4,
            },
        };

        let totals = report.totals().unwrap();
        assert_eq!(totals, expected_totals);
    }
}
