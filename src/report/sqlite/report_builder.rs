use std::{
    ops::RangeFrom,
    path::{Path, PathBuf},
};

use rand::{rngs::StdRng, Rng, SeedableRng};
use rusqlite::{Connection, Transaction};

use super::{models::Insertable, open_database, SqliteReport};
use crate::{
    error::{CodecovError, Result},
    report::{models, ReportBuilder},
};

/// Returned by [`SqliteReportBuilder::transaction`]. Contains the actual
/// implementation for most of the `ReportBuilder` trait except for `build()`
/// which is implemented on [`SqliteReportBuilder`]. All
/// `SqliteReportBuilderTx`s created by a `SqliteReportBuilder` must
/// go out of scope before `SqliteReportBuilder::build()` can be called because
/// their `conn` member mutably borrows the SQLite database and prevents
/// `build()` from moving it into a `SqliteReport`.
pub struct SqliteReportBuilderTx<'a> {
    id_sequence: &'a mut RangeFrom<i64>,
    rng: &'a mut StdRng,

    pub filename: &'a Path,
    pub conn: Transaction<'a>,
}

impl<'a> SqliteReportBuilderTx<'a> {
    pub fn rollback(self) -> Result<()> {
        Ok(self.conn.rollback()?)
    }
}

/// Implementation of the [`ReportBuilder`] trait to build [`SqliteReport`]s.
/// The [`SqliteReportBuilder::transaction`] method returns a
/// [`SqliteReportBuilderTx`], an auxiliary `ReportBuilder` implementation which
/// will run its operations in a transaction that gets committed when the
/// `SqliteReportBuilderTx` goes out of scope. A non-transaction
/// `SqliteReportBuilder`'s `ReportBuilder` functions (except for `build()`)
/// call `self.transaction()?` for each call and delegate to the
/// `SqliteReportBuilderTx` implementation.
pub struct SqliteReportBuilder {
    pub filename: PathBuf,
    pub conn: Connection,

    /// A single sequence is shared for [`CoverageSample`], [`BranchesData`],
    /// [`MethodData`], and [`SpanData`].
    id_sequence: RangeFrom<i64>,

    rng: StdRng,
}

impl SqliteReportBuilder {
    fn new_with_rng(filename: PathBuf, rng: StdRng) -> Result<SqliteReportBuilder> {
        let conn = open_database(&filename)?;
        Ok(SqliteReportBuilder {
            filename,
            conn,
            id_sequence: 0..,
            rng,
        })
    }

    pub fn new_with_seed(filename: PathBuf, seed: u64) -> Result<SqliteReportBuilder> {
        Self::new_with_rng(filename, StdRng::seed_from_u64(seed))
    }

    pub fn new(filename: PathBuf) -> Result<SqliteReportBuilder> {
        Self::new_with_rng(filename, StdRng::from_entropy())
    }

    /// Create a [`SqliteReportBuilderTx`] with a [`rusqlite::Transaction`] that
    /// will automatically commit itself when it goes out of scope.
    ///
    /// Each `Transaction` holds a mutable reference to `self.conn` and prevents
    /// `self.build()` from being called.
    pub fn transaction(&mut self) -> Result<SqliteReportBuilderTx<'_>> {
        let mut builder_tx = SqliteReportBuilderTx {
            filename: &self.filename,
            conn: self.conn.transaction()?,
            id_sequence: &mut self.id_sequence,
            rng: &mut self.rng,
        };
        builder_tx
            .conn
            .set_drop_behavior(rusqlite::DropBehavior::Commit);
        Ok(builder_tx)
    }
}

impl ReportBuilder<SqliteReport> for SqliteReportBuilder {
    fn insert_file(&mut self, path: &str) -> Result<models::SourceFile> {
        self.transaction()?.insert_file(path)
    }

    fn insert_context(&mut self, name: &str) -> Result<models::Context> {
        self.transaction()?.insert_context(name)
    }

    fn insert_coverage_sample(
        &mut self,
        sample: models::CoverageSample,
    ) -> Result<models::CoverageSample> {
        self.transaction()?.insert_coverage_sample(sample)
    }

    fn multi_insert_coverage_sample(
        &mut self,
        samples: Vec<&mut models::CoverageSample>,
    ) -> Result<()> {
        self.transaction()?.multi_insert_coverage_sample(samples)
    }

    fn insert_branches_data(
        &mut self,
        branch: models::BranchesData,
    ) -> Result<models::BranchesData> {
        self.transaction()?.insert_branches_data(branch)
    }

    fn multi_insert_branches_data(
        &mut self,
        branches: Vec<&mut models::BranchesData>,
    ) -> Result<()> {
        self.transaction()?.multi_insert_branches_data(branches)
    }

    fn insert_method_data(&mut self, method: models::MethodData) -> Result<models::MethodData> {
        self.transaction()?.insert_method_data(method)
    }

    fn multi_insert_method_data(&mut self, methods: Vec<&mut models::MethodData>) -> Result<()> {
        self.transaction()?.multi_insert_method_data(methods)
    }

    fn insert_span_data(&mut self, span: models::SpanData) -> Result<models::SpanData> {
        self.transaction()?.insert_span_data(span)
    }

    fn multi_insert_span_data(&mut self, spans: Vec<&mut models::SpanData>) -> Result<()> {
        self.transaction()?.multi_insert_span_data(spans)
    }

    fn associate_context(&mut self, assoc: models::ContextAssoc) -> Result<models::ContextAssoc> {
        self.transaction()?.associate_context(assoc)
    }

    fn multi_associate_context(&mut self, assocs: Vec<&mut models::ContextAssoc>) -> Result<()> {
        self.transaction()?.multi_associate_context(assocs)
    }

    fn insert_raw_upload(&mut self, raw_upload: models::RawUpload) -> Result<models::RawUpload> {
        self.transaction()?.insert_raw_upload(raw_upload)
    }

    /// Consumes this builder and returns a [`SqliteReport`].
    ///
    /// If any
    /// [`SqliteReportBuilderTx`]s are still in scope, they hold a mutable
    /// reference to `self.conn` and prevent this function from being
    /// called:
    /// ```compile_fail,E0505
    /// # use codecov_rs::report::sqlite::*;
    /// # use codecov_rs::report::ReportBuilder;
    /// # use tempfile::tempdir;
    /// # let temp_dir = tempdir().unwrap();
    /// # let db_file = temp_dir.path().join("test.db");
    ///
    /// let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();
    ///
    /// let mut tx = report_builder.transaction().unwrap();
    /// let _ = tx.insert_file("foo.rs");
    /// let _ = tx.insert_file("bar.rs");
    ///
    /// // ERROR: cannot move out of `report_builder` because it is borrowed
    /// let report = report_builder.build().unwrap();
    /// ```
    ///
    /// Making sure they go out of scope will unblock calling `build()`:
    /// ```
    /// # use codecov_rs::report::sqlite::*;
    /// # use codecov_rs::report::ReportBuilder;
    /// # use tempfile::tempdir;
    /// # let temp_dir = tempdir().unwrap();
    /// # let db_file = temp_dir.path().join("test.db");
    ///
    /// let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();
    ///
    /// // `tx` will go out of scope at the end of this block
    /// {
    ///     let mut tx = report_builder.transaction().unwrap();
    ///     let _ = tx.insert_file("foo.rs");
    ///     let _ = tx.insert_file("bar.rs");
    /// }
    ///
    /// // Works fine now
    /// let report = report_builder.build().unwrap();
    /// ```
    ///
    /// Rolling the transaction back also works:
    /// ```
    /// # use codecov_rs::report::sqlite::*;
    /// # use codecov_rs::report::ReportBuilder;
    /// # use tempfile::tempdir;
    /// # let temp_dir = tempdir().unwrap();
    /// # let db_file = temp_dir.path().join("test.db");
    ///
    /// let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();
    ///
    /// let mut tx = report_builder.transaction().unwrap();
    /// let _ = tx.insert_file("foo.rs");
    /// let _ = tx.insert_file("bar.rs");
    /// let _ = tx.rollback();
    ///
    /// // Works fine now
    /// let report = report_builder.build().unwrap();
    /// ```
    fn build(self) -> Result<SqliteReport> {
        Ok(SqliteReport {
            filename: self.filename,
            conn: self.conn,
        })
    }
}

impl<'a> ReportBuilder<SqliteReport> for SqliteReportBuilderTx<'a> {
    fn insert_file(&mut self, path: &str) -> Result<models::SourceFile> {
        let model = models::SourceFile::new(path);
        model.insert(&self.conn)?;
        Ok(model)
    }

    fn insert_context(&mut self, name: &str) -> Result<models::Context> {
        let model = models::Context::new(name);
        model.insert(&self.conn)?;
        Ok(model)
    }

    fn insert_coverage_sample(
        &mut self,
        mut sample: models::CoverageSample,
    ) -> Result<models::CoverageSample> {
        // TODO handle error
        sample.local_sample_id = self.id_sequence.next().unwrap();
        sample.insert(&self.conn)?;
        Ok(sample)
    }

    fn multi_insert_coverage_sample(
        &mut self,
        mut samples: Vec<&mut models::CoverageSample>,
    ) -> Result<()> {
        for sample in &mut samples {
            sample.local_sample_id = self.id_sequence.next().unwrap();
        }
        models::CoverageSample::multi_insert(samples.iter().map(|v| &**v), &self.conn)?;
        Ok(())
    }

    fn insert_branches_data(
        &mut self,
        mut branch: models::BranchesData,
    ) -> Result<models::BranchesData> {
        // TODO handle error
        branch.local_branch_id = self.id_sequence.next().unwrap();
        branch.insert(&self.conn)?;
        Ok(branch)
    }

    fn multi_insert_branches_data(
        &mut self,
        mut branches: Vec<&mut models::BranchesData>,
    ) -> Result<()> {
        for branch in &mut branches {
            branch.local_branch_id = self.id_sequence.next().unwrap();
        }
        models::BranchesData::multi_insert(branches.iter().map(|v| &**v), &self.conn)?;
        Ok(())
    }

    fn insert_method_data(&mut self, mut method: models::MethodData) -> Result<models::MethodData> {
        // TODO handle error
        method.local_method_id = self.id_sequence.next().unwrap();
        method.insert(&self.conn)?;
        Ok(method)
    }

    fn multi_insert_method_data(
        &mut self,
        mut methods: Vec<&mut models::MethodData>,
    ) -> Result<()> {
        for method in &mut methods {
            method.local_method_id = self.id_sequence.next().unwrap();
        }
        models::MethodData::multi_insert(methods.iter().map(|v| &**v), &self.conn)?;
        Ok(())
    }

    fn insert_span_data(&mut self, mut span: models::SpanData) -> Result<models::SpanData> {
        // TODO handle error
        span.local_span_id = self.id_sequence.next().unwrap();
        span.insert(&self.conn)?;
        Ok(span)
    }

    fn multi_insert_span_data(&mut self, mut spans: Vec<&mut models::SpanData>) -> Result<()> {
        for span in &mut spans {
            span.local_span_id = self.id_sequence.next().unwrap();
        }
        models::SpanData::multi_insert(spans.iter().map(|v| &**v), &self.conn)?;
        Ok(())
    }

    fn associate_context(&mut self, assoc: models::ContextAssoc) -> Result<models::ContextAssoc> {
        assoc.insert(&self.conn)?;
        Ok(assoc)
    }

    fn multi_associate_context(&mut self, assocs: Vec<&mut models::ContextAssoc>) -> Result<()> {
        models::ContextAssoc::multi_insert(assocs.iter().map(|v| &**v), &self.conn)?;
        Ok(())
    }

    fn insert_raw_upload(
        &mut self,
        mut raw_upload: models::RawUpload,
    ) -> Result<models::RawUpload> {
        let mut stmt = self.conn.prepare_cached("INSERT INTO raw_upload (id, timestamp, raw_upload_url, flags, provider, build, name, job_name, ci_run_url, state, env, session_type, session_extras) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)")?;

        raw_upload.id = self.rng.gen();
        let _ = stmt.execute((
            &raw_upload.id,
            &raw_upload.timestamp,
            &raw_upload.raw_upload_url,
            &raw_upload.flags.as_ref().map(|v| v.to_string()),
            &raw_upload.provider,
            &raw_upload.build,
            &raw_upload.name,
            &raw_upload.job_name,
            &raw_upload.ci_run_url,
            &raw_upload.state,
            &raw_upload.env,
            &raw_upload.session_type,
            &raw_upload.session_extras.as_ref().map(|v| v.to_string()),
        ))?;

        Ok(raw_upload)
    }

    fn build(self) -> Result<SqliteReport> {
        Err(CodecovError::ReportBuilderError(
            "called `build()` on a transaction".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use rusqlite_migration::SchemaVersion;
    use serde_json::json;
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

        let expected_file = models::SourceFile::new("src/report.rs");
        let actual_file = report_builder.insert_file(&expected_file.path).unwrap();
        assert_eq!(actual_file, expected_file);

        let duplicate_result = report_builder.insert_file(&expected_file.path);
        assert_eq!(
            duplicate_result.unwrap_err().to_string(),
            "sqlite failure: 'UNIQUE constraint failed: source_file.id'"
        );
    }

    #[test]
    fn test_insert_context() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let expected_context = models::Context::new("foo");
        let actual_context = report_builder.insert_context("foo").unwrap();
        assert_eq!(actual_context, expected_context);

        let duplicate_result = report_builder.insert_context("foo");
        assert_eq!(
            duplicate_result.unwrap_err().to_string(),
            "sqlite failure: 'UNIQUE constraint failed: context.id'"
        );
    }

    #[test]
    fn test_insert_coverage_sample() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let file = report_builder.insert_file("src/report.rs").unwrap();
        let raw_upload = report_builder
            .insert_raw_upload(Default::default())
            .unwrap();

        let mut expected_sample = models::CoverageSample {
            local_sample_id: 1337, // this will be overwritten
            raw_upload_id: raw_upload.id,
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
        assert_ne!(
            expected_sample.local_sample_id,
            actual_sample.local_sample_id
        );
        assert_eq!(actual_sample.local_sample_id, 0);
        expected_sample.local_sample_id = actual_sample.local_sample_id;
        assert_eq!(actual_sample, expected_sample);

        let second_sample = report_builder
            .insert_coverage_sample(expected_sample.clone())
            .unwrap();
        assert_ne!(
            expected_sample.local_sample_id,
            second_sample.local_sample_id
        );
        assert_ne!(actual_sample.local_sample_id, second_sample.local_sample_id);
        assert_eq!(second_sample.local_sample_id, 1);
        expected_sample.local_sample_id = second_sample.local_sample_id;
        assert_eq!(second_sample, expected_sample);
    }

    #[test]
    fn test_multi_insert_coverage_sample() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let file = report_builder.insert_file("src/report.rs").unwrap();
        let raw_upload = report_builder
            .insert_raw_upload(Default::default())
            .unwrap();

        let mut samples: Vec<models::CoverageSample> = vec![
            models::CoverageSample {
                source_file_id: file.id,
                raw_upload_id: raw_upload.id,
                ..Default::default()
            };
            5
        ];
        report_builder
            .multi_insert_coverage_sample(samples.iter_mut().collect())
            .unwrap();

        let report = report_builder.build().unwrap();
        let samples = report.list_coverage_samples().unwrap();
        assert_eq!(
            samples,
            vec![
                models::CoverageSample {
                    local_sample_id: 0,
                    source_file_id: file.id,
                    raw_upload_id: raw_upload.id,
                    ..Default::default()
                },
                models::CoverageSample {
                    local_sample_id: 1,
                    source_file_id: file.id,
                    raw_upload_id: raw_upload.id,
                    ..Default::default()
                },
                models::CoverageSample {
                    local_sample_id: 2,
                    source_file_id: file.id,
                    raw_upload_id: raw_upload.id,
                    ..Default::default()
                },
                models::CoverageSample {
                    local_sample_id: 3,
                    source_file_id: file.id,
                    raw_upload_id: raw_upload.id,
                    ..Default::default()
                },
                models::CoverageSample {
                    local_sample_id: 4,
                    source_file_id: file.id,
                    raw_upload_id: raw_upload.id,
                    ..Default::default()
                },
            ]
        );
    }

    #[test]
    fn test_insert_branches_data() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let file = report_builder.insert_file("src/report.rs").unwrap();
        let raw_upload = report_builder
            .insert_raw_upload(Default::default())
            .unwrap();

        let coverage_sample = report_builder
            .insert_coverage_sample(models::CoverageSample {
                raw_upload_id: raw_upload.id,
                source_file_id: file.id,
                line_no: 1,
                coverage_type: models::CoverageType::Branch,
                hit_branches: Some(2),
                total_branches: Some(4),
                ..Default::default()
            })
            .unwrap();

        let mut expected_branch = models::BranchesData {
            local_branch_id: 1337, // this will be overwritten
            raw_upload_id: raw_upload.id,
            source_file_id: file.id,
            local_sample_id: coverage_sample.local_sample_id,
            hits: 0,
            branch_format: models::BranchFormat::Condition,
            branch: "0:jump".to_string(),
        };
        let actual_branch = report_builder
            .insert_branches_data(expected_branch.clone())
            .unwrap();
        assert_ne!(
            expected_branch.local_branch_id,
            actual_branch.local_branch_id
        );
        assert_eq!(actual_branch.local_branch_id, 1);
        expected_branch.local_branch_id = actual_branch.local_branch_id;
        assert_eq!(actual_branch, expected_branch);

        let second_branch = report_builder
            .insert_branches_data(expected_branch.clone())
            .unwrap();
        assert_ne!(
            expected_branch.local_branch_id,
            second_branch.local_branch_id
        );
        assert_ne!(actual_branch.local_branch_id, second_branch.local_branch_id);
        assert_eq!(second_branch.local_branch_id, 2);
        expected_branch.local_branch_id = second_branch.local_branch_id;
        assert_eq!(second_branch, expected_branch);
    }

    #[test]
    fn test_multi_insert_branches_data() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let file = report_builder.insert_file("src/report.rs").unwrap();
        let raw_upload = report_builder
            .insert_raw_upload(Default::default())
            .unwrap();
        let cov_sample = report_builder
            .insert_coverage_sample(models::CoverageSample {
                source_file_id: file.id,
                raw_upload_id: raw_upload.id,
                ..Default::default()
            })
            .unwrap();

        let mut branches: Vec<models::BranchesData> = vec![
            models::BranchesData {
                source_file_id: file.id,
                raw_upload_id: raw_upload.id,
                local_sample_id: cov_sample.local_sample_id,
                ..Default::default()
            };
            5
        ];
        report_builder
            .multi_insert_branches_data(branches.iter_mut().collect())
            .unwrap();

        let report = report_builder.build().unwrap();
        let branches = report.list_branches_for_sample(&cov_sample).unwrap();
        assert_eq!(
            branches,
            vec![
                models::BranchesData {
                    local_branch_id: 1,
                    source_file_id: file.id,
                    raw_upload_id: raw_upload.id,
                    local_sample_id: cov_sample.local_sample_id,
                    ..Default::default()
                },
                models::BranchesData {
                    local_branch_id: 2,
                    source_file_id: file.id,
                    raw_upload_id: raw_upload.id,
                    local_sample_id: cov_sample.local_sample_id,
                    ..Default::default()
                },
                models::BranchesData {
                    local_branch_id: 3,
                    source_file_id: file.id,
                    raw_upload_id: raw_upload.id,
                    local_sample_id: cov_sample.local_sample_id,
                    ..Default::default()
                },
                models::BranchesData {
                    local_branch_id: 4,
                    source_file_id: file.id,
                    raw_upload_id: raw_upload.id,
                    local_sample_id: cov_sample.local_sample_id,
                    ..Default::default()
                },
                models::BranchesData {
                    local_branch_id: 5,
                    source_file_id: file.id,
                    raw_upload_id: raw_upload.id,
                    local_sample_id: cov_sample.local_sample_id,
                    ..Default::default()
                },
            ]
        );
    }

    #[test]
    fn test_insert_method_data() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let file = report_builder.insert_file("src/report.rs").unwrap();

        let raw_upload = report_builder
            .insert_raw_upload(Default::default())
            .unwrap();

        let coverage_sample = report_builder
            .insert_coverage_sample(models::CoverageSample {
                raw_upload_id: raw_upload.id,
                source_file_id: file.id,
                line_no: 1, // line_no
                coverage_type: models::CoverageType::Branch,
                hit_branches: Some(2),   // hit_branches
                total_branches: Some(4), // total_branches
                ..Default::default()
            })
            .unwrap();

        let mut expected_method = models::MethodData {
            local_method_id: 1337, // this will be overwritten
            raw_upload_id: raw_upload.id,
            source_file_id: file.id,
            local_sample_id: coverage_sample.local_sample_id,
            line_no: Some(1),
            hit_branches: Some(1),
            total_branches: Some(2),
            hit_complexity_paths: Some(1),
            total_complexity: Some(2),
        };

        let actual_method = report_builder
            .insert_method_data(expected_method.clone())
            .unwrap();
        assert_ne!(
            actual_method.local_method_id,
            expected_method.local_method_id
        );
        assert_eq!(actual_method.local_method_id, 1);
        expected_method.local_method_id = actual_method.local_method_id;
        assert_eq!(actual_method, expected_method);

        let second_method = report_builder
            .insert_method_data(expected_method.clone())
            .unwrap();
        assert_ne!(second_method.local_method_id, actual_method.local_method_id);
        assert_ne!(
            second_method.local_method_id,
            expected_method.local_method_id
        );
        assert_eq!(second_method.local_method_id, 2);
        expected_method.local_method_id = second_method.local_method_id;
        assert_eq!(second_method, expected_method);
    }

    #[test]
    fn test_multi_insert_method_data() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let file = report_builder.insert_file("src/report.rs").unwrap();
        let raw_upload = report_builder
            .insert_raw_upload(Default::default())
            .unwrap();
        let cov_sample_1 = report_builder
            .insert_coverage_sample(models::CoverageSample {
                source_file_id: file.id,
                raw_upload_id: raw_upload.id,
                ..Default::default()
            })
            .unwrap();
        let cov_sample_2 = report_builder
            .insert_coverage_sample(models::CoverageSample {
                source_file_id: file.id,
                raw_upload_id: raw_upload.id,
                ..Default::default()
            })
            .unwrap();

        let mut methods: Vec<models::MethodData> = vec![
            models::MethodData {
                source_file_id: file.id,
                raw_upload_id: raw_upload.id,
                local_sample_id: cov_sample_1.local_sample_id,
                ..Default::default()
            },
            models::MethodData {
                source_file_id: file.id,
                raw_upload_id: raw_upload.id,
                local_sample_id: cov_sample_2.local_sample_id,
                ..Default::default()
            },
        ];
        report_builder
            .multi_insert_method_data(methods.iter_mut().collect())
            .unwrap();

        let report = report_builder.build().unwrap();
        let method_1 = report.get_method_for_sample(&cov_sample_1).unwrap();
        assert_eq!(
            method_1,
            Some(models::MethodData {
                local_method_id: 2,
                source_file_id: file.id,
                raw_upload_id: raw_upload.id,
                local_sample_id: cov_sample_1.local_sample_id,
                ..Default::default()
            })
        );
        let method_2 = report.get_method_for_sample(&cov_sample_2).unwrap();
        assert_eq!(
            method_2,
            Some(models::MethodData {
                local_method_id: 3,
                source_file_id: file.id,
                raw_upload_id: raw_upload.id,
                local_sample_id: cov_sample_2.local_sample_id,
                ..Default::default()
            })
        );
    }

    #[test]
    fn test_insert_span_data() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let file = report_builder.insert_file("src/report.rs").unwrap();
        let raw_upload = report_builder
            .insert_raw_upload(Default::default())
            .unwrap();

        let coverage_sample = report_builder
            .insert_coverage_sample(models::CoverageSample {
                raw_upload_id: raw_upload.id,
                source_file_id: file.id,
                line_no: 1, // line_no
                coverage_type: models::CoverageType::Branch,
                hit_branches: Some(2),   // hit_branches
                total_branches: Some(4), // total_branches
                ..Default::default()
            })
            .unwrap();

        let mut expected_span = models::SpanData {
            raw_upload_id: raw_upload.id,
            local_span_id: 1337, // this will be overwritten
            source_file_id: file.id,
            local_sample_id: Some(coverage_sample.local_sample_id),
            hits: 2,
            start_line: Some(1),
            start_col: Some(0),
            end_line: Some(30),
            end_col: Some(60),
        };
        let actual_span = report_builder
            .insert_span_data(expected_span.clone())
            .unwrap();
        assert_ne!(actual_span.local_span_id, expected_span.local_span_id);
        assert_eq!(actual_span.local_span_id, 1);
        expected_span.local_span_id = actual_span.local_span_id;
        assert_eq!(actual_span, expected_span);

        let second_span = report_builder
            .insert_span_data(expected_span.clone())
            .unwrap();
        assert_ne!(second_span.local_span_id, actual_span.local_span_id);
        assert_ne!(second_span.local_span_id, expected_span.local_span_id);
        assert_eq!(second_span.local_span_id, 2);
        expected_span.local_span_id = second_span.local_span_id;
        assert_eq!(second_span, expected_span);
    }

    #[test]
    fn test_multi_insert_span_data() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let file = report_builder.insert_file("src/report.rs").unwrap();
        let raw_upload = report_builder
            .insert_raw_upload(Default::default())
            .unwrap();
        let cov_sample = report_builder
            .insert_coverage_sample(models::CoverageSample {
                source_file_id: file.id,
                raw_upload_id: raw_upload.id,
                ..Default::default()
            })
            .unwrap();

        let mut spans: Vec<models::SpanData> = vec![
            models::SpanData {
                source_file_id: file.id,
                raw_upload_id: raw_upload.id,
                local_sample_id: Some(cov_sample.local_sample_id),
                ..Default::default()
            };
            5
        ];
        report_builder
            .multi_insert_span_data(spans.iter_mut().collect())
            .unwrap();

        let report = report_builder.build().unwrap();
        let branchs = report.list_spans_for_sample(&cov_sample).unwrap();
        assert_eq!(
            branchs,
            vec![
                models::SpanData {
                    local_span_id: 1,
                    source_file_id: file.id,
                    raw_upload_id: raw_upload.id,
                    local_sample_id: Some(cov_sample.local_sample_id),
                    ..Default::default()
                },
                models::SpanData {
                    local_span_id: 2,
                    source_file_id: file.id,
                    raw_upload_id: raw_upload.id,
                    local_sample_id: Some(cov_sample.local_sample_id),
                    ..Default::default()
                },
                models::SpanData {
                    local_span_id: 3,
                    source_file_id: file.id,
                    raw_upload_id: raw_upload.id,
                    local_sample_id: Some(cov_sample.local_sample_id),
                    ..Default::default()
                },
                models::SpanData {
                    local_span_id: 4,
                    source_file_id: file.id,
                    raw_upload_id: raw_upload.id,
                    local_sample_id: Some(cov_sample.local_sample_id),
                    ..Default::default()
                },
                models::SpanData {
                    local_span_id: 5,
                    source_file_id: file.id,
                    raw_upload_id: raw_upload.id,
                    local_sample_id: Some(cov_sample.local_sample_id),
                    ..Default::default()
                },
            ]
        );
    }

    #[test]
    fn test_insert_context_assoc() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let file = report_builder.insert_file("src/report.rs").unwrap();

        let raw_upload = report_builder
            .insert_raw_upload(Default::default())
            .unwrap();

        let coverage_sample = report_builder
            .insert_coverage_sample(models::CoverageSample {
                raw_upload_id: raw_upload.id,
                source_file_id: file.id,
                line_no: 1, // line_no
                coverage_type: models::CoverageType::Branch,
                hit_branches: Some(2),   // hit_branches
                total_branches: Some(4), // total_branches
                ..Default::default()
            })
            .unwrap();

        let span = report_builder
            .insert_span_data(models::SpanData {
                raw_upload_id: raw_upload.id,
                source_file_id: file.id,
                local_sample_id: Some(coverage_sample.local_sample_id),
                hits: 1,             // hits
                start_line: Some(1), // start_line
                start_col: Some(0),  // start_col
                end_line: Some(30),  // end_line
                end_col: Some(60),   // end_col
                ..Default::default()
            })
            .unwrap();

        let context = report_builder.insert_context("test_case").unwrap();

        let expected_assoc = models::ContextAssoc {
            context_id: context.id,
            raw_upload_id: raw_upload.id,
            local_sample_id: Some(coverage_sample.local_sample_id),
            local_span_id: Some(span.local_span_id),
        };
        let actual_assoc = report_builder
            .associate_context(models::ContextAssoc {
                context_id: context.id,
                raw_upload_id: raw_upload.id,
                local_sample_id: Some(coverage_sample.local_sample_id),
                local_span_id: Some(span.local_span_id),
            })
            .unwrap();
        assert_eq!(actual_assoc, expected_assoc);

        let duplicate_result = report_builder.associate_context(expected_assoc.clone());
        assert_eq!(
            duplicate_result.unwrap_err().to_string(),
            "sqlite failure: 'UNIQUE constraint failed: context_assoc.context_id, context_assoc.raw_upload_id, context_assoc.local_sample_id, context_assoc.local_span_id'"
        );
    }

    #[test]
    fn test_multi_associate_context() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let file = report_builder.insert_file("src/report.rs").unwrap();
        let raw_upload = report_builder
            .insert_raw_upload(Default::default())
            .unwrap();
        let cov_sample = report_builder
            .insert_coverage_sample(models::CoverageSample {
                source_file_id: file.id,
                raw_upload_id: raw_upload.id,
                ..Default::default()
            })
            .unwrap();

        let contexts = vec![
            report_builder.insert_context("test case 1").unwrap(),
            report_builder.insert_context("test case 2").unwrap(),
            report_builder.insert_context("test case 3").unwrap(),
            report_builder.insert_context("test case 4").unwrap(),
        ];

        let mut assocs: Vec<_> = contexts
            .iter()
            .map(|context| models::ContextAssoc {
                raw_upload_id: raw_upload.id,
                local_sample_id: Some(cov_sample.local_sample_id),
                context_id: context.id,
                ..Default::default()
            })
            .collect();

        report_builder
            .multi_associate_context(assocs.iter_mut().collect())
            .unwrap();

        let report = report_builder.build().unwrap();
        let associated_contexts = report.list_contexts_for_sample(&cov_sample).unwrap();
        assert_eq!(associated_contexts, contexts);
    }

    #[test]
    fn test_insert_raw_upload() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let inserted_upload = models::RawUpload {
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
            ..Default::default()
        };
        let inserted_upload = report_builder.insert_raw_upload(inserted_upload).unwrap();

        let report = report_builder.build().unwrap();
        let fetched_uploads = report.list_raw_uploads().unwrap();
        assert_eq!(fetched_uploads, &[inserted_upload]);
    }

    #[test]
    fn test_transaction_drop_behavior() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let tx = report_builder.transaction().unwrap();
        assert_eq!(tx.conn.drop_behavior(), rusqlite::DropBehavior::Commit);
    }

    #[test]
    fn test_transaction_cannot_build() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let tx = report_builder.transaction().unwrap();

        assert_eq!(
            tx.build().unwrap_err().to_string(),
            "report builder error: 'called `build()` on a transaction'"
        );
    }

    #[test]
    fn test_transaction_rollback() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let mut tx = report_builder.transaction().unwrap();
        tx.insert_file("foo.rs").unwrap();
        tx.insert_file("bar.rs").unwrap();
        tx.rollback().unwrap();

        let report = report_builder.build().unwrap();
        let files = report.list_files().unwrap();
        assert_eq!(files.len(), 0);
    }
}
