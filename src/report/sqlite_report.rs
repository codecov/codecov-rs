use std::path::PathBuf;

use include_dir::{include_dir, Dir};
use lazy_static::lazy_static;
use rusqlite::{Connection, Result};
use rusqlite_migration::Migrations;
use uuid::Uuid;

use crate::report::{models, Report, ReportBuilder};

static MIGRATIONS_DIR: Dir = include_dir!("$CARGO_MANIFEST_DIR/migrations");

lazy_static! {
    pub static ref MIGRATIONS: Migrations<'static> =
        Migrations::from_directory(&MIGRATIONS_DIR).unwrap();
}

pub struct SqliteReport {
    pub filename: PathBuf,
    pub conn: Connection,
}

fn open_database(filename: &PathBuf) -> Connection {
    let mut conn = Connection::open(&filename).expect("error opening database");
    MIGRATIONS
        .to_latest(&mut conn)
        .expect("error applying migrations");

    conn
}

impl SqliteReport {
    pub fn new(filename: PathBuf) -> SqliteReport {
        let conn = open_database(&filename);
        SqliteReport { filename, conn }
    }
}

impl Report for SqliteReport {
    // TODO: implement for real, just using for integration tests
    fn list_files(&self) -> Result<Vec<models::SourceFile>> {
        let mut stmt = self
            .conn
            // TODO: memoize prepared statements
            .prepare("SELECT id, path FROM source_file")?;
        let rows = stmt.query_map([], |row| {
            Ok(models::SourceFile {
                id: row.get(0)?,
                path: row.get(1)?,
            })
        })?;

        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    // TODO: implement for real, just using for integration tests
    fn list_contexts(&self) -> Result<Vec<models::Context>> {
        let mut stmt = self
            .conn
            // TODO: memoize prepared statements
            .prepare("SELECT id, context_type, name FROM context")?;
        let rows = stmt.query_map([], |row| {
            Ok(models::Context {
                id: row.get(0)?,
                context_type: row.get(1)?,
                name: row.get(2)?,
            })
        })?;

        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    // TODO implement for real, just using for integration tests
    fn list_coverage_samples(&self) -> Result<Vec<models::CoverageSample>> {
        let mut stmt = self
            .conn
            // TODO: memoize prepared statements
            .prepare("SELECT id, source_file_id, line_no, coverage_type, hits, hit_branches, total_branches FROM coverage_sample")?;
        let rows = stmt.query_map([], |row| {
            Ok(models::CoverageSample {
                id: row.get(0)?,
                source_file_id: row.get(1)?,
                line_no: row.get(2)?,
                coverage_type: row.get(3)?,
                hits: row.get(4)?,
                hit_branches: row.get(5)?,
                total_branches: row.get(6)?,
            })
        })?;

        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    // TODO implement for real, just using for integration tests
    fn list_contexts_for_sample(
        &self,
        sample: &models::CoverageSample,
    ) -> Result<Vec<models::Context>> {
        let mut stmt = self
            .conn
            // TODO: memoize prepared statements
            .prepare("SELECT context.id, context.context_type, context.name FROM context INNER JOIN context_assoc ON context.id = context_assoc.context_id WHERE context_assoc.sample_id = ?1")?;
        let rows = stmt.query_map([sample.id], |row| {
            Ok(models::Context {
                id: row.get(0)?,
                context_type: row.get(1)?,
                name: row.get(2)?,
            })
        })?;

        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }
}

pub struct SqliteReportBuilder {
    pub filename: PathBuf,
    pub conn: Connection,
}

impl SqliteReportBuilder {
    pub fn new(filename: PathBuf) -> SqliteReportBuilder {
        let conn = open_database(&filename);
        SqliteReportBuilder { filename, conn }
    }
}

impl ReportBuilder<SqliteReport> for SqliteReportBuilder {
    fn insert_file(&mut self, path: String) -> Result<models::SourceFile> {
        let mut stmt = self
            .conn
            // TODO: memoize prepared statements
            .prepare("INSERT INTO source_file (id, path) VALUES (?1, ?2) RETURNING id, path")?;

        stmt.query_row((seahash::hash(path.as_bytes()) as i64, path), |row| {
            Ok(models::SourceFile {
                id: row.get(0)?,
                path: row.get(1)?,
            })
        })
    }

    fn insert_context(
        &mut self,
        context_type: models::ContextType,
        name: &str,
    ) -> Result<models::Context> {
        // TODO: memoize prepared statements
        let mut stmt = self.conn.prepare("INSERT INTO context (id, context_type, name) VALUES (?1, ?2, ?3) RETURNING id, context_type, name")?;
        stmt.query_row(
            (
                seahash::hash(name.as_bytes()) as i64,
                context_type.to_string(),
                name,
            ),
            |row| {
                Ok(models::Context {
                    id: row.get(0)?,
                    context_type: row.get(1)?,
                    name: row.get(2)?,
                })
            },
        )
    }

    fn insert_coverage_sample(
        &mut self,
        source_file_id: i64,
        line_no: i64,
        coverage_type: models::CoverageType,
        hits: Option<i64>,
        hit_branches: Option<i64>,
        total_branches: Option<i64>,
    ) -> Result<models::CoverageSample> {
        let mut stmt = self.conn.prepare("INSERT INTO coverage_sample (id, source_file_id, line_no, coverage_type, hits, hit_branches, total_branches) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) RETURNING id, source_file_id, line_no, coverage_type, hits, hit_branches, total_branches")?;
        stmt.query_row(
            (
                Uuid::new_v4(),
                source_file_id,
                line_no,
                coverage_type,
                hits,
                hit_branches,
                total_branches,
            ),
            |row| {
                Ok(models::CoverageSample {
                    id: row.get(0)?,
                    source_file_id: row.get(1)?,
                    line_no: row.get(2)?,
                    coverage_type: row.get(3)?,
                    hits: row.get(4)?,
                    hit_branches: row.get(5)?,
                    total_branches: row.get(6)?,
                })
            },
        )
    }

    fn insert_branches_data(
        &mut self,
        source_file_id: i64,
        sample_id: Uuid,
        hits: i64,
        branch_format: models::BranchFormat,
        branch: String,
    ) -> Result<models::BranchesData> {
        let mut stmt = self.conn.prepare("INSERT INTO branches_data (id, source_file_id, sample_id, hits, branch_format, branch) VALUES (?1, ?2, ?3, ?4, ?5, ?6) RETURNING id, source_file_id, sample_id, hits, branch_format, branch")?;

        stmt.query_row(
            (
                Uuid::new_v4(),
                source_file_id,
                sample_id,
                hits,
                branch_format,
                branch,
            ),
            |row| {
                Ok(models::BranchesData {
                    id: row.get(0)?,
                    source_file_id: row.get(1)?,
                    sample_id: row.get(2)?,
                    hits: row.get(3)?,
                    branch_format: row.get(4)?,
                    branch: row.get(5)?,
                })
            },
        )
    }

    fn insert_method_data(
        &mut self,
        source_file_id: i64,
        sample_id: Option<Uuid>,
        line_no: Option<i64>,
        hit_branches: Option<i64>,
        total_branches: Option<i64>,
        hit_complexity_paths: Option<i64>,
        total_complexity: Option<i64>,
    ) -> Result<models::MethodData> {
        let mut stmt = self.conn.prepare("INSERT INTO method_data (id, source_file_id, sample_id, line_no, hit_branches, total_branches, hit_complexity_paths, total_complexity) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) RETURNING id, source_file_id, sample_id, line_no, hit_branches, total_branches, hit_complexity_paths, total_complexity")?;

        stmt.query_row(
            (
                Uuid::new_v4(),
                source_file_id,
                sample_id,
                line_no,
                hit_branches,
                total_branches,
                hit_complexity_paths,
                total_complexity,
            ),
            |row| {
                Ok(models::MethodData {
                    id: row.get(0)?,
                    source_file_id: row.get(1)?,
                    sample_id: row.get(2)?,
                    line_no: row.get(3)?,
                    hit_branches: row.get(4)?,
                    total_branches: row.get(5)?,
                    hit_complexity_paths: row.get(6)?,
                    total_complexity: row.get(7)?,
                })
            },
        )
    }

    fn insert_span_data(
        &mut self,
        source_file_id: i64,
        sample_id: Option<Uuid>,
        hits: i64,
        start_line: Option<i64>,
        start_col: Option<i64>,
        end_line: Option<i64>,
        end_col: Option<i64>,
    ) -> Result<models::SpanData> {
        let mut stmt = self.conn.prepare("INSERT INTO span_data (id, source_file_id, sample_id, hits, start_line, start_col, end_line, end_col) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) RETURNING id, source_file_id, sample_id, hits, start_line, start_col, end_line, end_col")?;

        stmt.query_row(
            (
                Uuid::new_v4(),
                source_file_id,
                sample_id,
                hits,
                start_line,
                start_col,
                end_line,
                end_col,
            ),
            |row| {
                Ok(models::SpanData {
                    id: row.get(0)?,
                    source_file_id: row.get(1)?,
                    sample_id: row.get(2)?,
                    hits: row.get(3)?,
                    start_line: row.get(4)?,
                    start_col: row.get(5)?,
                    end_line: row.get(6)?,
                    end_col: row.get(7)?,
                })
            },
        )
    }

    fn associate_context<'a>(
        &mut self,
        context_id: i64,
        sample: Option<&'a models::CoverageSample>,
        branches_data: Option<&'a models::BranchesData>,
        method_data: Option<&'a models::MethodData>,
        span_data: Option<&'a models::SpanData>,
    ) -> Result<models::ContextAssoc> {
        let mut stmt = self.conn.prepare("INSERT INTO context_assoc (context_id, sample_id, branch_id, method_id, span_id) VALUES (?1, ?2, ?3, ?4, ?5) RETURNING context_id, sample_id, branch_id, method_id, span_id")?;

        stmt.query_row(
            (
                context_id,
                sample.map(|s| s.id),
                branches_data.map(|b| b.id),
                method_data.map(|m| m.id),
                span_data.map(|s| s.id),
            ),
            |row| {
                Ok(models::ContextAssoc {
                    context_id: row.get(0)?,
                    sample_id: row.get(1)?,
                    branch_id: row.get(2)?,
                    method_id: row.get(3)?,
                    span_id: row.get(4)?,
                })
            },
        )
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
    use tempfile::TempDir;

    use super::*;

    struct Ctx {
        temp_dir: TempDir,
    }

    fn setup() -> Ctx {
        Ctx {
            temp_dir: TempDir::new().ok().unwrap(),
        }
    }

    #[test]
    fn test_open_database_new_file_runs_migrations() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        assert!(!db_file.exists());

        let conn = open_database(&db_file);
        assert_eq!(
            MIGRATIONS.current_version(&conn),
            Ok(SchemaVersion::Inside(NonZeroUsize::new(1).unwrap()))
        );
    }

    #[test]
    fn test_open_database_existing_file() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        assert!(!db_file.exists());

        {
            let conn = open_database(&db_file);
            let _ = conn.execute(
                "INSERT INTO source_file (id, path) VALUES (?1, ?2)",
                (1, "src/report.rs"),
            );
        }

        let conn = open_database(&db_file);
        let (id, path): (i64, String) = conn
            .query_row("SELECT id, path FROM source_file", [], |row| {
                Ok((row.get(0).unwrap(), row.get(1).unwrap()))
            })
            .unwrap();
        assert_eq!(id, 1);
        assert_eq!(path, "src/report.rs");
    }

    mod sqlite_report {
        use super::*;

        #[test]
        fn test_new_report_runs_migrations() {
            let ctx = setup();
            let db_file = ctx.temp_dir.path().join("db.sqlite");
            assert!(!db_file.exists());

            let report = SqliteReport::new(db_file);
            assert_eq!(
                MIGRATIONS.current_version(&report.conn),
                Ok(SchemaVersion::Inside(NonZeroUsize::new(1).unwrap()))
            );
        }
    }

    mod sqlite_report_builder {
        use super::*;

        fn hash_id(key: &str) -> i64 {
            seahash::hash(key.as_bytes()) as i64
        }

        #[test]
        fn test_new_report_builder_runs_migrations() {
            let ctx = setup();
            let db_file = ctx.temp_dir.path().join("db.sqlite");
            assert!(!db_file.exists());

            let report_builder = SqliteReportBuilder::new(db_file);
            assert_eq!(
                MIGRATIONS.current_version(&report_builder.conn),
                Ok(SchemaVersion::Inside(NonZeroUsize::new(1).unwrap()))
            );
        }

        #[test]
        fn test_insert_file() {
            let ctx = setup();
            let db_file = ctx.temp_dir.path().join("db.sqlite");
            let mut report_builder = SqliteReportBuilder::new(db_file);

            let expected_file = models::SourceFile {
                id: hash_id("src/report.rs"),
                path: "src/report.rs".to_string(),
            };
            let actual_file = report_builder
                .insert_file(expected_file.path.clone())
                .expect("error inserting file");
            assert_eq!(actual_file, expected_file);
        }

        #[test]
        fn test_insert_context() {
            let ctx = setup();
            let db_file = ctx.temp_dir.path().join("db.sqlite");
            let mut report_builder = SqliteReportBuilder::new(db_file);

            let expected_context = models::Context {
                id: hash_id("foo"),
                context_type: models::ContextType::Upload,
                name: "foo".to_string(),
            };
            let actual_context = report_builder
                .insert_context(expected_context.context_type, &expected_context.name)
                .expect("error inserting context");
            assert_eq!(actual_context, expected_context);
        }

        #[test]
        fn test_insert_coverage_sample() {
            let ctx = setup();
            let db_file = ctx.temp_dir.path().join("db.sqlite");
            let mut report_builder = SqliteReportBuilder::new(db_file);

            let file = report_builder
                .insert_file("src/report.rs".to_string())
                .expect("error inserting file");

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
                .insert_coverage_sample(
                    expected_sample.source_file_id,
                    expected_sample.line_no,
                    expected_sample.coverage_type,
                    expected_sample.hits,
                    expected_sample.hit_branches,
                    expected_sample.total_branches,
                )
                .expect("error inserting line");
            expected_sample.id = actual_sample.id.clone();
            assert_eq!(actual_sample, expected_sample);
        }

        #[test]
        fn test_insert_branches_data() {
            let ctx = setup();
            let db_file = ctx.temp_dir.path().join("db.sqlite");
            let mut report_builder = SqliteReportBuilder::new(db_file);

            let file = report_builder
                .insert_file("src/report.rs".to_string())
                .expect("error inserting file");

            let coverage_sample = report_builder
                .insert_coverage_sample(
                    file.id,
                    1, // line_no
                    models::CoverageType::Branch,
                    None,    // hits
                    Some(2), // hit_branches
                    Some(4), // total_branches
                )
                .expect("error inserting coverage sample");

            let mut expected_branch = models::BranchesData {
                id: Uuid::nil(), // Ignored
                source_file_id: file.id,
                sample_id: coverage_sample.id,
                hits: 0,
                branch_format: models::BranchFormat::Condition,
                branch: "0:jump".to_string(),
            };
            let actual_branch = report_builder
                .insert_branches_data(
                    expected_branch.source_file_id,
                    expected_branch.sample_id,
                    expected_branch.hits,
                    expected_branch.branch_format,
                    expected_branch.branch.clone(),
                )
                .expect("error inserting branches data");
            expected_branch.id = actual_branch.id;
            assert_eq!(actual_branch, expected_branch);
        }

        #[test]
        fn test_insert_method_data() {
            let ctx = setup();
            let db_file = ctx.temp_dir.path().join("db.sqlite");
            let mut report_builder = SqliteReportBuilder::new(db_file);

            let file = report_builder
                .insert_file("src/report.rs".to_string())
                .expect("error inserting file");

            let coverage_sample = report_builder
                .insert_coverage_sample(
                    file.id,
                    1, // line_no
                    models::CoverageType::Branch,
                    None,    // hits
                    Some(2), // hit_branches
                    Some(4), // total_branches
                )
                .expect("error inserting coverage sample");

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
                .insert_method_data(
                    expected_method.source_file_id,
                    expected_method.sample_id,
                    expected_method.line_no,
                    expected_method.hit_branches,
                    expected_method.total_branches,
                    expected_method.hit_complexity_paths,
                    expected_method.total_complexity,
                )
                .expect("error inserting method data");
            expected_method.id = actual_method.id;
            assert_eq!(actual_method, expected_method);
        }

        #[test]
        fn test_insert_span_data() {
            let ctx = setup();
            let db_file = ctx.temp_dir.path().join("db.sqlite");
            let mut report_builder = SqliteReportBuilder::new(db_file);

            let file = report_builder
                .insert_file("src/report.rs".to_string())
                .expect("error inserting file");

            let coverage_sample = report_builder
                .insert_coverage_sample(
                    file.id,
                    1, // line_no
                    models::CoverageType::Branch,
                    None,    // hits
                    Some(2), // hit_branches
                    Some(4), // total_branches
                )
                .expect("error inserting coverage sample");

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
                .insert_span_data(
                    expected_span.source_file_id,
                    expected_span.sample_id,
                    expected_span.hits,
                    expected_span.start_line,
                    expected_span.start_col,
                    expected_span.end_line,
                    expected_span.end_col,
                )
                .expect("error inserting span data");
            expected_span.id = actual_span.id;
            assert_eq!(actual_span, expected_span);
        }

        #[test]
        fn test_insert_context_assoc() {
            let ctx = setup();
            let db_file = ctx.temp_dir.path().join("db.sqlite");
            let mut report_builder = SqliteReportBuilder::new(db_file);

            let file = report_builder
                .insert_file("src/report.rs".to_string())
                .expect("error inserting file");

            let coverage_sample = report_builder
                .insert_coverage_sample(
                    file.id,
                    1, // line_no
                    models::CoverageType::Branch,
                    None,    // hits
                    Some(2), // hit_branches
                    Some(4), // total_branches
                )
                .expect("error inserting coverage sample");

            let branch = report_builder
                .insert_branches_data(
                    file.id,
                    coverage_sample.id,
                    0, // hits
                    models::BranchFormat::Condition,
                    "0:jump".to_string(),
                )
                .expect("error inserting branches data");

            let method = report_builder
                .insert_method_data(
                    file.id,
                    Some(coverage_sample.id),
                    Some(1), // line_no
                    Some(1), // hit_branches
                    Some(2), // total_branches
                    Some(1), // hit_complexity_paths
                    Some(2), // total_complexity_paths
                )
                .expect("error inserting method data");

            let span = report_builder
                .insert_span_data(
                    file.id,
                    Some(coverage_sample.id),
                    1,        // hits
                    Some(1),  // start_line
                    Some(0),  // start_col
                    Some(30), // end_line
                    Some(60), // end_col
                )
                .expect("error inserting span data");

            let context = report_builder
                .insert_context(models::ContextType::Upload, &"upload".to_string())
                .expect("error inserting context");

            let expected_assoc = models::ContextAssoc {
                context_id: context.id,
                sample_id: Some(coverage_sample.id),
                branch_id: Some(branch.id),
                method_id: Some(method.id),
                span_id: Some(span.id),
            };
            let actual_assoc = report_builder
                .associate_context(
                    context.id,
                    Some(&coverage_sample),
                    Some(&branch),
                    Some(&method),
                    Some(&span),
                )
                .expect("error inserting assoc row");
            assert_eq!(actual_assoc, expected_assoc);
        }
    }
}
