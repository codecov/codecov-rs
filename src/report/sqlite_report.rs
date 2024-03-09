use std::path::PathBuf;

use include_dir::{include_dir, Dir};
use lazy_static::lazy_static;
use rusqlite::{Connection, Result};
use rusqlite_migration::Migrations;

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
    fn insert_file(&mut self, file: models::SourceFile) -> Result<models::SourceFile> {
        let mut stmt = self
            .conn
            // TODO: memoize prepared statements
            .prepare("INSERT INTO source_file (path) VALUES (?1) RETURNING id, path")?;
        stmt.query_row([file.path], |row| {
            Ok(models::SourceFile {
                id: row.get(0)?,
                path: row.get(1)?,
            })
        })
    }

    fn insert_context(&mut self, context: models::Context) -> Result<models::Context> {
        // TODO: memoize prepared statements
        let mut stmt = self.conn.prepare("INSERT INTO context (context_type, name) VALUES (?1, ?2) RETURNING id, context_type, name")?;
        stmt.query_row([context.context_type.to_string(), context.name], |row| {
            Ok(models::Context {
                id: row.get(0)?,
                context_type: row.get(1)?,
                name: row.get(2)?,
            })
        })
    }

    fn insert_line(
        &mut self,
        line: models::LineStatus,
        context: &models::Context,
    ) -> Result<models::LineStatus> {
        // TODO: memoize prepared statements
        let mut stmt = self.conn.prepare("INSERT INTO line_status (source_file_id, line_no, coverage_status) VALUES (?1, ?2, ?3) RETURNING id, source_file_id, line_no, coverage_status")?;
        let line = stmt.query_row(
            [
                line.source_file_id,
                line.line_no,
                line.coverage_status as i32,
            ],
            |row| {
                Ok(models::LineStatus {
                    id: row.get(0)?,
                    source_file_id: row.get(1)?,
                    line_no: row.get(2)?,
                    coverage_status: row.get(3)?,
                })
            },
        )?;

        match (line.id, context.id) {
            (Some(line_id), Some(context_id)) => {
                let mut context_assoc = self
                    .conn
                    // TODO: memoize prepared statements
                    .prepare("INSERT INTO context_assoc (context_id, line_id) VALUES (?1, ?2)")?;
                context_assoc.execute([context_id, line_id])?;
                Ok(line)
            }
            _ => {
                // TODO create an error type since there isn't a corresponding rusqlite::Error
                // type
                panic!("missing line.id and/or context.id");
            }
        }
    }

    fn insert_branch(
        &mut self,
        branch: models::BranchStatus,
        context: &models::Context,
    ) -> Result<models::BranchStatus> {
        // TODO: memoize prepared statements
        let mut stmt = self.conn.prepare("INSERT INTO branch_status (source_file_id, start_line_no, end_line_no, coverage_status) VALUES (?1, ?2, ?3, ?4) RETURNING id, source_file_id, start_line_no, end_line_no, coverage_status")?;
        let branch = stmt.query_row(
            [
                branch.source_file_id,
                branch.start_line_no,
                branch.end_line_no,
                branch.coverage_status as i32,
            ],
            |row| {
                Ok(models::BranchStatus {
                    id: row.get(0)?,
                    source_file_id: row.get(1)?,
                    start_line_no: row.get(2)?,
                    end_line_no: row.get(3)?,
                    coverage_status: row.get(4)?,
                })
            },
        )?;

        match (branch.id, context.id) {
            (Some(branch_id), Some(context_id)) => {
                let mut context_assoc = self
                    .conn
                    // TODO: memoize prepared statements
                    .prepare("INSERT INTO context_assoc (context_id, branch_id) VALUES (?1, ?2)")?;
                context_assoc.execute([context_id, branch_id])?;
                Ok(branch)
            }
            _ => {
                // TODO create an error type since there isn't a corresponding rusqlite::Error
                // type
                panic!("missing branch.id and/or context.id");
            }
        }
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
        let (id, path): (i32, String) = conn
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

            let file = models::SourceFile {
                id: None,
                path: "src/report.rs".to_string(),
            };
            let file = report_builder
                .insert_file(file)
                .expect("error inserting file");
            assert_eq!(file.id, Some(1));
            assert_eq!(file.path, "src/report.rs");
        }

        #[test]
        fn test_insert_context() {
            let ctx = setup();
            let db_file = ctx.temp_dir.path().join("db.sqlite");
            let mut report_builder = SqliteReportBuilder::new(db_file);

            let context = models::Context {
                id: None,
                context_type: models::ContextType::Upload,
                name: "foo".to_string(),
            };
            let context = report_builder
                .insert_context(context)
                .expect("error inserting context");
            assert_eq!(context.id, Some(1));
            assert_eq!(context.context_type, models::ContextType::Upload);
            assert_eq!(context.name, "foo");
        }

        #[test]
        fn test_insert_line() {
            let ctx = setup();
            let db_file = ctx.temp_dir.path().join("db.sqlite");
            let mut report_builder = SqliteReportBuilder::new(db_file);

            let context = models::Context {
                id: None,
                context_type: models::ContextType::Upload,
                name: "foo".to_string(),
            };
            let context = report_builder
                .insert_context(context)
                .expect("error inserting context");

            let file = models::SourceFile {
                id: None,
                path: "src/report.rs".to_string(),
            };
            let file = report_builder
                .insert_file(file)
                .expect("error inserting file");

            let line = models::LineStatus {
                id: None,
                source_file_id: file.id.expect("failed to get file id"),
                line_no: 1,
                coverage_status: models::CoverageStatus::Hit,
            };
            let line = report_builder
                .insert_line(line, &context)
                .expect("error inserting line");
            assert_eq!(line.id, Some(1));
            assert_eq!(line.source_file_id, 1);
            assert_eq!(line.line_no, 1);
            assert_eq!(line.coverage_status, models::CoverageStatus::Hit);

            let line_context_assoc = report_builder.conn.query_row(
                "SELECT context_id, line_id FROM context_assoc WHERE context_id=?1 AND line_id=?2",
                [context.id.unwrap(), line.id.unwrap()],
                |row| {
                    Ok(models::ContextAssoc {
                        context_id: row.get(0)?,
                        line_id: Some(row.get(1)?),
                        branch_id: None,
                    })
                },
            ).expect("error fetching context_assoc");
            assert_eq!(line_context_assoc.context_id, context.id.unwrap());
            assert_eq!(line_context_assoc.line_id, line.id);
            assert_eq!(line_context_assoc.branch_id, None);
        }

        #[test]
        fn test_insert_branch() {
            let ctx = setup();
            let db_file = ctx.temp_dir.path().join("db.sqlite");
            let mut report_builder = SqliteReportBuilder::new(db_file);

            let context = models::Context {
                id: None,
                context_type: models::ContextType::Upload,
                name: "foo".to_string(),
            };
            let context = report_builder
                .insert_context(context)
                .expect("error inserting context");

            let file = models::SourceFile {
                id: None,
                path: "src/report.rs".to_string(),
            };
            let file = report_builder
                .insert_file(file)
                .expect("error inserting file");

            let branch = models::BranchStatus {
                id: None,
                source_file_id: file.id.expect("failed to get file id"),
                start_line_no: 1,
                end_line_no: 2,
                coverage_status: models::CoverageStatus::Hit,
            };
            let branch = report_builder
                .insert_branch(branch, &context)
                .expect("error inserting branch");
            assert_eq!(branch.id, Some(1));
            assert_eq!(branch.source_file_id, 1);
            assert_eq!(branch.start_line_no, 1);
            assert_eq!(branch.end_line_no, 2);
            assert_eq!(branch.coverage_status, models::CoverageStatus::Hit);

            let branch_context_assoc = report_builder.conn.query_row(
                "SELECT context_id, branch_id FROM context_assoc WHERE context_id=?1 AND branch_id=?2",
                [context.id.unwrap(), branch.id.unwrap()],
                |row| {
                    Ok(models::ContextAssoc {
                        context_id: row.get(0)?,
                        branch_id: Some(row.get(1)?),
                        line_id: None,
                    })
                },
            ).expect("error fetching context_assoc");
            assert_eq!(branch_context_assoc.context_id, context.id.unwrap());
            assert_eq!(branch_context_assoc.branch_id, branch.id);
            assert_eq!(branch_context_assoc.line_id, None);
        }
    }
}
