/*!
 * SQLite-based implementation of the [`super::Report`] and
 * [`super::ReportBuilder`] traits.
 *
 * Notes on SQLite performance:
 * - Some `ORDER BY` clauses are to make writing test cases simple and may
 *   not be necessary
 * - Tables with UUID/BLOB PKs are declared [`WITHOUT ROWID`](https://www.sqlite.org/withoutrowid.html)
 */
use std::path::PathBuf;

use include_dir::{include_dir, Dir};
use lazy_static::lazy_static;
use rusqlite::Connection;
use rusqlite_migration::Migrations;

use crate::error::Result;

mod models;
mod report;
mod report_builder;

pub use models::*;
pub use report::*;
pub use report_builder::*;

static MIGRATIONS_DIR: Dir = include_dir!("$CARGO_MANIFEST_DIR/migrations");

lazy_static! {
    static ref MIGRATIONS: Migrations<'static> =
        Migrations::from_directory(&MIGRATIONS_DIR).unwrap();
}

fn open_database(filename: &PathBuf) -> Result<Connection> {
    let mut conn = Connection::open(filename)?;
    MIGRATIONS.to_latest(&mut conn)?;

    Ok(conn)
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

        let conn = open_database(&db_file).unwrap();
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
            let conn = open_database(&db_file).unwrap();
            let _ = conn.execute(
                "INSERT INTO source_file (id, path) VALUES (?1, ?2)",
                (1, "src/report.rs"),
            );
        }

        let conn = open_database(&db_file).unwrap();
        let (id, path): (i64, String) = conn
            .query_row("SELECT id, path FROM source_file", [], |row| {
                Ok((row.get(0).unwrap(), row.get(1).unwrap()))
            })
            .unwrap();
        assert_eq!(id, 1);
        assert_eq!(path, "src/report.rs");
    }
}
