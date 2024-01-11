use std::path::PathBuf;

use diesel::{sqlite::SqliteConnection, Connection};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

pub mod models;
pub mod schema;

pub struct Report {
    pub filename: PathBuf,
    pub conn: SqliteConnection,
}

impl Report {
    pub fn new(filename: PathBuf) -> Report {
        // TODO: handle errors/results properly
        let mut conn = SqliteConnection::establish(filename.to_str().unwrap())
            .ok()
            .unwrap();
        conn.run_pending_migrations(MIGRATIONS).ok();

        Report { filename, conn }
    }
}

#[cfg(test)]
mod tests {
    use diesel::{QueryDsl, RunQueryDsl, SelectableHelper};
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
    fn test_new_report() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        assert!(!db_file.exists());

        let mut report = Report::new(db_file);
        assert!(!report.conn.has_pending_migration(MIGRATIONS).ok().unwrap());
    }

    #[test]
    fn test_existing_report() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        assert!(!db_file.exists());

        let mut new_report = Report::new(db_file.clone());

        let mock_context = models::Context {
            id: 0,
            context_type: schema::ContextType::TestCase,
            name: "mock_context".to_string(),
        };
        diesel::insert_into(schema::context::table)
            .values(&mock_context)
            .execute(&mut new_report.conn)
            .expect("failed to add mock context");

        let mut existing_report = Report::new(db_file.clone());
        let contexts = schema::context::table
            .select(models::Context::as_select())
            .load(&mut existing_report.conn)
            .expect("error loading contexts");
        assert!(contexts.len() == 1);
    }
}
