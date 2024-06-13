/*!
 * Trait implementations for [`crate::report::models`] types for use with
 * `rusqlite`.
 *
 * `ToSql`/`FromSql` allow enums to be used as model fields.
 * `TryFrom<&rusqlite::Row>` allows models to be automatically constructed
 * from query results (provided the query's column names are
 * named appropriately). [`Insertable`] takes care of the boilerplate to
 * insert a model into the database when provided a few constants for each
 * model.
 */

use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};

use super::super::models::*;
use crate::{error::Result, parsers::json::JsonVal};

/// Takes care of the boilerplate to insert a model into the database.
/// Implementers must provide four things:
/// - `const INSERT_QUERY_PRELUDE: &'static str;`: the "INSERT INTO ... (...)
///   VALUES " bit of a query
/// - `const INSERT_PLACEHOLDER: &'static str;`: a tuple with the appropriate
///   number of `?`s to represent a single record. Placed after the "VALUES"
///   keyword in an insert query.
/// - `fn param_bindings(&self) -> [&dyn rusqlite::ToSql; FIELD_COUNT]`: a
///   function which returns an array of `ToSql` trait objects that should bind
///   to each of the `?`s in `INSERT_PLACEHOLDER`.
/// - `fn assign_id(&mut self)`: a function which generates and sets an
///   appropriate ID for the model.
///
/// Example:
/// ```
/// # use codecov_rs::report::sqlite::Insertable;
/// struct File {
///      id: i64,
///      path: String,
/// }
///
/// impl Insertable<2> for File {
///     const INSERT_QUERY_PRELUDE: &'static str = "INSERT INTO file (id, path) VALUES ";
///     const INSERT_PLACEHOLDER: &'static str = "(?, ?)";
///
///     fn param_bindings(&self) -> [&dyn rusqlite::ToSql; 2] {
///         [
///             &self.id as &dyn rusqlite::ToSql,
///             &self.path as &dyn rusqlite::ToSql,
///         ]
///     }
///
///     fn assign_id(&mut self) {
///         self.id = seahash::hash(self.path.as_bytes()) as i64;
///     }
/// }
/// ```
///
/// IDs are not assigned automatically; assign your own to models before you
/// insert them.
pub trait Insertable<const FIELD_COUNT: usize> {
    const INSERT_QUERY_PRELUDE: &'static str;

    const INSERT_PLACEHOLDER: &'static str;

    fn param_bindings(&self) -> [&dyn rusqlite::ToSql; FIELD_COUNT];

    fn assign_id(&mut self);

    fn insert(model: &Self, conn: &rusqlite::Connection) -> Result<()> {
        let mut stmt = conn.prepare_cached(
            // Maybe turn this in to a lazily-initialized static
            format!("{}{}", Self::INSERT_QUERY_PRELUDE, Self::INSERT_PLACEHOLDER).as_str(),
        )?;
        stmt.execute(rusqlite::params_from_iter(model.param_bindings()))?;

        Ok(())
    }
}

/// Can't implement foreign traits (`ToSql`/`FromSql`) on foreign types
/// (`serde_json::Value`) so this helper function fills in.
pub fn json_value_from_sql(s: String, col: usize) -> rusqlite::Result<JsonVal> {
    serde_json::from_str(s.as_str()).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(col, rusqlite::types::Type::Text, Box::new(e))
    })
}

impl ToSql for CoverageType {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match self {
            CoverageType::Line => Ok("l".into()),
            CoverageType::Branch => Ok("b".into()),
            CoverageType::Method => Ok("m".into()),
        }
    }
}

impl FromSql for CoverageType {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let variant = match value.as_str()? {
            "l" => CoverageType::Line,
            "b" => CoverageType::Branch,
            "m" => CoverageType::Method,
            _ => panic!("Uh oh"),
        };
        Ok(variant)
    }
}

impl ToSql for BranchFormat {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match self {
            BranchFormat::Line => Ok("l".into()),
            BranchFormat::Condition => Ok("c".into()),
            BranchFormat::BlockAndBranch => Ok("bb".into()),
        }
    }
}

impl FromSql for BranchFormat {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let variant = match value.as_str()? {
            "l" => BranchFormat::Line,
            "c" => BranchFormat::Condition,
            "bb" => BranchFormat::BlockAndBranch,
            _ => panic!("Uh oh"),
        };
        Ok(variant)
    }
}

impl ToSql for ContextType {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(self.to_string().into())
    }
}

impl FromSql for ContextType {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        value
            .as_str()?
            .parse()
            .map_err(|e| FromSqlError::Other(Box::new(e)))
    }
}

impl<'a> std::convert::TryFrom<&'a rusqlite::Row<'a>> for SourceFile {
    type Error = rusqlite::Error;

    fn try_from(row: &'a ::rusqlite::Row) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.get(row.as_ref().column_index("id")?)?,
            path: row.get(row.as_ref().column_index("path")?)?,
        })
    }
}

impl Insertable<2> for SourceFile {
    const INSERT_QUERY_PRELUDE: &'static str = "INSERT INTO source_file (id, path) VALUES ";
    const INSERT_PLACEHOLDER: &'static str = "(?, ?)";

    fn param_bindings(&self) -> [&dyn rusqlite::ToSql; 2] {
        [
            &self.id as &dyn rusqlite::ToSql,
            &self.path as &dyn rusqlite::ToSql,
        ]
    }

    fn assign_id(&mut self) {
        self.id = seahash::hash(self.path.as_bytes()) as i64;
    }
}

impl<'a> std::convert::TryFrom<&'a rusqlite::Row<'a>> for CoverageSample {
    type Error = rusqlite::Error;

    fn try_from(row: &'a ::rusqlite::Row) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.get(row.as_ref().column_index("id")?)?,
            source_file_id: row.get(row.as_ref().column_index("source_file_id")?)?,
            line_no: row.get(row.as_ref().column_index("line_no")?)?,
            coverage_type: row.get(row.as_ref().column_index("coverage_type")?)?,
            hits: row.get(row.as_ref().column_index("hits")?)?,
            hit_branches: row.get(row.as_ref().column_index("hit_branches")?)?,
            total_branches: row.get(row.as_ref().column_index("total_branches")?)?,
        })
    }
}

impl Insertable<7> for CoverageSample {
    const INSERT_QUERY_PRELUDE: &'static str = "INSERT INTO coverage_sample (id, source_file_id, line_no, coverage_type, hits, hit_branches, total_branches) VALUES ";
    const INSERT_PLACEHOLDER: &'static str = "(?, ?, ?, ?, ?, ?, ?)";

    fn param_bindings(&self) -> [&dyn rusqlite::ToSql; 7] {
        [
            &self.id as &dyn rusqlite::ToSql,
            &self.source_file_id as &dyn rusqlite::ToSql,
            &self.line_no as &dyn rusqlite::ToSql,
            &self.coverage_type as &dyn rusqlite::ToSql,
            &self.hits as &dyn rusqlite::ToSql,
            &self.hit_branches as &dyn rusqlite::ToSql,
            &self.total_branches as &dyn rusqlite::ToSql,
        ]
    }

    fn assign_id(&mut self) {
        self.id = uuid::Uuid::new_v4();
    }
}

impl<'a> std::convert::TryFrom<&'a rusqlite::Row<'a>> for BranchesData {
    type Error = rusqlite::Error;

    fn try_from(row: &'a ::rusqlite::Row) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.get(row.as_ref().column_index("id")?)?,
            source_file_id: row.get(row.as_ref().column_index("source_file_id")?)?,
            sample_id: row.get(row.as_ref().column_index("sample_id")?)?,
            hits: row.get(row.as_ref().column_index("hits")?)?,
            branch_format: row.get(row.as_ref().column_index("branch_format")?)?,
            branch: row.get(row.as_ref().column_index("branch")?)?,
        })
    }
}

impl Insertable<6> for BranchesData {
    const INSERT_QUERY_PRELUDE: &'static str = "INSERT INTO branches_data (id, source_file_id, sample_id, hits, branch_format, branch) VALUES ";
    const INSERT_PLACEHOLDER: &'static str = "(?, ?, ?, ?, ?, ?)";

    fn param_bindings(&self) -> [&dyn rusqlite::ToSql; 6] {
        [
            &self.id as &dyn rusqlite::ToSql,
            &self.source_file_id as &dyn rusqlite::ToSql,
            &self.sample_id as &dyn rusqlite::ToSql,
            &self.hits as &dyn rusqlite::ToSql,
            &self.branch_format as &dyn rusqlite::ToSql,
            &self.branch as &dyn rusqlite::ToSql,
        ]
    }

    fn assign_id(&mut self) {
        self.id = uuid::Uuid::new_v4();
    }
}

impl<'a> std::convert::TryFrom<&'a rusqlite::Row<'a>> for MethodData {
    type Error = rusqlite::Error;

    fn try_from(row: &'a ::rusqlite::Row) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.get(row.as_ref().column_index("id")?)?,
            source_file_id: row.get(row.as_ref().column_index("source_file_id")?)?,
            sample_id: row.get(row.as_ref().column_index("sample_id")?)?,
            line_no: row.get(row.as_ref().column_index("line_no")?)?,
            hit_branches: row.get(row.as_ref().column_index("hit_branches")?)?,
            total_branches: row.get(row.as_ref().column_index("total_branches")?)?,
            hit_complexity_paths: row.get(row.as_ref().column_index("hit_complexity_paths")?)?,
            total_complexity: row.get(row.as_ref().column_index("total_complexity")?)?,
        })
    }
}

impl Insertable<8> for MethodData {
    const INSERT_QUERY_PRELUDE: &'static str = "INSERT INTO method_data (id, source_file_id, sample_id, line_no, hit_branches, total_branches, hit_complexity_paths, total_complexity) VALUES ";
    const INSERT_PLACEHOLDER: &'static str = "(?, ?, ?, ?, ?, ?, ?, ?)";

    fn param_bindings(&self) -> [&dyn rusqlite::ToSql; 8] {
        [
            &self.id as &dyn rusqlite::ToSql,
            &self.source_file_id as &dyn rusqlite::ToSql,
            &self.sample_id as &dyn rusqlite::ToSql,
            &self.line_no as &dyn rusqlite::ToSql,
            &self.hit_branches as &dyn rusqlite::ToSql,
            &self.total_branches as &dyn rusqlite::ToSql,
            &self.hit_complexity_paths as &dyn rusqlite::ToSql,
            &self.total_complexity as &dyn rusqlite::ToSql,
        ]
    }

    fn assign_id(&mut self) {
        self.id = uuid::Uuid::new_v4();
    }
}

impl<'a> std::convert::TryFrom<&'a rusqlite::Row<'a>> for SpanData {
    type Error = rusqlite::Error;

    fn try_from(row: &'a ::rusqlite::Row) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.get(row.as_ref().column_index("id")?)?,
            source_file_id: row.get(row.as_ref().column_index("source_file_id")?)?,
            sample_id: row.get(row.as_ref().column_index("sample_id")?)?,
            hits: row.get(row.as_ref().column_index("hits")?)?,
            start_line: row.get(row.as_ref().column_index("start_line")?)?,
            start_col: row.get(row.as_ref().column_index("start_col")?)?,
            end_line: row.get(row.as_ref().column_index("end_line")?)?,
            end_col: row.get(row.as_ref().column_index("end_col")?)?,
        })
    }
}

impl Insertable<8> for SpanData {
    const INSERT_QUERY_PRELUDE: &'static str = "INSERT INTO span_data (id, source_file_id, sample_id, hits, start_line, start_col, end_line, end_col) VALUES ";
    const INSERT_PLACEHOLDER: &'static str = "(?, ?, ?, ?, ?, ?, ?, ?)";

    fn param_bindings(&self) -> [&dyn rusqlite::ToSql; 8] {
        [
            &self.id as &dyn rusqlite::ToSql,
            &self.source_file_id as &dyn rusqlite::ToSql,
            &self.sample_id as &dyn rusqlite::ToSql,
            &self.hits as &dyn rusqlite::ToSql,
            &self.start_line as &dyn rusqlite::ToSql,
            &self.start_col as &dyn rusqlite::ToSql,
            &self.end_line as &dyn rusqlite::ToSql,
            &self.end_col as &dyn rusqlite::ToSql,
        ]
    }

    fn assign_id(&mut self) {
        self.id = uuid::Uuid::new_v4();
    }
}

impl<'a> std::convert::TryFrom<&'a rusqlite::Row<'a>> for ContextAssoc {
    type Error = rusqlite::Error;

    fn try_from(row: &'a ::rusqlite::Row) -> Result<Self, Self::Error> {
        Ok(Self {
            context_id: row.get(row.as_ref().column_index("context_id")?)?,
            sample_id: row.get(row.as_ref().column_index("sample_id")?)?,
            branch_id: row.get(row.as_ref().column_index("branch_id")?)?,
            method_id: row.get(row.as_ref().column_index("method_id")?)?,
            span_id: row.get(row.as_ref().column_index("span_id")?)?,
        })
    }
}

impl Insertable<5> for ContextAssoc {
    const INSERT_QUERY_PRELUDE: &'static str =
        "INSERT INTO context_assoc (context_id, sample_id, branch_id, method_id, span_id) VALUES ";
    const INSERT_PLACEHOLDER: &'static str = "(?, ?, ?, ?, ?)";

    fn param_bindings(&self) -> [&dyn rusqlite::ToSql; 5] {
        [
            &self.context_id as &dyn rusqlite::ToSql,
            &self.sample_id as &dyn rusqlite::ToSql,
            &self.branch_id as &dyn rusqlite::ToSql,
            &self.method_id as &dyn rusqlite::ToSql,
            &self.span_id as &dyn rusqlite::ToSql,
        ]
    }

    fn assign_id(&mut self) {}
}

impl<'a> std::convert::TryFrom<&'a rusqlite::Row<'a>> for Context {
    type Error = rusqlite::Error;

    fn try_from(row: &'a ::rusqlite::Row) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.get(row.as_ref().column_index("id")?)?,
            context_type: row.get(row.as_ref().column_index("context_type")?)?,
            name: row.get(row.as_ref().column_index("name")?)?,
        })
    }
}

impl Insertable<3> for Context {
    const INSERT_QUERY_PRELUDE: &'static str =
        "INSERT INTO context (id, context_type, name) VALUES ";
    const INSERT_PLACEHOLDER: &'static str = "(?, ?, ?)";

    fn param_bindings(&self) -> [&dyn rusqlite::ToSql; 3] {
        [
            &self.id as &dyn rusqlite::ToSql,
            &self.context_type as &dyn rusqlite::ToSql,
            &self.name as &dyn rusqlite::ToSql,
        ]
    }

    fn assign_id(&mut self) {
        self.id = seahash::hash(self.name.as_bytes()) as i64;
    }
}

impl<'a> std::convert::TryFrom<&'a rusqlite::Row<'a>> for UploadDetails {
    type Error = rusqlite::Error;

    fn try_from(row: &'a ::rusqlite::Row) -> Result<Self, Self::Error> {
        let flags_index = row.as_ref().column_index("flags")?;
        let flags = if let Some(flags) = row.get(flags_index)? {
            Some(json_value_from_sql(flags, flags_index)?)
        } else {
            None
        };
        let session_extras_index = row.as_ref().column_index("session_extras")?;
        let session_extras = if let Some(session_extras) = row.get(session_extras_index)? {
            Some(json_value_from_sql(session_extras, session_extras_index)?)
        } else {
            None
        };
        Ok(Self {
            context_id: row.get(row.as_ref().column_index("context_id")?)?,
            timestamp: row.get(row.as_ref().column_index("timestamp")?)?,
            raw_upload_url: row.get(row.as_ref().column_index("raw_upload_url")?)?,
            flags,
            provider: row.get(row.as_ref().column_index("provider")?)?,
            build: row.get(row.as_ref().column_index("build")?)?,
            name: row.get(row.as_ref().column_index("name")?)?,
            job_name: row.get(row.as_ref().column_index("job_name")?)?,
            ci_run_url: row.get(row.as_ref().column_index("ci_run_url")?)?,
            state: row.get(row.as_ref().column_index("state")?)?,
            env: row.get(row.as_ref().column_index("env")?)?,
            session_type: row.get(row.as_ref().column_index("session_type")?)?,
            session_extras,
        })
    }
}

impl<'a> std::convert::TryFrom<&'a rusqlite::Row<'a>> for CoverageTotals {
    type Error = rusqlite::Error;

    fn try_from(row: &'a ::rusqlite::Row) -> Result<Self, Self::Error> {
        Ok(Self {
            hit_lines: row.get(row.as_ref().column_index("hit_lines")?)?,
            total_lines: row.get(row.as_ref().column_index("total_lines")?)?,
            hit_branches: row.get(row.as_ref().column_index("hit_branches")?)?,
            total_branches: row.get(row.as_ref().column_index("total_branches")?)?,
            total_branch_roots: row.get(row.as_ref().column_index("total_branch_roots")?)?,
            hit_methods: row.get(row.as_ref().column_index("hit_methods")?)?,
            total_methods: row.get(row.as_ref().column_index("total_methods")?)?,
            hit_complexity_paths: row.get(row.as_ref().column_index("hit_complexity_paths")?)?,
            total_complexity: row.get(row.as_ref().column_index("total_complexity")?)?,
        })
    }
}

impl<'a> std::convert::TryFrom<&'a rusqlite::Row<'a>> for ReportTotals {
    type Error = rusqlite::Error;

    fn try_from(row: &'a ::rusqlite::Row) -> Result<Self, Self::Error> {
        Ok(Self {
            files: row.get(row.as_ref().column_index("file_count")?)?,
            uploads: row.get(row.as_ref().column_index("upload_count")?)?,
            test_cases: row.get(row.as_ref().column_index("test_case_count")?)?,
            coverage: row.try_into()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::{
        super::{super::Report, SqliteReport},
        *,
    };
    use crate::error::CodecovError;

    #[derive(PartialEq, Debug)]
    struct TestModel {
        id: i64,
        data: String,
    }

    impl Insertable<2> for TestModel {
        const INSERT_QUERY_PRELUDE: &'static str = "INSERT INTO test (id, data) VALUES ";
        const INSERT_PLACEHOLDER: &'static str = "(?, ?)";

        fn param_bindings(&self) -> [&dyn rusqlite::ToSql; 2] {
            [
                &self.id as &dyn rusqlite::ToSql,
                &self.data as &dyn rusqlite::ToSql,
            ]
        }

        fn assign_id(&mut self) {
            self.id = seahash::hash(self.data.as_bytes()) as i64;
        }
    }

    impl<'a> std::convert::TryFrom<&'a rusqlite::Row<'a>> for TestModel {
        type Error = rusqlite::Error;

        fn try_from(row: &'a ::rusqlite::Row) -> Result<Self, Self::Error> {
            Ok(Self {
                id: row.get(row.as_ref().column_index("id")?)?,
                data: row.get(row.as_ref().column_index("data")?)?,
            })
        }
    }

    struct Ctx {
        _temp_dir: TempDir,
        report: SqliteReport,
    }

    fn setup() -> Ctx {
        let temp_dir = TempDir::new().ok().unwrap();
        let db_file = temp_dir.path().join("db.sqlite");
        let report = SqliteReport::new(db_file).unwrap();

        report
            .conn
            .execute(
                "CREATE TABLE test (id INTEGER PRIMARY KEY, data VARCHAR)",
                [],
            )
            .unwrap();

        Ctx {
            _temp_dir: temp_dir,
            report,
        }
    }

    fn list_test_models(report: &SqliteReport) -> Vec<TestModel> {
        let mut stmt = report
            .conn
            .prepare_cached("SELECT id, data FROM test ORDER BY id ASC")
            .unwrap();
        let models = stmt
            .query_map([], |row| row.try_into())
            .unwrap()
            .collect::<rusqlite::Result<Vec<TestModel>>>()
            .unwrap();

        models
    }

    #[test]
    fn test_test_model_single_insert() {
        let ctx = setup();

        let model = TestModel {
            id: 5,
            data: "foo".to_string(),
        };

        <TestModel as Insertable<2>>::insert(&model, &ctx.report.conn).unwrap();
        let duplicate_result = <TestModel as Insertable<2>>::insert(&model, &ctx.report.conn);

        let test_models = list_test_models(&ctx.report);
        assert_eq!(test_models, vec![model]);

        match duplicate_result {
            Err(CodecovError::SqliteError(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error {
                    code: rusqlite::ffi::ErrorCode::ConstraintViolation,
                    extended_code: 1555,
                },
                Some(s),
            ))) => {
                assert_eq!(s, String::from("UNIQUE constraint failed: test.id"));
            }
            _ => {
                assert!(false);
            }
        }
    }

    #[test]
    fn test_source_file_single_insert() {
        let ctx = setup();

        let model = SourceFile {
            id: 0,
            path: "src/report/report.rs".to_string(),
        };

        <SourceFile as Insertable<2>>::insert(&model, &ctx.report.conn).unwrap();
        let duplicate_result = <SourceFile as Insertable<2>>::insert(&model, &ctx.report.conn);

        let files = ctx.report.list_files().unwrap();
        assert_eq!(files, vec![model]);

        match duplicate_result {
            Err(CodecovError::SqliteError(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error {
                    code: rusqlite::ffi::ErrorCode::ConstraintViolation,
                    extended_code: 1555,
                },
                Some(s),
            ))) => {
                assert_eq!(s, String::from("UNIQUE constraint failed: source_file.id"));
            }
            _ => {
                assert!(false);
            }
        }
    }

    #[test]
    fn test_context_single_insert() {
        let ctx = setup();

        let model = Context {
            id: 0,
            context_type: ContextType::Upload,
            name: "test_upload".to_string(),
        };

        <Context as Insertable<3>>::insert(&model, &ctx.report.conn).unwrap();
        let duplicate_result = <Context as Insertable<3>>::insert(&model, &ctx.report.conn);

        let contexts = ctx.report.list_contexts().unwrap();
        assert_eq!(contexts, vec![model]);

        match duplicate_result {
            Err(CodecovError::SqliteError(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error {
                    code: rusqlite::ffi::ErrorCode::ConstraintViolation,
                    extended_code: 1555,
                },
                Some(s),
            ))) => {
                assert_eq!(s, String::from("UNIQUE constraint failed: context.id"));
            }
            _ => {
                assert!(false);
            }
        }
    }

    #[test]
    fn test_context_assoc_single_insert() {
        let ctx = setup();

        let model = ContextAssoc {
            context_id: 0,
            sample_id: Some(uuid::Uuid::new_v4()),
            ..Default::default()
        };

        <ContextAssoc as Insertable<5>>::insert(&model, &ctx.report.conn).unwrap();
        let assoc: ContextAssoc = ctx
            .report
            .conn
            .query_row(
                "SELECT context_id, sample_id, branch_id, method_id, span_id FROM context_assoc",
                [],
                |row| row.try_into(),
            )
            .unwrap();
        assert_eq!(assoc, model);

        let duplicate_result = <ContextAssoc as Insertable<5>>::insert(&model, &ctx.report.conn);
        match duplicate_result {
            Err(CodecovError::SqliteError(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error {
                    code: rusqlite::ffi::ErrorCode::ConstraintViolation,
                    extended_code: 1555,
                },
                Some(s),
            ))) => {
                assert_eq!(
                    s,
                    String::from(
                        "UNIQUE constraint failed: context_assoc.context_id, context_assoc.sample_id"
                    )
                );
            }
            _ => {
                assert!(false);
            }
        }
    }

    #[test]
    fn test_coverage_sample_single_insert() {
        let ctx = setup();

        <SourceFile as Insertable<2>>::insert(
            &SourceFile {
                id: 0,
                ..Default::default()
            },
            &ctx.report.conn,
        )
        .unwrap();

        let model = CoverageSample {
            id: uuid::Uuid::new_v4(),
            source_file_id: 0,
            ..Default::default()
        };

        <CoverageSample as Insertable<7>>::insert(&model, &ctx.report.conn).unwrap();
        let duplicate_result = <CoverageSample as Insertable<7>>::insert(&model, &ctx.report.conn);

        let samples = ctx.report.list_coverage_samples().unwrap();
        assert_eq!(samples, vec![model]);

        match duplicate_result {
            Err(CodecovError::SqliteError(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error {
                    code: rusqlite::ffi::ErrorCode::ConstraintViolation,
                    extended_code: 1555,
                },
                Some(s),
            ))) => {
                assert_eq!(
                    s,
                    String::from("UNIQUE constraint failed: coverage_sample.id")
                );
            }
            _ => {
                assert!(false);
            }
        }
    }

    #[test]
    fn test_branches_data_single_insert() {
        let ctx = setup();

        <SourceFile as Insertable<2>>::insert(
            &SourceFile {
                id: 0,
                ..Default::default()
            },
            &ctx.report.conn,
        )
        .unwrap();

        let sample_id = uuid::Uuid::new_v4();
        <CoverageSample as Insertable<7>>::insert(
            &CoverageSample {
                id: sample_id,
                source_file_id: 0,
                ..Default::default()
            },
            &ctx.report.conn,
        )
        .unwrap();

        let model = BranchesData {
            id: uuid::Uuid::new_v4(),
            sample_id,
            source_file_id: 0,
            ..Default::default()
        };

        <BranchesData as Insertable<6>>::insert(&model, &ctx.report.conn).unwrap();
        let duplicate_result = <BranchesData as Insertable<6>>::insert(&model, &ctx.report.conn);

        let branch: BranchesData = ctx.report
            .conn
            .query_row(
                "SELECT id, source_file_id, sample_id, hits, branch_format, branch FROM branches_data",
                [],
                |row| row.try_into(),
            ).unwrap();
        assert_eq!(branch, model);

        match duplicate_result {
            Err(CodecovError::SqliteError(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error {
                    code: rusqlite::ffi::ErrorCode::ConstraintViolation,
                    extended_code: 1555,
                },
                Some(s),
            ))) => {
                assert_eq!(
                    s,
                    String::from("UNIQUE constraint failed: branches_data.id")
                );
            }
            _ => {
                assert!(false);
            }
        }
    }

    #[test]
    fn test_method_data_single_insert() {
        let ctx = setup();

        <SourceFile as Insertable<2>>::insert(
            &SourceFile {
                id: 0,
                ..Default::default()
            },
            &ctx.report.conn,
        )
        .unwrap();

        let model = MethodData {
            id: uuid::Uuid::new_v4(),
            source_file_id: 0,
            ..Default::default()
        };

        <MethodData as Insertable<8>>::insert(&model, &ctx.report.conn).unwrap();
        let duplicate_result = <MethodData as Insertable<8>>::insert(&model, &ctx.report.conn);

        let method: MethodData = ctx.report
            .conn
            .query_row(
                "SELECT id, source_file_id, sample_id, line_no, hit_branches, total_branches, hit_complexity_paths, total_complexity FROM method_data",
                [],
                |row| row.try_into(),
            ).unwrap();
        assert_eq!(method, model);

        match duplicate_result {
            Err(CodecovError::SqliteError(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error {
                    code: rusqlite::ffi::ErrorCode::ConstraintViolation,
                    extended_code: 1555,
                },
                Some(s),
            ))) => {
                assert_eq!(s, String::from("UNIQUE constraint failed: method_data.id"));
            }
            _ => {
                assert!(false);
            }
        }
    }

    #[test]
    fn test_span_data_single_insert() {
        let ctx = setup();

        <SourceFile as Insertable<2>>::insert(
            &SourceFile {
                id: 0,
                ..Default::default()
            },
            &ctx.report.conn,
        )
        .unwrap();

        let model = SpanData {
            id: uuid::Uuid::new_v4(),
            source_file_id: 0,
            ..Default::default()
        };

        <SpanData as Insertable<8>>::insert(&model, &ctx.report.conn).unwrap();
        let duplicate_result = <SpanData as Insertable<8>>::insert(&model, &ctx.report.conn);

        let branch: SpanData = ctx.report
            .conn
            .query_row(
                "SELECT id, source_file_id, sample_id, hits, start_line, start_col, end_line, end_col FROM span_data",
                [],
                |row| row.try_into(),
            ).unwrap();
        assert_eq!(branch, model);

        match duplicate_result {
            Err(CodecovError::SqliteError(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error {
                    code: rusqlite::ffi::ErrorCode::ConstraintViolation,
                    extended_code: 1555,
                },
                Some(s),
            ))) => {
                assert_eq!(s, String::from("UNIQUE constraint failed: span_data.id"));
            }
            _ => {
                assert!(false);
            }
        }
    }
}
