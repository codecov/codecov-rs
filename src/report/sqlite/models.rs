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
/// }
/// ```
///
/// IDs are not assigned automatically; assign your own to models before you
/// insert them.
pub trait Insertable<const FIELD_COUNT: usize> {
    const INSERT_QUERY_PRELUDE: &'static str;

    const INSERT_PLACEHOLDER: &'static str;

    fn param_bindings(&self) -> [&dyn rusqlite::ToSql; FIELD_COUNT];

    fn insert(model: &Self, conn: &rusqlite::Connection) -> Result<()> {
        let mut stmt = conn.prepare_cached(
            // Maybe turn this in to a lazily-initialized static
            format!("{}{}", Self::INSERT_QUERY_PRELUDE, Self::INSERT_PLACEHOLDER).as_str(),
        )?;
        stmt.execute(rusqlite::params_from_iter(model.param_bindings()))?;

        Ok(())
    }

    fn multi_insert<'a, I>(mut models: I, conn: &rusqlite::Connection) -> Result<()>
    where
        I: Iterator<Item = &'a Self> + ExactSizeIterator,
        Self: 'a,
    {
        let var_limit = conn.limit(rusqlite::limits::Limit::SQLITE_LIMIT_VARIABLE_NUMBER) as usize;
        // If each model takes up `FIELD_COUNT` variables, we can fit `var_limit /
        // FIELD_COUNT` complete models in each "page" of our query
        let page_size = var_limit / FIELD_COUNT;

        // Integer division tells us how many full pages there are. If there is a
        // non-zero remainder, there is one final incomplete page.
        let model_count = models.len();
        let page_count = match (model_count / page_size, model_count % page_size) {
            (page_count, 0) => page_count,
            (page_count, _) => page_count + 1,
        };

        let (mut query, mut previous_page_size) = (String::new(), 0);
        for _ in 0..page_count {
            // If there are fewer than `page_size` pages left, the iterator will just take
            // everything.
            let page_iter = models.by_ref().take(page_size);

            // We can reuse our query string if the current page is the same size as the
            // last one. If not, we have to rebuild the query string.
            let current_page_size = page_iter.len();
            if current_page_size != previous_page_size {
                query = format!(" {},", Self::INSERT_PLACEHOLDER).repeat(current_page_size);
                query.insert_str(0, Self::INSERT_QUERY_PRELUDE);
                // Remove trailing comma
                query.pop();
                previous_page_size = current_page_size;
            }

            let mut stmt = conn.prepare_cached(query.as_str())?;
            let params = page_iter.flat_map(|model| model.param_bindings());
            stmt.execute(rusqlite::params_from_iter(params))?;
        }

        conn.flush_prepared_statement_cache();

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
}

impl<'a> std::convert::TryFrom<&'a rusqlite::Row<'a>> for CoverageSample {
    type Error = rusqlite::Error;

    fn try_from(row: &'a ::rusqlite::Row) -> Result<Self, Self::Error> {
        Ok(Self {
            raw_upload_id: row.get(row.as_ref().column_index("raw_upload_id")?)?,
            local_sample_id: row.get(row.as_ref().column_index("local_sample_id")?)?,
            source_file_id: row.get(row.as_ref().column_index("source_file_id")?)?,
            line_no: row.get(row.as_ref().column_index("line_no")?)?,
            coverage_type: row.get(row.as_ref().column_index("coverage_type")?)?,
            hits: row.get(row.as_ref().column_index("hits")?)?,
            hit_branches: row.get(row.as_ref().column_index("hit_branches")?)?,
            total_branches: row.get(row.as_ref().column_index("total_branches")?)?,
        })
    }
}

impl Insertable<8> for CoverageSample {
    const INSERT_QUERY_PRELUDE: &'static str = "INSERT INTO coverage_sample (raw_upload_id, local_sample_id, source_file_id, line_no, coverage_type, hits, hit_branches, total_branches) VALUES ";
    const INSERT_PLACEHOLDER: &'static str = "(?, ?, ?, ?, ?, ?, ?, ?)";

    fn param_bindings(&self) -> [&dyn rusqlite::ToSql; 8] {
        [
            &self.raw_upload_id as &dyn rusqlite::ToSql,
            &self.local_sample_id as &dyn rusqlite::ToSql,
            &self.source_file_id as &dyn rusqlite::ToSql,
            &self.line_no as &dyn rusqlite::ToSql,
            &self.coverage_type as &dyn rusqlite::ToSql,
            &self.hits as &dyn rusqlite::ToSql,
            &self.hit_branches as &dyn rusqlite::ToSql,
            &self.total_branches as &dyn rusqlite::ToSql,
        ]
    }
}

impl<'a> std::convert::TryFrom<&'a rusqlite::Row<'a>> for BranchesData {
    type Error = rusqlite::Error;

    fn try_from(row: &'a ::rusqlite::Row) -> Result<Self, Self::Error> {
        Ok(Self {
            raw_upload_id: row.get(row.as_ref().column_index("raw_upload_id")?)?,
            local_branch_id: row.get(row.as_ref().column_index("local_branch_id")?)?,
            source_file_id: row.get(row.as_ref().column_index("source_file_id")?)?,
            local_sample_id: row.get(row.as_ref().column_index("local_sample_id")?)?,
            hits: row.get(row.as_ref().column_index("hits")?)?,
            branch_format: row.get(row.as_ref().column_index("branch_format")?)?,
            branch: row.get(row.as_ref().column_index("branch")?)?,
        })
    }
}

impl Insertable<7> for BranchesData {
    const INSERT_QUERY_PRELUDE: &'static str = "INSERT INTO branches_data (raw_upload_id, local_branch_id, source_file_id, local_sample_id, hits, branch_format, branch) VALUES ";
    const INSERT_PLACEHOLDER: &'static str = "(?, ?, ?, ?, ?, ?, ?)";

    fn param_bindings(&self) -> [&dyn rusqlite::ToSql; 7] {
        [
            &self.raw_upload_id as &dyn rusqlite::ToSql,
            &self.local_branch_id as &dyn rusqlite::ToSql,
            &self.source_file_id as &dyn rusqlite::ToSql,
            &self.local_sample_id as &dyn rusqlite::ToSql,
            &self.hits as &dyn rusqlite::ToSql,
            &self.branch_format as &dyn rusqlite::ToSql,
            &self.branch as &dyn rusqlite::ToSql,
        ]
    }
}

impl<'a> std::convert::TryFrom<&'a rusqlite::Row<'a>> for MethodData {
    type Error = rusqlite::Error;

    fn try_from(row: &'a ::rusqlite::Row) -> Result<Self, Self::Error> {
        Ok(Self {
            raw_upload_id: row.get(row.as_ref().column_index("raw_upload_id")?)?,
            local_method_id: row.get(row.as_ref().column_index("local_method_id")?)?,
            source_file_id: row.get(row.as_ref().column_index("source_file_id")?)?,
            local_sample_id: row.get(row.as_ref().column_index("local_sample_id")?)?,
            line_no: row.get(row.as_ref().column_index("line_no")?)?,
            hit_branches: row.get(row.as_ref().column_index("hit_branches")?)?,
            total_branches: row.get(row.as_ref().column_index("total_branches")?)?,
            hit_complexity_paths: row.get(row.as_ref().column_index("hit_complexity_paths")?)?,
            total_complexity: row.get(row.as_ref().column_index("total_complexity")?)?,
        })
    }
}

impl Insertable<9> for MethodData {
    const INSERT_QUERY_PRELUDE: &'static str = "INSERT INTO method_data (raw_upload_id, local_method_id, source_file_id, local_sample_id, line_no, hit_branches, total_branches, hit_complexity_paths, total_complexity) VALUES ";
    const INSERT_PLACEHOLDER: &'static str = "(?, ?, ?, ?, ?, ?, ?, ?, ?)";

    fn param_bindings(&self) -> [&dyn rusqlite::ToSql; 9] {
        [
            &self.raw_upload_id as &dyn rusqlite::ToSql,
            &self.local_method_id as &dyn rusqlite::ToSql,
            &self.source_file_id as &dyn rusqlite::ToSql,
            &self.local_sample_id as &dyn rusqlite::ToSql,
            &self.line_no as &dyn rusqlite::ToSql,
            &self.hit_branches as &dyn rusqlite::ToSql,
            &self.total_branches as &dyn rusqlite::ToSql,
            &self.hit_complexity_paths as &dyn rusqlite::ToSql,
            &self.total_complexity as &dyn rusqlite::ToSql,
        ]
    }
}

impl<'a> std::convert::TryFrom<&'a rusqlite::Row<'a>> for SpanData {
    type Error = rusqlite::Error;

    fn try_from(row: &'a ::rusqlite::Row) -> Result<Self, Self::Error> {
        Ok(Self {
            raw_upload_id: row.get(row.as_ref().column_index("raw_upload_id")?)?,
            local_span_id: row.get(row.as_ref().column_index("local_span_id")?)?,
            source_file_id: row.get(row.as_ref().column_index("source_file_id")?)?,
            local_sample_id: row.get(row.as_ref().column_index("local_sample_id")?)?,
            hits: row.get(row.as_ref().column_index("hits")?)?,
            start_line: row.get(row.as_ref().column_index("start_line")?)?,
            start_col: row.get(row.as_ref().column_index("start_col")?)?,
            end_line: row.get(row.as_ref().column_index("end_line")?)?,
            end_col: row.get(row.as_ref().column_index("end_col")?)?,
        })
    }
}

impl Insertable<9> for SpanData {
    const INSERT_QUERY_PRELUDE: &'static str = "INSERT INTO span_data (raw_upload_id, local_span_id, source_file_id, local_sample_id, hits, start_line, start_col, end_line, end_col) VALUES ";
    const INSERT_PLACEHOLDER: &'static str = "(?, ?, ?, ?, ?, ?, ?, ?, ?)";

    fn param_bindings(&self) -> [&dyn rusqlite::ToSql; 9] {
        [
            &self.raw_upload_id as &dyn rusqlite::ToSql,
            &self.local_span_id as &dyn rusqlite::ToSql,
            &self.source_file_id as &dyn rusqlite::ToSql,
            &self.local_sample_id as &dyn rusqlite::ToSql,
            &self.hits as &dyn rusqlite::ToSql,
            &self.start_line as &dyn rusqlite::ToSql,
            &self.start_col as &dyn rusqlite::ToSql,
            &self.end_line as &dyn rusqlite::ToSql,
            &self.end_col as &dyn rusqlite::ToSql,
        ]
    }
}

impl<'a> std::convert::TryFrom<&'a rusqlite::Row<'a>> for ContextAssoc {
    type Error = rusqlite::Error;

    fn try_from(row: &'a ::rusqlite::Row) -> Result<Self, Self::Error> {
        Ok(Self {
            context_id: row.get(row.as_ref().column_index("context_id")?)?,
            raw_upload_id: row.get(row.as_ref().column_index("raw_upload_id")?)?,
            local_sample_id: row.get(row.as_ref().column_index("local_sample_id")?)?,
            local_span_id: row.get(row.as_ref().column_index("local_span_id")?)?,
        })
    }
}

impl Insertable<4> for ContextAssoc {
    const INSERT_QUERY_PRELUDE: &'static str =
        "INSERT INTO context_assoc (context_id, raw_upload_id, local_sample_id, local_span_id) VALUES ";
    const INSERT_PLACEHOLDER: &'static str = "(?, ?, ?, ?)";

    fn param_bindings(&self) -> [&dyn rusqlite::ToSql; 4] {
        [
            &self.context_id as &dyn rusqlite::ToSql,
            &self.raw_upload_id as &dyn rusqlite::ToSql,
            &self.local_sample_id as &dyn rusqlite::ToSql,
            &self.local_span_id as &dyn rusqlite::ToSql,
        ]
    }
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
}

impl<'a> std::convert::TryFrom<&'a rusqlite::Row<'a>> for RawUpload {
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
            id: row.get(row.as_ref().column_index("id")?)?,
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
        super::{
            super::{Report, ReportBuilder},
            SqliteReport, SqliteReportBuilder,
        },
        *,
    };

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
        temp_dir: TempDir,
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

        Ctx { temp_dir, report }
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

        let error = duplicate_result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "sqlite failure: 'UNIQUE constraint failed: test.id'"
        );
    }

    #[test]
    fn test_test_model_multi_insert() {
        let ctx = setup();

        // We lower the limit to force the multi_insert pagination logic to kick in.
        // We'll use 5 models, each with 2 variables, so we need 10 variables total.
        // Setting the limit to 4 should wind up using multiple pages.
        let _ = ctx
            .report
            .conn
            .set_limit(rusqlite::limits::Limit::SQLITE_LIMIT_VARIABLE_NUMBER, 4);

        let models_to_insert = vec![
            TestModel {
                id: 1,
                data: "foo".to_string(),
            },
            TestModel {
                id: 2,
                data: "bar".to_string(),
            },
            TestModel {
                id: 3,
                data: "baz".to_string(),
            },
            TestModel {
                id: 4,
                data: "abc".to_string(),
            },
            TestModel {
                id: 5,
                data: "def".to_string(),
            },
        ];

        <TestModel as Insertable<2>>::multi_insert(models_to_insert.iter(), &ctx.report.conn)
            .unwrap();

        let test_models = list_test_models(&ctx.report);
        assert_eq!(test_models, models_to_insert);
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

        let error = duplicate_result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "sqlite failure: 'UNIQUE constraint failed: source_file.id'"
        );
    }

    #[test]
    fn test_context_single_insert() {
        let ctx = setup();

        let model = Context {
            id: 0,
            context_type: ContextType::TestCase,
            name: "test_upload".to_string(),
        };

        <Context as Insertable<3>>::insert(&model, &ctx.report.conn).unwrap();
        let duplicate_result = <Context as Insertable<3>>::insert(&model, &ctx.report.conn);

        let contexts = ctx.report.list_contexts().unwrap();
        assert_eq!(contexts, vec![model]);

        let error = duplicate_result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "sqlite failure: 'UNIQUE constraint failed: context.id'"
        );
    }

    #[test]
    fn test_context_assoc_single_insert() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let raw_upload = report_builder
            .insert_raw_upload(Default::default())
            .unwrap();
        let context = report_builder
            .insert_context(ContextType::TestCase, "foo")
            .unwrap();

        let report = report_builder.build().unwrap();

        let model = ContextAssoc {
            context_id: context.id,
            raw_upload_id: raw_upload.id,
            local_sample_id: Some(rand::random()),
            local_span_id: None,
        };

        <ContextAssoc as Insertable<4>>::insert(&model, &report.conn).unwrap();
        let assoc: ContextAssoc = report
            .conn
            .query_row(
                "SELECT context_id, raw_upload_id, local_sample_id, local_span_id FROM context_assoc",
                [],
                |row| row.try_into(),
            )
            .unwrap();
        assert_eq!(assoc, model);

        /* TODO: Figure out how to re-enable this part of the test
        let duplicate_result = <ContextAssoc as Insertable<4>>::insert(&model, &ctx.report.conn);
        let error = duplicate_result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "sqlite failure: 'UNIQUE constraint failed: context_assoc.context_id, context_assoc.raw_upload_id, context_assoc.local_sample_id'"
        );
        */
    }

    #[test]
    fn test_coverage_sample_single_insert() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let source_file = report_builder.insert_file("foo.rs".to_string()).unwrap();
        let raw_upload = report_builder
            .insert_raw_upload(Default::default())
            .unwrap();

        let report = report_builder.build().unwrap();

        let model = CoverageSample {
            raw_upload_id: raw_upload.id,
            local_sample_id: rand::random(),
            source_file_id: source_file.id,
            ..Default::default()
        };

        <CoverageSample as Insertable<8>>::insert(&model, &report.conn).unwrap();
        let duplicate_result = <CoverageSample as Insertable<8>>::insert(&model, &report.conn);

        let samples = report.list_coverage_samples().unwrap();
        assert_eq!(samples, vec![model]);

        let error = duplicate_result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "sqlite failure: 'UNIQUE constraint failed: coverage_sample.raw_upload_id, coverage_sample.local_sample_id'"
        );
    }

    #[test]
    fn test_branches_data_single_insert() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let source_file = report_builder.insert_file("path".to_string()).unwrap();
        let raw_upload = report_builder
            .insert_raw_upload(Default::default())
            .unwrap();

        let report = report_builder.build().unwrap();

        let local_sample_id = rand::random();
        <CoverageSample as Insertable<8>>::insert(
            &CoverageSample {
                raw_upload_id: raw_upload.id,
                local_sample_id,
                source_file_id: source_file.id,
                ..Default::default()
            },
            &report.conn,
        )
        .unwrap();

        let model = BranchesData {
            raw_upload_id: raw_upload.id,
            local_branch_id: rand::random(),
            local_sample_id,
            source_file_id: source_file.id,
            ..Default::default()
        };

        <BranchesData as Insertable<7>>::insert(&model, &report.conn).unwrap();
        let duplicate_result = <BranchesData as Insertable<7>>::insert(&model, &report.conn);

        let branch: BranchesData = report
            .conn
            .query_row(
                "SELECT local_branch_id, source_file_id, local_sample_id, raw_upload_id, hits, branch_format, branch FROM branches_data",
                [],
                |row| row.try_into(),
            ).unwrap();
        assert_eq!(branch, model);

        let error = duplicate_result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "sqlite failure: 'UNIQUE constraint failed: branches_data.raw_upload_id, branches_data.local_branch_id'"
        );
    }

    #[test]
    fn test_method_data_single_insert() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let source_file = report_builder.insert_file("foo.rs".to_string()).unwrap();
        let raw_upload = report_builder
            .insert_raw_upload(Default::default())
            .unwrap();
        let coverage_sample = report_builder
            .insert_coverage_sample(CoverageSample {
                raw_upload_id: raw_upload.id,
                source_file_id: source_file.id,
                ..Default::default()
            })
            .unwrap();

        let report = report_builder.build().unwrap();

        let model = MethodData {
            raw_upload_id: raw_upload.id,
            local_method_id: rand::random(),
            local_sample_id: coverage_sample.local_sample_id,
            source_file_id: source_file.id,
            ..Default::default()
        };

        <MethodData as Insertable<9>>::insert(&model, &report.conn).unwrap();
        let duplicate_result = <MethodData as Insertable<9>>::insert(&model, &report.conn);

        let method: MethodData = report
            .conn
            .query_row(
                "SELECT raw_upload_id, local_method_id, source_file_id, local_sample_id, line_no, hit_branches, total_branches, hit_complexity_paths, total_complexity FROM method_data",
                [],
                |row| row.try_into(),
            ).unwrap();
        assert_eq!(method, model);

        let error = duplicate_result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "sqlite failure: 'UNIQUE constraint failed: method_data.raw_upload_id, method_data.local_method_id'"
        );
    }

    #[test]
    fn test_span_data_single_insert() {
        let ctx = setup();
        let db_file = ctx.temp_dir.path().join("db.sqlite");
        let mut report_builder = SqliteReportBuilder::new(db_file).unwrap();

        let source_file = report_builder.insert_file("foo.rs".to_string()).unwrap();
        let raw_upload = report_builder
            .insert_raw_upload(Default::default())
            .unwrap();

        let report = report_builder.build().unwrap();

        let model = SpanData {
            raw_upload_id: raw_upload.id,
            local_span_id: rand::random(),
            source_file_id: source_file.id,
            ..Default::default()
        };

        <SpanData as Insertable<9>>::insert(&model, &report.conn).unwrap();
        let duplicate_result = <SpanData as Insertable<9>>::insert(&model, &report.conn);

        let branch: SpanData = report
            .conn
            .query_row(
                "SELECT raw_upload_id, local_span_id, source_file_id, local_sample_id, hits, start_line, start_col, end_line, end_col FROM span_data",
                [],
                |row| row.try_into(),
            ).unwrap();
        assert_eq!(branch, model);

        let error = duplicate_result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "sqlite failure: 'UNIQUE constraint failed: span_data.raw_upload_id, span_data.local_span_id'"
        );
    }
}
