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

use rusqlite::types::{FromSql, FromSqlResult, ToSql, ToSqlOutput, ValueRef};

use super::super::models::*;
use crate::{error::Result, parsers::json::JsonVal};

/// Takes care of the boilerplate to insert a model into the database.
/// Implementers must provide three things:
/// - `const TABLE_NAME`: The name of the table
/// - `const FIELDS`: The names of all the fields
/// - `fn extend_params`: A function that fills in the field values. The number
///   and order of params has to match those in `FIELDS`.
///
/// # Examples
///
/// ```
/// # use codecov_rs::report::sqlite::Insertable;
/// struct File {
///      id: i64,
///      path: String,
/// }
///
/// impl Insertable for File {
///     const TABLE_NAME: &'static str = "file";
///     const FIELDS: &'static [&'static str] = &["id", "path"];
///
///     fn extend_params<'a>(&'a self, params: &mut Vec<&'a dyn rusqlite::ToSql>) {
///         params.extend(&[
///             &self.id as &dyn rusqlite::ToSql,
///             &self.path as &dyn rusqlite::ToSql,
///         ])
///     }
/// }
/// ```
///
/// IDs are not assigned automatically; assign your own to models before you
/// insert them.
pub trait Insertable {
    /// The name of the table.
    const TABLE_NAME: &'static str;
    /// The field names to be inserted.
    const FIELDS: &'static [&'static str];

    /// This method is supposed to extend the input `params` with the parameters
    /// matching the `FIELDS`.
    fn extend_params<'a>(&'a self, params: &mut Vec<&'a dyn rusqlite::ToSql>);

    /// Determines the maximum chunk size depending on the number of fields and
    /// placeholder limit.
    fn maximum_chunk_size(conn: &rusqlite::Connection) -> usize {
        let var_limit = conn.limit(rusqlite::limits::Limit::SQLITE_LIMIT_VARIABLE_NUMBER) as usize;
        // If each model takes up `FIELDS` variables, we can fit `var_limit /
        // FIELDS` complete models in each "page" of our query
        var_limit / Self::FIELDS.len()
    }

    /// Dynamically builds an `INSERT` query suitable for the given number of
    /// `rows`.
    fn build_query(rows: usize) -> String {
        let mut query = format!("INSERT INTO {} (", Self::TABLE_NAME);
        let mut placeholder = String::from('(');

        for (i, field) in Self::FIELDS.iter().enumerate() {
            if i > 0 {
                placeholder.push_str(", ");
                query.push_str(", ");
            }
            placeholder.push('?');
            query.push_str(field);
        }
        placeholder.push(')');
        query.push_str(") VALUES ");

        for i in 0..rows {
            if i > 0 {
                query.push_str(", ");
            }
            query.push_str(&placeholder);
        }
        query.push(';');

        query
    }

    fn insert(&self, conn: &rusqlite::Connection) -> Result<()> {
        let mut stmt = conn.prepare_cached(&Self::build_query(1))?;
        let mut params = vec![];
        self.extend_params(&mut params);
        stmt.execute(params.as_slice())?;

        Ok(())
    }

    fn multi_insert<'a, I>(mut models: I, conn: &rusqlite::Connection) -> Result<()>
    where
        I: Iterator<Item = &'a Self> + ExactSizeIterator,
        Self: 'a,
    {
        let chunk_size = Self::maximum_chunk_size(conn);

        let mut params = Vec::with_capacity(Self::FIELDS.len() * (models.len().min(chunk_size)));

        // first: insert huge chunks using a single prepared (cached) query
        if models.len() >= chunk_size {
            let mut chunked_stmt = conn.prepare_cached(&Self::build_query(chunk_size))?;
            while models.len() >= chunk_size {
                for row in models.by_ref().take(chunk_size) {
                    row.extend_params(&mut params);
                }
                chunked_stmt.execute(params.as_slice())?;
                params.clear();
            }
        }

        // then: insert the remainder
        if models.len() > 0 {
            // this statement is not cached, as the number of models / params can be
            // different for every call
            let mut remainder_stmt = conn.prepare(&Self::build_query(models.len()))?;

            for row in models {
                row.extend_params(&mut params);
            }
            remainder_stmt.execute(params.as_slice())?;
            params.clear();
        }

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

impl<'a> std::convert::TryFrom<&'a rusqlite::Row<'a>> for SourceFile {
    type Error = rusqlite::Error;

    fn try_from(row: &'a ::rusqlite::Row) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.get(row.as_ref().column_index("id")?)?,
            path: row.get(row.as_ref().column_index("path")?)?,
        })
    }
}

impl Insertable for SourceFile {
    const TABLE_NAME: &'static str = "source_file";
    const FIELDS: &'static [&'static str] = &["id", "path"];

    fn extend_params<'a>(&'a self, params: &mut Vec<&'a dyn rusqlite::ToSql>) {
        params.extend(&[
            &self.id as &dyn rusqlite::ToSql,
            &self.path as &dyn rusqlite::ToSql,
        ])
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

impl Insertable for CoverageSample {
    const TABLE_NAME: &'static str = "coverage_sample";
    const FIELDS: &'static [&'static str] = &[
        "raw_upload_id",
        "local_sample_id",
        "source_file_id",
        "line_no",
        "coverage_type",
        "hits",
        "hit_branches",
        "total_branches",
    ];

    fn extend_params<'a>(&'a self, params: &mut Vec<&'a dyn rusqlite::ToSql>) {
        params.extend(&[
            &self.raw_upload_id as &dyn rusqlite::ToSql,
            &self.local_sample_id as &dyn rusqlite::ToSql,
            &self.source_file_id as &dyn rusqlite::ToSql,
            &self.line_no as &dyn rusqlite::ToSql,
            &self.coverage_type as &dyn rusqlite::ToSql,
            &self.hits as &dyn rusqlite::ToSql,
            &self.hit_branches as &dyn rusqlite::ToSql,
            &self.total_branches as &dyn rusqlite::ToSql,
        ])
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

impl Insertable for BranchesData {
    const TABLE_NAME: &'static str = "branches_data";
    const FIELDS: &'static [&'static str] = &[
        "raw_upload_id",
        "local_branch_id",
        "source_file_id",
        "local_sample_id",
        "hits",
        "branch_format",
        "branch",
    ];

    fn extend_params<'a>(&'a self, params: &mut Vec<&'a dyn rusqlite::ToSql>) {
        params.extend(&[
            &self.raw_upload_id as &dyn rusqlite::ToSql,
            &self.local_branch_id as &dyn rusqlite::ToSql,
            &self.source_file_id as &dyn rusqlite::ToSql,
            &self.local_sample_id as &dyn rusqlite::ToSql,
            &self.hits as &dyn rusqlite::ToSql,
            &self.branch_format as &dyn rusqlite::ToSql,
            &self.branch as &dyn rusqlite::ToSql,
        ])
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

impl Insertable for MethodData {
    const TABLE_NAME: &'static str = "method_data";
    const FIELDS: &'static [&'static str] = &[
        "raw_upload_id",
        "local_method_id",
        "source_file_id",
        "local_sample_id",
        "line_no",
        "hit_branches",
        "total_branches",
        "hit_complexity_paths",
        "total_complexity",
    ];

    fn extend_params<'a>(&'a self, params: &mut Vec<&'a dyn rusqlite::ToSql>) {
        params.extend(&[
            &self.raw_upload_id as &dyn rusqlite::ToSql,
            &self.local_method_id as &dyn rusqlite::ToSql,
            &self.source_file_id as &dyn rusqlite::ToSql,
            &self.local_sample_id as &dyn rusqlite::ToSql,
            &self.line_no as &dyn rusqlite::ToSql,
            &self.hit_branches as &dyn rusqlite::ToSql,
            &self.total_branches as &dyn rusqlite::ToSql,
            &self.hit_complexity_paths as &dyn rusqlite::ToSql,
            &self.total_complexity as &dyn rusqlite::ToSql,
        ])
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

impl Insertable for SpanData {
    const TABLE_NAME: &'static str = "span_data";
    const FIELDS: &'static [&'static str] = &[
        "raw_upload_id",
        "local_span_id",
        "source_file_id",
        "local_sample_id",
        "hits",
        "start_line",
        "start_col",
        "end_line",
        "end_col",
    ];

    fn extend_params<'a>(&'a self, params: &mut Vec<&'a dyn rusqlite::ToSql>) {
        params.extend(&[
            &self.raw_upload_id as &dyn rusqlite::ToSql,
            &self.local_span_id as &dyn rusqlite::ToSql,
            &self.source_file_id as &dyn rusqlite::ToSql,
            &self.local_sample_id as &dyn rusqlite::ToSql,
            &self.hits as &dyn rusqlite::ToSql,
            &self.start_line as &dyn rusqlite::ToSql,
            &self.start_col as &dyn rusqlite::ToSql,
            &self.end_line as &dyn rusqlite::ToSql,
            &self.end_col as &dyn rusqlite::ToSql,
        ])
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

impl Insertable for ContextAssoc {
    const TABLE_NAME: &'static str = "context_assoc";
    const FIELDS: &'static [&'static str] = &[
        "context_id",
        "raw_upload_id",
        "local_sample_id",
        "local_span_id",
    ];

    fn extend_params<'a>(&'a self, params: &mut Vec<&'a dyn rusqlite::ToSql>) {
        params.extend(&[
            &self.context_id as &dyn rusqlite::ToSql,
            &self.raw_upload_id as &dyn rusqlite::ToSql,
            &self.local_sample_id as &dyn rusqlite::ToSql,
            &self.local_span_id as &dyn rusqlite::ToSql,
        ])
    }
}

impl<'a> std::convert::TryFrom<&'a rusqlite::Row<'a>> for Context {
    type Error = rusqlite::Error;

    fn try_from(row: &'a ::rusqlite::Row) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.get(row.as_ref().column_index("id")?)?,
            name: row.get(row.as_ref().column_index("name")?)?,
        })
    }
}

impl Insertable for Context {
    const TABLE_NAME: &'static str = "context";
    const FIELDS: &'static [&'static str] = &["id", "name"];

    fn extend_params<'a>(&'a self, params: &mut Vec<&'a dyn rusqlite::ToSql>) {
        params.extend(&[
            &self.id as &dyn rusqlite::ToSql,
            &self.name as &dyn rusqlite::ToSql,
        ])
    }
}

impl Insertable for RawUpload {
    const TABLE_NAME: &'static str = "raw_upload";
    const FIELDS: &'static [&'static str] = &[
        "id",
        "timestamp",
        "raw_upload_url",
        "flags",
        "provider",
        "build",
        "name",
        "job_name",
        "ci_run_url",
        "state",
        "env",
        "session_type",
        "session_extras",
    ];

    fn extend_params<'a>(&'a self, params: &mut Vec<&'a dyn rusqlite::ToSql>) {
        params.extend(&[
            &self.id as &dyn rusqlite::ToSql,
            &self.timestamp as &dyn rusqlite::ToSql,
            &self.raw_upload_url as &dyn rusqlite::ToSql,
            &self.flags as &dyn rusqlite::ToSql,
            &self.provider as &dyn rusqlite::ToSql,
            &self.build as &dyn rusqlite::ToSql,
            &self.name as &dyn rusqlite::ToSql,
            &self.job_name as &dyn rusqlite::ToSql,
            &self.ci_run_url as &dyn rusqlite::ToSql,
            &self.state as &dyn rusqlite::ToSql,
            &self.env as &dyn rusqlite::ToSql,
            &self.session_type as &dyn rusqlite::ToSql,
            &self.session_extras as &dyn rusqlite::ToSql,
        ])
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
    use serde_json::json;
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

    impl Insertable for TestModel {
        const TABLE_NAME: &'static str = "test";
        const FIELDS: &'static [&'static str] = &["id", "data"];

        fn extend_params<'a>(&'a self, params: &mut Vec<&'a dyn rusqlite::ToSql>) {
            params.extend(&[
                &self.id as &dyn rusqlite::ToSql,
                &self.data as &dyn rusqlite::ToSql,
            ])
        }
    }

    #[test]
    fn query_builder() {
        let query = TestModel::build_query(1);
        assert_eq!(query, "INSERT INTO test (id, data) VALUES (?, ?);");

        let query = TestModel::build_query(3);
        assert_eq!(
            query,
            "INSERT INTO test (id, data) VALUES (?, ?), (?, ?), (?, ?);"
        );
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
        let report = SqliteReport::open(db_file).unwrap();

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

        model.insert(&ctx.report.conn).unwrap();
        let duplicate_result = model.insert(&ctx.report.conn);

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

        // Our chunk-size is set to 50, so inserting more than twice that will use
        // multiple chunks, as well as using single inserts for the remainder.

        let models_to_insert: Vec<_> = (0..111)
            .map(|id| TestModel {
                id,
                data: format!("Test {id}"),
            })
            .collect();

        TestModel::multi_insert(models_to_insert.iter(), &ctx.report.conn).unwrap();

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

        model.insert(&ctx.report.conn).unwrap();
        let duplicate_result = model.insert(&ctx.report.conn);

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
            name: "test_upload".to_string(),
        };

        model.insert(&ctx.report.conn).unwrap();
        let duplicate_result = model.insert(&ctx.report.conn);

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
        let mut report_builder = SqliteReportBuilder::open(db_file).unwrap();

        let raw_upload = report_builder
            .insert_raw_upload(Default::default())
            .unwrap();
        let context = report_builder.insert_context("foo").unwrap();

        let report = report_builder.build().unwrap();

        let model = ContextAssoc {
            context_id: context.id,
            raw_upload_id: raw_upload.id,
            local_sample_id: Some(rand::random()),
            local_span_id: None,
        };

        model.insert(&report.conn).unwrap();
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
        let mut report_builder = SqliteReportBuilder::open(db_file).unwrap();

        let source_file = report_builder.insert_file("foo.rs").unwrap();
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

        model.insert(&report.conn).unwrap();
        let duplicate_result = model.insert(&report.conn);

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
        let mut report_builder = SqliteReportBuilder::open(db_file).unwrap();

        let source_file = report_builder.insert_file("path").unwrap();
        let raw_upload = report_builder
            .insert_raw_upload(Default::default())
            .unwrap();

        let report = report_builder.build().unwrap();

        let local_sample_id = rand::random();
        CoverageSample {
            raw_upload_id: raw_upload.id,
            local_sample_id,
            source_file_id: source_file.id,
            ..Default::default()
        }
        .insert(&report.conn)
        .unwrap();

        let model = BranchesData {
            raw_upload_id: raw_upload.id,
            local_branch_id: rand::random(),
            local_sample_id,
            source_file_id: source_file.id,
            ..Default::default()
        };

        model.insert(&report.conn).unwrap();
        let duplicate_result = model.insert(&report.conn);

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
        let mut report_builder = SqliteReportBuilder::open(db_file).unwrap();

        let source_file = report_builder.insert_file("foo.rs").unwrap();
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

        model.insert(&report.conn).unwrap();
        let duplicate_result = model.insert(&report.conn);

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
        let mut report_builder = SqliteReportBuilder::open(db_file).unwrap();

        let source_file = report_builder.insert_file("foo.rs").unwrap();
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

        model.insert(&report.conn).unwrap();
        let duplicate_result = model.insert(&report.conn);

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

    #[test]
    fn test_raw_upload_single_insert() {
        let ctx = setup();

        let model = RawUpload {
            id: 5,
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

        model.insert(&ctx.report.conn).unwrap();
        let duplicate_result = model.insert(&ctx.report.conn);

        let uploads = ctx.report.list_raw_uploads().unwrap();
        assert_eq!(uploads, vec![model]);

        let error = duplicate_result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "sqlite failure: 'UNIQUE constraint failed: raw_upload.id'"
        );
    }
}
