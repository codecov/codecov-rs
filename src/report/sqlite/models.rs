/*!
 * Trait implementations for [`crate::report::models`] types for use with
 * `rusqlite`.
 *
 * `ToSql`/`FromSql` allow enums to be used as model fields.
 * `TryFrom<&rusqlite::Row>` allows models to be automatically constructed
 * from query results (provided the query's column names are
 * named appropriately).
 */

use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};

use super::super::models::*;
use crate::parsers::json::JsonVal;

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
