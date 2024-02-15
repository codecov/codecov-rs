use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use strum_macros::{Display, EnumString};

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum CoverageStatus {
    Hit = 1,
    Miss,
    Partial,
}

impl ToSql for CoverageStatus {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok((*self as i32).into())
    }
}

impl FromSql for CoverageStatus {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let variant = match value.as_i64()? {
            1 => CoverageStatus::Hit,
            2 => CoverageStatus::Miss,
            3 => CoverageStatus::Partial,
            _ => panic!("Uh oh"),
        };
        Ok(variant)
    }
}

#[derive(EnumString, Display, Debug, PartialEq)]
pub enum ContextType {
    TestCase,
    Upload,
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

pub struct SourceFile {
    pub id: Option<i32>,
    pub path: String,
}

pub struct LineStatus {
    pub id: Option<i32>,
    pub source_file_id: i32,
    pub line_no: i32,
    pub coverage_status: CoverageStatus,
}

pub struct BranchStatus {
    pub id: Option<i32>,
    pub source_file_id: i32,
    pub start_line_no: i32,
    pub end_line_no: i32,
    pub coverage_status: CoverageStatus,
}

pub struct ContextAssoc {
    pub context_id: i32,
    pub line_id: Option<i32>,
    pub branch_id: Option<i32>,
}

pub struct Context {
    pub id: Option<i32>,
    pub context_type: ContextType,
    pub name: String,
}
