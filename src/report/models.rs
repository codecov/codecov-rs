use diesel::{Associations, Identifiable, Insertable, Queryable, Selectable};

use crate::report::schema::*;

#[derive(Queryable, Identifiable, Selectable, Insertable, Debug, PartialEq)]
#[diesel(table_name = source_file)]
pub struct SourceFile {
    pub id: i32,
    pub path: String,
}

#[derive(Queryable, Identifiable, Selectable, Associations, Insertable, Debug, PartialEq)]
#[diesel(belongs_to(SourceFile))]
#[diesel(table_name = line_status)]
pub struct LineStatus {
    pub id: i32,
    pub source_file_id: i32,
    pub line_no: i32,
    pub coverage_status: CoverageStatus,
}

#[derive(Queryable, Identifiable, Selectable, Associations, Insertable, Debug, PartialEq)]
#[diesel(belongs_to(SourceFile))]
#[diesel(table_name = branch_status)]
pub struct BranchStatus {
    pub id: i32,
    pub source_file_id: i32,
    pub start_line_no: i32,
    pub end_line_no: i32,
    pub coverage_status: CoverageStatus,
}

#[derive(Queryable, Identifiable, Selectable, Associations, Insertable, Debug, PartialEq)]
#[diesel(belongs_to(Context))]
#[diesel(belongs_to(LineStatus, foreign_key = line_id))]
#[diesel(belongs_to(BranchStatus, foreign_key = branch_id))]
#[diesel(table_name = context_assoc)]
#[diesel(primary_key(context_id, line_id, branch_id))]
pub struct ContextAssoc {
    pub context_id: i32,
    pub line_id: Option<i32>,
    pub branch_id: Option<i32>,
}

#[derive(Queryable, Identifiable, Selectable, Insertable, Debug, PartialEq)]
#[diesel(table_name = context)]
pub struct Context {
    pub id: i32,
    pub name: String,
}
