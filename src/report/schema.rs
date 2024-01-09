use diesel::sql_types::SqlType;

#[derive(diesel_derive_enum::DbEnum, Debug, PartialEq)]
pub enum CoverageStatus {
    Hit,
    Miss,
    Partial,
}

diesel::table! {
    use diesel::sql_types::{VarChar, Integer};
    source_file (id) {
        id -> Integer,
        path -> VarChar,
    }
}

diesel::table! {
    use diesel::sql_types::Integer;
    use super::CoverageStatusMapping;
    line_status (id) {
        id -> Integer,
        source_file_id -> Integer,
        line_no -> Integer, // BigInt?
        coverage_status -> CoverageStatusMapping,
    }
}

diesel::table! {
    use diesel::sql_types::Integer;
    use super::CoverageStatusMapping;
    branch_status (id) {
        id -> Integer,
        source_file_id -> Integer,
        start_line_no -> Integer, // BigInt?
        end_line_no -> Integer, // BigInt?
        coverage_status -> CoverageStatusMapping,
    }
}

diesel::table! {
    use diesel::sql_types::{Nullable, Integer};
    context_assoc (context_id, line_id, branch_id) {
        context_id -> Integer,
        line_id -> Nullable<Integer>,
        branch_id -> Nullable<Integer>,
    }
}

diesel::table! {
    use diesel::sql_types::{VarChar, Integer};
    context (id) {
        id -> Integer,
        name -> VarChar,
    }
}

diesel::joinable!(line_status -> source_file (source_file_id));
diesel::joinable!(branch_status -> source_file (source_file_id));

diesel::joinable!(context_assoc -> line_status (line_id));
diesel::joinable!(context_assoc -> branch_status (branch_id));
diesel::joinable!(context_assoc -> context (context_id));

diesel::allow_tables_to_appear_in_same_query!(
    source_file,
    line_status,
    branch_status,
    context_assoc,
    context
);
