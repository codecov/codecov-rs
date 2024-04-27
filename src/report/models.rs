/*!
 * Database models for coverage data. SQLite schema/migrations in
 * `/migrations`. [Inspired by
 * coverage.py](https://coverage.readthedocs.io/en/latest/dbschema.html).
 *
 * Library users probably want to start by reading the docs for
 * [`crate::report::Report`]/[`crate::report::SqliteReport`] and
 * [`crate::report::ReportBuilder`]/[`crate::report::SqliteReportBuilder`].
 *
 * An overview of the models and their relationships:
 * - Each source file in a report should have a [`SourceFile`] record.
 * - Each line in that source file, whether it's a statement, a branch, or a
 *   method declaration, should have a [`CoverageSample`] record with its
 *   `source_file_id` field pointed at its source file.
 *   - For lines/statements/method declarations, the `hits` field should
 *     contain a hit count
 *   - For branches, the `hit_branches` and `total_branches` fields should
 *     be filled in instead
 * - The [`Context`] and [`ContextAssoc`] models can be used to tag
 *   measurements with context (e.g. "these measurements were on Windows",
 *   "these measurements were from TestSuiteFoo") and enable
 *   querying/filtering.
 * - [`BranchesData`], [`MethodData`], and [`SpanData`] are optional. These
 *   models are for information that is not provided in every format or is
 *   provided in different shapes, and records should be tied to a
 *   [`CoverageSample`].
 *   - [`BranchesData`] can store information about which specific branches
 *     were hit or missed
 *   - [`MethodData`] can store information including cyclomatic complexity
 *     or aggregated line/branch coverage
 *   - [`SpanData`] can store coverage information that isn't presented
 *     line-by-line. Some formats report that a range from line X to line Y
 *     is covered, or that only part of a single line is covered.
 *
 * Some choices were made that make merging multiple reports together very
 * simple:
 * - [`SourceFile`] and [`Context`] use hashes of their names as an ID so
 *   every report they appear in will come up with the same ID
 * - Measurement models ([`CoverageSample`], [`BranchesData`],
 *   [`MethodData`], [`SpanData`]) use UUIDv4s for IDs to essentially
 *   guarantee different reports will not use the same IDs
 *
 * These properties make merging essentially just concatenation.
 * [`SourceFile`]s and [`Context`] can be merged into an existing report
 * with `INSERT OR IGNORE`, and the rest can be merged with a
 * regular `INSERT` without needing to update any foreign keys or anything.
 *
 * SQLite `INTEGER` values are variable-size but they can be up to 64 bits,
 * signed, so numeric types use `i64`. Since our numeric data is
 * non-negative, we're effectively using using `u32`s in an `i64` wrapper.
 * If we wind up needing `u64`s, we can probably cast to `i64` before saving
 * and cast back to `u64` when querying.
 */

use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use strum_macros::{Display, EnumString};
use uuid::Uuid;

use crate::parsers::json::JsonVal;

#[derive(PartialEq, Debug, Clone, Copy, Default)]
pub enum CoverageType {
    #[default]
    Line = 1,
    Branch,
    Method,
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

#[derive(PartialEq, Debug, Clone, Copy, Default)]
pub enum BranchFormat {
    /// Indicates that the value in the `branch` field is the line number that a
    /// branch lands on. "26", "28", "30"
    #[default]
    Line,

    /// Indicates that the value in the `branch` field refers to the 0th, 1st,
    /// ...Nth branch stemming from the same statement. "0:jump", "1", "2",
    /// "3"...
    Condition,

    /// Indicates that the value in the `branch` field contains the "block ID"
    /// and "branch ID" which globally identify the branch. "0:0", "0:1",
    /// "1:0", "1:1"...
    BlockAndBranch,
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

#[derive(EnumString, Display, Debug, PartialEq, Clone, Copy, Default)]
pub enum ContextType {
    /// A [`Context`] with this type represents a test case, and every
    /// [`CoverageSample`] associated with it is a measurement that applies
    /// to that test case.
    TestCase,

    /// A [`Context`] with this type represents a distinct upload with coverage
    /// data. For instance, a Windows test runner and a macOS test runner
    /// may send coverage data for the same code in two separate uploads.
    #[default]
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

#[derive(PartialEq, Debug, Default)]
pub struct SourceFile {
    /// Should be a hash of the path
    pub id: i64,
    pub path: String,
}

/// Each line in a source file should have a [`CoverageSample`] record when
/// possible, whether it's a line/statement, a branch, or a method declaration.
/// The `coverage_sample` table should be sufficient to paint green/yellow/red
/// lines in a UI.
///
/// A line is fully covered if:
/// - its `coverage_type` is [`CoverageType::Line`] or [`CoverageType::Method`]
///   and its `hit` value is not 0
/// - its `coverage_type` is [`CoverageType::Branch`] and its `hit_branches`
///   value is equal to its `total_branches` value
///
/// A line is not covered if:
/// - its `coverage_type` is [`CoverageType::Line`] or [`CoverageType::Method`]
///   and its `hit` value is 0
/// - its `coverage_type` is [`CoverageType::Branch`] and its `hit_branches`
///   value is 0
///
/// A line is partially covered if:
/// - its `coverage_type` is [`CoverageType::Branch`] and its `hit_branches`
///   value is less than its `total_branches` value (but greater than 0)
#[derive(PartialEq, Debug, Clone, Default)]
pub struct CoverageSample {
    pub id: Uuid,
    pub source_file_id: i64,
    pub line_no: i64,
    pub coverage_type: CoverageType,

    /// The number of times the line was run.
    /// Should be filled out for lines and methods.
    pub hits: Option<i64>,

    /// The number of branches stemming from this line that were run.
    /// Should be filled out for branches.
    pub hit_branches: Option<i64>,

    /// The number of possible branches stemming from this line that could have
    /// been run. Should be filled out for branches
    pub total_branches: Option<i64>,
}

/// If raw coverage data includes information about which specific branches
/// stemming from some line were or weren't covered, it can be stored here.
#[derive(PartialEq, Debug, Default, Clone)]
pub struct BranchesData {
    pub id: Uuid,
    pub source_file_id: i64,

    /// The [`CoverageSample`] record for the line this branch stems from.
    pub sample_id: Uuid,

    /// The number of times this particular branch was run.
    pub hits: i64,

    /// The "shape" of the branch identifier saved in the `branch` field.
    pub branch_format: BranchFormat,

    /// An identifier of some kind (see `branch_format`) distinguishing this
    /// branch from others that stem from the same line.
    pub branch: String,
}

/// If raw coverage data includes additional metrics for methods (cyclomatic
/// complexity, aggregated branch coverage) or details like its name or
/// signature, they can be stored here.
#[derive(PartialEq, Debug, Default, Clone)]
pub struct MethodData {
    pub id: Uuid,
    pub source_file_id: i64,

    /// The [`CoverageSample`] record for the line this method was declared on,
    /// if known.
    pub sample_id: Option<Uuid>,

    /// The line this method was declared on, in case it's known but we don't
    /// have a [`CoverageSample`] for it.
    pub line_no: Option<i64>,

    /// The aggregated number of branches that were hit across this method.
    pub hit_branches: Option<i64>,

    /// The aggregated total number of possible branches that could have been
    /// hit across this method.
    pub total_branches: Option<i64>,

    /// The number of "cyclomatic complexity paths" that were covered in this
    /// method.
    pub hit_complexity_paths: Option<i64>,

    /// Total cyclomatic complexity of the method.
    pub total_complexity: Option<i64>,
}

/// If raw coverage data presents coverage information in terms of `(start_line,
/// end_line)` or `((start_line, start_col), (end_line, end_col))` coordinates,
/// it can be stored here.
///
/// For example, consider this hypothetical span:
/// - `hits: 3`
/// - `start_line: 3`
/// - `start_col: 10`
/// - `end_line: 7`
/// - `end_col: null`
///
/// That information can be stored straightforwardly in this table as-is.
/// However, you can also infer that lines 3-7 were all hit 3 times and create
/// [`CoverageSample`] records for them.
#[derive(PartialEq, Debug, Default, Clone)]
pub struct SpanData {
    pub id: Uuid,
    pub source_file_id: i64,

    /// A [`CoverageSample`] that is tied to this span, if there is a singular
    /// one to pick. If a span is for a subsection of a single line, we
    /// should be able to link a [`CoverageSample`].
    pub sample_id: Option<Uuid>,

    /// The number of times this span was run.
    pub hits: i64,

    /// The line in the source file that this span starts on.
    pub start_line: Option<i64>,

    /// The column within `start_line` that this span starts on.
    pub start_col: Option<i64>,

    /// The line in the source file that this span ends on.
    pub end_line: Option<i64>,

    /// The column within `end_line` that this span starts on.
    pub end_col: Option<i64>,
}

/// Ties a [`Context`] to specific measurement data.
#[derive(PartialEq, Debug, Default, Clone)]
pub struct ContextAssoc {
    pub context_id: i64,
    pub sample_id: Option<Uuid>,
    pub branch_id: Option<Uuid>,
    pub method_id: Option<Uuid>,
    pub span_id: Option<Uuid>,
}

/// Context that can be associated with measurements to allow querying/filtering
/// based on test cases, platforms, or other dimensions.
#[derive(PartialEq, Debug, Default, Clone)]
pub struct Context {
    /// Should be a hash of the name
    pub id: i64,
    pub context_type: ContextType,

    /// Some sort of unique name for this context, such as a test case name or a
    /// CI results URI.
    pub name: String,
}

#[derive(PartialEq, Debug, Default, Clone)]
pub struct UploadDetails {
    pub context_id: i64,
    pub timestamp: Option<i64>,
    pub raw_upload_url: Option<String>,
    /// JSON array
    pub flags: Option<JsonVal>,
    pub provider: Option<String>,
    pub build: Option<String>,
    pub name: Option<String>,
    pub job_name: Option<String>,
    pub ci_run_url: Option<String>,
    pub state: Option<String>,
    pub env: Option<String>,
    pub session_type: Option<String>,
    pub session_extras: Option<JsonVal>,
}
