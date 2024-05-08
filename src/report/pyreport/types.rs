use std::collections::HashMap;

pub use super::super::models::CoverageType;
use crate::parsers::json::JsonVal;

/// Enum representing the possible values of the "coverage" field in a
/// ReportLine or LineSession object.
///
/// Most of the time, we can parse this field into a `HitCount` or
/// `BranchesTaken`.
#[derive(Clone, Debug, PartialEq)]
pub enum PyreportCoverage {
    /// Contains the number of times the target was hit (or sometimes just 0 or
    /// 1). Most formats represent line and method coverage this way. In some
    /// chunks files it is mistakenly used for branch coverage.
    HitCount(u32),

    /// Contains the number of branches taken and the total number of branches
    /// possible. Ex: "1/2". Most formats represent branch coverage this
    /// way. In some chunks files it is mistakenly used for method coverage.
    BranchesTaken { covered: u32, total: u32 },

    /// Indicates that the target is partially covered but we don't know about
    /// covered/missed branches.
    Partial(),
}

/// Enum representing the possible values of the "complexity" field in a
/// [`ReportLine`] or [`LineSession`]. This is generally (exclusively?) used for
/// method coverage.
#[derive(Clone, Debug, PartialEq)]
pub enum Complexity {
    /// Contains the total cyclomatic complexity of the target.
    Total(u32),

    /// Contains the number of paths covered and the total cyclomatic complexity
    /// of the target.
    PathsTaken { covered: u32, total: u32 },
}

/// Enum representing the possible shapes of data about missing branch coverage.
#[derive(Clone, Debug, PartialEq)]
pub enum MissingBranch {
    /// Identifies a specific branch by its "block" and "branch" numbers chosen
    /// by the instrumentation. Lcov does it this way.
    BlockAndBranch(u32, u32),

    /// Identifies a specific branch as one of a set of conditions tied to a
    /// line. In Cobertura, this condition may be accompanied by a "type" such
    /// as "jump".
    Condition(u32, Option<String>),

    /// Identifies a specific branch as a line number the branch is located at.
    Line(u32),
}

/// Struct representing a subspan of a single line and its coverage status.
#[derive(Debug, Clone, PartialEq)]
pub struct Partial {
    pub start_col: Option<u32>,
    pub end_col: Option<u32>,
    pub coverage: PyreportCoverage,
}

/// Represents the coverage measurements taken for a specific "session". Each
/// `LineSession` will correspond to a
/// [`crate::report::models::CoverageSample`].
///
/// Each upload to our system constitutes a "session" and may be tagged with
/// flags or other context that we want to filter on when viewing report data.
#[derive(Debug, PartialEq)]
pub struct LineSession {
    /// This ID indicates which session the measurement was taken in. It can be
    /// used as a key in `buf.state.report_json_sessions` to get the ID of a
    /// [`crate::report::models::Context`] in order to create a
    /// [`crate::report::models::ContextAssoc`].
    pub session_id: usize,

    /// The coverage measurement that was taken in this session. The
    /// `CoverageType` is "inherited" from the [`ReportLine`] that this
    /// `LineSession` is a part of.
    pub coverage: PyreportCoverage,

    /// A list of specific branches/conditions stemming from this line that were
    /// not covered if this line is a branch. May be omitted, or may be null.
    pub branches: Option<Option<Vec<MissingBranch>>>,

    /// A list of "line partials" which indicate different coverage values for
    /// different subspans of this line. May be omitted, or may be null.
    pub partials: Option<Option<Vec<Partial>>>,

    /// A measure of the cyclomatic complexity of the method declared on this
    /// line if this line is a method. May be omitted, or may be null.
    pub complexity: Option<Option<Complexity>>,
}

/// Enum representing a label which is applicable for a particular measurement.
/// An example of a label is a test case that was running when the measurement
/// was taken.
///
/// Newer reports store IDs which can later be mapped back to an original string
/// label with an index found in the chunks file header. Older reports stored
/// many copies of the full strings.
#[derive(Clone, Debug)]
pub enum RawLabel {
    /// A numeric ID that was assigned to this label. The original label can be
    /// accessed in the `"labels_index"` key in the chunks file's header.
    /// For our parser's purposes, we can access the ID of the
    /// [`crate::report::models::Context`] created for this label in
    /// `buf.state.labels_index`.
    LabelId(u32),

    /// The name of the label. If we have encountered this label before, it
    /// should be in `buf.state.labels_index` pointing at the ID for a
    /// [`crate::report::models::Context`]. Otherwise, we should create that
    /// `Context` + mapping ourselves.
    LabelName(String),
}

/// An object that is similar to a [`LineSession`], containing coverage
/// measurements specific to a session. It is mostly redundant and ignored in
/// this parser, save for the `labels` field which is not found anywhere else.
#[derive(Clone, Debug, PartialEq)]
pub struct CoverageDatapoint {
    /// This ID indicates which session the measurement was taken in. It can be
    /// used as a key in `buf.state.report_json_sessions` to get the ID of a
    /// [`crate::report::models::Context`] in order to create a
    /// [`crate::report::models::ContextAssoc`].
    pub session_id: u32,

    /// A redundant copy of the coverage measurement for a session. We prefer
    /// the value from the [`LineSession`].
    pub _coverage: PyreportCoverage,

    /// A redundant copy of the `CoverageType`. We use the value from the
    /// [`ReportLine`].
    ///
    /// Technically this field is optional, but the way we serialize it when
    /// it's missing is identical to the way we serialize
    /// [`crate::report::models::CoverageType::Line`] so there's
    /// no way to tell which it is when deserializing.
    pub _coverage_type: Option<CoverageType>,

    /// A list of labels (e.g. test cases) that apply to this datapoint.
    pub labels: Vec<String>,
}

/// Contains all of the coverage measurements for a line in a source file.
///
/// The `coverage` field and `_complexity` fields contain aggregates of those
/// fields across all [`LineSession`]s in `sessions`.
///
/// `datapoints`, if present, contains mostly-redundant [`CoverageDatapoint`]s.
/// This is where [`RawLabel`]s are found.
///
/// `_messages`, `_complexity` are ignored. `coverage` is used to detect/correct
/// malformed input data and is thrown away in favor of the coverage data in
/// each `LineSession`.
#[derive(Debug, PartialEq)]
pub struct ReportLine {
    /// An aggregated coverage status across all of the [`LineSession`]s in
    /// `sessions`.
    pub coverage: PyreportCoverage,

    pub coverage_type: CoverageType,

    /// The list of measurements taken for this line. Each of these corresponds
    /// to a [`CoverageSample`] record in a `SqliteReport`.
    pub sessions: Vec<LineSession>,

    /// Long forgotten field that takes up space.
    pub _messages: Option<Option<JsonVal>>,

    /// An aggregated complexity metric across all of the [`LineSession`]s in
    /// `sessions`.
    pub _complexity: Option<Option<Complexity>>,

    /// The list of [`CoverageDatapoint`]s for this line. `CoverageDatapoint` is
    /// largely redundant but its `labels` field is the only place where label
    /// data is recorded (e.g. which test case was running when this
    /// measurement was collected).
    pub datapoints: Option<Option<HashMap<u32, CoverageDatapoint>>>,
}

/// Account for some quirks and malformed data. See code comments for details.
pub(crate) fn normalize_coverage_measurement(
    coverage: &PyreportCoverage,
    coverage_type: &CoverageType,
) -> (PyreportCoverage, CoverageType) {
    match (coverage, coverage_type) {
        // Clojure uses `true` to indicate partial coverage without giving any specific information
        // about the missed/covered branches. Our parsers for other formats just make up "1/2" and
        // move on, so we do that here as well.
        (PyreportCoverage::Partial(), _) => (
            PyreportCoverage::BranchesTaken {
                covered: 1,
                total: 2,
            },
            *coverage_type,
        ),

        // For method coverage, Jacoco contains aggregated instruction coverage, branch coverage,
        // and cyclomatic complexity metrics. If the branch coverage fields are present and
        // non-zero, our parser will prefer them even though method coverage should be an int.
        // We normalize by treating the numerator of the branch coverage as a hit count, so "0/2"
        // is a miss but "3/4" is 3 hits. Not great, but the data is bugged to begin with.
        (PyreportCoverage::BranchesTaken { covered, .. }, CoverageType::Method) => {
            (PyreportCoverage::HitCount(*covered), CoverageType::Method)
        }

        // Our Go parser does not properly fill out the `coverage_type` field. If the `coverage`
        // field has branch data in it, override the coverage type.
        (PyreportCoverage::BranchesTaken { .. }, CoverageType::Line) => {
            (coverage.clone(), CoverageType::Branch)
        }

        // We see some instances of Scale Scoverage reports being incorrectly translated into
        // Cobertura reports. In Scoverage, 0 for branch coverage means miss, 1 means partial, and
        // 2 means hit. It seems when converting to Cobertura, the value is taken as a raw hit
        // count and not coverted to `branch-rate` or something, and our Cobertura parser doesn't
        // handle it. So, we handle it here.
        (PyreportCoverage::HitCount(n), CoverageType::Branch) => {
            assert!(*n == 0 || *n == 1 || *n == 2); // TODO soften assert
            (
                PyreportCoverage::BranchesTaken {
                    covered: *n,
                    total: 2,
                },
                CoverageType::Branch,
            )
        }

        // Everything's fine.
        (_, _) => (coverage.clone(), *coverage_type),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_coverage_measurement() {
        // Cases that don't need adjustmnet
        assert_eq!(
            normalize_coverage_measurement(&PyreportCoverage::HitCount(3), &CoverageType::Line),
            (PyreportCoverage::HitCount(3), CoverageType::Line)
        );
        assert_eq!(
            normalize_coverage_measurement(&PyreportCoverage::HitCount(3), &CoverageType::Method),
            (PyreportCoverage::HitCount(3), CoverageType::Method)
        );
        assert_eq!(
            normalize_coverage_measurement(
                &PyreportCoverage::BranchesTaken {
                    covered: 2,
                    total: 4
                },
                &CoverageType::Branch
            ),
            (
                PyreportCoverage::BranchesTaken {
                    covered: 2,
                    total: 4
                },
                CoverageType::Branch
            )
        );

        // Cases that need adjustment
        assert_eq!(
            normalize_coverage_measurement(&PyreportCoverage::Partial(), &CoverageType::Branch),
            (
                PyreportCoverage::BranchesTaken {
                    covered: 1,
                    total: 2
                },
                CoverageType::Branch
            )
        );
        assert_eq!(
            normalize_coverage_measurement(&PyreportCoverage::HitCount(1), &CoverageType::Branch),
            (
                PyreportCoverage::BranchesTaken {
                    covered: 1,
                    total: 2
                },
                CoverageType::Branch
            )
        );
        assert_eq!(
            normalize_coverage_measurement(
                &PyreportCoverage::BranchesTaken {
                    covered: 1,
                    total: 2
                },
                &CoverageType::Line
            ),
            (
                PyreportCoverage::BranchesTaken {
                    covered: 1,
                    total: 2
                },
                CoverageType::Branch
            )
        );
        assert_eq!(
            normalize_coverage_measurement(
                &PyreportCoverage::BranchesTaken {
                    covered: 1,
                    total: 2
                },
                &CoverageType::Method,
            ),
            (PyreportCoverage::HitCount(1), CoverageType::Method,)
        );
    }
}
