use winnow::{
    combinator::{alt, opt, preceded, separated_pair, seq, terminated},
    PResult, Parser,
};

use crate::parsers::{
    nullable, parse_u32, ws, Report, ReportBuilder, ReportOutputStream, StrStream,
};

/// Enum representing the possible values of the "coverage" field in a
/// ReportLine or LineSession object.
///
/// Most of the time, we can parse this field into a `HitCount` or
/// `BranchesTaken`.
///
/// TODO: What exactly does an int mean for branch/method coverage? What does a
/// fraction mean for method coverage?
#[derive(Clone)]
pub enum PyreportCoverage {
    /// Contains the number of times the target was hit (or sometimes just 0 or
    /// 1). Most formats represent line and method coverage this way. Some
    /// use it for branch coverage.
    HitCount(u32),

    /// Contains the number of branches taken and the total number of branches
    /// possible. Ex: "1/2". Most formats represent branch coverage this
    /// way. Some use it for method coverage.
    BranchesTaken(u32, u32),

    /// Indicates that the target is partially covered but we don't know about
    /// covered/missed branches.
    Partial(),
}

/// Parses the possible values of the "coverage" field in a `ReportLine` or
/// `LineSession`. See [`PyreportCoverage`]. Most of the time, this field can be
/// parsed into a `HitCount` or `BranchesTaken`.
///
/// Clojure's Cloverage tool [uses `true` for partial coverage](https://github.com/cloverage/cloverage/blob/87fd10f82ea7c0f47c03354105e513b160d1e047/cloverage/src/cloverage/report/codecov.clj#L10)
/// with no information about covered/missed branches, and this quirk made its
/// way into the chunks format as well.
///
/// Examples: `0`, `1`, `"0/2"`, `"3/4"`, `"2/2"`, `true`
pub fn coverage<S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<PyreportCoverage> {
    alt((
        // Clojure's Cloverage tool does this.
        "true".value(PyreportCoverage::Partial()),
        // Examples: "0/2", "1/2", "2/2"
        preceded(
            '"',
            terminated(separated_pair(parse_u32, '/', parse_u32), '"'),
        )
        .map(move |(covered, total)| PyreportCoverage::BranchesTaken(covered, total)),
        // Examples: 0, 40
        parse_u32.map(PyreportCoverage::HitCount),
    ))
    .parse_next(buf)
}

/// The types of coverage that a `ReportLine` can describe.
#[derive(Clone)]
pub enum CoverageType {
    Line,
    Branch,
    Method,
}

/// Parses the coverage type described by a `ReportLine`. Beware: this field may
/// be inaccurate.
///
/// For example, in a chunks file for a Go project, the "coverage type" field is
/// always `null` when some of the values in the "coverage" field indicate the
/// line describes branch coverage.
///
/// Examples: `null`, `"line"`, `"b"`, `"branch"`, `"m"`, `"method"`
pub fn coverage_type<S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<CoverageType> {
    alt((
        alt(("\"line\"", "null")).value(CoverageType::Line),
        alt(("\"b\"", "\"branch\"")).value(CoverageType::Branch),
        alt(("\"m\"", "\"method\"")).value(CoverageType::Method),
    ))
    .parse_next(buf)
}

/// Enum representing the possible values of the "complexity" field in a
/// `ReportLine` or `LineSession`. This is generally (exclusively?) used for
/// method coverage.
#[derive(Clone)]
pub enum Complexity {
    /// Contains the total cyclomatic complexity of the target.
    Total(u32),

    /// Contains the number of paths covered and the total cyclomatic complexity
    /// of the target.
    PathsTaken(u32, u32),
}

/// Parses value of the "complexity" field in a `ReportLine` or `LineSession`.
///
/// Examples: `1`, `3`, `[0, 1]`, `[2, 2]`
pub fn complexity<S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<Complexity> {
    alt((
        preceded(
            '[',
            terminated(separated_pair(parse_u32, (ws, ',', ws), parse_u32), ']'),
        )
        .map(move |(covered, total)| Complexity::PathsTaken(covered, total)),
        parse_u32.map(Complexity::Total),
    ))
    .parse_next(buf)
}

pub struct LineSession {
    session_id: u32,
    coverage: PyreportCoverage,
    branches: Option<Option<u32>>,
    partials: Option<Option<u32>>,
    complexity: Option<Option<Complexity>>,
}

/// TODO what the hell is this
/// shared and i think sentry have, like, ["24", "27"]
/// a lot of things have, like, ["0:jump", "1", "2", "3", "4"]
pub fn missing_branch<S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<&str> {
    Ok("")
}

/// TODO this appears to be, like, [1, 2] or [3, 4]?
/// the values are always line columns
/// - generally [[sc, ec, cov]]
/// - if el == sl:
///     - partials=[[sc, ec, cov]]
///     - jetbrains, elm do this
/// - if el > sl:
///     - elm does this
///     - every line between gets an entry with no partials + inherited coverage
///     - start line has partials=[[None, ec, cov]] (why ec??? it’s on a
///       different line. insane)
pub fn span<S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<(u32, u32)> {
    Ok((0, 0))
}

/// Parses a `LineSession`. A `ReportLine` has a `LineSession` for each session
/// in the report containing coverage information from that session. Each
/// `LineSession` from a pyreport corresponds to a row in our sqlite report.
/// TODO fact check
///
/// A `LineSession` is a list with 2-5 fields in this order:
/// - `session_id`: the session's ID. This ID corresponds to the keys in the
///   "sessions" dict in [`crate::parsers::pyreport_shim::parse_session`].
/// - `coverage`: whether the target was missed, partially covered, or
///   completely covered
/// - `branches`: the number of missing branches TODO
/// - `partials`: TODO
/// - `complexity`: the target's cyclomatic complexity, and sometimes the number
///   of paths covered
///
/// Trailing null fields may be omitted.
pub fn line_session<S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<LineSession> {
    // TODO do i want to even return a linesession struct or just immediately insert
    seq! {LineSession {
        _: '[',
        session_id: parse_u32,
        _: (ws, ',', ws),
        coverage: coverage,
        _: opt((ws, ',', ws)),
        branches: opt(nullable(parse_u32)), // TODO this is wrong, apparently branches can be
                                            // `["0:jump", 1] or `["0:jump"]` lol. what
        _: opt((ws, ',', ws)),
        partials: opt(nullable(parse_u32)),
        _: opt((ws, ',', ws)),
        complexity: opt(nullable(complexity)),
        _: ']',
    }}
    .parse_next(buf)
    /* // original way to parse
    let _ = preceded(
        '[',
        terminated(
            (
                parse_u32,                 // session_id
                coverage,                  // coverage
                opt(nullable(parse_u32)),  // branches or null
                opt(nullable(parse_u32)),  // partials or null
                opt(nullable(complexity)), // complexity or null
            ),
            ']',
        ),
    )
    .parse_next(buf);

    Ok(())
    */
}

/*

/// Parses a ReportLine https://github.com/codecov/shared/blob/e97a9f422a6e224b315d6dc3821f9f5ebe9b2ddd/shared/reports/types.py#L158-L166
/// TODO may need to override coverage type
/// [
///     coverage: usually int for how many hits, sometimes a fraction probably when branch coverage
///     type: CoverageType, https://github.com/codecov/worker/blob/04f390fcaa30908295b7e18da23eba0594982aa1/services/report/report_builder.py#L15
///     sessions: list[LineSession],
///     messages: ?, might be dead code, a parser called "v1.py" references it but doesn't use
///     complexity: Union[int, tuple(int, int)],
///     datapoints: list[CoverageDatapoint],
/// ]
///
/// example: [6, null, [[0, 6]]]
/// [
///     6,
///     null,
///     [
///         [
///             0,
///             6
///         ]
///     ]
/// ]
pub fn line<S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<()> {


    Ok(())
}

pub fn chunk<S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<()> {
    separated(0.., line, '\n').parse_next(buf)
}

pub fn parse_chunks<S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<()> {
    let parse_header = opt(terminated(json_value, "<<<<< end_of_header >>>>>\n"));
    let parse_chunks = separated(0.., chunk, "<<<<< end_of_chunk >>>>>\n")

    (parse_header, parse_chunks).parse_next(buf)
}



*/
