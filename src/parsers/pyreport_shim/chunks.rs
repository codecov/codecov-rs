use std::{collections::HashMap, fmt, fmt::Debug, marker::PhantomData};

use winnow::{
    combinator::{alt, opt, preceded, separated, separated_pair, seq, terminated},
    error::{ContextError, ErrMode},
    stream::Stream,
    PResult, Parser, Stateful,
};

use crate::{
    parsers::{
        json::{json_value, parse_object, parse_str, JsonVal},
        nullable, parse_u32, ws, Report, ReportBuilder, ReportBuilderCtx, StrStream,
    },
    report::models::{Context, ContextType},
};

#[derive(PartialEq, Debug)]
struct ChunkCtx {
    index: u32,
    current_line: u32,
}

#[derive(PartialEq)]
pub struct ParseCtx<R: Report, B: ReportBuilder<R>> {
    db: ReportBuilderCtx<R, B>,
    labels_index: HashMap<String, i32>,
    chunk: ChunkCtx,
    report_json_files: HashMap<usize, i32>,
    report_json_sessions: HashMap<usize, i32>,
}

pub type ReportOutputStream<S, R, B> = Stateful<S, ParseCtx<R, B>>;

impl<R: Report, B: ReportBuilder<R>> ParseCtx<R, B> {
    pub fn new(
        report_builder: B,
        report_json_files: HashMap<usize, i32>,
        report_json_sessions: HashMap<usize, i32>,
    ) -> ParseCtx<R, B> {
        ParseCtx {
            labels_index: HashMap::new(),
            db: ReportBuilderCtx {
                report_builder,
                _phantom: PhantomData,
            },
            chunk: ChunkCtx {
                index: 0,
                current_line: 0,
            },
            report_json_files,
            report_json_sessions,
        }
    }
}

impl<R: Report, B: ReportBuilder<R>> Debug for ParseCtx<R, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParseCtx")
            .field("db", &self.db)
            .field("labels_index", &self.labels_index)
            .field("chunk", &self.chunk)
            .finish()
    }
}

const FILE_HEADER_TERMINATOR: &str = "<<<<< end_of_header >>>>>";
const END_OF_CHUNK: &str = "<<<<< end_of_chunk >>>>>";

/// Enum representing the possible values of the "coverage" field in a
/// ReportLine or LineSession object.
///
/// Most of the time, we can parse this field into a `HitCount` or
/// `BranchesTaken`.
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

/// Enum representing the possible shapes of data about missing branch coverage.
#[derive(Clone)]
pub enum MissingBranch {
    /// Identifies a specific branch by its "block" and "branch" numbers chosen
    /// by the instrumentation. Lcov does it this way.
    BlockAndBranch(u32, u32),

    /// Identifies a specific branch as one of a set of conditions tied to a
    /// line.
    Condition(u32, Option<String>),

    /// Identifies a specific branch as a line number the branch is located at.
    Line(u32),
}

/// Attempts to parse the values in the "branches" field of a `LineSession`
/// which is a list of missing branches.
///
/// There are myriad ways different coverage formats have represented branch
/// coverage data and they each show up in chunks files in their own quirky way.
///
/// - `["0:0", "0:1", "1:0", "1:1"]` is an example of
///   [`MissingBranch::BlockAndBranch`] coverage. This is how the chunks file
///   represents Lcov `BRDA` branch records.
/// - `["0:jump", "1", "2", "3"]` is an example of [`MissingBranch::Condition`]
///   coverage. This is how Cobertura does it sometimes?
/// - `["26", "27"]` is an example of [`MissingBranch::Line`] coverage. This is
///   how Cobertura does it when generated by coverage.py.
///
/// We lack a way to convert between formats so we are unable to normalize this
/// data.
///
/// [There may yet be more ways this shows
/// up](https://github.com/codecov/worker/blob/07405e0ae925f00aa7bb3e2d828537010901154b/services/report/languages/cobertura.py#L112-L114).
/// We'll try our best, and that'll have to do.
pub fn missing_branches<'a, S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<Vec<MissingBranch>>
where
    S: Stream<Slice = &'a str>,
{
    let block_and_branch = separated_pair(parse_u32, ':', parse_u32);
    let block_and_branch = preceded('"', terminated(block_and_branch, '"'));
    let block_and_branch =
        block_and_branch.map(move |(block, branch)| MissingBranch::BlockAndBranch(block, branch));

    let condition_type = opt(preceded(':', "jump"));

    let condition = (parse_u32, condition_type);
    let condition = preceded('"', terminated(condition, '"'));
    let condition = condition.map(move |(cond, cond_type)| {
        MissingBranch::Condition(cond, cond_type.map(move |s: &str| s.to_string()))
    });

    let line = preceded('"', terminated(parse_u32, '"')).map(MissingBranch::Line);

    preceded(
        '[',
        terminated(
            alt((
                separated(0.., block_and_branch, (ws, ',', ws)),
                separated(0.., condition, (ws, ',', ws)),
                separated(0.., line, (ws, ',', ws)),
            )),
            ']',
        ),
    )
    .parse_next(buf)
}

/// Parses values in the "partials" field of a `LineSession`. These values don't
/// necessarily have to do with partial branch coverage; what they describe is
/// the coverage status of different subspans of a single line.
///
/// Examples:
/// - `[null, 10, 0]`: This line was not covered from its start until column 10
/// - `[11, 30, 1]`: This line was covered from column 11 to column 30
/// - `[31, 40, 0]`: This line was not covered from column 31 to column 40
/// - `[41, null, 1]`: This line was covered from column 41 until its end
///
/// Not all subspans of a line will necessarily be covered.
///
/// TODO write about the formats that only record when start_line and end_line
/// match
pub fn partial_spans<S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<Vec<(u32, u32, PyreportCoverage)>> {
    let span = separated_pair(parse_u32, (ws, ',', ws), parse_u32);
    let span_with_coverage = separated_pair(span, (ws, ',', ws), coverage)
        .map(move |((start, end), coverage)| (start, end, coverage));
    let span_with_coverage = preceded('[', terminated(span_with_coverage, ']'));

    preceded(
        '[',
        terminated(separated(0.., span_with_coverage, (ws, ',', ws)), ']'),
    )
    .parse_next(buf)
}

pub struct LineSession {
    session_id: u32,
    coverage: PyreportCoverage,
    branches: Option<Option<Vec<MissingBranch>>>,
    partials: Option<Option<Vec<(u32, u32, PyreportCoverage)>>>,
    complexity: Option<Option<Complexity>>,
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
pub fn line_session<'a, S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<LineSession>
where
    S: Stream<Slice = &'a str>,
{
    seq! {LineSession {
        _: '[',
        session_id: parse_u32,
        _: (ws, ',', ws),
        coverage: coverage,
        _: opt((ws, ',', ws)),
        branches: opt(nullable(missing_branches)),
        _: opt((ws, ',', ws)),
        partials: opt(nullable(partial_spans)),
        _: opt((ws, ',', ws)),
        complexity: opt(nullable(complexity)),
        _: ']',
    }}
    .parse_next(buf)
}

/// No idea what this field contains
pub fn messages<'a, S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<JsonVal>
where
    S: Stream<Slice = &'a str>,
{
    json_value.parse_next(buf)
}

/// Enum representing a label which is applicable for a particular measurement.
/// An example of a label is a test case that was running when the measurement
/// was taken.
///
/// Newer reports store IDs which can later be mapped back to an original string
/// label with an index found in the chunks file header. Older reports stored
/// many copies of the full strings.
#[derive(Clone)]
pub enum RawLabel {
    LabelId(u32),
    LabelName(String),
}

/// Parses an individual [`RawLabel`] in a [`CoverageDatapoint`].
///
/// Examples:
/// - `"Th2dMtk4M_codecov"`
/// - `"tests/unit/test_analytics_tracking.py::test_get_tools_manager"`
/// - `1`
/// - `5`
pub fn label<S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<String> {
    let raw_label = alt((
        parse_u32.map(RawLabel::LabelId),
        parse_str.map(RawLabel::LabelName),
    ))
    .parse_next(buf)?;

    let labels_index_key = match raw_label {
        RawLabel::LabelId(id) => id.to_string(),
        RawLabel::LabelName(name) => name,
    };

    match buf.state.labels_index.get(&labels_index_key) {
        Some(_) => Ok(labels_index_key),
        None => {
            let context = Context {
                id: None,
                context_type: ContextType::TestCase,
                name: labels_index_key.clone(),
            };
            // TODO handle error
            let context = buf.state.db.report_builder.insert_context(context).unwrap();
            buf.state
                .labels_index
                .insert(context.name, context.id.unwrap());
            Ok(labels_index_key)
        }
    }
}

#[derive(Clone)]
pub struct CoverageDatapoint {
    session_id: u32,
    coverage: PyreportCoverage,
    coverage_type: Option<CoverageType>,
    labels: Vec<String>,
}

pub fn coverage_datapoint<S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<CoverageDatapoint> {
    seq! {CoverageDatapoint {
        _: '[',
        session_id: parse_u32,
        _: (ws, ',', ws),
        coverage: coverage,
        _: (ws, ',', ws),
        coverage_type: nullable(coverage_type),
        _: (ws, ',', ws),
        labels: preceded('[', terminated(separated(0.., label, (ws, ',', ws)), ']')),
        _: ']',
    }}
    .parse_next(buf)
}

pub struct ReportLine {
    coverage: PyreportCoverage,
    coverage_type: CoverageType,
    sessions: Vec<LineSession>,
    messages: Option<Option<JsonVal>>,
    complexity: Option<Option<Complexity>>,
    datapoints: Option<Option<Vec<CoverageDatapoint>>>,
}

/// Account for quirks and malformed data that have been root-caused.
fn normalize_report_line(report_line: &mut ReportLine) {
    match (&report_line.coverage, &report_line.coverage_type) {
        // Clojure uses `true` to indicate partial coverage without giving any specific information
        // about the missed/covered branches. Our parsers for other formats just make up "1/2" and
        // move on, so we do that here as well.
        (PyreportCoverage::Partial(), _) => {
            report_line.coverage = PyreportCoverage::BranchesTaken(1, 2);
        }

        // For method coverage, Jacoco contains aggregated instruction coverage, branch coverage,
        // and cyclomatic complexity metrics. If the branch coverage fields are present and
        // non-zero, our parser will prefer them even though method coverage should be an int.
        // We normalize by treating the numerator of the branch coverage as a hit count, so "0/2"
        // is a miss but "3/4" is 3 hits. Not great, but the data is bugged to begin with.
        (PyreportCoverage::BranchesTaken(covered, _), CoverageType::Method) => {
            report_line.coverage = PyreportCoverage::HitCount(*covered);
        }

        // Our Go parser does not properly fill out the `coverage_type` field. If the `coverage`
        // field has branch data in it, override the coverage type.
        (PyreportCoverage::BranchesTaken(_, _), CoverageType::Line) => {
            report_line.coverage_type = CoverageType::Branch;
        }

        // We see some instances of Scale Scoverage reports being incorrectly translated into
        // Cobertura reports. In Scoverage, 0 for branch coverage means miss, 1 means partial, and
        // 2 means hit. It seems when converting to Cobertura, the value is taken as a raw hit
        // count and not coverted to `branch-rate` or something, and our Cobertura parser doesn't
        // handle it. So, we handle it here.
        (PyreportCoverage::HitCount(n), CoverageType::Branch) => {
            assert!(*n == 0 || *n == 1 || *n == 2); // TODO soften assert
            report_line.coverage = PyreportCoverage::BranchesTaken(*n, 2);
        }
        (_, _) => {}
    }
}

/// TODO override the coverage type
/// This is probably where we cut and insert
pub fn report_line<'a, S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<ReportLine>
where
    S: Stream<Slice = &'a str>,
{
    buf.state.chunk.current_line += 1;
    let mut report_line = seq! {ReportLine {
        _: '[',
        coverage: coverage,
        _: (ws, ',', ws),
        coverage_type: coverage_type,
        _: (ws, ',', ws),
        sessions: preceded('[', terminated(separated(0.., line_session, (ws, ',', ws)), ']')),
        _: (ws, ',', ws),
        messages: opt(nullable(messages)),
        _: (ws, ',', ws),
        complexity: opt(nullable(complexity)),
        _: (ws, ',', ws),
        datapoints: opt(nullable(preceded('[', terminated(separated(0.., coverage_datapoint, (ws, ',', ws)), ']')))),
        _: ']',
    }}
    .parse_next(buf)?;

    normalize_report_line(&mut report_line);
    Ok(report_line)
}

/// TODO verify keys are what we expect
pub fn chunk_header<S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<HashMap<String, JsonVal>> {
    parse_object.parse_next(buf)
}

pub fn chunk<'a, S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<()>
where
    S: Stream<Slice = &'a str>,
{
    buf.state.chunk.index += 1; // nyeh nyeh nyeh
    buf.state.chunk.current_line = 0;
    let _: PResult<Vec<_>> = preceded(
        opt((chunk_header, '\n')),
        separated(0.., opt(report_line), '\n'),
    )
    .parse_next(buf);
    Ok(())
}

/// TODO verify keys are what we expect
pub fn file_header<S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<HashMap<String, JsonVal>> {
    terminated(parse_object, (FILE_HEADER_TERMINATOR, '\n')).parse_next(buf)
}

pub fn chunks_file<'a, S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<()>
where
    S: Stream<Slice = &'a str>,
{
    // If there is a labels_index in the file header, its key will be a stringified
    // int that serves as an identifier within the chunks file and the value
    // will be the label name. We will insert all of the label names into the
    // database and populate this hashmap with the stringified int and the new
    // database primary key.
    //
    // If there is not a labels_index in the file header, or it is empty, this means
    // the chunks file either has no labels or refers to them by their full name
    // everywhere. We will pass this empty hashmap into the parsers and if they
    // find a label which is not in the map, they will insert it into the
    // database and then put the (name, db_pk) pair into the hashmap.
    //
    // Either way, the key is a string. It's a little gross but it will work.

    let mut file_header = opt(file_header).parse_next(buf)?;
    if let Some(file_header) = file_header.as_mut() {
        match file_header.remove("labels_index") {
            Some(JsonVal::Object(value)) => {
                for (index, name) in value.into_iter() {
                    let JsonVal::Str(name) = name else {
                        return Err(ErrMode::Cut(ContextError::new()));
                    };
                    let context = Context {
                        id: None,
                        context_type: ContextType::TestCase,
                        name,
                    };
                    // TODO handle error
                    let context = buf.state.db.report_builder.insert_context(context).unwrap();
                    buf.state.labels_index.insert(index, context.id.unwrap());
                }
            }
            Some(JsonVal::Null) | None => {}
            Some(_) => {
                return Err(ErrMode::Cut(ContextError::new()));
            }
        }
    }

    let _: Vec<_> = separated(0.., chunk, (END_OF_CHUNK, '\n')).parse_next(buf)?;

    Ok(())
}
