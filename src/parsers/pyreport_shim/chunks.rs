mod utils;

use std::{collections::HashMap, fmt, fmt::Debug, marker::PhantomData};

use winnow::{
    combinator::{
        alt, delimited, eof, opt, peek, preceded, separated, separated_pair, seq, terminated,
    },
    error::{ContextError, ErrMode, ErrorKind, FromExternalError},
    stream::Stream,
    PResult, Parser, Stateful,
};

use crate::{
    parsers::{
        json::{json_value, parse_object, parse_str, JsonMap, JsonVal},
        nullable, parse_u32, ws, Report, ReportBuilder, ReportBuilderCtx, StrStream,
    },
    report::models::{ContextType, CoverageType},
};

#[derive(PartialEq, Debug)]
struct ChunkCtx {
    /// The index of this chunk in the overall sequence of chunks tells us which
    /// [`crate::report::models::SourceFile`] this chunk corresponds to.
    index: usize,

    /// Each line in a chunk corresponds to a line in the source file.
    current_line: i64,
}

/// Context needed to parse a chunks file.
#[derive(PartialEq)]
pub struct ParseCtx<R: Report, B: ReportBuilder<R>> {
    /// Rather than returning parsed results, we write them to this
    /// `report_builder`.
    pub db: ReportBuilderCtx<R, B>,

    /// Tracks the labels that we've already added to the report. The key is the
    /// identifier for the label inside the chunks file and the value is the
    /// ID of the [`crate::report::models::Context`] we created for it in
    /// the output. If a `"labels_index"` key is present in the chunks file
    /// header, this is populated all at once and the key is a numeric ID.
    /// Otherwise, this is populated as new labels are encountered and the key
    /// is the full name of the label.
    labels_index: HashMap<String, i64>,

    /// Context within the current chunk.
    chunk: ChunkCtx,

    /// The output of the report JSON parser includes a map from `chunk_index`
    /// to the ID of the [`crate::report::models::SourceFile`] that the
    /// chunk corresponds to.
    report_json_files: HashMap<usize, i64>,

    /// The output of the report JSON parser includes a map from `session_id` to
    /// the ID of the [`crate::report::models::Context`] that the session
    /// corresponds to.
    report_json_sessions: HashMap<usize, i64>,
}

pub type ReportOutputStream<S, R, B> = Stateful<S, ParseCtx<R, B>>;

impl<R: Report, B: ReportBuilder<R>> ParseCtx<R, B> {
    pub fn new(
        report_builder: B,
        report_json_files: HashMap<usize, i64>,
        report_json_sessions: HashMap<usize, i64>,
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

const FILE_HEADER_TERMINATOR: &str = "\n<<<<< end_of_header >>>>>\n";
const END_OF_CHUNK: &str = "\n<<<<< end_of_chunk >>>>>\n";

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

/// Parses the possible values of the "coverage" field in a [`ReportLine`] or
/// [`LineSession`]. See [`PyreportCoverage`]. Most of the time, this field can
/// be parsed into a `HitCount` or `BranchesTaken`.
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
        delimited('"', separated_pair(parse_u32, '/', parse_u32), '"')
            .map(move |(covered, total)| PyreportCoverage::BranchesTaken { covered, total }),
        // Examples: 0, 40
        parse_u32.map(PyreportCoverage::HitCount),
    ))
    .parse_next(buf)
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

/// Parses value of the "complexity" field in a `ReportLine` or `LineSession`.
///
/// Examples: `1`, `3`, `[0, 1]`, `[2, 2]`
pub fn complexity<S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<Complexity> {
    alt((
        delimited(
            ('[', ws),
            separated_pair(parse_u32, (ws, ',', ws), parse_u32),
            (ws, ']'),
        )
        .map(move |(covered, total)| Complexity::PathsTaken { covered, total }),
        parse_u32.map(Complexity::Total),
    ))
    .parse_next(buf)
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

/// Attempts to parse the values in the "branches" field of a [`LineSession`]
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
    let block_and_branch = delimited('"', block_and_branch, '"');
    let block_and_branch =
        block_and_branch.map(move |(block, branch)| MissingBranch::BlockAndBranch(block, branch));

    let condition_type = opt(preceded(':', "jump"));

    let condition = (parse_u32, condition_type);
    let condition = delimited('"', condition, '"');
    let condition = condition.map(move |(cond, cond_type)| {
        MissingBranch::Condition(cond, cond_type.map(move |s: &str| s.to_string()))
    });

    let line = delimited('"', parse_u32, '"').map(MissingBranch::Line);

    delimited(
        ('[', ws),
        alt((
            // Match 1 or more in the first two cases. If we matched 0 or more, the first case
            // would technically always succeed and never try later ones.
            separated(1.., line, (ws, ',', ws)),
            separated(1.., block_and_branch, (ws, ',', ws)),
            // Match 0 or more in the last case to allow for an empty list.
            separated(0.., condition, (ws, ',', ws)),
        )),
        (ws, ']'),
    )
    .parse_next(buf)
}

#[derive(Debug, Clone, PartialEq)]
pub struct Partial {
    start_col: Option<u32>,
    end_col: Option<u32>,
    coverage: PyreportCoverage,
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
/// Some coverage formats report coverage "spans" or "locs" which can be spread
/// across multiple lines. Our parsers generally only record spans that start
/// and end on the same line in the chunks file, or we split a single span into
/// two: one for the start line and one for the end line. The fact that lines
/// between are part of the span is lost.
pub fn partial_spans<S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<Vec<Partial>> {
    let span = separated_pair(nullable(parse_u32), (ws, ',', ws), nullable(parse_u32));
    let span_with_coverage = separated_pair(span, (ws, ',', ws), coverage).map(
        move |((start_col, end_col), coverage)| Partial {
            start_col,
            end_col,
            coverage,
        },
    );
    let span_with_coverage = delimited('[', span_with_coverage, ']');

    delimited('[', separated(0.., span_with_coverage, (ws, ',', ws)), ']').parse_next(buf)
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
    session_id: usize,

    /// The coverage measurement that was taken in this session. The
    /// `CoverageType` is "inherited" from the [`ReportLine`] that this
    /// `LineSession` is a part of.
    coverage: PyreportCoverage,

    /// A list of specific branches/conditions stemming from this line that were
    /// not covered if this line is a branch. May be omitted, or may be null.
    branches: Option<Option<Vec<MissingBranch>>>,

    /// A list of "line partials" which indicate different coverage values for
    /// different subspans of this line. May be omitted, or may be null.
    partials: Option<Option<Vec<Partial>>>,

    /// A measure of the cyclomatic complexity of the method declared on this
    /// line if this line is a method. May be omitted, or may be null.
    complexity: Option<Option<Complexity>>,
}

/// Parses a [`LineSession`]. Each [`LineSession`] corresponds to a
/// [`crate::report::models::CoverageSample`] in the output report.
///
/// A [`ReportLine`] has a [`LineSession`] for each upload ("session") sent to
/// us for a commit. The [`LineSession`] contains the coverage measurements for
/// that session.
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
        session_id: parse_u32.map(|n| n as usize),
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

/// No idea what this field contains. Guessing it's JSON so if we ever encounter
/// it we can at least consume it off the stream and continue parsing.
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

/// Parses an individual [`RawLabel`] in a [`CoverageDatapoint`].
///
/// Examples:
/// - `"Th2dMtk4M_codecov"`
/// - `"tests/unit/test_analytics_tracking.py::test_get_tools_manager"`
/// - `1`
/// - `5`
///
/// If the label is already in `buf.state.labels_index`, return it as a string.
/// If it's not, insert it into the database, insert a mapping from the label to
/// the DB PK, and then return it as a string.
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
            let context = buf
                .state
                .db
                .report_builder
                .insert_context(ContextType::TestCase, &labels_index_key)
                .map_err(|e| ErrMode::from_external_error(buf, ErrorKind::Fail, e))?;
            buf.state.labels_index.insert(context.name, context.id);
            Ok(labels_index_key)
        }
    }
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
    session_id: u32,

    /// A redundant copy of the coverage measurement for a session. We prefer
    /// the value from the [`LineSession`].
    _coverage: PyreportCoverage,

    /// A redundant copy of the `CoverageType`. We use the value from the
    /// [`ReportLine`].
    ///
    /// Technically this field is optional, but the way we serialize it when
    /// it's missing is identical to the way we serialize
    /// [`crate::report::models::CoverageType::Line`] so there's
    /// no way to tell which it is when deserializing.
    _coverage_type: Option<CoverageType>,

    /// A list of labels (e.g. test cases) that apply to this datapoint.
    labels: Vec<String>,
}

/// Parses the (largely redundant) [`CoverageDatapoint`]. Most of its fields are
/// also found on [`ReportLine`] or [`LineSession`], except for the `labels`
/// field.
///
/// Technically `_coverage_type` is optional, but the way it gets serialized
/// when it's missing is identical to the way we serialize
/// [`crate::report::models::CoverageType::Line`] so there's no way to tell
/// which it is when deserializing.
pub fn coverage_datapoint<S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<(u32, CoverageDatapoint)> {
    let datapoint = seq! {CoverageDatapoint {
        _: '[',
        session_id: parse_u32,
        _: (ws, ',', ws),
        _coverage: coverage,
        _: (ws, ',', ws),
        _coverage_type: nullable(coverage_type),
        _: (ws, ',', ws),
        labels: delimited('[', separated(0.., label, (ws, ',', ws)), ']'),
        _: ']',
    }}
    .parse_next(buf)?;
    Ok((datapoint.session_id, datapoint))
}

#[derive(Debug, PartialEq)]
pub struct ReportLine {
    coverage: PyreportCoverage,
    coverage_type: CoverageType,
    sessions: Vec<LineSession>,
    _messages: Option<Option<JsonVal>>,
    _complexity: Option<Option<Complexity>>,
    datapoints: Option<Option<HashMap<u32, CoverageDatapoint>>>,
}

/// Account for some quirks and malformed data. See code comments for details.
fn normalize_coverage_measurement(
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

/// Parses a [`ReportLine`]. A [`ReportLine`] itself does not correspond to
/// anything in the output, but it's an umbrella that includes all of the data
/// tied to a line/[`CoverageSample`].
///
/// This parser performs all the writes it can to the output
/// stream and only returns a `ReportLine` for tests. The `report_line_or_empty`
/// parser which wraps this and supports empty lines returns `Ok(())`.
pub fn report_line<'a, S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<ReportLine>
where
    S: Stream<Slice = &'a str>,
{
    let mut report_line = seq! {ReportLine {
        _: '[',
        coverage: coverage,
        _: (ws, ',', ws),
        coverage_type: coverage_type,
        _: (ws, ',', ws),
        sessions: delimited('[', separated(0.., line_session, (ws, ',', ws)), ']'),
//        _: (ws, ',', ws),
        _messages: opt(preceded((ws, ',', ws), nullable(messages))),
//        _: (ws, ',', ws),
        _complexity: opt(preceded((ws, ',', ws), nullable(complexity))),
//        _: (ws, ',', ws),
        datapoints: opt(preceded((ws, ',', ws), nullable(delimited('[', separated(0.., coverage_datapoint, (ws, ',', ws)), ']')))),
        _: ']',
    }}
    .parse_next(buf)?;

    // Fix issues like recording branch coverage with `CoverageType::Method`
    let (correct_coverage, correct_type) =
        normalize_coverage_measurement(&report_line.coverage, &report_line.coverage_type);
    report_line.coverage = correct_coverage;
    report_line.coverage_type = correct_type;

    // Fix the `coverage` values in each `LineSession` as well
    for line_session in report_line.sessions.iter_mut() {
        let (correct_coverage, _) =
            normalize_coverage_measurement(&line_session.coverage, &report_line.coverage_type);
        line_session.coverage = correct_coverage;
    }

    utils::save_report_line(&report_line, &mut buf.state)
        .map_err(|e| ErrMode::from_external_error(buf, ErrorKind::Fail, e))?;
    Ok(report_line)
}

/// Parses each line in a chunk. A line may be empty, or it may contain a
/// [`ReportLine`]. Either way, we need to update the `current_line` value in
/// our parser context.
///
/// The `report_line` parser writes all the data it can to the output
/// stream so we don't actually need to return anything to our caller.
pub fn report_line_or_empty<'a, S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<()>
where
    S: Stream<Slice = &'a str>,
{
    buf.state.chunk.current_line += 1;

    // A line is empty if the next character is `\n` or EOF. The `END_OF_CHUNK`
    // marker includes a leading `\n`, so if the next character is `\n` we have
    // to make sure it isn't part of `END_OF_CHUNK`.

    // A line is empty if the next character is `\n` or EOF. We don't consume that
    // next character from the stream though - we leave it there as either the
    // delimeter between lines or part of `END_OF_CHUNK`.
    let empty_line = peek(alt((eof, "\n"))).value(());
    let populated_line = report_line.value(());
    alt((populated_line, empty_line)).parse_next(buf)
}

/// Each chunk may begin with a JSON object containing:
/// - "present_sessions": a list of sessions referenced
///
/// TODO: Verify that all keys are known.
pub fn chunk_header<S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<JsonMap<String, JsonVal>> {
    terminated(parse_object, '\n').parse_next(buf)
}

/// Parses a "chunk". A chunk contains all of the line-by-line measurements for
/// a file. The Nth chunk corresponds to the file whose entry in
/// `buf.state.report_json_files` has N in its `chunks_index` field.
///
/// Each new chunk will reset `buf.state.chunk.current_line` to 0 when it starts
/// and increment `buf.state.chunk.index` when it ends so that the next chunk
/// can associate its data with the correct file.
pub fn chunk<'a, S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<()>
where
    S: Stream<Slice = &'a str>,
{
    // New chunk, start back at line 0.
    buf.state.chunk.current_line = 0;

    let _: Vec<_> =
        preceded(chunk_header, separated(1.., report_line_or_empty, '\n')).parse_next(buf)?;

    // Advance our chunk index so we can associate the data from the next chunk with
    // the correct file from the report JSON.
    buf.state.chunk.index += 1;

    Ok(())
}

/// Chunks files sometimes begin with a JSON object followed by a terminator
/// string. The JSON object contains:
/// - `"labels_index"`: assigns a numeric ID to each label to save space
///
/// If the `"labels_index"` key is present, this parser will insert each label
/// into the report as a [`crate::report::models::Context`] and create a mapping
/// in `buf.state.labels_index` from numeric ID in the header to the
/// new `Context`'s ID in the output report. If the `"labels_index"` key is
/// _not_ present, we will populate `buf.state.labels_index` gradually as we
/// encounter new labels during parsing.
pub fn chunks_file_header<S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<()> {
    let header = terminated(parse_object, FILE_HEADER_TERMINATOR).parse_next(buf)?;

    let labels_iter = header
        .get("labels_index")
        .and_then(JsonVal::as_object)
        .into_iter()
        .flatten();
    for (index, name) in labels_iter {
        let Some(name) = name.as_str() else {
            return Err(ErrMode::Cut(ContextError::new()));
        };
        let context = buf
            .state
            .db
            .report_builder
            .insert_context(ContextType::TestCase, name)
            .map_err(|e| ErrMode::from_external_error(buf, ErrorKind::Fail, e))?;
        buf.state.labels_index.insert(index.clone(), context.id);
    }

    Ok(())
}

/// Parses a chunks file. A chunks file contains an optional header and a series
/// of 1 or more "chunks" separated by an `END_OF_CHUNK` terminator.
pub fn parse_chunks_file<'a, S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<()>
where
    S: Stream<Slice = &'a str>,
{
    let _: Vec<_> =
        preceded(opt(chunks_file_header), separated(1.., chunk, END_OF_CHUNK)).parse_next(buf)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use mockall::predicate::*;

    use super::*;
    use crate::report::{models::*, MockReport, MockReportBuilder};

    type TestStream<'a> = ReportOutputStream<&'a str, MockReport, MockReportBuilder<MockReport>>;

    struct Ctx {
        parse_ctx: ParseCtx<MockReport, MockReportBuilder<MockReport>>,
    }

    fn setup() -> Ctx {
        let report_builder = MockReportBuilder::new();
        let report_json_files = HashMap::from([(0, 0), (1, 1), (2, 2)]);
        let report_json_sessions = HashMap::from([(0, 0), (1, 1), (2, 2)]);

        let parse_ctx = ParseCtx::new(report_builder, report_json_files, report_json_sessions);

        Ctx { parse_ctx }
    }

    fn stub_report_builder(report_builder: &mut MockReportBuilder<MockReport>) {
        report_builder
            .expect_insert_coverage_sample()
            .returning(|_, _, _, _, _, _| {
                Ok(CoverageSample {
                    ..Default::default()
                })
            });
        report_builder
            .expect_insert_branches_data()
            .returning(|_, _, _, _, _| {
                Ok(BranchesData {
                    ..Default::default()
                })
            });
        report_builder
            .expect_insert_method_data()
            .returning(|_, _, _, _, _, _, _| {
                Ok(MethodData {
                    ..Default::default()
                })
            });
        report_builder
            .expect_insert_span_data()
            .returning(|_, _, _, _, _, _, _| {
                Ok(SpanData {
                    ..Default::default()
                })
            });
        report_builder
            .expect_associate_context()
            .returning(|_, _, _, _, _| {
                Ok(ContextAssoc {
                    ..Default::default()
                })
            });
        report_builder.expect_insert_context().returning(|_, name| {
            Ok(Context {
                name: name.to_string(),
                ..Default::default()
            })
        });
    }

    #[test]
    fn test_pyreport_coverage() {
        let test_ctx = setup();
        let mut buf = TestStream {
            input: "",
            state: test_ctx.parse_ctx,
        };

        let test_cases = [
            ("1", Ok(PyreportCoverage::HitCount(1))),
            ("3", Ok(PyreportCoverage::HitCount(3))),
            ("1.23456e5", Ok(PyreportCoverage::HitCount(123456))),
            // Clamp values to u32 range
            ("99999999999999", Ok(PyreportCoverage::HitCount(u32::MAX))),
            (
                "\"1/2\"",
                Ok(PyreportCoverage::BranchesTaken {
                    covered: 1,
                    total: 2,
                }),
            ),
            (
                "\"4/4\"",
                Ok(PyreportCoverage::BranchesTaken {
                    covered: 4,
                    total: 4,
                }),
            ),
            ("true", Ok(PyreportCoverage::Partial())),
            // Malformed inputs
            ("malformed", Err(ErrMode::Backtrack(ContextError::new()))),
            ("false", Err(ErrMode::Backtrack(ContextError::new()))),
            ("\"true\"", Err(ErrMode::Backtrack(ContextError::new()))),
            ("\"1\"", Err(ErrMode::Backtrack(ContextError::new()))),
            ("\"1/\"", Err(ErrMode::Backtrack(ContextError::new()))),
            ("\"/2\"", Err(ErrMode::Backtrack(ContextError::new()))),
            ("\"1/2", Err(ErrMode::Backtrack(ContextError::new()))),
            // TODO: Make these cases error. Currently this parser accepts any number and
            // clamps/truncates to u32.
            ("3.4", Ok(PyreportCoverage::HitCount(3))),
            ("-3", Ok(PyreportCoverage::HitCount(0))),
            // TODO: Make this case error or clamp to fractions <= 1
            (
                "\"5/4\"",
                Ok(PyreportCoverage::BranchesTaken {
                    covered: 5,
                    total: 4,
                }),
            ),
            // This input is invalid because it's not encapsulated by ""s. Ideally we would
            // error here, but instead we parse this as `HitCount(1)` and rely on
            // the parent to yell when it encounters the `/` instead of a `,` or
            // whatever.
            ("1/2", Ok(PyreportCoverage::HitCount(1))),
        ];

        for test_case in test_cases {
            buf.input = test_case.0;
            assert_eq!(coverage.parse_next(&mut buf), test_case.1);
        }
    }

    #[test]
    fn test_coverage_type() {
        let test_ctx = setup();
        let mut buf = TestStream {
            input: "",
            state: test_ctx.parse_ctx,
        };

        let test_cases = [
            ("null", Ok(CoverageType::Line)),
            ("\"line\"", Ok(CoverageType::Line)),
            ("\"b\"", Ok(CoverageType::Branch)),
            ("\"branch\"", Ok(CoverageType::Branch)),
            ("\"m\"", Ok(CoverageType::Method)),
            ("\"method\"", Ok(CoverageType::Method)),
        ];

        for test_case in test_cases {
            buf.input = test_case.0;
            assert_eq!(coverage_type.parse_next(&mut buf), test_case.1);
        }
    }

    #[test]
    fn test_complexity() {
        let test_ctx = setup();
        let mut buf = TestStream {
            input: "",
            state: test_ctx.parse_ctx,
        };

        let test_cases = [
            ("1", Ok(Complexity::Total(1))),
            ("5", Ok(Complexity::Total(5))),
            ("1.2345e4", Ok(Complexity::Total(12345))),
            ("999999999999999", Ok(Complexity::Total(u32::MAX))),
            (
                "[5, 5]",
                Ok(Complexity::PathsTaken {
                    covered: 5,
                    total: 5,
                }),
            ),
            (
                "[5, 1.2345e4]",
                Ok(Complexity::PathsTaken {
                    covered: 5,
                    total: 12345,
                }),
            ),
            (
                "[   5     ,5 ]", // Ignore whitespace
                Ok(Complexity::PathsTaken {
                    covered: 5,
                    total: 5,
                }),
            ),
            // Malformed inputs
            ("\"1\"", Err(ErrMode::Backtrack(ContextError::new()))),
            ("[1, 5 5]", Err(ErrMode::Backtrack(ContextError::new()))),
            ("[1, 5, 6]", Err(ErrMode::Backtrack(ContextError::new()))),
            ("\"[1, 5]\"", Err(ErrMode::Backtrack(ContextError::new()))),
            (
                "[\"1\", \"5\"]",
                Err(ErrMode::Backtrack(ContextError::new())),
            ),
            ("[1, 5", Err(ErrMode::Backtrack(ContextError::new()))),
            ("[1, ]", Err(ErrMode::Backtrack(ContextError::new()))),
            ("[, 3]", Err(ErrMode::Backtrack(ContextError::new()))),
            ("[1]", Err(ErrMode::Backtrack(ContextError::new()))),
            ("one", Err(ErrMode::Backtrack(ContextError::new()))),
            // TODO: Make these cases error. Currently the parser accepts any number and
            // clamps/truncates to u32 range.
            ("-3", Ok(Complexity::Total(0))),
            ("3.4", Ok(Complexity::Total(3))),
            // TODO: Make this case error or clamp to ratios <= 1.
            (
                "[2, 1]",
                Ok(Complexity::PathsTaken {
                    covered: 2,
                    total: 1,
                }),
            ),
        ];

        for test_case in test_cases {
            buf.input = test_case.0;
            assert_eq!(complexity.parse_next(&mut buf), test_case.1);
        }
    }

    #[test]
    fn test_missing_branches() {
        let test_ctx = setup();
        let mut buf = TestStream {
            input: "",
            state: test_ctx.parse_ctx,
        };

        let test_cases = [
            ("[]", Ok(vec![])),
            (
                "[\"0:jump\"]",
                Ok(vec![MissingBranch::Condition(0, Some("jump".to_string()))]),
            ),
            (
                "[\"0:jump\", \"1\", \"2\"]",
                Ok(vec![
                    MissingBranch::Condition(0, Some("jump".to_string())),
                    MissingBranch::Condition(1, None),
                    MissingBranch::Condition(2, None),
                ]),
            ),
            (
                "[\"26\", \"28\"]",
                Ok(vec![MissingBranch::Line(26), MissingBranch::Line(28)]),
            ),
            (
                "[\"0:0\", \"0:1\", \"1:0\", \"1:1\"]",
                Ok(vec![
                    MissingBranch::BlockAndBranch(0, 0),
                    MissingBranch::BlockAndBranch(0, 1),
                    MissingBranch::BlockAndBranch(1, 0),
                    MissingBranch::BlockAndBranch(1, 1),
                ]),
            ),
            // Malformed inputs
            ("[26, 28]", Err(ErrMode::Backtrack(ContextError::new()))),
            ("[\"26\", 28]", Err(ErrMode::Backtrack(ContextError::new()))),
            ("[0:jump, 28]", Err(ErrMode::Backtrack(ContextError::new()))),
            (
                "\"0:jump\", \"28\"",
                Err(ErrMode::Backtrack(ContextError::new())),
            ),
            (
                "\"[\"26\", \"28\"]\"",
                Err(ErrMode::Backtrack(ContextError::new())),
            ),
            (
                "[\"26\", \"28\"",
                Err(ErrMode::Backtrack(ContextError::new())),
            ),
            (
                // Can't switch types in the middle of a list
                "[\"0:jump\", \"0:1\", \"26\"]",
                Err(ErrMode::Backtrack(ContextError::new())),
            ),
            (
                // Can't switch types in the middle of a list
                "[\"0:1\", \"0:jump\", \"26\"]",
                Err(ErrMode::Backtrack(ContextError::new())),
            ),
            (
                // Can't switch types in the middle of a list
                "[\"26\", \"0:jump\", \"0:1\"]",
                Err(ErrMode::Backtrack(ContextError::new())),
            ),
            (
                // Can't switch types in the middle of a list. Actually expected this to pass
                // because `"26"` is a valid `Condition` value, but it fails
                "[\"26\", \"0:jump\"]",
                Err(ErrMode::Backtrack(ContextError::new())),
            ),
        ];

        for test_case in test_cases {
            buf.input = test_case.0;
            assert_eq!(missing_branches.parse_next(&mut buf), test_case.1);
        }
    }

    #[test]
    fn test_partial_spans() {
        let test_ctx = setup();
        let mut buf = TestStream {
            input: "",
            state: test_ctx.parse_ctx,
        };

        let test_cases = [
            ("[]", Ok(vec![])),
            (
                "[[null, 10, 1]]",
                Ok(vec![Partial {
                    start_col: None,
                    end_col: Some(10),
                    coverage: PyreportCoverage::HitCount(1),
                }]),
            ),
            (
                "[[10, null, 0]]",
                Ok(vec![Partial {
                    start_col: Some(10),
                    end_col: None,
                    coverage: PyreportCoverage::HitCount(0),
                }]),
            ),
            (
                "[[null, 10, 1], [10, null, 0]]",
                Ok(vec![
                    Partial {
                        start_col: None,
                        end_col: Some(10),
                        coverage: PyreportCoverage::HitCount(1),
                    },
                    Partial {
                        start_col: Some(10),
                        end_col: None,
                        coverage: PyreportCoverage::HitCount(0),
                    },
                ]),
            ),
            (
                "[[5, 10, 3]]",
                Ok(vec![Partial {
                    start_col: Some(5),
                    end_col: Some(10),
                    coverage: PyreportCoverage::HitCount(3),
                }]),
            ),
            // Technically supported, but not expected
            (
                "[[null, 10, \"2/2\"]]",
                Ok(vec![Partial {
                    start_col: None,
                    end_col: Some(10),
                    coverage: PyreportCoverage::BranchesTaken {
                        covered: 2,
                        total: 2,
                    },
                }]),
            ),
            (
                "[[null, 10, true]]",
                Ok(vec![Partial {
                    start_col: None,
                    end_col: Some(10),
                    coverage: PyreportCoverage::Partial(),
                }]),
            ),
            // Malformed inputs
            ("[5, 10, 3]", Err(ErrMode::Backtrack(ContextError::new()))),
            ("[[5, 10, 3]", Err(ErrMode::Backtrack(ContextError::new()))),
            ("[5, 10, 3]]", Err(ErrMode::Backtrack(ContextError::new()))),
            (
                "[[\"5\", \"10\", 3]]",
                Err(ErrMode::Backtrack(ContextError::new())),
            ),
            (
                "[[\"5\", \"null\", 3]]",
                Err(ErrMode::Backtrack(ContextError::new())),
            ),
            (
                "[[5, 10, 3, 5]]",
                Err(ErrMode::Backtrack(ContextError::new())),
            ),
            ("[[5, 3]]", Err(ErrMode::Backtrack(ContextError::new()))),
            // TODO: Reject when end_col is smaller than start_col
            (
                "[[5, 3, 5]]",
                Ok(vec![Partial {
                    start_col: Some(5),
                    end_col: Some(3),
                    coverage: PyreportCoverage::HitCount(5),
                }]),
            ),
        ];

        for test_case in test_cases {
            buf.input = test_case.0;
            assert_eq!(partial_spans.parse_next(&mut buf), test_case.1);
        }
    }

    #[test]
    fn test_line_session() {
        let test_ctx = setup();
        let mut buf = TestStream {
            input: "",
            state: test_ctx.parse_ctx,
        };

        let test_cases = [
            (
                "[0, 1]",
                Ok(LineSession {
                    session_id: 0,
                    coverage: PyreportCoverage::HitCount(1),
                    branches: None,
                    partials: None,
                    complexity: None,
                }),
            ),
            (
                "[0, 1, null, null, null]",
                Ok(LineSession {
                    session_id: 0,
                    coverage: PyreportCoverage::HitCount(1),
                    branches: Some(None),
                    partials: Some(None),
                    complexity: Some(None),
                }),
            ),
            (
                "[0, 1, [\"0:jump\"]]",
                Ok(LineSession {
                    session_id: 0,
                    coverage: PyreportCoverage::HitCount(1),
                    branches: Some(Some(vec![MissingBranch::Condition(
                        0,
                        Some("jump".to_string()),
                    )])),
                    partials: None,
                    complexity: None,
                }),
            ),
            (
                "[0, 1, null, [[10, 15, 1]], null]",
                Ok(LineSession {
                    session_id: 0,
                    coverage: PyreportCoverage::HitCount(1),
                    branches: Some(None),
                    partials: Some(Some(vec![Partial {
                        start_col: Some(10),
                        end_col: Some(15),
                        coverage: PyreportCoverage::HitCount(1),
                    }])),
                    complexity: Some(None),
                }),
            ),
            (
                "[0, 1, null, null, 3]",
                Ok(LineSession {
                    session_id: 0,
                    coverage: PyreportCoverage::HitCount(1),
                    branches: Some(None),
                    partials: Some(None),
                    complexity: Some(Some(Complexity::Total(3))),
                }),
            ),
            (
                "[0, 1, null, null, [1, 2]]",
                Ok(LineSession {
                    session_id: 0,
                    coverage: PyreportCoverage::HitCount(1),
                    branches: Some(None),
                    partials: Some(None),
                    complexity: Some(Some(Complexity::PathsTaken {
                        covered: 1,
                        total: 2,
                    })),
                }),
            ),
            // Malformed inputs
            ("[0]", Err(ErrMode::Backtrack(ContextError::new()))),
            ("[0, 1", Err(ErrMode::Backtrack(ContextError::new()))),
            ("0, 1]", Err(ErrMode::Backtrack(ContextError::new()))),
            ("[0, null]", Err(ErrMode::Backtrack(ContextError::new()))),
            ("[null, 1]", Err(ErrMode::Backtrack(ContextError::new()))),
            ("[\"0\", 1]", Err(ErrMode::Backtrack(ContextError::new()))),
            ("[0, \"1\"]", Err(ErrMode::Backtrack(ContextError::new()))),
            (
                "[0, 1, null, null, null, null]",
                Err(ErrMode::Backtrack(ContextError::new())),
            ),
            (
                // TODO: Should fail. `partials` must be preceded by `branches` or `null` but it
                // isn't here.
                "[0, 1, [[10, 15, 1]]]",
                Ok(LineSession {
                    session_id: 0,
                    coverage: PyreportCoverage::HitCount(1),
                    branches: None,
                    partials: Some(Some(vec![Partial {
                        start_col: Some(10),
                        end_col: Some(15),
                        coverage: PyreportCoverage::HitCount(1),
                    }])),
                    complexity: None,
                }),
            ),
        ];

        for test_case in test_cases {
            buf.input = test_case.0;
            assert_eq!(line_session.parse_next(&mut buf), test_case.1);
        }
    }

    #[test]
    fn test_messages() {
        let test_ctx = setup();
        let mut buf = TestStream {
            input: "",
            state: test_ctx.parse_ctx,
        };

        // No idea what `messages` actually is! Guessing it's JSON.
        let test_cases = [
            ("null", Ok(JsonVal::Null)),
            ("{}", Ok(JsonVal::Object(JsonMap::new()))),
        ];

        for test_case in test_cases {
            buf.input = test_case.0;
            assert_eq!(messages.parse_next(&mut buf), test_case.1);
        }
    }

    #[test]
    fn test_label() {
        let test_ctx = setup();
        let mut buf = TestStream {
            input: "",
            state: test_ctx.parse_ctx,
        };

        buf.state.labels_index = HashMap::from([
            ("already_inserted".to_string(), 100),
            ("1".to_string(), 101),
        ]);

        // Parsing a label that is already in `labels_index` should just return it
        buf.input = "\"already_inserted\"";
        buf.state.db.report_builder.expect_insert_context().times(0);
        assert_eq!(
            label.parse_next(&mut buf),
            Ok("already_inserted".to_string())
        );

        // If we parse a number like `1`, we should look for `"1"` in the labels index.
        buf.input = "1";
        buf.state.db.report_builder.expect_insert_context().times(0);
        assert_eq!(label.parse_next(&mut buf), Ok("1".to_string()));

        // Parsing a label that is not already in `labels_index` should insert it
        buf.state
            .db
            .report_builder
            .expect_insert_context()
            .with(eq(ContextType::TestCase), eq("not_already_inserted"))
            .returning(|_, _| {
                Ok(Context {
                    ..Default::default()
                })
            })
            .times(1);
        buf.input = "\"not_already_inserted\"";
        assert_eq!(
            label.parse_next(&mut buf),
            Ok("not_already_inserted".to_string())
        );

        // Malformed labels should never get to inserting
        let malformed_test_cases = [
            // Not wrapped in quotes
            "already_inserted",
            "\"already_inserted",
            "already_inserted\"",
            "[\"already_inserted\"]",
        ];

        for test_case in malformed_test_cases {
            buf.input = test_case;
            assert_eq!(
                label.parse_next(&mut buf),
                Err(ErrMode::Backtrack(ContextError::new()))
            );
        }
    }

    #[test]
    fn test_coverage_datapoint() {
        let test_ctx = setup();
        let mut buf = TestStream {
            input: "",
            state: test_ctx.parse_ctx,
        };

        // See `test_label()` for testing this logic. Stub the report_builder stuff for
        // these tests.
        buf.state
            .db
            .report_builder
            .expect_insert_context()
            .returning(|_, name| {
                Ok(Context {
                    name: name.to_string(),
                    ..Default::default()
                })
            });

        let valid_test_cases = [
            (
                "[1, \"2/2\", \"b\", [\"test_case\"]]",
                Ok((
                    1,
                    CoverageDatapoint {
                        session_id: 1,
                        _coverage: PyreportCoverage::BranchesTaken {
                            covered: 2,
                            total: 2,
                        },
                        _coverage_type: Some(CoverageType::Branch),
                        labels: vec!["test_case".to_string()],
                    },
                )),
            ),
            (
                "[1, 2, null, []]",
                Ok((
                    1,
                    CoverageDatapoint {
                        session_id: 1,
                        _coverage: PyreportCoverage::HitCount(2),
                        _coverage_type: Some(CoverageType::Line),
                        labels: vec![],
                    },
                )),
            ),
            (
                "[3, true, null, [1, 2, 3]]",
                Ok((
                    3,
                    CoverageDatapoint {
                        session_id: 3,
                        _coverage: PyreportCoverage::Partial(),
                        _coverage_type: Some(CoverageType::Line),
                        labels: vec!["1".to_string(), "2".to_string(), "3".to_string()],
                    },
                )),
            ),
        ];

        assert!(buf.state.labels_index.is_empty());
        for test_case in valid_test_cases {
            buf.input = test_case.0;
            assert_eq!(coverage_datapoint.parse_next(&mut buf), test_case.1);
        }
        assert_eq!(buf.state.labels_index.len(), 4);
        assert!(buf.state.labels_index.contains_key("test_case"));
        assert!(buf.state.labels_index.contains_key("1"));
        assert!(buf.state.labels_index.contains_key("2"));
        assert!(buf.state.labels_index.contains_key("3"));

        let invalid_test_cases = [
            ("[]", Err(ErrMode::Backtrack(ContextError::new()))),
            ("[1, 2]", Err(ErrMode::Backtrack(ContextError::new()))),
            (
                "[1, 2, \"b\"]",
                Err(ErrMode::Backtrack(ContextError::new())),
            ),
            (
                "[1, 2, \"b\", []",
                Err(ErrMode::Backtrack(ContextError::new())),
            ),
            ("", Err(ErrMode::Backtrack(ContextError::new()))),
            (
                "[1, 2, null, []",
                Err(ErrMode::Backtrack(ContextError::new())),
            ),
            (
                "1, 2, null, []]",
                Err(ErrMode::Backtrack(ContextError::new())),
            ),
            (
                "[1, 2, null, [test_case, test_case_2]",
                Err(ErrMode::Backtrack(ContextError::new())),
            ),
        ];
        for test_case in invalid_test_cases {
            buf.input = test_case.0;
            assert_eq!(coverage_datapoint.parse_next(&mut buf), test_case.1);
        }
    }

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

    #[test]
    fn test_report_line() {
        let test_ctx = setup();
        let mut buf = TestStream {
            input: "",
            state: test_ctx.parse_ctx,
        };
        buf.state.labels_index.insert("test_case".to_string(), 100);

        // The logic which inserts all the data from a `ReportLine` into
        // `buf.state.report_builder` is tested in
        // `src/parsers/pyreport_shim/chunks/utils.rs`. Stub `report_builder` here.
        stub_report_builder(&mut buf.state.db.report_builder);

        let test_cases = [
            (
                "[1, null, [[0, 1]]]",
                Ok(ReportLine {
                    coverage: PyreportCoverage::HitCount(1),
                    coverage_type: CoverageType::Line,
                    sessions: vec![LineSession {
                        session_id: 0,
                        coverage: PyreportCoverage::HitCount(1),
                        branches: None,
                        partials: None,
                        complexity: None,
                    }],
                    _messages: None,
                    _complexity: None,
                    datapoints: None,
                }),
            ),
            (
                "[1, null, [[0, 1], [1, 1]]]",
                Ok(ReportLine {
                    coverage: PyreportCoverage::HitCount(1),
                    coverage_type: CoverageType::Line,
                    sessions: vec![
                        LineSession {
                            session_id: 0,
                            coverage: PyreportCoverage::HitCount(1),
                            branches: None,
                            partials: None,
                            complexity: None,
                        },
                        LineSession {
                            session_id: 1,
                            coverage: PyreportCoverage::HitCount(1),
                            branches: None,
                            partials: None,
                            complexity: None,
                        },
                    ],
                    _messages: None,
                    _complexity: None,
                    datapoints: None,
                }),
            ),
            (
                "[1, null, [[0, 1]], null, 3]",
                Ok(ReportLine {
                    coverage: PyreportCoverage::HitCount(1),
                    coverage_type: CoverageType::Line,
                    sessions: vec![LineSession {
                        session_id: 0,
                        coverage: PyreportCoverage::HitCount(1),
                        branches: None,
                        partials: None,
                        complexity: None,
                    }],
                    _messages: Some(Some(JsonVal::Null)),
                    _complexity: Some(Some(Complexity::Total(3))),
                    datapoints: None,
                }),
            ),
            (
                "[1, null, [[0, 1]], null, null, []]",
                Ok(ReportLine {
                    coverage: PyreportCoverage::HitCount(1),
                    coverage_type: CoverageType::Line,
                    sessions: vec![LineSession {
                        session_id: 0,
                        coverage: PyreportCoverage::HitCount(1),
                        branches: None,
                        partials: None,
                        complexity: None,
                    }],
                    _messages: Some(Some(JsonVal::Null)),
                    _complexity: Some(None),
                    datapoints: Some(Some(HashMap::new())),
                }),
            ),
            (
                "[1, null, [[0, 1]], null, null, [[0, 1, null, [\"test_case\"]]]]",
                Ok(ReportLine {
                    coverage: PyreportCoverage::HitCount(1),
                    coverage_type: CoverageType::Line,
                    sessions: vec![LineSession {
                        session_id: 0,
                        coverage: PyreportCoverage::HitCount(1),
                        branches: None,
                        partials: None,
                        complexity: None,
                    }],
                    _messages: Some(Some(JsonVal::Null)),
                    _complexity: Some(None),
                    datapoints: Some(Some(HashMap::from([(
                        0,
                        CoverageDatapoint {
                            session_id: 0,
                            _coverage: PyreportCoverage::HitCount(1),
                            _coverage_type: Some(CoverageType::Line),
                            labels: vec!["test_case".to_string()],
                        },
                    )]))),
                }),
            ),
            (
                "[\"2/2\", \"b\", [[0, \"2/2\"]], null, null, [[0, \"2/2\", \"b\", [\"test_case\"]]]]",
                Ok(ReportLine {
                    coverage: PyreportCoverage::BranchesTaken{covered: 2, total: 2},
                    coverage_type: CoverageType::Branch,
                    sessions: vec![LineSession {
                        session_id: 0,
                        coverage: PyreportCoverage::BranchesTaken{covered: 2, total: 2},
                        branches: None,
                        partials: None,
                        complexity: None,
                    }],
                    _messages: Some(Some(JsonVal::Null)),
                    _complexity: Some(None),
                    datapoints: Some(Some(HashMap::from([(
                        0,
                        CoverageDatapoint {
                            session_id: 0,
                            _coverage: PyreportCoverage::BranchesTaken{covered: 2, total: 2},
                            _coverage_type: Some(CoverageType::Branch),
                            labels: vec!["test_case".to_string()],
                        },
                    )]))),
                }),
            ),
            (
                "[1, \"m\", [[0, 1]], null, null, [[0, 1, \"m\", [\"test_case\"]]]]",
                Ok(ReportLine {
                    coverage: PyreportCoverage::HitCount(1),
                    coverage_type: CoverageType::Method,
                    sessions: vec![LineSession {
                        session_id: 0,
                        coverage: PyreportCoverage::HitCount(1),
                        branches: None,
                        partials: None,
                        complexity: None,
                    }],
                    _messages: Some(Some(JsonVal::Null)),
                    _complexity: Some(None),
                    datapoints: Some(Some(HashMap::from([(
                        0,
                        CoverageDatapoint {
                            session_id: 0,
                            _coverage: PyreportCoverage::HitCount(1),
                            _coverage_type: Some(CoverageType::Method),
                            labels: vec!["test_case".to_string()],
                        },
                    )]))),
                }),
            ),
            // Malformed inputs
            (
                // Unquoted coverage type
                "[1, \"m\", [[0, 1]], null, null, [[0, 1, m, [\"test_case\"]]]]",
                Err(ErrMode::Backtrack(ContextError::new())),
            ),
            (
                // Quoted coverage field
                "[\"1\", \"m\", [[0, 1]], null, null, [[0, 1, \"m\", [\"test_case\"]]]]",
                Err(ErrMode::Backtrack(ContextError::new())),
            ),
            (
                // Missing closing brace
                "[1, \"m\", [[0, 1]], null, null, [[0, 1, \"m\", [\"test_case\"]]]",
                Err(ErrMode::Backtrack(ContextError::new())),
            ),
            (
                // Trailing comma
                "[1, \"m\", [[0, 1]], null, null,]",
                Err(ErrMode::Backtrack(ContextError::new())),
            ),
            (
                // Missing `sessions`
                "[1, \"m\"]",
                Err(ErrMode::Backtrack(ContextError::new())),
            ),
        ];

        for test_case in test_cases {
            buf.input = test_case.0;
            assert_eq!(report_line.parse_next(&mut buf), test_case.1);
        }
    }

    #[test]
    fn test_report_line_or_empty() {
        let test_ctx = setup();
        let mut buf = TestStream {
            input: "",
            state: test_ctx.parse_ctx,
        };

        buf.state.labels_index.insert("test_case".to_string(), 100);
        stub_report_builder(&mut buf.state.db.report_builder);

        let valid_test_cases = [
            // Test that empty lines will still advance the `current_line` state
            ("\n", Ok(())),
            ("\n", Ok(())),
            ("\n", Ok(())),
            ("[1, null, [[0, 1]]]", Ok(())),
            ("[1, null, [[0, 1]], null, 3]", Ok(())),
            ("[\"2/2\", \"b\", [[0, \"2/2\"]], null, null, [[0, \"2/2\", \"b\", [\"test_case\"]]]]", Ok(())),
            ("\n", Ok(())),
            // The last line in the entire chunks file ends in EOF, not \n
            ("", Ok(())),
            // `END_OF_CHUNK` begins with a `\n` so we know the current line is empty
            (END_OF_CHUNK, Ok(())),
        ];
        let expected_line_count = valid_test_cases.len();

        assert_eq!(buf.state.chunk.current_line, 0);
        for test_case in valid_test_cases {
            buf.input = test_case.0;
            assert_eq!(report_line_or_empty.parse_next(&mut buf), test_case.1);
        }
        assert_eq!(buf.state.chunk.current_line as usize, expected_line_count);

        buf.state.chunk.current_line = 0;
        let invalid_test_cases = [
            (
                // Quoted coverage field
                "[\"1\", \"m\", [[0, 1]], null, null, [[0, 1, \"m\", [\"test_case\"]]]]",
                Err(ErrMode::Backtrack(ContextError::new())),
            ),
            (
                // Missing closing brace
                "[1, \"m\", [[0, 1]], null, null, [[0, 1, \"m\", [\"test_case\"]]]",
                Err(ErrMode::Backtrack(ContextError::new())),
            ),
            (
                // Trailing comma
                "[1, \"m\", [[0, 1]], null, null,]",
                Err(ErrMode::Backtrack(ContextError::new())),
            ),
        ];
        let expected_line_count = invalid_test_cases.len();
        for test_case in invalid_test_cases {
            buf.input = test_case.0;
            assert_eq!(report_line_or_empty.parse_next(&mut buf), test_case.1);
        }
        // We still increment the line number even for malformed lines so that we don't
        // throw off subsequent lines that are well-formed.
        assert_eq!(buf.state.chunk.current_line as usize, expected_line_count);
    }

    #[test]
    fn test_chunk_header() {
        let test_ctx = setup();
        let mut buf = TestStream {
            input: "",
            state: test_ctx.parse_ctx,
        };

        let test_cases = [
            ("{}\n", Ok(JsonMap::new())),
            (
                "{\"present_sessions\": []}\n",
                Ok(JsonMap::from_iter([(
                    "present_sessions".to_string(),
                    JsonVal::Array(vec![]),
                )])),
            ),
            // Missing newline
            ("{}", Err(ErrMode::Backtrack(ContextError::new()))),
            // Missing dict and newline
            ("", Err(ErrMode::Backtrack(ContextError::new()))),
            // Missing dict
            ("\n", Err(ErrMode::Backtrack(ContextError::new()))),
            (
                "present_sessions: []",
                Err(ErrMode::Backtrack(ContextError::new())),
            ),
        ];

        for test_case in test_cases {
            buf.input = test_case.0;
            assert_eq!(chunk_header.parse_next(&mut buf), test_case.1);
        }
    }

    #[test]
    fn test_chunk() {
        let test_ctx = setup();
        let mut buf = TestStream {
            input: "",
            state: test_ctx.parse_ctx,
        };
        stub_report_builder(&mut buf.state.db.report_builder);

        // (input, (result, expected_line_count))
        let test_cases = [
            // We consume `{}\n` to parse the header, leaving the stream empty.
            // `report_line_or_empty` recognizes this as an empty line terminated by EOF, so it
            // succeeds.
            ("{}\n", (Ok(()), 1)),
            // Similar to the above. `{}\n` is the header, then one empty line terminated by `\n`,
            // and then a second empty line terminated by EOF.
            ("{}\n\n", (Ok(()), 2)),
            // No trailing newlines. There is a single line of data following the header, and then
            // that's it.
            ("{}\n[1, null, [[0, 1]]]", (Ok(()), 1)),
            (
                // `{}\n` is the header, then we have two lines of data delimited by `\n`
                "{}\n[1, null, [[0, 1]]]\n[0, null, [[0, 1]]]",
                (Ok(()), 2),
            ),
            (
                // Same as above, but the trailing newline represents an extra empty line
                "{}\n[1, null, [[0, 1]]]\n[0, null, [[0, 1]]]\n",
                (Ok(()), 3),
            ),
            (
                // Same as above, but the trailing newline represents an extra empty line
                "{}\n[1, null, [[0, 1]]]\n\n\n[0, null, [[0, 1]]]\n\n",
                (Ok(()), 6),
            ),
            (
                // One line of data followed by the "end of chunk" delimiter. We don't consider the
                // delimiter to be a line, but attempting to parse it as one still increments the
                // line count.
                "{}\n[1, null, [[0, 1]]]\n<<<<< end_of_chunk >>>>>\n\n",
                (Ok(()), 2),
            ),
            // Malformed
            // Missing newline after header
            ("{}", (Err(ErrMode::Backtrack(ContextError::new())), 0)),
            // Missing header
            ("\n\n", (Err(ErrMode::Backtrack(ContextError::new())), 0)),
            (
                // Malformed report line. Attempting the parse still increments the line count.
                "{}\n[1, null, [[0, 1]]\n\n",
                (Err(ErrMode::Backtrack(ContextError::new())), 1),
            ),
            (
                // Malformed header
                "{[]}\n\n",
                (Err(ErrMode::Backtrack(ContextError::new())), 0),
            ),
        ];

        for test_case in test_cases {
            buf.state.chunk.index = 0;
            buf.input = test_case.0;
            let expected = test_case.1;
            assert_eq!(chunk.parse_next(&mut buf), expected.0);
            assert_eq!(buf.state.chunk.current_line, expected.1);
        }
    }

    #[test]
    fn test_chunks_file_header() {
        let test_ctx = setup();
        let mut buf = TestStream {
            input: "",
            state: test_ctx.parse_ctx,
        };

        buf.state
            .db
            .report_builder
            .expect_insert_context()
            .with(eq(ContextType::TestCase), eq("1".to_string()))
            .returning(|_, name| {
                Ok(Context {
                    name: name.to_string(),
                    ..Default::default()
                })
            });

        buf.state
            .db
            .report_builder
            .expect_insert_context()
            .with(eq(ContextType::TestCase), eq("test_name".to_string()))
            .returning(|_, name| {
                Ok(Context {
                    name: name.to_string(),
                    ..Default::default()
                })
            });

        assert!(buf.state.labels_index.is_empty());
        let test_cases = [
            (
                "{\"labels_index\": {\"1\": \"test_name\"}}\n<<<<< end_of_header >>>>>\n",
                Ok(()),
            ),
            ("{\"labels_index\": {\"test_name\": \"test_name\"}}\n<<<<< end_of_header >>>>>\n", Ok(())),
            (
                // This unrecognized key is just ignored
                "{\"not_labels_index\": {\"test_name_2\": \"test_name_2\"}}\n<<<<< end_of_header >>>>>\n",
                Ok(()),
            ),
            ("{", Err(ErrMode::Backtrack(ContextError::new()))),
            ("", Err(ErrMode::Backtrack(ContextError::new()))),
            (
                // Missing terminator
                "{\"labels_index\": {\"1\": \"test_name\"}}",
                Err(ErrMode::Backtrack(ContextError::new())),
            ),
            (
                // Missing newline before terminator
                "{\"labels_index\": {\"1\": \"test_name\"}}<<<<< end_of_header >>>>>\n",
                Err(ErrMode::Backtrack(ContextError::new())),
            ),
        ];

        for test_case in test_cases {
            buf.input = &test_case.0;
            assert_eq!(chunks_file_header.parse_next(&mut buf), test_case.1);
        }
        assert_eq!(buf.state.labels_index.len(), 2);
        assert!(buf.state.labels_index.contains_key("1"));
        assert!(buf.state.labels_index.contains_key("test_name"));
    }

    #[test]
    fn test_parse_chunks_file() {
        let test_ctx = setup();
        let mut buf = TestStream {
            input: "",
            state: test_ctx.parse_ctx,
        };
        buf.state.labels_index.insert("test_case".to_string(), 100);
        stub_report_builder(&mut buf.state.db.report_builder);

        // (input, (result, expected_chunk_index, expected_line_count))
        let test_cases = [
            // Header and one chunk with an empty line
            ("{}\n<<<<< end_of_header >>>>>\n{}\n", (Ok(()), 1, 1)),
            // No header, one chunk with a populated line and an  empty line
            ("{}\n[1, null, [[0, 1]]]\n", (Ok(()), 1, 2)),
            (
                // No header, two chunks, the second having just one empty line
                "{}\n[1, null, [[0, 1]]]\n\n<<<<< end_of_chunk >>>>>\n{}\n",
                (Ok(()), 2, 1),
            ),
            (
                // Header, two chunks, the second having multiple data lines and an empty line
                "{}\n<<<<< end_of_header >>>>>\n{}\n[1, null, [[0, 1]]]\n\n<<<<< end_of_chunk >>>>>\n{}\n[1, null, [[0, 1]]]\n[1, null, [[0, 1]]]\n",
                (Ok(()), 2, 3),
            ),
            // Malformed
            (
                // Header but 0 chunks
                "{}\n<<<<< end_of_header >>>>>\n\n",
                (Err(ErrMode::Backtrack(ContextError::new())), 0, 0),
            ),
            // No header (fine) but 0 chunks
            ("", (Err(ErrMode::Backtrack(ContextError::new())), 0, 0)),
            (
                // Malformed report line. Attempting the line parse still increments the line count.
                "{}\n[1, null, [[0, 1]]\n<<<<< end_of_chunk >>>>>\n{}\n\n",
                (Err(ErrMode::Backtrack(ContextError::new())), 0, 1)
            ),
        ];

        for test_case in test_cases {
            buf.state.chunk.index = 0;
            buf.state.chunk.current_line = 0;
            buf.input = test_case.0;
            let expected_result = test_case.1;
            assert_eq!(parse_chunks_file.parse_next(&mut buf), expected_result.0);
            assert_eq!(buf.state.chunk.index, expected_result.1);
            assert_eq!(buf.state.chunk.current_line, expected_result.2);
        }
    }
}
