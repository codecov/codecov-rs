use std::collections::HashMap;

use winnow::{
    combinator::{cut_err, preceded, separated, terminated},
    error::{ContextError, ErrMode},
    PResult, Parser,
};

use crate::{
    parsers::{
        json::{parse_kv, specific_key, JsonVal},
        ws, Report, ReportBuilder, ReportOutputStream, StrStream,
    },
    report::models,
};

/// Parses a key-value pair where the key is a filename and the value is a
/// `ReportFileSummary`. We primarily care about the chunks_index field and can
/// compute the totals on-demand later.
///
/// The format is messy and can only be fully understood by reading the Python
/// source in our `shared` repository's
/// [`shared/reports/resources.py`](https://github.com/codecov/shared/tree/main/shared/reports/resources.py) and
/// [`shared/reports/types.py`](https://github.com/codecov/shared/blob/main/shared/reports/types.py).
/// Nevertheless, the common case will be described here.
///
/// At a high level, the input looks like:
/// ```notrust
/// "filename.rs": [
///     chunks_index: int,
///     file_totals: ReportTotals,
///     session_totals: SessionTotalsArray,
///     diff_totals: ReportTotals (probably),
/// ]
/// ```
/// with `int` being normal and the other types being from our Python code:
/// - [`ReportFileSummary`](https://github.com/codecov/shared/blob/e97a9f422a6e224b315d6dc3821f9f5ebe9b2ddd/shared/reports/types.py#L361-L367)
/// - [`ReportTotals`](https://github.com/codecov/shared/blob/e97a9f422a6e224b315d6dc3821f9f5ebe9b2ddd/shared/reports/types.py#L30-L45)
/// - [`SessionTotalsArray`](https://github.com/codecov/shared/blob/e97a9f422a6e224b315d6dc3821f9f5ebe9b2ddd/shared/reports/types.py#L263-L272)
///
/// `SessionTotalsArray` will normally be a dict mapping a session ID to a
/// `SessionTotals` (which is just a type alias for `ReportTotals`) but there is
/// a legacy format.
///
/// Input example:
/// ```notrust
///    "src/report.rs": [
///      0,             # index in chunks
///      [              # file totals
///        0,           # > files
///        45,          # > lines
///        45,          # > hits
///        0,           # > misses
///        0,           # > partials
///        "100",       # > coverage %
///        0,           # > branches
///        0,           # > methods
///        0,           # > messages
///        0,           # > sessions
///        0,           # > complexity
///        0,           # > complexity_total
///        0            # > diff
///      ],
///      {              # session totals
///        "0": [       # > key: session id
///          0,         # > files
///          45,        # > lines
///          45,        # > hits
///          0,         # > misses
///          0,         # > partials
///          "100"      # > coverage
///        ],
///        "meta": {
///          "session_count": 1
///        }
///      },
///      null           # diff totals
///    ],
/// ```
pub fn report_file<S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<(usize, i32)> {
    let (filename, file_summary) = preceded(ws, terminated(parse_kv, ws)).parse_next(buf)?;

    let JsonVal::Array(vals) = file_summary else {
        return Err(ErrMode::Cut(ContextError::new()));
    };

    let Some(JsonVal::Num(chunks_index)) = vals.first() else {
        return Err(ErrMode::Cut(ContextError::new()));
    };

    let file = models::SourceFile {
        id: None,
        path: filename,
    };
    // TODO handle converting between error types
    let file = buf.state.report_builder.insert_file(file).unwrap();

    // TODO handle error
    Ok((*chunks_index as usize, file.id.unwrap()))
}

/// Parses a key-value pair where the key is a session index and the value is an
/// encoded `Session`. A session essentially just an upload. We can compute
/// session-specific coverage totals on-demand later and only care about other
/// details for now.
///
/// The format is messy and can only be fully understood by reading the Python
/// source in our `shared` repository's
/// [`shared/reports/resources.py`](https://github.com/codecov/shared/tree/main/shared/reports/resources.py),
/// [`shared/reports/types.py`](https://github.com/codecov/shared/blob/main/shared/reports/types.py),
/// and [`shared/utils/sessions.py`](https://github.com/codecov/shared/blob/main/shared/utils/sessions.py).
/// Nevertheless, the common case will be described here.
///
/// At a high level, the input looks like:
/// ```notrust
/// "session index": [
///     "t": ReportTotals,          # Coverage totals for this report
///     "d": int,                   # time
///     "a": str,                   # archive (URL of raw upload)
///     "f": list[str],             # flags
///     "c": str,                   # provider
///     "n": str,                   # build
///     "N": str,                   # name
///     "j": str,                   # CI job name
///     "u": str,                   # CI job run URL
///     "p": str,                   # state
///     "e": str,                   # env
///     "st": str,                  # session type
///     "se": dict,                 # session extras
/// ]
/// ```
/// with most types being normal and others coming from our Python code:
/// - [`ReportTotals`](https://github.com/codecov/shared/blob/e97a9f422a6e224b315d6dc3821f9f5ebe9b2ddd/shared/reports/types.py#L30-L45).
/// - [`Session`](https://github.com/codecov/shared/blob/e97a9f422a6e224b315d6dc3821f9f5ebe9b2ddd/shared/utils/sessions.py#L111-L128O)
///
/// Input example:
/// ```notrust
///    "0": {                   # session index
///      "t": [                 # session totals
///        3,                   # files in session
///        94,                  # lines
///        52,                  # hits
///        42,                  # misses
///        0,                   # partials
///        "55.31915",          # coverage %
///        0,                   # branches
///        0,                   # methods
///        0,                   # messages
///        0,                   # sessions
///        0,                   # complexity
///        0,                   # complexity_total
///        0                    # diff
///      ],
///      "d": 1704827412,       # timestamp
///                             # archive (raw upload URL)
///      "a": "v4/raw/2024-01-09/<cut>/<cut>/<cut>/340c0c0b-a955-46a0-9de9-3a9b5f2e81e2.txt",
///      "f": [],               # flags
///      "c": null,             # provider
///      "n": null,             # build
///      "N": null,             # name
///      "j": "codecov-rs CI",  # CI job name
///                             # CI job run URL
///      "u": "https://github.com/codecov/codecov-rs/actions/runs/7465738121",
///      "p": null,             # state
///      "e": null,             # env
///      "st": "uploaded",      # session type
///      "se": {}               # session extras
///    }
/// ```
pub fn report_session<S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<(usize, i32)> {
    let (session_index, encoded_session) =
        preceded(ws, terminated(parse_kv, ws)).parse_next(buf)?;

    let (Ok(session_index), JsonVal::Object(vals)) =
        (session_index.parse::<usize>(), encoded_session)
    else {
        return Err(ErrMode::Cut(ContextError::new()));
    };

    // arbitrarily choosing job name since "N" is unpopulated
    let Some(JsonVal::Str(name)) = vals.get("j") else {
        return Err(ErrMode::Cut(ContextError::new()));
    };

    let context = models::Context {
        id: None,
        context_type: models::ContextType::Upload,
        name: name.clone(),
    };
    // TODO handle error
    let context = buf.state.report_builder.insert_context(context).unwrap();

    // TODO handle error
    Ok((session_index, context.id.unwrap()))
}

/// Parses the JSON object that corresponds to the "files" key. Because there
/// could be many files, we parse each key/value pair one at a time.
pub fn report_files_dict<S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<HashMap<usize, i32>> {
    cut_err(preceded(
        (ws, '{', ws),
        terminated(separated(0.., report_file, (ws, ',', ws)), (ws, '}', ws)),
    ))
    .parse_next(buf)
}

/// Parses the JSON object that corresponds to the "sessions" key. Because there
/// could be many sessions, we parse each key/value pair one at a time.
pub fn report_sessions_dict<S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<HashMap<usize, i32>> {
    cut_err(preceded(
        (ws, '{', ws),
        terminated(separated(0.., report_session, (ws, ',', ws)), (ws, '}', ws)),
    ))
    .parse_next(buf)
}

/// Parses a "report JSON" object which contains information about the files and
/// "sessions" in a report. A session is more-or-less a single upload, and they
/// are represented in our schema as a "context" which may be tied to a line.
///
/// At a high level, the format looks something like:
/// ```notrust
/// {
///     "files": {
///         "filename": ReportFileSummary,
///         ...
///     },
///     "sessions": {
///         "session index": Session,
///         ...
///     }
/// }
/// ```
///
/// The types can only be completely understood by reading their implementations
/// in our Python code:
/// - [`ReportFileSummary`](https://github.com/codecov/shared/blob/e97a9f422a6e224b315d6dc3821f9f5ebe9b2ddd/shared/reports/types.py#L361-L367)
/// - [`Session`](https://github.com/codecov/shared/blob/e97a9f422a6e224b315d6dc3821f9f5ebe9b2ddd/shared/utils/sessions.py#L111-L128O)
pub fn parse_report_json<S: StrStream, R: Report, B: ReportBuilder<R>>(
    buf: &mut ReportOutputStream<S, R, B>,
) -> PResult<(HashMap<usize, i32>, HashMap<usize, i32>)> {
    let parse_files = preceded(
        specific_key("files"),
        terminated(report_files_dict, (ws, ',', ws)),
    );
    let parse_sessions = preceded(
        specific_key("sessions"),
        terminated(report_sessions_dict, ws),
    );
    cut_err(preceded(
        (ws, '{', ws),
        terminated((parse_files, parse_sessions), (ws, '}', ws)),
    ))
    .parse_next(buf)
}

#[cfg(test)]
mod tests {
    use mockall::predicate::*;

    use super::*;
    use crate::{
        parsers::ParseCtx,
        report::{MockReport, MockReportBuilder},
    };

    type TestStream<'a> = ReportOutputStream<&'a str, MockReport, MockReportBuilder<MockReport>>;

    struct Ctx {
        parse_ctx: ParseCtx<MockReport, MockReportBuilder<MockReport>>,
    }

    fn setup() -> Ctx {
        let report_builder = MockReportBuilder::new();
        let parse_ctx = ParseCtx {
            report_builder,
            _phantom: std::marker::PhantomData,
        };

        Ctx { parse_ctx }
    }

    mod report_json {
        use super::*;

        fn test_report_file(path: &str, input: &str) -> PResult<(usize, i32)> {
            let ctx = setup();
            let mut buf = TestStream {
                input,
                state: ctx.parse_ctx,
            };

            let expected_file = models::SourceFile {
                id: None,
                path: path.to_string(),
            };
            let inserted_file = models::SourceFile {
                id: Some(1),
                path: path.to_string(),
            };

            buf.state
                .report_builder
                .expect_insert_file()
                .with(eq(expected_file))
                .return_once(move |_| Ok(inserted_file));

            report_file.parse_next(&mut buf)
        }

        #[test]
        fn test_report_file_simple_valid_case() {
            assert_eq!(
                test_report_file("src/report.rs", "\"src/report.rs\": [0, [], {}, null]",),
                Ok((0, 1))
            );
        }

        #[test]
        fn test_report_file_malformed_key() {
            assert_eq!(
                test_report_file("src/report.rs", "src/report.rs\": [0, [], {}, null]",),
                Err(ErrMode::Backtrack(ContextError::new()))
            );
        }

        #[test]
        fn test_report_key_wrong_type() {
            assert_eq!(
                test_report_file("src/report.rs", "5: [0, [], {}, null]",),
                Err(ErrMode::Backtrack(ContextError::new()))
            );
        }

        #[test]
        fn test_report_file_chunks_index_wrong_type() {
            assert_eq!(
                test_report_file("src/report.rs", "\"src/report.rs\": [\"0\", [], {}, null]",),
                Err(ErrMode::Cut(ContextError::new()))
            );
        }

        #[test]
        fn test_report_file_file_summary_wrong_type() {
            assert_eq!(
                test_report_file(
                    "src/report.rs",
                    "\"src/report.rs\": {\"chunks_index\": 0, \"totals\": []}",
                ),
                Err(ErrMode::Cut(ContextError::new()))
            );
        }

        #[test]
        fn test_report_file_file_summary_empty() {
            assert_eq!(
                test_report_file("src/report.rs", "\"src/report.rs\": []",),
                Err(ErrMode::Cut(ContextError::new()))
            );
        }

        fn test_report_session(name: &str, input: &str) -> PResult<(usize, i32)> {
            let ctx = setup();
            let mut buf = TestStream {
                input,
                state: ctx.parse_ctx,
            };

            let expected_context = models::Context {
                id: None,
                context_type: models::ContextType::Upload,
                name: name.to_string(),
            };
            let inserted_context = models::Context {
                id: Some(1),
                context_type: models::ContextType::Upload,
                name: name.to_string(),
            };

            buf.state
                .report_builder
                .expect_insert_context()
                .with(eq(expected_context))
                .return_once(move |_| Ok(inserted_context));

            report_session.parse_next(&mut buf)
        }

        #[test]
        fn test_report_session_simple_valid_case() {
            assert_eq!(
                test_report_session("codecov-rs CI", "\"0\": {\"j\": \"codecov-rs CI\"}",),
                Ok((0, 1))
            );
        }

        #[test]
        fn test_report_session_malformed_session_index() {
            assert_eq!(
                test_report_session("codecov-rs CI", "'0\": {\"j\": \"codecov-rs CI\"}",),
                Err(ErrMode::Backtrack(ContextError::new()))
            );
        }

        #[test]
        fn test_report_session_session_index_not_numeric() {
            assert_eq!(
                test_report_session("codecov-rs CI", "\"str\": {\"j\": \"codecov-rs CI\"}",),
                Err(ErrMode::Cut(ContextError::new()))
            );
        }

        #[test]
        fn test_report_session_session_index_float() {
            assert_eq!(
                test_report_session("codecov-rs CI", "\"3.34\": {\"j\": \"codecov-rs CI\"}",),
                Err(ErrMode::Cut(ContextError::new()))
            );
        }

        #[test]
        fn test_report_session_missing_job_key() {
            assert_eq!(
                test_report_session("codecov-rs CI", "\"0\": {\"x\": \"codecov-rs CI\"}",),
                Err(ErrMode::Cut(ContextError::new()))
            );
        }

        #[test]
        fn test_report_session_job_key_wrong_type() {
            assert_eq!(
                test_report_session("codecov-rs CI", "\"0\": {\"j\": []}",),
                Err(ErrMode::Cut(ContextError::new()))
            );
        }

        #[test]
        fn test_report_session_encoded_session_wrong_type() {
            assert_eq!(
                test_report_session("codecov-rs CI", "\"0\": [\"j\", []]",),
                Err(ErrMode::Cut(ContextError::new()))
            );
        }

        fn test_report_files_dict(paths: &[&str], input: &str) -> PResult<HashMap<usize, i32>> {
            let ctx = setup();
            let mut buf = TestStream {
                input,
                state: ctx.parse_ctx,
            };

            for (i, path) in paths.iter().enumerate() {
                let expected_file = models::SourceFile {
                    id: None,
                    path: path.to_string(),
                };
                let inserted_file = models::SourceFile {
                    id: Some(i as i32),
                    path: path.to_string(),
                };
                buf.state
                    .report_builder
                    .expect_insert_file()
                    .with(eq(expected_file))
                    .return_once(move |_| Ok(inserted_file));
            }

            report_files_dict.parse_next(&mut buf)
        }

        #[test]
        fn test_report_files_dict_single_valid_file() {
            assert_eq!(
                test_report_files_dict(
                    &["src/report.rs"],
                    "{\"src/report.rs\": [0, [], {}, null]}",
                ),
                Ok(HashMap::from([(0, 0)]))
            );
        }

        #[test]
        fn test_report_files_dict_multiple_valid_files() {
            assert_eq!(test_report_files_dict(
                &["src/report.rs", "src/report/models.rs"],
                "{\"src/report.rs\": [0, [], {}, null], \"src/report/models.rs\": [1, [], {}, null]}",
            ), Ok(HashMap::from([(0, 0), (1, 1)])));
        }

        #[test]
        fn test_report_files_dict_multiple_valid_files_trailing_comma() {
            assert_eq!(test_report_files_dict(
                &["src/report.rs", "src/report/models.rs"],
                "{\"src/report.rs\": [0, [], {}, null], \"src/report/models.rs\": [1, [], {}, null],}",
            ), Err(ErrMode::Cut(ContextError::new())));
        }

        #[test]
        fn test_report_files_dict_multiple_files_same_index() {
            // TODO this is how winnow handles accumulating into collections but it's not
            // the behavior that we want. we want to error
            assert_eq!(test_report_files_dict(
                &["src/report.rs", "src/report/models.rs"],
                "{\"src/report.rs\": [0, [], {}, null], \"src/report/models.rs\": [0, [], {}, null]}",
            ), Ok(HashMap::from([(0, 1)])));
        }

        #[test]
        fn test_report_files_dict_single_invalid_file() {
            assert_eq!(
                test_report_files_dict(
                    &["src/report.rs"],
                    "{\"src/report.rs\": [null, [], {}, null]}",
                ),
                Err(ErrMode::Cut(ContextError::new()))
            );
        }

        #[test]
        fn test_report_files_dict_invalid_file_after_valid_file() {
            assert_eq!(test_report_files_dict(
                &["src/report.rs", "src/report/models.rs"],
                "{\"src/report.rs\": [0, [], {}, null], \"src/report/models.rs\": [null, [], {}, null]}",
            ), Err(ErrMode::Cut(ContextError::new())));
        }

        #[test]
        fn test_report_files_dict_wrong_type() {
            assert_eq!(test_report_files_dict(
                &["src/report.rs", "src/report/models.rs"],
                "[\"src/report.rs\": [0, [], {}, null], \"src/report/models.rs\": [1, [], {}, null]]",
            ), Err(ErrMode::Cut(ContextError::new())));
        }

        #[test]
        fn test_report_files_dict_no_files() {
            assert_eq!(test_report_files_dict(&[], "{}",), Ok(HashMap::new()));
        }

        fn test_report_sessions_dict(names: &[&str], input: &str) -> PResult<HashMap<usize, i32>> {
            let ctx = setup();
            let mut buf = TestStream {
                input,
                state: ctx.parse_ctx,
            };

            for (i, name) in names.iter().enumerate() {
                let expected_context = models::Context {
                    id: None,
                    context_type: models::ContextType::Upload,
                    name: name.to_string(),
                };
                let inserted_context = models::Context {
                    id: Some(i as i32),
                    context_type: models::ContextType::Upload,
                    name: name.to_string(),
                };
                buf.state
                    .report_builder
                    .expect_insert_context()
                    .with(eq(expected_context))
                    .return_once(move |_| Ok(inserted_context));
            }

            report_sessions_dict.parse_next(&mut buf)
        }

        #[test]
        fn test_report_sessions_dict_single_valid_session() {
            assert_eq!(
                test_report_sessions_dict(
                    &["codecov-rs CI"],
                    "{\"0\": {\"j\": \"codecov-rs CI\"}}",
                ),
                Ok(HashMap::from([(0, 0)]))
            );
        }

        #[test]
        fn test_report_sessions_dict_multiple_valid_sessions() {
            assert_eq!(
                test_report_sessions_dict(
                    &["codecov-rs CI", "codecov-rs CI 2"],
                    "{\"0\": {\"j\": \"codecov-rs CI\"}, \"1\": {\"j\": \"codecov-rs CI 2\"}}",
                ),
                Ok(HashMap::from([(0, 0), (1, 1)]))
            );
        }

        #[test]
        fn test_report_sessions_dict_multiple_valid_sessions_trailing_comma() {
            assert_eq!(
                test_report_sessions_dict(
                    &["codecov-rs CI", "codecov-rs CI 2"],
                    "{\"0\": {\"j\": \"codecov-rs CI\"}, \"1\": {\"j\": \"codecov-rs CI 2\"},}",
                ),
                Err(ErrMode::Cut(ContextError::new()))
            );
        }

        #[test]
        fn test_report_sessions_dict_multiple_sessions_same_index() {
            // TODO this is how winnow handles accumulating into collections but it's not
            // the behavior that we want. we want to error
            assert_eq!(
                test_report_sessions_dict(
                    &["codecov-rs CI", "codecov-rs CI 2"],
                    "{\"0\": {\"j\": \"codecov-rs CI\"}, \"0\": {\"j\": \"codecov-rs CI 2\"}}",
                ),
                Ok(HashMap::from([(0, 1)]))
            );
        }

        #[test]
        fn test_report_sessions_dict_single_invalid_session() {
            assert_eq!(
                test_report_sessions_dict(
                    &["codecov-rs CI"],
                    "{\"0\": {\"xj\": \"codecov-rs CI\"}}",
                ),
                Err(ErrMode::Cut(ContextError::new()))
            );
        }

        #[test]
        fn test_report_sessions_dict_invalid_session_after_valid_session() {
            assert_eq!(
                test_report_sessions_dict(
                    &["codecov-rs CI", "codecov-rs CI 2"],
                    "{\"0\": {\"j\": \"codecov-rs CI\"}, \"1\": {\"xj\": \"codecov-rs CI 2\"}}",
                ),
                Err(ErrMode::Cut(ContextError::new()))
            );
        }

        #[test]
        fn test_report_sessions_dict_wrong_type() {
            assert_eq!(
                test_report_sessions_dict(
                    &["codecov-rs CI"],
                    "{\"0\": [\"j\": \"codecov-rs CI\"}]",
                ),
                Err(ErrMode::Cut(ContextError::new()))
            );
        }

        #[test]
        fn test_report_sessions_dict_no_sessions() {
            assert_eq!(test_report_sessions_dict(&[], "{}",), Ok(HashMap::new()));
        }

        fn test_report_json(
            paths: &[&str],
            session_names: &[&str],
            input: &str,
        ) -> PResult<(HashMap<usize, i32>, HashMap<usize, i32>)> {
            let ctx = setup();
            let mut buf = TestStream {
                input,
                state: ctx.parse_ctx,
            };

            for (i, path) in paths.iter().enumerate() {
                let expected_file = models::SourceFile {
                    id: None,
                    path: path.to_string(),
                };
                let inserted_file = models::SourceFile {
                    id: Some(i as i32),
                    path: path.to_string(),
                };
                buf.state
                    .report_builder
                    .expect_insert_file()
                    .with(eq(expected_file))
                    .return_once(move |_| Ok(inserted_file));
            }

            for (i, name) in session_names.iter().enumerate() {
                let expected_context = models::Context {
                    id: None,
                    context_type: models::ContextType::Upload,
                    name: name.to_string(),
                };
                let inserted_context = models::Context {
                    id: Some(i as i32),
                    context_type: models::ContextType::Upload,
                    name: name.to_string(),
                };
                buf.state
                    .report_builder
                    .expect_insert_context()
                    .with(eq(expected_context))
                    .return_once(move |_| Ok(inserted_context));
            }

            parse_report_json.parse_next(&mut buf)
        }

        #[test]
        fn test_report_json_simple_valid_case() {
            assert_eq!(test_report_json(
                &["src/report.rs"],
                &["codecov-rs CI"],
                "{\"files\": {\"src/report.rs\": [0, {}, [], null]}, \"sessions\": {\"0\": {\"j\": \"codecov-rs CI\"}}}",
            ), Ok((HashMap::from([(0, 0)]), HashMap::from([(0, 0)]))));
        }

        #[test]
        fn test_report_json_two_files_two_sessions() {
            assert_eq!(test_report_json(
                &["src/report.rs", "src/report/models.rs"],
                &["codecov-rs CI", "codecov-rs CI 2"],
                "{\"files\": {\"src/report.rs\": [0, {}, [], null], \"src/report/models.rs\": [1, {}, [], null]}, \"sessions\": {\"0\": {\"j\": \"codecov-rs CI\"}, \"1\": {\"j\": \"codecov-rs CI 2\"}}}",
            ), Ok((HashMap::from([(0, 0), (1, 1)]), HashMap::from([(0, 0), (1, 1)]))));
        }

        #[test]
        fn test_report_json_empty_files() {
            assert_eq!(test_report_json(
                &[],
                &["codecov-rs CI", "codecov-rs CI 2"],
                "{\"files\": {}, \"sessions\": {\"0\": {\"j\": \"codecov-rs CI\"}, \"1\": {\"j\": \"codecov-rs CI 2\"}}}",
            ), Ok((HashMap::new(), HashMap::from([(0, 0), (1, 1)]))));
        }

        #[test]
        fn test_report_json_empty_sessions() {
            assert_eq!(test_report_json(
                &["src/report.rs", "src/report/models.rs"],
                &[],
                "{\"files\": {\"src/report.rs\": [0, {}, [], null], \"src/report/models.rs\": [1, {}, [], null]}, \"sessions\": {}}",
            ), Ok((HashMap::from([(0, 0), (1, 1)]), HashMap::new())));
        }

        #[test]
        fn test_report_json_empty() {
            assert_eq!(
                test_report_json(&[], &[], "{\"files\": {}, \"sessions\": {}}",),
                Ok((HashMap::new(), HashMap::new()))
            );
        }

        #[test]
        fn test_report_json_sessions_before_files() {
            assert_eq!(test_report_json(
                &["src/report.rs", "src/report/models.rs"],
                &["codecov-rs CI", "codecov-rs CI 2"],
                "{\"sessions\": {\"0\": {\"j\": \"codecov-rs CI\"}, \"1\": {\"j\": \"codecov-rs CI 2\"}}, \"files\": {\"src/report.rs\": [0, {}, [], null], \"src/report/models.rs\": [1, {}, [], null]}}",
            ), Err(ErrMode::Cut(ContextError::new())));
        }

        #[test]
        fn test_report_json_missing_files() {
            assert_eq!(test_report_json(
                &["src/report.rs", "src/report/models.rs"],
                &["codecov-rs CI", "codecov-rs CI 2"],
                "{\"sessions\": {\"0\": {\"j\": \"codecov-rs CI\"}, \"1\": {\"j\": \"codecov-rs CI 2\"}}}",
            ), Err(ErrMode::Cut(ContextError::new())));
        }

        #[test]
        fn test_report_json_missing_sessions() {
            assert_eq!(test_report_json(
                &["src/report.rs", "src/report/models.rs"],
                &["codecov-rs CI", "codecov-rs CI 2"],
                "{\"files\": {\"src/report.rs\": [0, {}, [], null], \"src/report/models.rs\": [1, {}, [], null]}}",
            ), Err(ErrMode::Cut(ContextError::new())));
        }

        #[test]
        fn test_report_json_one_invalid_file() {
            assert_eq!(test_report_json(
                &["src/report.rs", "src/report/models.rs"],
                &["codecov-rs CI", "codecov-rs CI 2"],
                "{\"files\": {\"src/report.rs\": [0, {}, [], null], \"src/report/models.rs\": [null, {}, [], null]}, \"sessions\": {\"0\": {\"j\": \"codecov-rs CI\"}, \"1\": {\"j\": \"codecov-rs CI 2\"}}}",
            ), Err(ErrMode::Cut(ContextError::new())));
        }

        #[test]
        fn test_report_json_one_invalid_session() {
            assert_eq!(test_report_json(
                &["src/report.rs", "src/report/models.rs"],
                &["codecov-rs CI", "codecov-rs CI 2"],
                "{\"files\": {\"src/report.rs\": [0, {}, [], null], \"src/report/models.rs\": [1, {}, [], null]}, \"sessions\": {\"0\": {\"j\": \"codecov-rs CI\"}, \"j\": {\"xj\": \"codecov-rs CI 2\"}}}",
            ), Err(ErrMode::Cut(ContextError::new())));
        }
    }
}
