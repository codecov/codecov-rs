//! Parses a "report JSON" object which contains information about the files and
//! "sessions" in a report. A session is more-or-less a single upload, and they
//! are represented in our schema as a "context" which may be tied to a line.
//!
//! At a high level, the format looks something like:
//! ```json
//! {
//!     "files": {
//!         "filename": ReportFileSummary,
//!         ...
//!     },
//!     "sessions": {
//!         "session index": Session,
//!         ...
//!     }
//! }
//! ```
//!
//! The types can only be completely understood by reading their implementations
//! in our Python code:
//! - [`ReportFileSummary`](https://github.com/codecov/shared/blob/e97a9f422a6e224b315d6dc3821f9f5ebe9b2ddd/shared/reports/types.py#L361-L367)
//! - [`Session`](https://github.com/codecov/shared/blob/e97a9f422a6e224b315d6dc3821f9f5ebe9b2ddd/shared/utils/sessions.py#L111-L128O)
//!
//! ## Files
//!
//! The `files` are key-value pairs where the key is a filename and the value is
//! a `ReportFileSummary`. We primarily care about the chunks_index field and
//! can compute the totals on-demand later.
//!
//! The format is messy and can only be fully understood by reading the Python
//! source in our `shared` repository's
//! [`shared/reports/resources.py`](https://github.com/codecov/shared/tree/main/shared/reports/resources.py) and
//! [`shared/reports/types.py`](https://github.com/codecov/shared/blob/main/shared/reports/types.py).
//! Nevertheless, the common case will be described here.
//!
//! At a high level, the input looks like:
//! ```notrust
//! "filename.rs": [
//!     chunks_index: int,
//!     file_totals: ReportTotals,
//!     session_totals: null, // (formerly SessionTotalsArray, but ignored now)
//!     diff_totals: ReportTotals (probably),
//! ]
//! ```
//! with `int` being normal and the other types being from our Python code:
//! - [`ReportFileSummary`](https://github.com/codecov/shared/blob/e97a9f422a6e224b315d6dc3821f9f5ebe9b2ddd/shared/reports/types.py#L361-L367)
//! - [`ReportTotals`](https://github.com/codecov/shared/blob/e97a9f422a6e224b315d6dc3821f9f5ebe9b2ddd/shared/reports/types.py#L30-L45)
//! - [`SessionTotalsArray`](https://github.com/codecov/shared/blob/e97a9f422a6e224b315d6dc3821f9f5ebe9b2ddd/shared/reports/types.py#L263-L272)
//!
//! `SessionTotalsArray` no longer exists, but older reports may still have it.
//! It's a dict mapping a session ID to a `SessionTotals` (which is just a type
//! alias for `ReportTotals` and a "meta" key with extra information including
//! how many sessions there are in the map, and old reports may still have it.
//! There's an even older format which is just a flat list. In any case, we
//! ignore the field now.
//!
//! Input example:
//! ```json
//!    "src/report.rs": [
//!      0,             # index in chunks
//!      [              # file totals
//!        0,           # > files
//!        45,          # > lines
//!        45,          # > hits
//!        0,           # > misses
//!        0,           # > partials
//!        "100",       # > coverage %
//!        0,           # > branches
//!        0,           # > methods
//!        0,           # > messages
//!        0,           # > sessions
//!        0,           # > complexity
//!        0,           # > complexity_total
//!        0            # > diff
//!      ],
//!      {              # session totals (usually null nowadays)
//!        "0": [       # > key: session id
//!          0,         # > files
//!          45,        # > lines
//!          45,        # > hits
//!          0,         # > misses
//!          0,         # > partials
//!          "100"      # > coverage
//!        ],
//!        "meta": {
//!          "session_count": 1
//!        }
//!      },
//!      null           # diff totals
//!    ],
//! ```
//!
//! ## Sessions
//!
//! The `sessions` are key-value pairs where the key is a session index and the
//! value is an encoded `Session`. A session essentially just an upload. We can
//! compute session-specific coverage totals on-demand later and only care about
//! other details for now.
//!
//! The format is messy and can only be fully understood by reading the Python
//! source in our `shared` repository's
//! [`shared/reports/resources.py`](https://github.com/codecov/shared/tree/main/shared/reports/resources.py),
//! [`shared/reports/types.py`](https://github.com/codecov/shared/blob/main/shared/reports/types.py),
//! and [`shared/utils/sessions.py`](https://github.com/codecov/shared/blob/main/shared/utils/sessions.py).
//! Nevertheless, the common case will be described here.
//!
//! At a high level, the input looks like:
//! ```notrust
//! "session index": [
//!     "t": ReportTotals,          # Coverage totals for this report
//!     "d": int,                   # time
//!     "a": str,                   # archive (URL of raw upload)
//!     "f": list[str],             # flags
//!     "c": str,                   # provider
//!     "n": str,                   # build
//!     "N": str,                   # name
//!     "j": str,                   # CI job name
//!     "u": str,                   # CI job run URL
//!     "p": str,                   # state
//!     "e": str,                   # env
//!     "st": str,                  # session type
//!     "se": dict,                 # session extras
//! ]
//! ```
//! with most types being normal and others coming from our Python code:
//! - [`ReportTotals`](https://github.com/codecov/shared/blob/e97a9f422a6e224b315d6dc3821f9f5ebe9b2ddd/shared/reports/types.py#L30-L45).
//! - [`Session`](https://github.com/codecov/shared/blob/e97a9f422a6e224b315d6dc3821f9f5ebe9b2ddd/shared/utils/sessions.py#L111-L128O)
//!
//! Input example:
//! ```notrust
//!    "0": {                   # session index
//!      "t": [                 # session totals
//!        3,                   # files in session
//!        94,                  # lines
//!        52,                  # hits
//!        42,                  # misses
//!        0,                   # partials
//!        "55.31915",          # coverage %
//!        0,                   # branches
//!        0,                   # methods
//!        0,                   # messages
//!        0,                   # sessions
//!        0,                   # complexity
//!        0,                   # complexity_total
//!        0                    # diff
//!      ],
//!      "d": 1704827412,       # timestamp
//!                             # archive (raw upload URL)
//!      "a": "v4/raw/2024-01-09/<cut>/<cut>/<cut>/340c0c0b-a955-46a0-9de9-3a9b5f2e81e2.txt",
//!      "f": [],               # flags
//!      "c": null,             # provider
//!      "n": null,             # build
//!      "N": null,             # name
//!      "j": "codecov-rs CI",  # CI job name
//!                             # CI job run URL
//!      "u": "https://github.com/codecov/codecov-rs/actions/runs/7465738121",
//!      "p": null,             # state
//!      "e": null,             # env
//!      "st": "uploaded",      # session type
//!      "se": {}               # session extras
//!    }
//! ```

use std::collections::{BTreeMap, HashMap};

use serde::{de::IgnoredAny, Deserialize};
use serde_json::Value;

use crate::{
    error::CodecovError,
    report::{models, Report, ReportBuilder},
};

#[derive(Debug, Deserialize)]
struct ReportJson {
    // NOTE: these two are `BTreeMap` only to have stable iteration order in tests
    files: BTreeMap<String, File>,
    sessions: BTreeMap<usize, Session>,
}

#[derive(Debug, Deserialize)]
// this really is:
// - index in chunks
// - file totals
// - session totals
// - diff totals
struct File(usize, IgnoredAny, IgnoredAny, IgnoredAny);

#[derive(Debug, Deserialize)]
struct Session {
    #[serde(rename = "d")]
    timestamp: Option<i64>,
    #[serde(rename = "a")]
    raw_upload_url: Option<String>,
    #[serde(rename = "f")]
    flags: Option<Value>,
    #[serde(rename = "c")]
    provider: Option<String>,
    #[serde(rename = "n")]
    build: Option<String>,
    #[serde(rename = "N")]
    name: Option<String>,
    #[serde(rename = "j")]
    job_name: Option<String>,
    #[serde(rename = "u")]
    ci_run_url: Option<String>,
    #[serde(rename = "p")]
    state: Option<String>,
    #[serde(rename = "e")]
    env: Option<String>,
    #[serde(rename = "st")]
    session_type: Option<String>,
    #[serde(rename = "se")]
    session_extras: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct ParsedReportJson {
    pub files: HashMap<usize, i64>,
    pub sessions: HashMap<usize, i64>,
}

pub fn parse_report_json<B, R>(
    input: &[u8],
    builder: &mut B,
) -> Result<ParsedReportJson, CodecovError>
where
    B: ReportBuilder<R>,
    R: Report,
{
    let report: ReportJson = serde_json::from_slice(input)?;

    let mut files = HashMap::with_capacity(report.files.len());
    for (filename, file) in report.files {
        let chunk_index = file.0;

        let file = builder.insert_file(&filename)?;
        files.insert(chunk_index, file.id);
    }

    let mut sessions = HashMap::with_capacity(report.sessions.len());
    for (session_index, session) in report.sessions {
        let raw_upload = models::RawUpload {
            id: 0,
            timestamp: session.timestamp,
            raw_upload_url: session.raw_upload_url,
            flags: session.flags,
            provider: session.provider,
            build: session.build,
            name: session.name,
            job_name: session.job_name,
            ci_run_url: session.ci_run_url,
            state: session.state,
            env: session.env,
            session_type: session.session_type,
            session_extras: session.session_extras,
        };

        let raw_upload = builder.insert_raw_upload(raw_upload)?;

        sessions.insert(session_index, raw_upload.id);
    }

    Ok(ParsedReportJson { files, sessions })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::test_report::TestReportBuilder;

    #[test]
    fn test_report_json_simple_valid_case() {
        let input = br#"{"files": {"src/report.rs": [0, {}, [], null]}, "sessions": {"0": {"j": "codecov-rs CI"}}}"#;

        let mut report_builder = TestReportBuilder::default();
        let _parsed = parse_report_json(input, &mut report_builder).unwrap();

        let report = report_builder.build().unwrap();
        assert_eq!(report.files, &[models::SourceFile::new("src/report.rs")]);
        assert_eq!(
            report.uploads,
            &[models::RawUpload {
                id: 0,
                job_name: Some("codecov-rs CI".into()),
                ..Default::default()
            }]
        );
    }

    #[test]
    fn test_report_json_two_files_two_sessions() {
        let input = br#"{"files": {"src/report.rs": [0, {}, [], null], "src/report/models.rs": [1, {}, [], null]}, "sessions": {"0": {"j": "codecov-rs CI"}, "1": {"j": "codecov-rs CI 2"}}}"#;

        let mut report_builder = TestReportBuilder::default();
        let _parsed = parse_report_json(input, &mut report_builder).unwrap();

        let report = report_builder.build().unwrap();
        assert_eq!(
            report.files,
            &[
                models::SourceFile::new("src/report.rs"),
                models::SourceFile::new("src/report/models.rs")
            ]
        );
        assert_eq!(
            report.uploads,
            &[
                models::RawUpload {
                    id: 0,
                    job_name: Some("codecov-rs CI".into()),
                    ..Default::default()
                },
                models::RawUpload {
                    id: 1,
                    job_name: Some("codecov-rs CI 2".into()),
                    ..Default::default()
                },
            ]
        );
    }

    #[test]
    fn test_report_json_empty_files() {
        let input = br#"{"files": {}, "sessions": {"0": {"j": "codecov-rs CI"}, "1": {"j": "codecov-rs CI 2"}}}"#;

        let mut report_builder = TestReportBuilder::default();
        let _parsed = parse_report_json(input, &mut report_builder).unwrap();

        let report = report_builder.build().unwrap();
        assert_eq!(report.files, &[]);
        assert_eq!(
            report.uploads,
            &[
                models::RawUpload {
                    id: 0,
                    job_name: Some("codecov-rs CI".into()),
                    ..Default::default()
                },
                models::RawUpload {
                    id: 1,
                    job_name: Some("codecov-rs CI 2".into()),
                    ..Default::default()
                },
            ]
        );
    }

    #[test]
    fn test_report_json_empty_sessions() {
        let input = br#"{"files": {"src/report.rs": [0, {}, [], null], "src/report/models.rs": [1, {}, [], null]}, "sessions": {}}"#;

        let mut report_builder = TestReportBuilder::default();
        let _parsed = parse_report_json(input, &mut report_builder).unwrap();

        let report = report_builder.build().unwrap();
        assert_eq!(
            report.files,
            &[
                models::SourceFile::new("src/report.rs"),
                models::SourceFile::new("src/report/models.rs")
            ]
        );
        assert_eq!(report.uploads, &[]);
    }

    #[test]
    fn test_report_json_empty() {
        let input = br#"{"files": {}, "sessions": {}}"#;

        let mut report_builder = TestReportBuilder::default();
        let _parsed = parse_report_json(input, &mut report_builder).unwrap();

        let report = report_builder.build().unwrap();
        assert_eq!(report.files, &[]);
        assert_eq!(report.uploads, &[]);
    }

    #[test]
    fn test_report_json_missing_files() {
        let input =
            br#"{"sessions": {"0": {"j": "codecov-rs CI"}, "1": {"j": "codecov-rs CI 2"}}}"#;

        let mut report_builder = TestReportBuilder::default();
        parse_report_json(input, &mut report_builder).unwrap_err();
    }

    #[test]
    fn test_report_json_missing_sessions() {
        let input = br#"{"files": {"src/report.rs": [0, {}, [], null], "src/report/models.rs": [1, {}, [], null]}}"#;

        let mut report_builder = TestReportBuilder::default();
        parse_report_json(input, &mut report_builder).unwrap_err();
    }

    #[test]
    fn test_report_json_one_invalid_file() {
        let input = br#"{"files": {"src/report.rs": [0, {}, [], null], "src/report/models.rs": [null, {}, [], null]}, "sessions": {"0": {"j": "codecov-rs CI"}, "1": {"j": "codecov-rs CI 2"}}}"#;

        let mut report_builder = TestReportBuilder::default();
        parse_report_json(input, &mut report_builder).unwrap_err();
    }

    #[test]
    fn test_report_json_one_invalid_session() {
        let input = br#"{"files": {"src/report.rs": [0, {}, [], null], "src/report/models.rs": [1, {}, [], null]}, "sessions": {"0": {"j": "codecov-rs CI"}, "j": {"xj": "codecov-rs CI 2"}}}"#;

        let mut report_builder = TestReportBuilder::default();
        parse_report_json(input, &mut report_builder).unwrap_err();
    }
}
