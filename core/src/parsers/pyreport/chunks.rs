//! A parser for the `chunks` file format.
//!
//! A chunks file contains an optional header and a series of 1 or more
//! "chunks", separated by an `END_OF_CHUNK` terminator.
//!
//! Chunks files sometimes begin with a JSON object followed by an
//! `END_OF_HEADER` terminator string.
//! The JSON object contains:
//! - `"labels_index"`: assigns a numeric ID to each label to save space
//!
//! If the `"labels_index"` key is present, this parser will insert each label
//! into the report as a [`Context`](models::Context) and create a mapping
//! in `buf.state.labels_index` from numeric ID in the header to the
//! new [`Context`](models::Context)'s ID in the output report. If the
//! `"labels_index"` key is _not_ present, we will populate
//! `buf.state.labels_index` gradually as we encounter new labels during
//! parsing.
//!
//! A chunk contains all of the line-by-line measurements for
//! a file. The Nth chunk corresponds to the file whose entry in
//! `buf.state.report_json_files` has N in its `chunks_index` field.
//!
//! Each new chunk will reset `buf.state.chunk.current_line` to 0 when it starts
//! and increment `buf.state.chunk.index` when it ends so that the next chunk
//! can associate its data with the correct file.
//!
//! A line may be empty, or it may contain a [`LineRecord`].
//! A [`LineRecord`] itself does not correspond to anything in the output,
//! but it's an umbrella that includes all of the data
//! tied to a line/[`CoverageSample`](models::CoverageSample).
//!
//! This parser performs all the writes it can to the output
//! stream and only returns a [`ReportLine`] for tests. The
//! `report_line_or_empty` parser which wraps this and supports empty lines
//! returns `Ok(())`.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::OnceLock;
use std::{fmt, mem};

use memchr::{memchr, memmem};
use serde::de::IgnoredAny;
use serde::{de, Deserialize};

use super::report_json::ParsedReportJson;
use super::utils;
use crate::error::CodecovError;
use crate::report::pyreport::types::{
    self, CoverageType, MissingBranch, Partial, PyreportCoverage, ReportLine,
};
use crate::report::pyreport::{CHUNKS_FILE_END_OF_CHUNK, CHUNKS_FILE_HEADER_TERMINATOR};
use crate::report::{Report, ReportBuilder};

#[derive(PartialEq, Debug)]
pub struct ChunkCtx {
    /// The index of this chunk in the overall sequence of chunks tells us which
    /// [`SourceFile`](models::SourceFile) this chunk corresponds to.
    pub index: usize,

    /// Each line in a chunk corresponds to a line in the source file.
    pub current_line: i64,
}

/// Context needed to parse a chunks file.
#[derive(PartialEq)]
pub struct ParseCtx<R: Report, B: ReportBuilder<R>> {
    /// Rather than returning parsed results, we write them to this
    /// `report_builder`.
    pub report_builder: B,
    // FIXME: Rust, you are drunk. We need `R`.
    _phantom: PhantomData<R>,

    /// Tracks the labels that we've already added to the report. The key is the
    /// identifier for the label inside the chunks file and the value is the
    /// ID of the [`Context`](models::Context) we created for it in
    /// the output. If a `"labels_index"` key is present in the chunks file
    /// header, this is populated all at once and the key is a numeric ID.
    /// Otherwise, this is populated as new labels are encountered and the key
    /// is the full name of the label.
    pub labels_index: HashMap<String, i64>,

    /// Context within the current chunk.
    pub chunk: ChunkCtx,

    /// The output of the report JSON parser includes a map from `chunk_index`
    /// to the ID of the [`SourceFile`](models::SourceFile) that the
    /// chunk corresponds to.
    pub report_json_files: HashMap<usize, i64>,

    /// The output of the report JSON parser includes a map from `session_id` to
    /// the ID of the [`Context`](models::Context) that the session
    /// corresponds to.
    pub report_json_sessions: HashMap<usize, i64>,
}

impl<R: Report, B: ReportBuilder<R>> ParseCtx<R, B> {
    pub fn new(
        report_builder: B,
        report_json_files: HashMap<usize, i64>,
        report_json_sessions: HashMap<usize, i64>,
    ) -> ParseCtx<R, B> {
        ParseCtx {
            labels_index: HashMap::new(),
            report_builder,
            _phantom: PhantomData,
            chunk: ChunkCtx {
                index: 0,
                current_line: 0,
            },
            report_json_files,
            report_json_sessions,
        }
    }
}

impl<R: Report, B: ReportBuilder<R>> fmt::Debug for ParseCtx<R, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParseCtx")
            .field("report_builder", &format_args!("..."))
            .field("labels_index", &self.labels_index)
            .field("chunk", &self.chunk)
            .finish()
    }
}

pub fn parse_chunks_file<B, R>(
    input: &[u8],
    report_json: ParsedReportJson,
    mut builder: B,
) -> Result<(), CodecovError>
where
    B: ReportBuilder<R>,
    R: Report,
{
    let chunks_file = ChunksFile::new(input)?;

    let mut labels_index = HashMap::with_capacity(chunks_file.labels_index().len());
    for (index, name) in chunks_file.labels_index() {
        let context = builder.insert_context(name)?;
        labels_index.insert(index.clone(), context.id);
    }

    let mut ctx = ParseCtx::new(builder, report_json.files, report_json.sessions);
    ctx.labels_index = labels_index;

    let mut report_lines = vec![];

    let mut chunks = chunks_file.chunks();
    let mut chunk_no = 0;
    while let Some(mut chunk) = chunks.next_chunk()? {
        let mut line_no = 0;
        report_lines.clear();
        while let Some(line) = chunk.next_line()? {
            line_no += 1;
            if let Some(line) = line {
                let sessions = line
                    .2
                    .into_iter()
                    .map(|session| types::LineSession {
                        session_id: session.0,
                        coverage: session.1,
                        branches: session.2.into(),
                        partials: session.3.into(),
                        complexity: None, // TODO
                    })
                    .collect();

                let datapoints: Option<HashMap<_, _>> = line.5.map(|dps| {
                    dps.into_iter()
                        .map(|dp| (dp.0, types::CoverageDatapoint::from(dp)))
                        .collect()
                });

                if let Some(datapoints) = &datapoints {
                    for datapoint in datapoints.values() {
                        for label in &datapoint.labels {
                            if !ctx.labels_index.contains_key(label) {
                                let context = ctx.report_builder.insert_context(label)?;
                                ctx.labels_index.insert(label.into(), context.id);
                            }
                        }
                    }
                }

                let mut report_line = ReportLine {
                    line_no,
                    coverage: line.0,
                    coverage_type: line.1.unwrap_or_default(),
                    sessions,
                    _messages: None,
                    _complexity: None,
                    datapoints: Some(datapoints),
                };
                report_line.normalize();
                report_lines.push(report_line);
            }
        }

        ctx.chunk.index = chunk_no;
        utils::save_report_lines(&report_lines, &mut ctx)?;
        chunk_no += 1;
    }

    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum ChunksFileParseError {
    #[error("unexpected EOF")]
    UnexpectedEof,
    #[error("unexpected input")]
    UnexpectedInput,
    #[error("invalid file header")]
    InvalidFileHeader(#[source] serde_json::Error),
    #[error("invalid chunk header")]
    InvalidChunkHeader(#[source] serde_json::Error),
    #[error("invalid line record")]
    InvalidLineRecord(#[source] serde_json::Error),
}

impl PartialEq for ChunksFileParseError {
    fn eq(&self, other: &Self) -> bool {
        core::mem::discriminant(self) == core::mem::discriminant(other)
    }
}
impl Eq for ChunksFileParseError {}

#[derive(Debug)]
pub struct ChunksFile<'d> {
    file_header: FileHeader,
    input: &'d [u8],
}

impl<'d> ChunksFile<'d> {
    pub fn new(mut input: &'d [u8]) -> Result<Self, ChunksFileParseError> {
        static HEADER_FINDER: OnceLock<memmem::Finder> = OnceLock::new();
        let header_finder =
            HEADER_FINDER.get_or_init(|| memmem::Finder::new(CHUNKS_FILE_HEADER_TERMINATOR));

        let file_header = if let Some(pos) = header_finder.find(input) {
            let header_bytes = &input[..pos];
            input = &input[pos + header_finder.needle().len()..];
            let file_header: FileHeader = serde_json::from_slice(header_bytes)
                .map_err(ChunksFileParseError::InvalidFileHeader)?;
            file_header
        } else {
            FileHeader::default()
        };

        Ok(Self { file_header, input })
    }

    pub fn labels_index(&self) -> &HashMap<String, String> {
        &self.file_header.labels_index
    }

    pub fn chunks(&self) -> Chunks {
        Chunks { input: self.input }
    }
}

pub struct Chunks<'d> {
    input: &'d [u8],
}

impl<'d> Chunks<'d> {
    pub fn next_chunk(&mut self) -> Result<Option<Chunk<'d>>, ChunksFileParseError> {
        if self.input.is_empty() {
            return Ok(None);
        }

        static CHUNK_FINDER: OnceLock<memmem::Finder> = OnceLock::new();
        let chunk_finder =
            CHUNK_FINDER.get_or_init(|| memmem::Finder::new(CHUNKS_FILE_END_OF_CHUNK));

        let mut chunk_bytes = if let Some(pos) = chunk_finder.find(self.input) {
            let chunk_bytes = &self.input[..pos];
            self.input = &self.input[pos + chunk_finder.needle().len()..];
            chunk_bytes
        } else {
            mem::take(&mut self.input)
        };

        if chunk_bytes == b"null" {
            return Ok(Some(Chunk {
                chunk_header: ChunkHeader::default(),
                input: &[],
            }));
        }

        let header_bytes =
            next_line(&mut chunk_bytes).ok_or(ChunksFileParseError::UnexpectedInput)?;
        let chunk_header: ChunkHeader = serde_json::from_slice(header_bytes)
            .map_err(ChunksFileParseError::InvalidFileHeader)?;

        Ok(Some(Chunk {
            chunk_header,
            input: chunk_bytes,
        }))
    }
}

pub struct Chunk<'d> {
    chunk_header: ChunkHeader,
    input: &'d [u8],
}

impl Chunk<'_> {
    pub fn present_sessions(&self) -> &[u32] {
        &self.chunk_header.present_sessions
    }

    pub fn next_line(&mut self) -> Result<Option<Option<LineRecord>>, ChunksFileParseError> {
        let Some(line) = next_line(&mut self.input) else {
            return Ok(None);
        };

        if line.is_empty() {
            return Ok(Some(None));
        }

        let line_record: LineRecord =
            serde_json::from_slice(line).map_err(ChunksFileParseError::InvalidLineRecord)?;
        Ok(Some(Some(line_record)))
    }
}

fn next_line<'d>(input: &mut &'d [u8]) -> Option<&'d [u8]> {
    if input.is_empty() {
        return None;
    }

    let line_bytes = if let Some(pos) = memchr(b'\n', input) {
        let line_bytes = &input[..pos];
        *input = &input[pos + 1..];
        line_bytes
    } else {
        mem::take(input)
    };
    Some(line_bytes)
}

#[derive(Debug, PartialEq, Eq, Default, Deserialize)]
pub struct FileHeader {
    #[serde(default)]
    pub labels_index: HashMap<String, String>,
}

#[derive(Debug, PartialEq, Eq, Default, Deserialize)]
pub struct ChunkHeader {
    #[serde(default)]
    pub present_sessions: Vec<u32>,
}

#[derive(Debug, Clone, Deserialize)]
struct IgnoredAnyEq(IgnoredAny);
impl PartialEq for IgnoredAnyEq {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for IgnoredAnyEq {}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct LineRecord(
    /// coverage
    PyreportCoverage,
    /// coverage type
    Option<CoverageType>,
    /// sessions
    Vec<LineSession>,
    /// messages
    #[serde(default)]
    Option<IgnoredAnyEq>,
    /// complexity
    #[serde(default)]
    Option<IgnoredAnyEq>,
    /// datapoints
    #[serde(default)]
    Option<Vec<CoverageDatapoint>>,
);

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct LineSession(
    /// session id
    usize,
    /// coverage
    PyreportCoverage,
    /// branches
    #[serde(default)]
    Option<Vec<MissingBranch>>,
    /// partials
    #[serde(default)]
    Option<Vec<Partial>>,
    /// TODO: complexity
    #[serde(default)]
    Option<IgnoredAnyEq>,
);

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CoverageDatapoint(
    /// session id
    u32,
    /// coverage
    PyreportCoverage,
    /// coverage type
    #[serde(default)]
    Option<CoverageType>,
    /// labels
    #[serde(default)]
    Option<Vec<String>>,
);

impl From<CoverageDatapoint> for types::CoverageDatapoint {
    fn from(datapoint: CoverageDatapoint) -> Self {
        Self {
            session_id: datapoint.0,
            _coverage: datapoint.1,
            _coverage_type: datapoint.2,
            labels: datapoint.3.unwrap_or_default(),
        }
    }
}

impl<'s> TryFrom<&'s str> for CoverageType {
    type Error = &'s str;

    fn try_from(value: &'s str) -> Result<Self, Self::Error> {
        match value {
            "line" => Ok(Self::Line),
            "b" | "branch" => Ok(Self::Branch),
            "m" | "method" => Ok(Self::Method),
            s => Err(s),
        }
    }
}

impl<'de> Deserialize<'de> for PyreportCoverage {
    fn deserialize<D>(deserializer: D) -> Result<PyreportCoverage, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct CoverageVisitor;
        impl de::Visitor<'_> for CoverageVisitor {
            type Value = PyreportCoverage;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a coverage value")
            }

            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if v {
                    Ok(PyreportCoverage::Partial())
                } else {
                    Err(de::Error::invalid_value(de::Unexpected::Bool(v), &self))
                }
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(PyreportCoverage::HitCount(value as u32))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let invalid = || de::Error::invalid_value(de::Unexpected::Str(v), &self);
                let (covered, total) = v.split_once('/').ok_or_else(invalid)?;

                let covered: u32 = covered.parse().map_err(|_| invalid())?;
                let total: u32 = total.parse().map_err(|_| invalid())?;
                Ok(PyreportCoverage::BranchesTaken { covered, total })
            }
        }

        deserializer.deserialize_any(CoverageVisitor)
    }
}

impl<'de> Deserialize<'de> for MissingBranch {
    fn deserialize<D>(deserializer: D) -> Result<MissingBranch, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct MissingBranchVisitor;
        impl de::Visitor<'_> for MissingBranchVisitor {
            type Value = MissingBranch;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a missing branch value")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let invalid = || de::Error::invalid_value(de::Unexpected::Str(v), &self);

                if let Some((block, branch)) = v.split_once(":") {
                    let block: u32 = block.parse().map_err(|_| invalid())?;
                    let branch: u32 = branch.parse().map_err(|_| invalid())?;

                    return Ok(MissingBranch::BlockAndBranch(block, branch));
                }

                if let Some(condition) = v.strip_suffix(":jump") {
                    let condition: u32 = condition.parse().map_err(|_| invalid())?;

                    // TODO(swatinem): can we skip saving the `jump` here?
                    return Ok(MissingBranch::Condition(condition, Some("jump".into())));
                }

                let line: u32 = v.parse().map_err(|_| invalid())?;
                Ok(MissingBranch::Line(line))
            }
        }

        deserializer.deserialize_any(MissingBranchVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parsing_chunks() {
        let simple_line_record = LineRecord(
            PyreportCoverage::HitCount(1),
            None,
            vec![LineSession(
                0,
                PyreportCoverage::HitCount(1),
                None,
                None,
                None,
            )],
            None,
            None,
            None,
        );

        #[allow(clippy::type_complexity)]
        let cases: &[(
            &[u8], // input
            &[&[Option<LineRecord>]], // chunks: line records
        )] = &[
            (
                // Header and one chunk with an empty line
                b"{}\n<<<<< end_of_header >>>>>\n{}\n",
                &[&[]],
            ),
            (
                // No header, one chunk with a populated line and an empty line
                b"{}\n[1, null, [[0, 1]]]\n",
                &[&[Some(simple_line_record.clone())]],
            ),
            (
                // No header, two chunks, the second having just one empty line
                b"{}\n[1, null, [[0, 1]]]\n\n<<<<< end_of_chunk >>>>>\n{}\n",
                &[&[Some(simple_line_record.clone())],  &[]],
            ),
            (
                // Header, two chunks, the second having multiple data lines and an empty line
                b"{}\n<<<<< end_of_header >>>>>\n{}\n[1, null, [[0, 1]]]\n\n<<<<< end_of_chunk >>>>>\n{}\n[1, null, [[0, 1]]]\n[1, null, [[0, 1]]]\n",
                &[
                    &[Some(simple_line_record.clone())],
                    &[
                        Some(simple_line_record.clone()),
                        Some(simple_line_record.clone()),
                    ],
                ],
            ),
        ];

        for (input, expected_chunks) in cases {
            let chunks_file = ChunksFile::new(input).unwrap();
            let mut chunks = chunks_file.chunks();

            for expected_line_records in *expected_chunks {
                let mut chunk = chunks.next_chunk().unwrap().unwrap();

                let mut lines = vec![];
                while let Some(line) = chunk.next_line().unwrap() {
                    lines.push(line);
                }

                assert_eq!(lines, *expected_line_records);
            }
            assert!(chunks.next_chunk().unwrap().is_none());
        }
    }
}
