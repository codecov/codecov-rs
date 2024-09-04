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
//! into the report as a [`crate::report::models::Context`] and create a mapping
//! in `buf.state.labels_index` from numeric ID in the header to the
//! new `Context`'s ID in the output report. If the `"labels_index"` key is
//! _not_ present, we will populate `buf.state.labels_index` gradually as we
//! encounter new labels during parsing.
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
//! tied to a line/[`CoverageSample`].
//!
//! This parser performs all the writes it can to the output
//! stream and only returns a `ReportLine` for tests. The `report_line_or_empty`
//! parser which wraps this and supports empty lines returns `Ok(())`.

use std::{collections::HashMap, fmt, mem, sync::OnceLock};

use memchr::{memchr, memmem};
use serde::{de, de::IgnoredAny, Deserialize};

use crate::report::pyreport::{CHUNKS_FILE_END_OF_CHUNK, CHUNKS_FILE_HEADER_TERMINATOR};

#[derive(Debug, thiserror::Error)]
pub enum ParserError {
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

impl PartialEq for ParserError {
    fn eq(&self, other: &Self) -> bool {
        core::mem::discriminant(self) == core::mem::discriminant(other)
    }
}
impl Eq for ParserError {}

#[derive(Debug)]
pub struct ChunksFile<'d> {
    file_header: FileHeader,
    input: &'d [u8],
}

impl<'d> ChunksFile<'d> {
    pub fn new(mut input: &'d [u8]) -> Result<Self, ParserError> {
        static HEADER_FINDER: OnceLock<memmem::Finder> = OnceLock::new();
        let header_finder =
            HEADER_FINDER.get_or_init(|| memmem::Finder::new(CHUNKS_FILE_HEADER_TERMINATOR));

        let file_header = if let Some(pos) = header_finder.find(input) {
            let header_bytes = &input[..pos];
            input = &input[pos + header_finder.needle().len()..];
            let file_header: FileHeader =
                serde_json::from_slice(header_bytes).map_err(ParserError::InvalidFileHeader)?;
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
    pub fn next_chunk(&mut self) -> Result<Option<Chunk<'d>>, ParserError> {
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

        let header_bytes = next_line(&mut chunk_bytes).ok_or(ParserError::UnexpectedInput)?;
        let chunk_header: ChunkHeader =
            serde_json::from_slice(header_bytes).map_err(ParserError::InvalidFileHeader)?;

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

impl<'d> Chunk<'d> {
    pub fn present_sessions(&self) -> &[u32] {
        &self.chunk_header.present_sessions
    }

    pub fn next_line(&mut self) -> Result<Option<Option<LineRecord>>, ParserError> {
        let Some(line) = next_line(&mut self.input) else {
            return Ok(None);
        };

        if line.is_empty() {
            return Ok(Some(None));
        }

        let line_record: LineRecord =
            serde_json::from_slice(line).map_err(ParserError::InvalidLineRecord)?;
        return Ok(Some(Some(line_record)));
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
    Coverage,
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
    /// TODO: datapoints
    #[serde(default)]
    Option<IgnoredAnyEq>,
);

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct LineSession(
    /// session id
    u32,
    /// coverage
    Coverage,
    /// TODO: branches
    #[serde(default)]
    Option<IgnoredAnyEq>,
    /// TODO: partials
    #[serde(default)]
    Option<IgnoredAnyEq>,
    /// TODO: complexity
    #[serde(default)]
    Option<IgnoredAnyEq>,
);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(try_from = "&str")]
pub enum CoverageType {
    #[default]
    Line,
    Branch,
    Method,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Coverage {
    Partial,
    BranchTaken(u32, u32),
    HitCount(u32),
}

impl<'de> Deserialize<'de> for Coverage {
    fn deserialize<D>(deserializer: D) -> Result<Coverage, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct CoverageVisitor;
        impl<'de> de::Visitor<'de> for CoverageVisitor {
            type Value = Coverage;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a coverage value")
            }

            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if v {
                    Ok(Coverage::Partial)
                } else {
                    Err(de::Error::invalid_value(de::Unexpected::Bool(v), &self))
                }
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Coverage::HitCount(value as u32))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let invalid = || de::Error::invalid_value(de::Unexpected::Str(v), &self);
                let (covered, total) = v.split_once('/').ok_or_else(invalid)?;

                let covered: u32 = covered.parse().map_err(|_| invalid())?;
                let total: u32 = total.parse().map_err(|_| invalid())?;
                Ok(Coverage::BranchTaken(covered, total))
            }
        }

        deserializer.deserialize_any(CoverageVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parsing_events() {
        let simple_line_record = LineRecord(
            Coverage::HitCount(1),
            None,
            vec![LineSession(0, Coverage::HitCount(1), None, None, None)],
            None,
            None,
            None,
        );

        let cases: &[(
            &[u8], // input
            HashMap<String, String>, // labels index
            &[(&[u32], &[Option<LineRecord>])], // chunks: session ids, line records
        )] = &[
            (
                // Header and one chunk with an empty line
                b"{}\n<<<<< end_of_header >>>>>\n{}\n",
                HashMap::default(),
                &[(&[], &[])],
            ),
            (
                // No header, one chunk with a populated line and an empty line
                b"{}\n[1, null, [[0, 1]]]\n",
                HashMap::default(),
                &[(&[], &[Some(simple_line_record.clone())])],
            ),
            (
                // No header, two chunks, the second having just one empty line
                b"{}\n[1, null, [[0, 1]]]\n\n<<<<< end_of_chunk >>>>>\n{}\n",
                HashMap::default(),
                &[(&[], &[Some(simple_line_record.clone())]), (&[], &[])],
            ),
            (
                // Header, two chunks, the second having multiple data lines and an empty line
                b"{}\n<<<<< end_of_header >>>>>\n{}\n[1, null, [[0, 1]]]\n\n<<<<< end_of_chunk >>>>>\n{}\n[1, null, [[0, 1]]]\n[1, null, [[0, 1]]]\n",
                HashMap::default(),
                &[
                    (&[], &[Some(simple_line_record.clone())]),
                    (
                        &[],
                        &[
                            Some(simple_line_record.clone()),
                            Some(simple_line_record.clone()),
                        ],
                    ),
                ],
            ),
        ];

        for (input, expected_labels_index, expected_chunks) in cases {
            let chunks_file = ChunksFile::new(input).unwrap();
            let mut chunks = chunks_file.chunks();

            assert_eq!(chunks_file.labels_index(), expected_labels_index);

            for (expected_sessions, expected_line_records) in *expected_chunks {
                let mut chunk = chunks.next_chunk().unwrap().unwrap();

                assert_eq!(chunk.present_sessions(), *expected_sessions);

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
