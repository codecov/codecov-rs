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

use std::{collections::HashMap, fmt};

use serde::{de, de::IgnoredAny, Deserialize};

#[derive(Debug)]
pub struct Parser<'d> {
    // TODO: these are pub just for debugging
    pub rest: &'d [u8],
    pub expecting: Expecting,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ParserEvent {
    EmptyLineRecord,
    LineRecord(LineRecord),
    EmptyChunk,
    FileHeader(FileHeader),
    ChunkHeader(ChunkHeader),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expecting {
    FileHeader,
    ChunkHeader,
    LineRecord,
    EndOfChunk,
}

const END_OF_CHUNK: &[u8] = b"<<<<< end_of_chunk >>>>>";
const END_OF_HEADER: &[u8] = b"<<<<< end_of_header >>>>>";

// `slice::split_once` is still unstable:
// <https://doc.rust-lang.org/std/primitive.slice.html#method.split_once>
fn slice_split_once(slice: &[u8], pred: u8) -> Option<(&[u8], &[u8])> {
    let index = slice.iter().position(|b| *b == pred)?;
    Some((&slice[..index], &slice[index + 1..]))
}

impl<'d> Parser<'d> {
    pub fn new(input: &'d [u8]) -> Self {
        Self {
            rest: input,
            expecting: Expecting::FileHeader,
        }
    }

    pub fn next(&mut self) -> Result<Option<ParserEvent>, ParserError> {
        loop {
            let Some((line, rest)) = slice_split_once(self.rest, b'\n') else {
                return Ok(None);
            };
            self.rest = rest;

            if self.expecting == Expecting::LineRecord {
                if line.is_empty() {
                    return Ok(Some(ParserEvent::EmptyLineRecord));
                }
                if line == END_OF_CHUNK {
                    self.expecting = Expecting::ChunkHeader;
                    continue;
                }

                let line_record: LineRecord =
                    serde_json::from_slice(line).map_err(ParserError::InvalidLineRecord)?;
                return Ok(Some(ParserEvent::LineRecord(line_record)));
            }

            if self.expecting == Expecting::EndOfChunk {
                if line != END_OF_CHUNK {
                    return Err(ParserError::UnexpectedInput);
                }

                self.expecting = Expecting::ChunkHeader;
                continue;
            }

            // else: expecting a file or chunk header

            // this is an empty chunk (header)
            if line == b"null" {
                self.expecting = Expecting::EndOfChunk;

                return Ok(Some(ParserEvent::EmptyChunk));
            }

            // otherwise, the header has to be a JSON object
            if !line.starts_with(b"{") {
                return Err(ParserError::UnexpectedInput);
            }
            if self.expecting == Expecting::FileHeader {
                if let Some((next_line, rest)) = slice_split_once(self.rest, b'\n') {
                    if next_line == END_OF_HEADER {
                        self.rest = rest;
                        self.expecting = Expecting::ChunkHeader;

                        let file_header: FileHeader =
                            serde_json::from_slice(line).map_err(ParserError::InvalidFileHeader)?;
                        return Ok(Some(ParserEvent::FileHeader(file_header)));
                    }
                }
            }
            // else: chunk header

            self.expecting = Expecting::LineRecord;

            let chunk_header: ChunkHeader =
                serde_json::from_slice(line).map_err(ParserError::InvalidChunkHeader)?;
            return Ok(Some(ParserEvent::ChunkHeader(chunk_header)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type ParserItem = Result<Option<ParserEvent>, ParserError>;

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

        let cases: &[(&[u8], &[ParserItem])] = &[
            (
                // Header and one chunk with an empty line
                b"{}\n<<<<< end_of_header >>>>>\n{}\n",
                &[
                    Ok(Some(ParserEvent::FileHeader(FileHeader::default()))),
                    Ok(Some(ParserEvent::ChunkHeader(ChunkHeader::default()))),
                    Ok(None),
                ],
            ),
            (
                // No header, one chunk with a populated line and an empty line
                b"{}\n[1, null, [[0, 1]]]\n",
                &[
                    Ok(Some(ParserEvent::ChunkHeader(ChunkHeader::default()))),
                    Ok(Some(ParserEvent::LineRecord(simple_line_record.clone()))),
                    Ok(None),
                ],
            ),
            (
                // No header, two chunks, the second having just one empty line
                b"{}\n[1, null, [[0, 1]]]\n\n<<<<< end_of_chunk >>>>>\n{}\n",
                &[
                    Ok(Some(ParserEvent::ChunkHeader(ChunkHeader::default()))),
                    Ok(Some(ParserEvent::LineRecord(simple_line_record.clone()))),
                    Ok(Some(ParserEvent::EmptyLineRecord)),
                    Ok(Some(ParserEvent::ChunkHeader(ChunkHeader::default()))),
                    Ok(None),
                ],
            ),
            (
                // Header, two chunks, the second having multiple data lines and an empty line
                b"{}\n<<<<< end_of_header >>>>>\n{}\n[1, null, [[0, 1]]]\n\n<<<<< end_of_chunk >>>>>\n{}\n[1, null, [[0, 1]]]\n[1, null, [[0, 1]]]\n",
                &[
                    Ok(Some(ParserEvent::FileHeader(FileHeader::default()))),
                    Ok(Some(ParserEvent::ChunkHeader(ChunkHeader::default()))),
                    Ok(Some(ParserEvent::LineRecord(simple_line_record.clone()))),
                    Ok(Some(ParserEvent::EmptyLineRecord)),
                    Ok(Some(ParserEvent::ChunkHeader(ChunkHeader::default()))),
                    Ok(Some(ParserEvent::LineRecord(simple_line_record.clone()))),
                    Ok(Some(ParserEvent::LineRecord(simple_line_record.clone()))),
                    Ok(None),
                ],
            ),
        ];

        for (input, expected_events) in cases {
            let mut parser = Parser::new(input);

            for expected_event in *expected_events {
                dbg!(std::str::from_utf8(parser.rest).unwrap(), parser.expecting);
                let event = parser.next();
                assert_eq!(dbg!(event), *expected_event);
            }
        }
    }
}
