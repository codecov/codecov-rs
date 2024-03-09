pub mod json;

#[cfg(feature = "pyreport_shim")]
pub mod pyreport_shim;

use std::{fmt, fmt::Debug, marker::PhantomData};

use winnow::{
    stream::{AsBStr, Compare, ParseSlice, Stream, StreamIsPartial},
    token::take_while,
    PResult, Parser, Stateful,
};

use crate::report::{Report, ReportBuilder};

pub trait CharStream = Stream<Token = char> + StreamIsPartial;
pub trait StrStream = CharStream + for<'a> Compare<&'a str> + AsBStr
where
    <Self as Stream>::IterOffsets: Clone,
    <Self as Stream>::Slice: ParseSlice<f64>;

#[derive(PartialEq)]
pub struct ParseCtx<R: Report, B: ReportBuilder<R>> {
    pub report_builder: B,
    _phantom: PhantomData<R>,
}

impl<R: Report, B: ReportBuilder<R>> ParseCtx<R, B> {
    pub fn new(report_builder: B) -> ParseCtx<R, B> {
        ParseCtx {
            report_builder: report_builder,
            _phantom: PhantomData,
        }
    }
}

impl<R: Report, B: ReportBuilder<R>> Debug for ParseCtx<R, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParseCtx")
            //            .field("report_builder", &self.report_builder)
            .finish()
    }
}

pub type ReportOutputStream<S, R, B> = Stateful<S, ParseCtx<R, B>>;

/// Characters considered whitespace for the `ws` parser.
const WHITESPACE: &[char] = &[' ', '\t', '\n', '\r'];

/// Parses a series of whitespace characters, returning the series as a slice.
pub fn ws<S: CharStream>(buf: &mut S) -> PResult<<S as Stream>::Slice> {
    take_while(0.., WHITESPACE).parse_next(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ws() {
        assert_eq!(ws.parse_peek(" \r\t\n"), Ok(("", " \r\t\n")));
        assert_eq!(ws.parse_peek("  asd"), Ok(("asd", "  ")));
        assert_eq!(ws.parse_peek("asd  "), Ok(("asd  ", "")));
    }
}
