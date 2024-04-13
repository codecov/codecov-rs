pub mod json;

#[cfg(feature = "pyreport_shim")]
pub mod pyreport_shim;

use std::{fmt, fmt::Debug, marker::PhantomData};

use winnow::{
    ascii::float,
    combinator::alt,
    error::ParserError,
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
pub struct ReportBuilderCtx<R: Report, B: ReportBuilder<R>> {
    pub report_builder: B,
    _phantom: PhantomData<R>,
}

impl<R: Report, B: ReportBuilder<R>> ReportBuilderCtx<R, B> {
    pub fn new(report_builder: B) -> ReportBuilderCtx<R, B> {
        ReportBuilderCtx {
            report_builder,
            _phantom: PhantomData,
        }
    }
}

impl<R: Report, B: ReportBuilder<R>> Debug for ReportBuilderCtx<R, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReportBuilderCtx")
            //            .field("report_builder", &self.report_builder)
            .finish()
    }
}

/// Characters considered whitespace for the `ws` parser.
const WHITESPACE: &[char] = &[' ', '\t', '\n', '\r'];

/// Parses a series of whitespace characters, returning the series as a slice.
pub fn ws<S: CharStream>(buf: &mut S) -> PResult<<S as Stream>::Slice> {
    take_while(0.., WHITESPACE).parse_next(buf)
}

/// Parses an unsigned decimal number with support for scientific notation.
/// Truncates floats, clamps numbers not in the `u32` range.
pub fn parse_u32<S: StrStream>(buf: &mut S) -> PResult<u32> {
    float.map(move |x: f64| x as u32).parse_next(buf)
}

pub fn nullable<Input: StrStream, Output, Error, ParseNext>(
    parser: ParseNext,
) -> impl Parser<Input, Option<Output>, Error>
where
    ParseNext: Parser<Input, Output, Error>,
    Error: ParserError<Input>,
    Output: Clone,
{
    alt((parser.map(Some), "null".value(None::<Output>)))
}

#[cfg(test)]
mod tests {
    use winnow::{
        ascii::{alpha1, dec_uint, float},
        error::{ContextError, ErrMode},
    };

    use super::*;

    #[test]
    fn test_ws() {
        assert_eq!(ws.parse_peek(" \r\t\n"), Ok(("", " \r\t\n")));
        assert_eq!(ws.parse_peek("  asd"), Ok(("asd", "  ")));
        assert_eq!(ws.parse_peek("asd  "), Ok(("asd  ", "")));
    }

    #[test]
    fn test_parse_u32() {
        assert_eq!(parse_u32.parse_peek("30"), Ok(("", 30)));
        assert_eq!(parse_u32.parse_peek("30 "), Ok((" ", 30)));

        // Floats are truncated, not rounded
        assert_eq!(parse_u32.parse_peek("30.6 "), Ok((" ", 30)));
        assert_eq!(parse_u32.parse_peek("30.1 "), Ok((" ", 30)));

        // Scientific notation
        assert_eq!(parse_u32.parse_peek("1e+0"), Ok(("", 1)));
        assert_eq!(parse_u32.parse_peek("5.2e+5"), Ok(("", 520000)));
        assert_eq!(parse_u32.parse_peek("1.2345e+2"), Ok(("", 123)));
        assert_eq!(parse_u32.parse_peek("2.7e-5"), Ok(("", 0)));

        // Numbers are clamped to `u32` range
        assert_eq!(parse_u32.parse_peek("5000000000"), Ok(("", 4294967295)));
        assert_eq!(parse_u32.parse_peek("2.7e+20"), Ok(("", 4294967295)));
        assert_eq!(parse_u32.parse_peek("-1"), Ok(("", 0)));
        assert_eq!(parse_u32.parse_peek("-100"), Ok(("", 0)));
        assert_eq!(parse_u32.parse_peek("-4.2"), Ok(("", 0)));
        assert_eq!(parse_u32.parse_peek("-4.2e-1"), Ok(("", 0)));

        // Malformed
        assert_eq!(
            parse_u32.parse_peek(" 30"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
        assert_eq!(
            parse_u32.parse_peek("x30"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
    }

    #[test]
    fn test_nullable() {
        // with floats
        assert_eq!(
            nullable(float::<&str, f64, ContextError>).parse_peek("3.4"),
            Ok(("", Some(3.4)))
        );
        assert_eq!(
            nullable(float::<&str, f64, ContextError>).parse_peek("null"),
            Ok(("", None))
        );
        assert_eq!(
            nullable(float::<&str, f64, ContextError>).parse_peek("malformed"),
            Err(ErrMode::Backtrack(ContextError::new())),
        );
        assert_eq!(
            nullable(float::<&str, f64, ContextError>).parse_peek("nul"),
            Err(ErrMode::Backtrack(ContextError::new())),
        );

        // with decimals
        assert_eq!(
            nullable(dec_uint::<&str, u64, ContextError>).parse_peek("3.4"),
            Ok((".4", Some(3)))
        );
        assert_eq!(
            nullable(dec_uint::<&str, u64, ContextError>).parse_peek("null"),
            Ok(("", None))
        );
        assert_eq!(
            nullable(dec_uint::<&str, u64, ContextError>).parse_peek("malformed"),
            Err(ErrMode::Backtrack(ContextError::new())),
        );
        assert_eq!(
            nullable(dec_uint::<&str, u64, ContextError>).parse_peek("nul"),
            Err(ErrMode::Backtrack(ContextError::new())),
        );

        // with chars
        assert_eq!(
            nullable(alpha1::<&str, ContextError>).parse_peek("abcde"),
            Ok(("", Some("abcde")))
        );
        // this is an edge case - `alpha1` has no problem matching `"null"` so we should
        // let it
        assert_eq!(
            nullable(alpha1::<&str, ContextError>).parse_peek("null"),
            Ok(("", Some("null")))
        );
        assert_eq!(
            nullable(alpha1::<&str, ContextError>).parse_peek(".123."),
            Err(ErrMode::Backtrack(ContextError::new())),
        );
    }
}
