pub mod json;

use winnow::{
    stream::{AsBStr, Compare, ParseSlice, Stream, StreamIsPartial},
    token::take_while,
    PResult, Parser,
};

pub trait CharStream = Stream<Token = char> + StreamIsPartial;
pub trait StrStream = CharStream + for<'a> Compare<&'a str> + AsBStr
where
    <Self as Stream>::IterOffsets: Clone,
    <Self as Stream>::Slice: ParseSlice<f64>;

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
