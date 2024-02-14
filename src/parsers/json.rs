use std::collections::HashMap;

use winnow::{
    ascii::float,
    combinator::{alt, fold_repeat, preceded, separated, separated_pair, terminated},
    error::ContextError,
    stream::Stream,
    token::none_of,
    PResult, Parser,
};

use crate::parsers::{ws, StrStream};

/*
 * Parsers in this section return raw Rust types and may be useful to other
 * parsers.
 */

/// Parses the string "null", returning "null" as a slice.
pub fn parse_null<S: StrStream>(buf: &mut S) -> PResult<<S as Stream>::Slice> {
    "null".parse_next(buf)
}

/// Parses the strings "true" and "false", returning the corresponding `bool`s.
pub fn parse_bool<S: StrStream>(buf: &mut S) -> PResult<bool> {
    alt(("true".value(true), "false".value(false))).parse_next(buf)
}

/// Parses numeric strings, returning the value as an f64.
/// Handles scientific notation.
pub fn parse_num<S: StrStream>(buf: &mut S) -> PResult<f64> {
    float.parse_next(buf)
}

/// Parses a single character (which may be escaped), returning a `char`.
///
/// ```
/// # use codecov_rs::parsers::json::parse_char;
/// # use winnow::Parser;
/// assert_eq!(parse_char.parse_peek("a"), Ok(("", 'a')));
/// assert_eq!(parse_char.parse_peek("\\n"), Ok(("", '\n')));
/// ```
///
/// Consumes two characters if the first is a `\`.
pub fn parse_char<S: StrStream>(buf: &mut S) -> PResult<char> {
    let c = none_of('"').parse_next(buf);
    match c {
        Ok('\\') => {
            let escaped = buf.next_token().unwrap(); // TODO handle error
            match escaped {
                '"' | '\'' | '\\' => Ok(escaped),
                'n' => Ok('\n'),
                'r' => Ok('\r'),
                't' => Ok('\t'),
                _ => panic!("Unrecognized escape: {}", escaped),
            }
        }
        _ => c,
    }
}

/// Parses a series of characters between two `'"'` delimiters, returning a
/// `String`.
///
/// Characters are parsed with `parse_char` and thus may be escaped.
pub fn parse_str<S: StrStream>(buf: &mut S) -> PResult<String> {
    preceded(
        '"',
        terminated(
            fold_repeat(0.., parse_char, String::new, |mut s, c| {
                s.push(c);
                s
            }),
            '"',
        ),
    )
    .parse_next(buf)
}

/*
 * Parsers in this section return collections which may contain multiple
 * types. They use the JsonVal enum to express that within Rust's type system
 * and are thus json-specific.
 */

/// Enum with different constructors for each json type.
#[derive(Debug, PartialEq, Clone)]
pub enum JsonVal {
    Null,
    Bool(bool),
    Num(f64),
    Str(String),
    Array(Vec<JsonVal>),
    Object(HashMap<String, JsonVal>),
}

/// Parses a series of json objects between `[]`s and separated by a comma,
/// returning a `Vec<JsonVal>`.
pub fn parse_array<S: StrStream>(buf: &mut S) -> PResult<Vec<JsonVal>> {
    preceded(
        ('[', ws),
        terminated(separated(0.., json_value, ','), (ws, ']')),
    )
    .parse_next(buf)
}

/// Parses a key-value pair separated by a `:`, returning the key and value in a
/// tuple.
///
/// The key is parsed with `parse_str` and the value is a `JsonVal`.
pub fn parse_kv<S: StrStream>(buf: &mut S) -> PResult<(String, JsonVal)> {
    separated_pair(parse_str, (ws, ':', ws), json_value).parse_next(buf)
}

/// Parses a series of key-value pairs separated by a ':' and surrounded by
/// `{}`s, returning a `HashMap<String, JsonVal>`.
pub fn parse_object<S: StrStream>(buf: &mut S) -> PResult<HashMap<String, JsonVal>> {
    preceded(
        ('{', ws),
        terminated(separated(0.., parse_kv, (ws, ',', ws)), (ws, '}')),
    )
    .parse_next(buf)
}

/// Parses any json value, returning a `JsonVal`.
///
/// Whitespace is stripped before/after valid json values.
pub fn json_value<S: StrStream>(buf: &mut S) -> PResult<JsonVal> {
    preceded(
        ws,
        terminated(
            alt((
                parse_null.value(JsonVal::Null),
                parse_bool.map(JsonVal::Bool),
                parse_num.map(JsonVal::Num),
                parse_str.map(JsonVal::Str),
                parse_array.map(JsonVal::Array),
                parse_object.map(JsonVal::Object),
            )),
            ws,
        ),
    )
    .parse_next(buf)
}

/// Parses the next key + `:` delimiter and asserts that the key matches the
/// passed-in value. To get the corresponding value, parse with something like:
///
/// ```
/// # use codecov_rs::parsers::json::{specific_key, json_value, JsonVal};
/// # use winnow::combinator::preceded;
/// # use winnow::Parser;
/// let expected = Ok(("", JsonVal::Array(vec![])));
/// let result = preceded(specific_key("files"), json_value).parse_peek("\"files\": []");
/// assert_eq!(expected, result);
/// ```
///
/// Not used in generic json parsing but helpful when writing parsers for json
/// data that adheres to a schema.
pub fn specific_key<S: StrStream>(key: &str) -> impl Parser<S, String, ContextError> + '_ {
    move |i: &mut S| {
        preceded(ws, terminated(parse_str, (ws, ':', ws)))
            .verify(move |s: &String| s == key)
            .parse_next(i)
    }
}

#[cfg(test)]
mod tests {
    use winnow::error::{ContextError, ErrMode};

    use super::*;

    #[test]
    fn test_parse_null() {
        // test that an exact match succeeds
        assert_eq!(parse_null.parse_peek("null"), Ok(("", "null")));

        // test that trailing whitespace is not consumed / that trailing
        // characters don't fail
        assert_eq!(parse_null.parse_peek("null "), Ok((" ", "null")));

        // test that whitespace is not stripped
        assert_eq!(
            parse_null.parse_peek(" null"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );

        // test that unexpected leading tokens fail
        assert_eq!(
            parse_null.parse_peek("anull"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
    }

    #[test]
    fn test_parse_bool() {
        // test that exact matches succeed
        assert_eq!(parse_bool.parse_peek("true"), Ok(("", true)));
        assert_eq!(parse_bool.parse_peek("false"), Ok(("", false)));

        // test that trailing whitespace is not consumed / that trailing
        // characters don't fail
        assert_eq!(parse_bool.parse_peek("true "), Ok((" ", true)));
        assert_eq!(parse_bool.parse_peek("false "), Ok((" ", false)));

        // test that whitespace is not stripped
        assert_eq!(
            parse_bool.parse_peek(" true"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
        assert_eq!(
            parse_bool.parse_peek(" false"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );

        // test that unexpected leading tokens fail
        assert_eq!(
            parse_bool.parse_peek("atrue"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
        assert_eq!(
            parse_bool.parse_peek("afalse"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
    }

    #[test]
    fn test_parse_num() {
        // integers
        assert_eq!(parse_num.parse_peek("34949"), Ok(("", 34949.0)));
        assert_eq!(parse_num.parse_peek("-34949"), Ok(("", -34949.0)));

        // decimals
        assert_eq!(parse_num.parse_peek("404.0101"), Ok(("", 404.0101)));
        assert_eq!(parse_num.parse_peek("-404.0101"), Ok(("", -404.0101)));
        assert_eq!(parse_num.parse_peek(".05"), Ok(("", 0.05)));
        assert_eq!(parse_num.parse_peek("-.05"), Ok(("", -0.05)));

        // scientific notation
        assert_eq!(parse_num.parse_peek("3.3e5"), Ok(("", 330000.0)));
        assert_eq!(parse_num.parse_peek("3.3e+5"), Ok(("", 330000.0)));
        assert_eq!(parse_num.parse_peek("3.3e-5"), Ok(("", 0.000033)));
        assert_eq!(parse_num.parse_peek("-3.3e5"), Ok(("", -330000.0)));
        assert_eq!(parse_num.parse_peek("-3.3e+5"), Ok(("", -330000.0)));
        assert_eq!(parse_num.parse_peek("-3.3e-5"), Ok(("", -0.000033)));
        assert_eq!(parse_num.parse_peek("3.3E5"), Ok(("", 330000.0)));
        assert_eq!(parse_num.parse_peek("3.3E+5"), Ok(("", 330000.0)));
        assert_eq!(parse_num.parse_peek("3.3E-5"), Ok(("", 0.000033)));
        assert_eq!(parse_num.parse_peek("-3.3E5"), Ok(("", -330000.0)));
        assert_eq!(parse_num.parse_peek("-3.3E+5"), Ok(("", -330000.0)));
        assert_eq!(parse_num.parse_peek("-3.3E-5"), Ok(("", -0.000033)));

        // trailing input
        assert_eq!(parse_num.parse_peek("3.abcde"), Ok(("abcde", 3.0)));
        assert_eq!(parse_num.parse_peek("3..."), Ok(("..", 3.0)));
        assert_eq!(parse_num.parse_peek("3.455.303"), Ok((".303", 3.455)));

        // malformed
        assert_eq!(
            parse_num.parse_peek("."),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
        assert_eq!(
            parse_num.parse_peek("aajad3.405"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
    }

    #[test]
    fn test_parse_char() {
        assert_eq!(parse_char.parse_peek("a"), Ok(("", 'a')));

        // escaping
        assert_eq!(parse_char.parse_peek("\\n"), Ok(("", '\n')));
        assert_eq!(parse_char.parse_peek("\\r"), Ok(("", '\r')));
        assert_eq!(parse_char.parse_peek("\\t"), Ok(("", '\t')));
        assert_eq!(parse_char.parse_peek("\\\""), Ok(("", '"')));
        assert_eq!(parse_char.parse_peek("\\\'"), Ok(("", '\'')));
        assert_eq!(parse_char.parse_peek("\\\\"), Ok(("", '\\')));

        // pre-escaped characters
        assert_eq!(parse_char.parse_peek("\n"), Ok(("", '\n')));
        assert_eq!(parse_char.parse_peek("\r"), Ok(("", '\r')));
        assert_eq!(parse_char.parse_peek("\t"), Ok(("", '\t')));
        assert_eq!(parse_char.parse_peek("'"), Ok(("", '\'')));

        // trailing input
        assert_eq!(parse_char.parse_peek("abcde"), Ok(("bcde", 'a')));
        assert_eq!(parse_char.parse_peek("\\nbcde"), Ok(("bcde", '\n')));

        // can't lead with "
        assert_eq!(
            parse_char.parse_peek("\""),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
    }

    #[test]
    fn test_parse_str() {
        // normal cases
        assert_eq!(parse_str.parse_peek("\"\""), Ok(("", "".to_string())));
        assert_eq!(
            parse_str.parse_peek("\"hello world\""),
            Ok(("", "hello world".to_string()))
        );
        assert_eq!(
            parse_str.parse_peek("\"string with\nnewline\""),
            Ok(("", "string with\nnewline".to_string()))
        );
        assert_eq!(
            parse_str.parse_peek("\"string with\\nnewline\""),
            Ok(("", "string with\nnewline".to_string()))
        );
        assert_eq!(
            parse_str.parse_peek("\"str with backslash \\\\\""),
            Ok(("", "str with backslash \\".to_string()))
        );

        // trailing input
        assert_eq!(
            parse_str.parse_peek("\"hello world\", asdjasd"),
            Ok((", asdjasd", "hello world".to_string()))
        );

        // malformed
        assert_eq!(
            parse_str.parse_peek("no surrounding quotes"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
        assert_eq!(
            parse_str.parse_peek("\"no final quote"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
        assert_eq!(
            parse_str.parse_peek("no beginning quote\""),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
        assert_eq!(
            parse_str.parse_peek("\"str ending on escaped quote\\\""),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
    }

    #[test]
    fn test_parse_array() {
        assert_eq!(parse_array.parse_peek("[]"), Ok(("", vec![])));
        assert_eq!(
            parse_array.parse_peek("[3, null, true, false, \"str\", [], {}]"),
            Ok((
                "",
                vec![
                    JsonVal::Num(3.0),
                    JsonVal::Null,
                    JsonVal::Bool(true),
                    JsonVal::Bool(false),
                    JsonVal::Str("str".to_string()),
                    JsonVal::Array(vec![]),
                    JsonVal::Object(HashMap::new()),
                ]
            ))
        );

        // same test case as above but with superfluous whitespace peppered around
        assert_eq!(
            parse_array
                .parse_peek("[ 3    ,null ,  true , \n\t\tfalse, \t \"str\", [\n], {\r  \t \n}  ]"),
            Ok((
                "",
                vec![
                    JsonVal::Num(3.0),
                    JsonVal::Null,
                    JsonVal::Bool(true),
                    JsonVal::Bool(false),
                    JsonVal::Str("str".to_string()),
                    JsonVal::Array(vec![]),
                    JsonVal::Object(HashMap::new()),
                ]
            ))
        );

        // trailing input
        assert_eq!(parse_array.parse_peek("[]abcde"), Ok(("abcde", vec![])));
        assert_eq!(parse_array.parse_peek("[]]"), Ok(("]", vec![])));

        // malformed
        assert_eq!(
            parse_array.parse_peek("[4"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
        assert_eq!(
            parse_array.parse_peek("[4,]"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
        assert_eq!(
            parse_array.parse_peek("4[]"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
        assert_eq!(
            parse_array.parse_peek("[4, null, unquoted string]"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
        assert_eq!(
            parse_array.parse_peek("[4, null, {\"a\": 4]"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
        assert_eq!(
            parse_array.parse_peek("[4, null, [\"str\", false, true]"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
    }

    #[test]
    fn test_parse_kv() {
        assert_eq!(
            parse_kv.parse_peek("\"key\": null"),
            Ok(("", ("key".to_string(), JsonVal::Null)))
        );
        assert_eq!(
            parse_kv.parse_peek("\"key\": true"),
            Ok(("", ("key".to_string(), JsonVal::Bool(true))))
        );
        assert_eq!(
            parse_kv.parse_peek("\"key\": false"),
            Ok(("", ("key".to_string(), JsonVal::Bool(false))))
        );
        assert_eq!(
            parse_kv.parse_peek("\"key\": 4.4"),
            Ok(("", ("key".to_string(), JsonVal::Num(4.4))))
        );
        assert_eq!(
            parse_kv.parse_peek("\"key\": \"str value\""),
            Ok((
                "",
                ("key".to_string(), JsonVal::Str("str value".to_string()))
            ))
        );
        assert_eq!(
            parse_kv.parse_peek("\"key\": []"),
            Ok(("", ("key".to_string(), JsonVal::Array(vec![]))))
        );
        assert_eq!(
            parse_kv.parse_peek("\"key\": {}"),
            Ok(("", ("key".to_string(), JsonVal::Object(HashMap::new()))))
        );

        // empty string as a key is fine
        assert_eq!(
            parse_kv.parse_peek("\"\": null"),
            Ok(("", ("".to_string(), JsonVal::Null)))
        );

        // pepper superfluous whitespace around
        assert_eq!(
            parse_kv.parse_peek("\"key\"\n\t  :\n \t null"),
            Ok(("", ("key".to_string(), JsonVal::Null)))
        );

        // trailing input
        assert_eq!(
            parse_kv.parse_peek("\"key\": null, \"key2\": null"),
            Ok((", \"key2\": null", ("key".to_string(), JsonVal::Null)))
        );
        assert_eq!(
            parse_kv.parse_peek("\"key\": null}"),
            Ok(("}", ("key".to_string(), JsonVal::Null)))
        );
        assert_eq!(
            parse_kv.parse_peek("\"key\": null]"),
            Ok(("]", ("key".to_string(), JsonVal::Null)))
        );
        assert_eq!(
            parse_kv.parse_peek("\"key\": nulla"),
            Ok(("a", ("key".to_string(), JsonVal::Null)))
        );

        // malformed
        assert_eq!(
            parse_kv.parse_peek("key: null"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
        assert_eq!(
            parse_kv.parse_peek("\"key: null"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
        assert_eq!(
            parse_kv.parse_peek("\"key\":   "),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
        assert_eq!(
            parse_kv.parse_peek("key\": null"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
        assert_eq!(
            parse_kv.parse_peek("\"key\"; null"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
        assert_eq!(
            parse_kv.parse_peek("key: null"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
        assert_eq!(
            parse_kv.parse_peek("key: null"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
        assert_eq!(
            parse_kv.parse_peek("key: null"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
    }

    #[test]
    fn test_parse_object() {
        assert_eq!(parse_object.parse_peek("{}"), Ok(("", HashMap::new())));
        assert_eq!(
            parse_object.parse_peek("{\"key\": null}"),
            Ok(("", HashMap::from([("key".to_string(), JsonVal::Null)])))
        );
        assert_eq!(
            parse_object.parse_peek("{\"key\": null, \"key2\": null}"),
            Ok((
                "",
                HashMap::from([
                    ("key".to_string(), JsonVal::Null),
                    ("key2".to_string(), JsonVal::Null)
                ])
            ))
        );
        assert_eq!(
            parse_object.parse_peek("{   \"key\" \n \t:\t\n null\n}"),
            Ok(("", HashMap::from([("key".to_string(), JsonVal::Null)])))
        );

        // trailing input
        assert_eq!(
            parse_object.parse_peek("{}abcde"),
            Ok(("abcde", HashMap::new()))
        );
        assert_eq!(parse_object.parse_peek("{}}"), Ok(("}", HashMap::new())));
        assert_eq!(parse_object.parse_peek("{}]"), Ok(("]", HashMap::new())));

        // malformed
        assert_eq!(
            parse_object.parse_peek("{\"key\": null,}"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
        assert_eq!(
            parse_object.parse_peek("{\"key\": null"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
        assert_eq!(
            parse_object.parse_peek("\"key\": null"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
        assert_eq!(
            parse_object.parse_peek("key: null"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
        assert_eq!(
            parse_object.parse_peek("{\"key\": }"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
        assert_eq!(
            parse_object.parse_peek("{\"key\": , }"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
        assert_eq!(
            parse_object.parse_peek("abcde {\"key\": null}"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
    }

    #[test]
    fn test_json_value() {
        assert_eq!(json_value.parse_peek("null"), Ok(("", JsonVal::Null)));
        assert_eq!(json_value.parse_peek("true"), Ok(("", JsonVal::Bool(true))));
        assert_eq!(
            json_value.parse_peek("false"),
            Ok(("", JsonVal::Bool(false)))
        );
        assert_eq!(
            json_value.parse_peek("3.404"),
            Ok(("", JsonVal::Num(3.404)))
        );
        assert_eq!(
            json_value.parse_peek("\"test string\""),
            Ok(("", JsonVal::Str("test string".to_string())))
        );
        assert_eq!(
            json_value.parse_peek("[]"),
            Ok(("", JsonVal::Array(vec![])))
        );
        assert_eq!(
            json_value.parse_peek("{}"),
            Ok(("", JsonVal::Object(HashMap::new())))
        );

        assert_eq!(
            json_value.parse_peek("  \n\r\tnull\n   "),
            Ok(("", JsonVal::Null))
        );
        assert_eq!(
            json_value.parse_peek("\n\r true\r  "),
            Ok(("", JsonVal::Bool(true)))
        );
        assert_eq!(
            json_value.parse_peek("  \n false\n  "),
            Ok(("", JsonVal::Bool(false)))
        );
        assert_eq!(
            json_value.parse_peek("\n 3.404\t "),
            Ok(("", JsonVal::Num(3.404)))
        );
        assert_eq!(
            json_value.parse_peek("\n \"test string\"\n  "),
            Ok(("", JsonVal::Str("test string".to_string())))
        );
        assert_eq!(
            json_value.parse_peek("\r\r\n\t []\t \t\r   "),
            Ok(("", JsonVal::Array(vec![])))
        );
        assert_eq!(
            json_value.parse_peek("  \r {}\r\r\n"),
            Ok(("", JsonVal::Object(HashMap::new())))
        );

        // more complicated inputs
        assert_eq!(
            json_value.parse_peek("[null, true, false, 3.4, \"str\", [], {}]"),
            Ok((
                "",
                JsonVal::Array(vec![
                    JsonVal::Null,
                    JsonVal::Bool(true),
                    JsonVal::Bool(false),
                    JsonVal::Num(3.4),
                    JsonVal::Str("str".to_string()),
                    JsonVal::Array(vec![]),
                    JsonVal::Object(HashMap::new())
                ])
            ))
        );
        assert_eq!(
            json_value.parse_peek(
                "{\"null\": null, \"true\": true, \"false\": false, \"num\": 3.4, \"str\": \"str\", \"array\": [null, 3.3], \"object\": {\"k\": 4.4}}"
            ),
            Ok((
                "",
                JsonVal::Object(HashMap::from([
                    ("null".to_string(), JsonVal::Null),
                    ("true".to_string(), JsonVal::Bool(true)),
                    ("false".to_string(), JsonVal::Bool(false)),
                    ("num".to_string(), JsonVal::Num(3.4)),
                    ("str".to_string(), JsonVal::Str("str".to_string())),
                    ("array".to_string(), JsonVal::Array(vec![JsonVal::Null, JsonVal::Num(3.3)])),
                    ("object".to_string(), JsonVal::Object(HashMap::from([("k".to_string(), JsonVal::Num(4.4))])))
                ]))
            ))
        );
    }

    #[test]
    fn test_specific_key() {
        assert_eq!(
            specific_key("files").parse_peek("\"files\": {\"src/report.rs"),
            Ok(("{\"src/report.rs", "files".to_string()))
        );

        // malformed
        assert_eq!(
            specific_key("files").parse_peek("files\": {\"src"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
        assert_eq!(
            specific_key("files").parse_peek("\"files: {\"src"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
        assert_eq!(
            specific_key("files").parse_peek("leading\"files\": {\"src"),
            Err(ErrMode::Backtrack(ContextError::new()))
        );
    }
}
