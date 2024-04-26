pub use serde_json::{
    value::{Map as JsonMap, Number as JsonNumber},
    Value as JsonVal,
};
use winnow::{
    ascii::float,
    combinator::{alt, delimited, opt, preceded, repeat, separated, separated_pair},
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
pub fn parse_num<S: StrStream>(buf: &mut S) -> PResult<JsonNumber> {
    float.verify_map(JsonNumber::from_f64).parse_next(buf)
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
    delimited(
        '"',
        repeat(0.., parse_char).fold(String::new, |mut s, c| {
            s.push(c);
            s
        }),
        '"',
    )
    .parse_next(buf)
}

/*
 * Parsers in this section return collections which may contain multiple
 * types. They use the JsonVal enum to express that within Rust's type system
 * and are thus json-specific.
 */

/// Parses a series of json objects between `[]`s and separated by a comma,
/// returning a `Vec<JsonVal>`.
pub fn parse_array<S: StrStream>(buf: &mut S) -> PResult<Vec<JsonVal>> {
    delimited(('[', ws), separated(0.., json_value, ','), (ws, ']')).parse_next(buf)
}

/// Parses a key-value pair separated by a `:`, returning the key and value in a
/// tuple.
///
/// The key is parsed with `parse_str` and the value is a `JsonVal`.
pub fn parse_kv<S: StrStream>(buf: &mut S) -> PResult<(String, JsonVal)> {
    separated_pair(parse_str, (ws, ':', ws), json_value).parse_next(buf)
}

/// Parses a series of key-value pairs separated by a ':' and surrounded by
/// `{}`s, returning a `Map<String, JsonVal>`.
pub fn parse_object<S: StrStream>(buf: &mut S) -> PResult<JsonMap<String, JsonVal>> {
    // parse_kv.map(std::iter::once).map(serde_json::value::Map::from_iter).
    //    let start_map = parse_kv
    //        .map(std::iter::once)
    //        .map(serde_json::value::Map::from_iter);
    let add_to_map = |mut m: JsonMap<String, JsonVal>, (k, v)| {
        m.insert(k, v);
        m
    };
    delimited(
        ('{', ws),
        repeat(0.., preceded(opt((ws, ',', ws)), parse_kv)).fold(JsonMap::new, add_to_map),
        (ws, '}'),
    )
    .parse_next(buf)
}

/// Parses any json value, returning a `JsonVal`.
///
/// Whitespace is stripped before/after valid json values.
pub fn json_value<S: StrStream>(buf: &mut S) -> PResult<JsonVal> {
    delimited(
        ws,
        alt((
            parse_null.value(JsonVal::Null),
            parse_bool.map(JsonVal::Bool),
            parse_num.map(JsonVal::Number),
            parse_str.map(JsonVal::String),
            parse_array.map(JsonVal::Array),
            parse_object.map(JsonVal::Object),
        )),
        ws,
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
        delimited(ws, parse_str, (ws, ':', ws))
            .verify(move |s: &String| s == key)
            .parse_next(i)
    }
}

#[cfg(test)]
mod tests {
    use winnow::error::ErrMode;

    use super::*;

    #[test]
    fn test_parse_null() {
        // test that an exact match succeeds
        assert_eq!(parse_null.parse_peek("null"), Ok(("", "null")));

        // test that trailing whitespace is not consumed / that trailing
        // characters don't fail
        assert_eq!(parse_null.parse_peek("null "), Ok((" ", "null")));

        let malformed_test_cases = [
            " null", // test that whitespace is not stripped
            "anull", // test that unexpected leading tokens fail
        ];
        for test_case in &malformed_test_cases {
            assert_eq!(
                parse_null.parse_peek(*test_case),
                Err(ErrMode::Backtrack(ContextError::new())),
            );
        }
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

        let malformed_test_cases = [" true", " false", "atrue", "afalse"];
        for test_case in &malformed_test_cases {
            assert_eq!(
                parse_bool.parse_peek(*test_case),
                Err(ErrMode::Backtrack(ContextError::new())),
            );
        }
    }

    #[test]
    fn test_parse_num() {
        let json_num = |f| JsonNumber::from_f64(f).unwrap();
        // integers
        assert_eq!(parse_num.parse_peek("34949"), Ok(("", json_num(34949.0))));
        assert_eq!(parse_num.parse_peek("-34949"), Ok(("", json_num(-34949.0))));

        // decimals
        assert_eq!(
            parse_num.parse_peek("404.0101"),
            Ok(("", json_num(404.0101)))
        );
        assert_eq!(
            parse_num.parse_peek("-404.0101"),
            Ok(("", json_num(-404.0101)))
        );
        assert_eq!(parse_num.parse_peek(".05"), Ok(("", json_num(0.05))));
        assert_eq!(parse_num.parse_peek("-.05"), Ok(("", json_num(-0.05))));

        // scientific notation
        assert_eq!(parse_num.parse_peek("3.3e5"), Ok(("", json_num(330000.0))));
        assert_eq!(parse_num.parse_peek("3.3e+5"), Ok(("", json_num(330000.0))));
        assert_eq!(parse_num.parse_peek("3.3e-5"), Ok(("", json_num(0.000033))));
        assert_eq!(
            parse_num.parse_peek("-3.3e5"),
            Ok(("", json_num(-330000.0)))
        );
        assert_eq!(
            parse_num.parse_peek("-3.3e+5"),
            Ok(("", json_num(-330000.0)))
        );
        assert_eq!(
            parse_num.parse_peek("-3.3e-5"),
            Ok(("", json_num(-0.000033)))
        );
        assert_eq!(parse_num.parse_peek("3.3E5"), Ok(("", json_num(330000.0))));
        assert_eq!(parse_num.parse_peek("3.3E+5"), Ok(("", json_num(330000.0))));
        assert_eq!(parse_num.parse_peek("3.3E-5"), Ok(("", json_num(0.000033))));
        assert_eq!(
            parse_num.parse_peek("-3.3E5"),
            Ok(("", json_num(-330000.0)))
        );
        assert_eq!(
            parse_num.parse_peek("-3.3E+5"),
            Ok(("", json_num(-330000.0)))
        );
        assert_eq!(
            parse_num.parse_peek("-3.3E-5"),
            Ok(("", json_num(-0.000033)))
        );

        // trailing input
        assert_eq!(
            parse_num.parse_peek("3.abcde"),
            Ok(("abcde", json_num(3.0)))
        );
        assert_eq!(parse_num.parse_peek("3..."), Ok(("..", json_num(3.0))));
        assert_eq!(
            parse_num.parse_peek("3.455.303"),
            Ok((".303", json_num(3.455)))
        );

        let malformed_test_cases = [".", "aajad3.405"];
        for test_case in &malformed_test_cases {
            assert_eq!(
                parse_num.parse_peek(*test_case),
                Err(ErrMode::Backtrack(ContextError::new())),
            );
        }
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
        assert_eq!(
            parse_str.parse_peek("\"str with escaped quote \\\" \""),
            Ok(("", "str with escaped quote \" ".to_string()))
        );

        // trailing input
        assert_eq!(
            parse_str.parse_peek("\"hello world\", asdjasd"),
            Ok((", asdjasd", "hello world".to_string()))
        );

        // malformed
        let malformed_test_cases = [
            "no surrounding quotes",
            "\"no final quote",
            "no beginning quote\"",
            "\"str ending on escaped quote\\\"",
        ];
        for test_case in &malformed_test_cases {
            assert_eq!(
                parse_str.parse_peek(*test_case),
                Err(ErrMode::Backtrack(ContextError::new())),
            );
        }
    }

    #[test]
    fn test_parse_array() {
        assert_eq!(parse_array.parse_peek("[]"), Ok(("", vec![])));
        assert_eq!(
            parse_array.parse_peek("[3, null, true, false, \"str\", [], {}]"),
            Ok((
                "",
                vec![
                    JsonVal::Number(JsonNumber::from_f64(3.0).unwrap()),
                    JsonVal::Null,
                    JsonVal::Bool(true),
                    JsonVal::Bool(false),
                    JsonVal::String("str".to_string()),
                    JsonVal::Array(vec![]),
                    JsonVal::Object(JsonMap::new()),
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
                    JsonVal::Number(JsonNumber::from_f64(3.0).unwrap()),
                    JsonVal::Null,
                    JsonVal::Bool(true),
                    JsonVal::Bool(false),
                    JsonVal::String("str".to_string()),
                    JsonVal::Array(vec![]),
                    JsonVal::Object(JsonMap::new()),
                ]
            ))
        );

        // trailing input
        assert_eq!(parse_array.parse_peek("[]abcde"), Ok(("abcde", vec![])));
        assert_eq!(parse_array.parse_peek("[]]"), Ok(("]", vec![])));

        // malformed
        let malformed_test_cases = [
            "[4",
            "[4,]",
            "4[]",
            "[4, null, unquoted string]",
            "[4, null, {\"a\": 4]",
            "[4, null, [\"str\", false, true]",
        ];
        for test_case in &malformed_test_cases {
            assert_eq!(
                parse_array.parse_peek(*test_case),
                Err(ErrMode::Backtrack(ContextError::new())),
            );
        }
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
            Ok((
                "",
                (
                    "key".to_string(),
                    JsonVal::Number(JsonNumber::from_f64(4.4).unwrap())
                )
            )),
        );
        assert_eq!(
            parse_kv.parse_peek("\"key\": \"str value\""),
            Ok((
                "",
                ("key".to_string(), JsonVal::String("str value".to_string()))
            ))
        );
        assert_eq!(
            parse_kv.parse_peek("\"key\": []"),
            Ok(("", ("key".to_string(), JsonVal::Array(vec![]))))
        );
        assert_eq!(
            parse_kv.parse_peek("\"key\": {}"),
            Ok(("", ("key".to_string(), JsonVal::Object(JsonMap::new()))))
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
        let malformed_test_cases = [
            "key: null",
            "\"key: null",
            "\"key\":    ",
            "key\": null",
            "\"key\"; null",
            "key: null",
        ];
        for test_case in &malformed_test_cases {
            assert_eq!(
                parse_kv.parse_peek(*test_case),
                Err(ErrMode::Backtrack(ContextError::new())),
            );
        }
    }

    #[test]
    fn test_parse_object() {
        assert_eq!(parse_object.parse_peek("{}"), Ok(("", JsonMap::new())));
        assert_eq!(
            parse_object.parse_peek("{\"key\": null}"),
            Ok(("", JsonMap::from_iter([("key".to_string(), JsonVal::Null)])))
        );
        assert_eq!(
            parse_object.parse_peek("{\"key\": null, \"key2\": null}"),
            Ok((
                "",
                JsonMap::from_iter([
                    ("key".to_string(), JsonVal::Null),
                    ("key2".to_string(), JsonVal::Null)
                ])
            ))
        );
        assert_eq!(
            parse_object.parse_peek("{   \"key\" \n \t:\t\n null\n}"),
            Ok(("", JsonMap::from_iter([("key".to_string(), JsonVal::Null)])))
        );

        // trailing input
        assert_eq!(
            parse_object.parse_peek("{}abcde"),
            Ok(("abcde", JsonMap::new()))
        );
        assert_eq!(parse_object.parse_peek("{}}"), Ok(("}", JsonMap::new())));
        assert_eq!(parse_object.parse_peek("{}]"), Ok(("]", JsonMap::new())));

        // malformed
        let malformed_test_cases = [
            "{\"key\": null,}",
            "{\"key\": null",
            "\"key\": null",
            "key: null",
            "{\"key\": }",
            "{\"key\": , }",
            "abcde {\"key\": null}",
        ];
        for test_case in &malformed_test_cases {
            assert_eq!(
                parse_object.parse_peek(*test_case),
                Err(ErrMode::Backtrack(ContextError::new())),
            );
        }
    }

    #[test]
    fn test_json_value() {
        let test_cases = [
            "null",
            "true",
            "false",
            "3.404",
            "\"test string\"",
            "[]",
            "{}",
            "  \n\r\tnull\n   ",
            "\n\r true\r  ",
            "  \n false\n  ",
            "\n 3.404\t ",
            "\n \"test string\"\n  ",
            "\r\r\n\t []\t \t\r   ",
            "  \r {}\r\r\n",
            "[null, true, false, 3.4, \"str\", [], {}]",
            "{\"null\": null, \"true\": true, \"false\": false, \"num\": 3.4, \"str\": \"str\", \"array\": [null, 3.3], \"object\": {\"k\": 4.4}}",
        ];

        for test_case in &test_cases {
            let expected = serde_json::from_str(test_case).unwrap();
            assert_eq!(json_value.parse_peek(*test_case), Ok(("", expected)));
        }
    }

    #[test]
    fn test_specific_key() {
        assert_eq!(
            specific_key("files").parse_peek("\"files\": {\"src/report.rs"),
            Ok(("{\"src/report.rs", "files".to_string()))
        );

        // malformed
        let malformed_test_cases = [
            "files\": {\"src",
            "\"files: {\"src",
            "leading\"files\": {\"src",
        ];
        for test_case in &malformed_test_cases {
            assert_eq!(
                specific_key("files").parse_peek(*test_case),
                Err(ErrMode::Backtrack(ContextError::new()))
            );
        }
    }
}
