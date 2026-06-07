//! Neutral JSON/encoding helpers shared by checkpoint and passive storage (D96).
//!
//! This module owns the strict/canonical JSON byte contract used by storage,
//! checkpoints, and future V1 hash inputs. It is deliberately independent of
//! graph lifecycle and storage tier types.

use std::cmp::Ordering;
use std::error::Error;
use std::fmt;

use serde_json::Value;

pub type JsonValue = Value;
pub type JsonCodecResult<T> = Result<T, JsonCodecError>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JsonCodecError {
    Encode(String),
    Decode(String),
    Validation(String),
}

impl JsonCodecError {
    pub fn encode(message: impl Into<String>) -> Self {
        Self::Encode(message.into())
    }

    pub fn decode(message: impl Into<String>) -> Self {
        Self::Decode(message.into())
    }

    pub fn validation(message: impl Into<String>) -> Self {
        Self::Validation(message.into())
    }
}

impl fmt::Display for JsonCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Encode(message) => write!(f, "json encode failed: {message}"),
            Self::Decode(message) => write!(f, "json decode failed: {message}"),
            Self::Validation(message) => f.write_str(message),
        }
    }
}

impl Error for JsonCodecError {}

/// Typed byte codec for D82 storage binding helpers and D96 JSON surfaces.
pub trait Codec<T> {
    fn encode(&self, value: &T) -> JsonCodecResult<Vec<u8>>;
    fn decode(&self, bytes: &[u8]) -> JsonCodecResult<T>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct JsonCodec;

impl Codec<JsonValue> for JsonCodec {
    fn encode(&self, value: &JsonValue) -> JsonCodecResult<Vec<u8>> {
        stable_json_string(value).map(String::into_bytes)
    }

    fn decode(&self, bytes: &[u8]) -> JsonCodecResult<JsonValue> {
        serde_json::from_slice(bytes).map_err(|err| JsonCodecError::decode(err.to_string()))
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct StrictJsonCodec;

impl Codec<JsonValue> for StrictJsonCodec {
    fn encode(&self, value: &JsonValue) -> JsonCodecResult<Vec<u8>> {
        strict_canonical_json_bytes(value)
    }

    fn decode(&self, bytes: &[u8]) -> JsonCodecResult<JsonValue> {
        strict_json_decode(bytes)
    }
}

pub fn json_codec_for<T>() -> JsonCodec {
    let _ = std::marker::PhantomData::<T>;
    JsonCodec
}

pub fn strict_json_codec_for<T>() -> StrictJsonCodec {
    let _ = std::marker::PhantomData::<T>;
    StrictJsonCodec
}

/// Stable JSON string with object keys sorted by JavaScript UTF-16 code-unit order.
pub fn stable_json_string(value: &JsonValue) -> JsonCodecResult<String> {
    validate_strict_json_value(value, "$")?;
    canonical_json_string(value)
}

/// D113 neutral helper for strict canonical JSON UTF-8 bytes.
pub fn strict_canonical_json_bytes(value: &JsonValue) -> JsonCodecResult<Vec<u8>> {
    stable_json_string(value).map(String::into_bytes)
}

/// Decode strict canonical JSON bytes, rejecting malformed UTF-8, duplicate keys,
/// non-canonical byte shape, and values that fail the shared strict validator.
pub fn strict_json_decode(bytes: &[u8]) -> JsonCodecResult<JsonValue> {
    let text = std::str::from_utf8(bytes).map_err(|err| JsonCodecError::decode(err.to_string()))?;
    assert_no_duplicate_json_object_keys(text)?;
    let decoded: JsonValue =
        serde_json::from_str(text).map_err(|err| JsonCodecError::decode(err.to_string()))?;
    let canonical = strict_canonical_json_bytes(&decoded)?;
    if bytes != canonical.as_slice() {
        return Err(JsonCodecError::validation(
            "strictJsonCodec: bytes are not canonical stable JSON",
        ));
    }
    Ok(decoded)
}

/// Shared strict JSON value validator for checkpoint and storage canonical bytes.
pub fn validate_strict_json_value(value: &JsonValue, path: &str) -> JsonCodecResult<()> {
    validate_strict_json_value_inner(value, path, 0)
}

fn validate_strict_json_value_inner(
    value: &JsonValue,
    path: &str,
    depth: u32,
) -> JsonCodecResult<()> {
    if depth > 128 {
        return Err(JsonCodecError::validation(format!(
            "strictJsonCodec: JSON value at {path} exceeds maximum depth 128"
        )));
    }
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                validate_strict_json_value_inner(value, &format!("{path}.{key}"), depth + 1)?;
            }
        }
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                validate_strict_json_value_inner(value, &format!("{path}[{index}]"), depth + 1)?;
            }
        }
        Value::Number(number) => {
            let text = number.to_string();
            if text == "-0.0" || text == "-0" {
                return Err(JsonCodecError::validation(format!(
                    "strictJsonCodec: JSON number at {path} is not strict canonical JSON compatible"
                )));
            }
            if let Some(float) = number.as_f64() {
                let abs = float.abs();
                if abs > 0.0 && abs < f64::MIN_POSITIVE {
                    return Err(JsonCodecError::validation(format!(
                        "strictJsonCodec: JSON number at {path} is subnormal and not strict canonical JSON compatible"
                    )));
                }
            }
        }
        Value::Null | Value::Bool(_) | Value::String(_) => {}
    }
    Ok(())
}

fn canonical_json_string(value: &JsonValue) -> JsonCodecResult<String> {
    match value {
        Value::Null => Ok("null".to_owned()),
        Value::Bool(true) => Ok("true".to_owned()),
        Value::Bool(false) => Ok("false".to_owned()),
        Value::Number(number) => Ok(number.to_string()),
        Value::String(value) => {
            serde_json::to_string(value).map_err(|err| JsonCodecError::encode(err.to_string()))
        }
        Value::Array(values) => {
            let mut out = String::from("[");
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&canonical_json_string(value)?);
            }
            out.push(']');
            Ok(out)
        }
        Value::Object(map) => {
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort_by(|a, b| cmp_js_utf16(a, b));
            let mut out = String::from("{");
            for (index, key) in keys.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(
                    &serde_json::to_string(key)
                        .map_err(|err| JsonCodecError::encode(err.to_string()))?,
                );
                out.push(':');
                out.push_str(&canonical_json_string(
                    map.get(*key).expect("key came from map.keys()"),
                )?);
            }
            out.push('}');
            Ok(out)
        }
    }
}

fn cmp_js_utf16(a: &str, b: &str) -> Ordering {
    a.encode_utf16().cmp(b.encode_utf16())
}

fn assert_no_duplicate_json_object_keys(text: &str) -> JsonCodecResult<()> {
    struct Scanner<'a> {
        text: &'a str,
        index: usize,
    }

    impl Scanner<'_> {
        fn fail<T>(&self, message: impl Into<String>) -> JsonCodecResult<T> {
            Err(JsonCodecError::validation(format!(
                "strictJsonCodec: {}",
                message.into()
            )))
        }

        fn peek(&self) -> Option<u8> {
            self.text.as_bytes().get(self.index).copied()
        }

        fn skip_whitespace(&mut self) {
            while matches!(self.peek(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
                self.index += 1;
            }
        }

        fn read_json_string(&mut self) -> JsonCodecResult<String> {
            let start = self.index;
            self.index += 1;
            while self.index < self.text.len() {
                match self.peek() {
                    Some(b'"') => {
                        self.index += 1;
                        return serde_json::from_str(&self.text[start..self.index]).map_err(
                            |err| {
                                JsonCodecError::validation(format!(
                                    "strictJsonCodec: malformed JSON string: {err}"
                                ))
                            },
                        );
                    }
                    Some(b'\\') => {
                        self.index += 2;
                    }
                    Some(_) => {
                        self.index += 1;
                    }
                    None => break,
                }
            }
            self.fail("unterminated JSON string")
        }

        fn consume_literal(&mut self, literal: &str) -> JsonCodecResult<()> {
            if self.text[self.index..].starts_with(literal) {
                self.index += literal.len();
                Ok(())
            } else {
                self.fail(format!("malformed JSON near byte {}", self.index))
            }
        }

        fn consume_number(&mut self) -> JsonCodecResult<()> {
            let bytes = self.text.as_bytes();
            let start = self.index;
            if self.peek() == Some(b'-') {
                self.index += 1;
            }
            match self.peek() {
                Some(b'0') => self.index += 1,
                Some(b'1'..=b'9') => {
                    self.index += 1;
                    while matches!(self.peek(), Some(b'0'..=b'9')) {
                        self.index += 1;
                    }
                }
                _ => return self.fail(format!("malformed JSON number near byte {start}")),
            }
            if self.peek() == Some(b'.') {
                self.index += 1;
                if !matches!(self.peek(), Some(b'0'..=b'9')) {
                    return self.fail(format!("malformed JSON number near byte {start}"));
                }
                while matches!(self.peek(), Some(b'0'..=b'9')) {
                    self.index += 1;
                }
            }
            if matches!(self.peek(), Some(b'e' | b'E')) {
                self.index += 1;
                if matches!(self.peek(), Some(b'+' | b'-')) {
                    self.index += 1;
                }
                if !matches!(self.peek(), Some(b'0'..=b'9')) {
                    return self.fail(format!("malformed JSON number near byte {start}"));
                }
                while self.index < bytes.len() && matches!(self.peek(), Some(b'0'..=b'9')) {
                    self.index += 1;
                }
            }
            Ok(())
        }

        fn parse_value(&mut self, path: &str) -> JsonCodecResult<()> {
            self.skip_whitespace();
            match self.peek() {
                Some(b'{') => self.parse_object(path),
                Some(b'[') => self.parse_array(path),
                Some(b'"') => self.read_json_string().map(|_| ()),
                Some(b't') => self.consume_literal("true"),
                Some(b'f') => self.consume_literal("false"),
                Some(b'n') => self.consume_literal("null"),
                Some(b'-' | b'0'..=b'9') => self.consume_number(),
                _ => self.fail(format!("malformed JSON near byte {}", self.index)),
            }
        }

        fn parse_object(&mut self, path: &str) -> JsonCodecResult<()> {
            let mut keys = Vec::<String>::new();
            self.index += 1;
            self.skip_whitespace();
            if self.peek() == Some(b'}') {
                self.index += 1;
                return Ok(());
            }
            while self.index < self.text.len() {
                self.skip_whitespace();
                if self.peek() != Some(b'"') {
                    return self.fail(format!("expected object key near byte {}", self.index));
                }
                let key = self.read_json_string()?;
                if keys.iter().any(|seen| seen == &key) {
                    return Err(JsonCodecError::validation(format!(
                        "strictJsonCodec: duplicate object key {:?} at {path}",
                        key
                    )));
                }
                keys.push(key.clone());
                self.skip_whitespace();
                if self.peek() != Some(b':') {
                    return self.fail(format!(
                        "expected ':' after object key near byte {}",
                        self.index
                    ));
                }
                self.index += 1;
                self.parse_value(&format!("{path}.{key}"))?;
                self.skip_whitespace();
                match self.peek() {
                    Some(b',') => self.index += 1,
                    Some(b'}') => {
                        self.index += 1;
                        return Ok(());
                    }
                    _ => {
                        return self.fail(format!("expected ',' or '}}' near byte {}", self.index))
                    }
                }
            }
            self.fail("unterminated JSON object")
        }

        fn parse_array(&mut self, path: &str) -> JsonCodecResult<()> {
            self.index += 1;
            self.skip_whitespace();
            if self.peek() == Some(b']') {
                self.index += 1;
                return Ok(());
            }
            let mut item = 0;
            while self.index < self.text.len() {
                self.parse_value(&format!("{path}[{item}]"))?;
                item += 1;
                self.skip_whitespace();
                match self.peek() {
                    Some(b',') => self.index += 1,
                    Some(b']') => {
                        self.index += 1;
                        return Ok(());
                    }
                    _ => return self.fail(format!("expected ',' or ']' near byte {}", self.index)),
                }
            }
            self.fail("unterminated JSON array")
        }
    }

    let mut scanner = Scanner { text, index: 0 };
    scanner.parse_value("$")?;
    scanner.skip_whitespace();
    if scanner.index != text.len() {
        return Err(JsonCodecError::validation(format!(
            "strictJsonCodec: trailing JSON token near byte {}",
            scanner.index
        )));
    }
    Ok(())
}

pub type DecimalIntegerString = String;
pub type NonNegativeDecimalIntegerString = String;

pub fn is_decimal_integer_string(value: &str) -> bool {
    if value == "0" {
        return true;
    }
    let rest = value.strip_prefix('-').unwrap_or(value);
    !rest.is_empty()
        && !rest.starts_with('0')
        && rest.bytes().all(|byte| byte.is_ascii_digit())
        && value != "-0"
}

pub fn is_non_negative_decimal_integer_string(value: &str) -> bool {
    if value == "0" {
        return true;
    }
    !value.is_empty()
        && !value.starts_with('-')
        && !value.starts_with('0')
        && value.bytes().all(|byte| byte.is_ascii_digit())
}

pub fn assert_decimal_integer_string(
    value: impl Into<String>,
    label: &str,
) -> JsonCodecResult<DecimalIntegerString> {
    let value = value.into();
    if is_decimal_integer_string(&value) {
        Ok(value)
    } else {
        Err(JsonCodecError::validation(format!(
            "{label} must be a canonical decimal integer string"
        )))
    }
}

pub fn assert_non_negative_decimal_integer_string(
    value: impl Into<String>,
    label: &str,
) -> JsonCodecResult<NonNegativeDecimalIntegerString> {
    let value = value.into();
    if is_non_negative_decimal_integer_string(&value) {
        Ok(value)
    } else {
        Err(JsonCodecError::validation(format!(
            "{label} must be a canonical non-negative decimal integer string"
        )))
    }
}

pub fn i128_to_decimal_string(value: i128) -> DecimalIntegerString {
    value.to_string()
}

pub fn u128_to_non_negative_decimal_string(value: u128) -> NonNegativeDecimalIntegerString {
    value.to_string()
}

pub fn decimal_string_to_i128(value: &str) -> JsonCodecResult<i128> {
    assert_decimal_integer_string(value, "decimal integer")?
        .parse::<i128>()
        .map_err(|err| {
            JsonCodecError::validation(format!("decimal integer is outside i128 range: {err}"))
        })
}

pub fn non_negative_decimal_string_to_u128(value: &str) -> JsonCodecResult<u128> {
    assert_non_negative_decimal_integer_string(value, "decimal integer")?
        .parse::<u128>()
        .map_err(|err| {
            JsonCodecError::validation(format!("decimal integer is outside u128 range: {err}"))
        })
}
