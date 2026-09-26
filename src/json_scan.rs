//! Incremental reader for a JSON request carrying one large string member (the base64
//! blob of gentree `PutFile`), so the body never has to be held whole.
//!
//! The document is fed chunk by chunk. The value of one top-level member (the
//! *streamed* key) is unescaped straight into an [`Output`] for the caller to decode as
//! it arrives; a few other top-level string members are *captured* (small, capped);
//! everything else is validated and dropped. Memory stays bounded by the caps below
//! whatever the body size.
//!
//! It accepts the documents `serde_json::from_slice::<Value>` accepts, which is what
//! axum's `Json<Value>` extractor ran: strict RFC 8259 grammar, strings valid UTF-8
//! with only paired surrogate escapes, at most [`MAX_DEPTH`] nested containers, and
//! numbers that fit an f64 (each numeral is handed to serde_json itself). Duplicate
//! keys resolve like `Value`'s map: the last occurrence wins. Deliberate differences,
//! all for inputs no client sends:
//! - a numeral longer than [`MAX_NUMBER`] bytes is refused ([`ScanError::TooLong`]);
//! - a captured member longer than its cap is reported as [`Member::TooLong`];
//! - an object whose first key is serde_json's private `RawValue` marker is refused
//!   (with axum's `raw_value` feature, serde_json would parse that member's string as
//!   the object's JSON instead).

use std::fmt;

/// serde_json's recursion limit: it refuses the 128th nested container.
pub(crate) const MAX_DEPTH: usize = 127;

/// Longest numeral held for validation.
pub(crate) const MAX_NUMBER: usize = 4096;

/// Keys are compared on at most this many unescaped bytes (longer ones match nothing).
const KEY_CAP: usize = 64;

/// serde_json's `raw_value` marker key (`serde_json::raw::TOKEN`).
const RAW_VALUE_TOKEN: &[u8] = b"$serde_json::private::RawValue";

/// Last occurrence of a captured top-level member.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) enum Member {
    #[default]
    Absent,
    /// Present, but not a string.
    NotString,
    Text(String),
    /// A string longer than the capture cap.
    TooLong,
}

impl Member {
    /// The string value, as `Value::as_str` would give it (`None` if too long).
    pub(crate) fn as_str(&self) -> Option<&str> {
        match self {
            Self::Text(s) => Some(s),
            _ => None,
        }
    }
}

/// Last occurrence of the streamed member.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Streamed {
    #[default]
    Absent,
    NotString,
    /// A string: its unescaped bytes went to [`Output`] since the last restart.
    Text,
}

/// What a complete document held.
#[derive(Debug)]
pub(crate) struct Scanned {
    /// One per capture key, in the order given to [`JsonScanner::new`].
    pub captured: Vec<Member>,
    pub streamed: Streamed,
}

/// Unescaped streamed-member bytes produced by one [`JsonScanner::feed`] call.
#[derive(Debug, Default)]
pub(crate) struct Output {
    /// A new occurrence of the streamed member began: discard everything received for
    /// it before this call. `bytes` then holds only the new occurrence's start.
    pub restart: bool,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ScanError {
    /// Not a JSON document serde_json would accept.
    Syntax { at: u64, what: &'static str },
    /// A token over a memory cap.
    TooLong { what: &'static str },
}

impl fmt::Display for ScanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Syntax { at, what } => write!(f, "invalid JSON at byte {at}: {what}"),
            Self::TooLong { what } => write!(f, "{what} too long"),
        }
    }
}

/// A failure inside `feed`, positioned by the caller.
enum Fail {
    Syntax(&'static str),
    TooLong(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// A value (top level, after ':', after ',' in an array).
    Value,
    /// After '[': a value or ']'.
    ArrayStart,
    /// After '{': a key or '}'.
    ObjectStart,
    /// After ',' in an object: a key.
    Key,
    Colon,
    /// After a value in a container: ',' or the closing bracket.
    AfterValue,
    Str,
    Number,
    Literal,
    /// The top-level value is complete: only whitespace may follow.
    End,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Container {
    Object,
    Array,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Key,
    Streamed,
    Captured(usize),
    Skipped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
    Streamed,
    Captured(usize),
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Esc {
    None,
    /// After '\'.
    Backslash,
    /// Inside `\uXXXX`: `digits` read so far; `lead` is a pending high surrogate.
    Hex {
        n: u16,
        digits: u8,
        lead: Option<u16>,
    },
    /// After a high surrogate escape: the low one's '\' (or, if seen, its 'u') is next.
    Low {
        lead: u16,
        backslash: bool,
    },
}

pub(crate) struct JsonScanner {
    streamed_key: &'static [u8],
    capture_keys: &'static [&'static str],
    capture_cap: usize,

    state: State,
    /// Open containers, innermost last.
    stack: Vec<Container>,
    /// Bytes consumed by earlier `feed` calls.
    offset: u64,

    role: Role,
    esc: Esc,
    /// Continuation bytes still due for a raw UTF-8 sequence, and the next one's range.
    utf8_need: u8,
    utf8_lo: u8,
    utf8_hi: u8,

    key: Vec<u8>,
    key_long: bool,
    first_key: bool,
    /// The top-level member whose value comes next.
    member: Target,
    text: Vec<u8>,
    text_long: bool,
    number: Vec<u8>,
    literal: &'static [u8],

    captured: Vec<Member>,
    streamed: Streamed,
}

impl JsonScanner {
    /// Stream the top-level `streamed_key` member; capture each of `capture_keys` (a
    /// string of at most `capture_cap` unescaped bytes).
    pub(crate) fn new(
        streamed_key: &'static str,
        capture_keys: &'static [&'static str],
        capture_cap: usize,
    ) -> Self {
        Self {
            streamed_key: streamed_key.as_bytes(),
            capture_keys,
            capture_cap,
            state: State::Value,
            stack: Vec::new(),
            offset: 0,
            role: Role::Skipped,
            esc: Esc::None,
            utf8_need: 0,
            utf8_lo: 0x80,
            utf8_hi: 0xBF,
            key: Vec::new(),
            key_long: false,
            first_key: false,
            member: Target::Other,
            text: Vec::new(),
            text_long: false,
            number: Vec::new(),
            literal: b"",
            captured: vec![Member::Absent; capture_keys.len()],
            streamed: Streamed::Absent,
        }
    }

    /// Consume the next chunk of the document. Streamed-member bytes are appended to
    /// `out.bytes` (at most `input.len()` of them). Stops at the first error.
    pub(crate) fn feed(&mut self, input: &[u8], out: &mut Output) -> Result<(), ScanError> {
        let mut i = 0;
        while i < input.len() {
            let step = match self.state {
                State::Str => self.string(input, i, out),
                State::Number => self.number(input, i),
                State::Literal => self.literal(input[i]).map(|()| i + 1),
                _ => self.structural(input[i], out).map(|()| i + 1),
            };
            i = step.map_err(|f| self.error(f, i))?;
        }
        self.offset += input.len() as u64;
        Ok(())
    }

    /// End of the document: it must be complete.
    pub(crate) fn finish(mut self) -> Result<Scanned, ScanError> {
        if self.state == State::Number {
            // A top-level numeral ends at EOF.
            self.end_number().map_err(|f| self.error(f, 0))?;
        }
        if self.state != State::End {
            return Err(ScanError::Syntax {
                at: self.offset,
                what: "unexpected end of input",
            });
        }
        Ok(Scanned {
            captured: self.captured,
            streamed: self.streamed,
        })
    }

    fn error(&self, fail: Fail, i: usize) -> ScanError {
        match fail {
            Fail::Syntax(what) => ScanError::Syntax {
                at: self.offset + i as u64,
                what,
            },
            Fail::TooLong(what) => ScanError::TooLong { what },
        }
    }

    fn structural(&mut self, b: u8, out: &mut Output) -> Result<(), Fail> {
        if matches!(b, b' ' | b'\n' | b'\r' | b'\t') {
            return Ok(());
        }
        match self.state {
            State::Value => self.begin_value(b, out),
            State::ArrayStart if b == b']' => self.close(),
            State::ArrayStart => self.begin_value(b, out),
            State::ObjectStart if b == b'}' => self.close(),
            State::ObjectStart | State::Key if b == b'"' => {
                self.key.clear();
                self.key_long = false;
                self.first_key = self.state == State::ObjectStart;
                self.begin_string(Role::Key);
                Ok(())
            }
            State::ObjectStart | State::Key => Err(Fail::Syntax("expected a string key")),
            State::Colon if b == b':' => {
                self.state = State::Value;
                Ok(())
            }
            State::Colon => Err(Fail::Syntax("expected ':'")),
            State::AfterValue => match (b, self.stack.last()) {
                (b',', Some(Container::Object)) => {
                    self.state = State::Key;
                    Ok(())
                }
                (b',', Some(Container::Array)) => {
                    self.state = State::Value;
                    Ok(())
                }
                (b'}', Some(Container::Object)) | (b']', Some(Container::Array)) => self.close(),
                _ => Err(Fail::Syntax("expected ',' or a closing bracket")),
            },
            State::End => Err(Fail::Syntax("trailing characters")),
            State::Str | State::Number | State::Literal => unreachable!("handled by feed"),
        }
    }

    fn begin_value(&mut self, b: u8, out: &mut Output) -> Result<(), Fail> {
        // A value directly inside the top-level object belongs to `self.member`.
        let member = (self.stack == [Container::Object]).then_some(self.member);
        if b == b'"' {
            let role = match member {
                Some(Target::Streamed) => {
                    out.restart = true;
                    out.bytes.clear();
                    self.streamed = Streamed::Text;
                    Role::Streamed
                }
                Some(Target::Captured(i)) => {
                    self.text.clear();
                    self.text_long = false;
                    Role::Captured(i)
                }
                _ => Role::Skipped,
            };
            self.begin_string(role);
            return Ok(());
        }
        match member {
            Some(Target::Streamed) => self.streamed = Streamed::NotString,
            Some(Target::Captured(i)) => self.captured[i] = Member::NotString,
            _ => {}
        }
        match b {
            b'{' | b'[' => {
                if self.stack.len() >= MAX_DEPTH {
                    return Err(Fail::Syntax("recursion limit exceeded"));
                }
                if b == b'{' {
                    self.stack.push(Container::Object);
                    self.state = State::ObjectStart;
                } else {
                    self.stack.push(Container::Array);
                    self.state = State::ArrayStart;
                }
            }
            b't' | b'f' | b'n' => {
                self.literal = match b {
                    b't' => b"rue",
                    b'f' => b"alse",
                    _ => b"ull",
                };
                self.state = State::Literal;
            }
            b'-' | b'0'..=b'9' => {
                self.number.clear();
                self.number.push(b);
                self.state = State::Number;
            }
            _ => return Err(Fail::Syntax("expected a value")),
        }
        Ok(())
    }

    fn value_done(&mut self) {
        self.state = if self.stack.is_empty() {
            State::End
        } else {
            State::AfterValue
        };
    }

    fn close(&mut self) -> Result<(), Fail> {
        self.stack.pop();
        self.value_done();
        Ok(())
    }

    fn literal(&mut self, b: u8) -> Result<(), Fail> {
        match self.literal.split_first() {
            Some((&want, rest)) if want == b => {
                self.literal = rest;
                if rest.is_empty() {
                    self.value_done();
                }
                Ok(())
            }
            _ => Err(Fail::Syntax("invalid literal")),
        }
    }

    /// Collect a numeral; its end (not consumed) is handled by `structural`.
    fn number(&mut self, input: &[u8], i: usize) -> Result<usize, Fail> {
        let run = input[i..]
            .iter()
            .position(|&c| !matches!(c, b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E'))
            .unwrap_or(input.len() - i);
        if self.number.len() + run > MAX_NUMBER {
            return Err(Fail::TooLong("numeral"));
        }
        self.number.extend_from_slice(&input[i..i + run]);
        if i + run < input.len() {
            self.end_number()?;
        }
        Ok(i + run)
    }

    fn end_number(&mut self) -> Result<(), Fail> {
        // serde_json parses numerals the same wherever they appear, so let it decide
        // (grammar, and whether the value fits an f64).
        serde_json::from_slice::<serde_json::Value>(&self.number)
            .map_err(|_| Fail::Syntax("invalid number"))?;
        self.number.clear();
        self.value_done();
        Ok(())
    }

    fn begin_string(&mut self, role: Role) {
        self.role = role;
        self.esc = Esc::None;
        self.utf8_need = 0;
        self.state = State::Str;
    }

    fn emit(&mut self, bytes: &[u8], out: &mut Output) {
        match self.role {
            Role::Key => {
                if self.key.len() + bytes.len() > KEY_CAP {
                    self.key_long = true;
                } else if !self.key_long {
                    self.key.extend_from_slice(bytes);
                }
            }
            Role::Streamed => out.bytes.extend_from_slice(bytes),
            Role::Captured(_) => {
                if self.text.len() + bytes.len() > self.capture_cap {
                    self.text_long = true;
                    self.text = Vec::new();
                } else if !self.text_long {
                    self.text.extend_from_slice(bytes);
                }
            }
            Role::Skipped => {}
        }
    }

    fn emit_char(&mut self, cp: u32, out: &mut Output) -> Result<(), Fail> {
        let c = char::from_u32(cp).ok_or(Fail::Syntax("invalid escape"))?;
        self.emit(c.encode_utf8(&mut [0; 4]).as_bytes(), out);
        Ok(())
    }

    /// Scan string content from `input[i..]`; returns where it stopped (the end of the
    /// input, or just past the closing quote).
    fn string(&mut self, input: &[u8], mut i: usize, out: &mut Output) -> Result<usize, Fail> {
        while i < input.len() {
            let b = input[i];
            i += 1;
            match self.esc {
                Esc::None if self.utf8_need > 0 => {
                    if !(self.utf8_lo..=self.utf8_hi).contains(&b) {
                        return Err(Fail::Syntax("invalid UTF-8 in string"));
                    }
                    self.utf8_need -= 1;
                    (self.utf8_lo, self.utf8_hi) = (0x80, 0xBF);
                    self.emit(&[b], out);
                }
                Esc::None => match b {
                    b'"' => {
                        self.end_string()?;
                        return Ok(i);
                    }
                    b'\\' => self.esc = Esc::Backslash,
                    0x00..=0x1F => return Err(Fail::Syntax("control character in string")),
                    0x20..=0x7F => {
                        // A run of plain ASCII in one go (the bulk of a base64 blob).
                        let start = i - 1;
                        let run = input[i..]
                            .iter()
                            .position(|&c| c == b'"' || c == b'\\' || c < 0x20 || c >= 0x80)
                            .unwrap_or(input.len() - i);
                        i += run;
                        self.emit(&input[start..i], out);
                    }
                    _ => {
                        // Same acceptance as `str::from_utf8`: no overlongs, surrogates
                        // or code points past U+10FFFF.
                        let (need, lo, hi) = match b {
                            0xC2..=0xDF => (1, 0x80, 0xBF),
                            0xE0 => (2, 0xA0, 0xBF),
                            0xE1..=0xEC | 0xEE..=0xEF => (2, 0x80, 0xBF),
                            0xED => (2, 0x80, 0x9F),
                            0xF0 => (3, 0x90, 0xBF),
                            0xF1..=0xF3 => (3, 0x80, 0xBF),
                            0xF4 => (3, 0x80, 0x8F),
                            _ => return Err(Fail::Syntax("invalid UTF-8 in string")),
                        };
                        (self.utf8_need, self.utf8_lo, self.utf8_hi) = (need, lo, hi);
                        self.emit(&[b], out);
                    }
                },
                Esc::Backslash => {
                    let c = match b {
                        b'"' | b'\\' | b'/' => b,
                        b'b' => 0x08,
                        b'f' => 0x0C,
                        b'n' => b'\n',
                        b'r' => b'\r',
                        b't' => b'\t',
                        b'u' => {
                            self.esc = Esc::Hex {
                                n: 0,
                                digits: 0,
                                lead: None,
                            };
                            continue;
                        }
                        _ => return Err(Fail::Syntax("invalid escape")),
                    };
                    self.esc = Esc::None;
                    self.emit(&[c], out);
                }
                Esc::Hex { n, digits, lead } => {
                    let d = (b as char)
                        .to_digit(16)
                        .ok_or(Fail::Syntax("invalid escape"))?;
                    let n = n << 4 | d as u16;
                    if digits < 3 {
                        self.esc = Esc::Hex {
                            n,
                            digits: digits + 1,
                            lead,
                        };
                        continue;
                    }
                    // As serde_json's `parse_unicode_escape` when validating: a
                    // surrogate escape must be a high one directly followed by a low one.
                    self.esc = Esc::None;
                    let low = (0xDC00..=0xDFFF).contains(&n);
                    match lead {
                        None if (0xD800..=0xDBFF).contains(&n) => {
                            self.esc = Esc::Low {
                                lead: n,
                                backslash: false,
                            };
                        }
                        None if !low => self.emit_char(u32::from(n), out)?,
                        Some(hi) if low => {
                            let cp = 0x1_0000
                                + ((u32::from(hi) - 0xD800) << 10 | (u32::from(n) - 0xDC00));
                            self.emit_char(cp, out)?;
                        }
                        _ => return Err(Fail::Syntax("unpaired surrogate escape")),
                    }
                }
                Esc::Low { lead, backslash } => match (backslash, b) {
                    (false, b'\\') => {
                        self.esc = Esc::Low {
                            lead,
                            backslash: true,
                        };
                    }
                    (true, b'u') => {
                        self.esc = Esc::Hex {
                            n: 0,
                            digits: 0,
                            lead: Some(lead),
                        };
                    }
                    _ => return Err(Fail::Syntax("unpaired surrogate escape")),
                },
            }
        }
        Ok(i)
    }

    fn end_string(&mut self) -> Result<(), Fail> {
        match self.role {
            Role::Key => {
                if self.first_key && !self.key_long && self.key == RAW_VALUE_TOKEN {
                    return Err(Fail::Syntax("serde_json RawValue marker key not supported"));
                }
                if self.stack.len() == 1 {
                    self.member = match (&self.key[..], self.key_long) {
                        (_, true) => Target::Other,
                        (k, false) if k == self.streamed_key => Target::Streamed,
                        (k, false) => self
                            .capture_keys
                            .iter()
                            .position(|c| c.as_bytes() == k)
                            .map_or(Target::Other, Target::Captured),
                    };
                }
                self.state = State::Colon;
                return Ok(());
            }
            Role::Captured(i) => {
                self.captured[i] = if self.text_long {
                    Member::TooLong
                } else {
                    // Validated as UTF-8 on the way in.
                    String::from_utf8(std::mem::take(&mut self.text))
                        .map(Member::Text)
                        .map_err(|_| Fail::Syntax("invalid UTF-8 in string"))?
                };
            }
            Role::Streamed | Role::Skipped => {}
        }
        self.value_done();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use serde_json::Value;

    use super::*;

    const FIELDS: &[&str] = &["fileHash", "filePath"];

    /// Scan `doc` fed in pieces cut at `cuts`; the streamed bytes are those of the last
    /// occurrence, as a caller honouring `restart` would keep them.
    fn scan(doc: &[u8], cuts: &[usize]) -> Result<(Scanned, Vec<u8>), ScanError> {
        let mut cuts: Vec<usize> = cuts.iter().copied().filter(|&c| c <= doc.len()).collect();
        cuts.extend([0, doc.len()]);
        cuts.sort_unstable();
        let mut sc = JsonScanner::new("payload", FIELDS, 64);
        let mut out = Output::default();
        let mut streamed = Vec::new();
        for w in cuts.windows(2) {
            sc.feed(&doc[w[0]..w[1]], &mut out)?;
            if std::mem::take(&mut out.restart) {
                streamed.clear();
            }
            streamed.append(&mut out.bytes);
        }
        let scanned = sc.finish()?;
        if scanned.streamed != Streamed::Text {
            // A caller refuses the document then; what an earlier occurrence sent is moot.
            streamed.clear();
        }
        Ok((scanned, streamed))
    }

    /// What `Value` gives for the same document, in the scanner's terms.
    fn expected(doc: &[u8]) -> Option<(Vec<Member>, Streamed, Vec<u8>)> {
        let v: Value = serde_json::from_slice(doc).ok()?;
        let member = |k: &str| match v.get(k) {
            None => Member::Absent,
            Some(Value::String(s)) if s.len() > 64 => Member::TooLong,
            Some(Value::String(s)) => Member::Text(s.clone()),
            Some(_) => Member::NotString,
        };
        let (streamed, bytes) = match v.get("payload") {
            None => (Streamed::Absent, Vec::new()),
            Some(Value::String(s)) => (Streamed::Text, s.clone().into_bytes()),
            Some(_) => (Streamed::NotString, Vec::new()),
        };
        Some((FIELDS.iter().map(|k| member(k)).collect(), streamed, bytes))
    }

    fn agrees(doc: &[u8], cuts: &[usize]) -> std::result::Result<(), String> {
        let got = scan(doc, cuts)
            .ok()
            .map(|(s, bytes)| (s.captured, s.streamed, bytes));
        let want = expected(doc);
        if got == want {
            Ok(())
        } else {
            Err(format!(
                "{:?} cuts {cuts:?}: scanner {got:?}, serde_json {want:?}",
                String::from_utf8_lossy(doc)
            ))
        }
    }

    #[test]
    fn splits_anywhere_including_inside_escapes() {
        // '/' escaped as "\/" (as some encoders write base64), \u escapes (one a
        // surrogate pair), escaped keys, a duplicate blob member, raw UTF-8.
        let doc = r#" { "fileH\u0061sh" : "ab\/c" , "payload":"QU\/\u0041+/=\n", "x":[1,-2.5e3,{"y":null}],
            "filePath":"d\u00e9j\u00e0 \ud83d\ude00 \u00e9", "p\u0061yload" : "QUJD\/w==" , "z":"é😀"}"#
            .as_bytes();
        let (s, bytes) = scan(doc, &[]).unwrap();
        assert_eq!(bytes, b"QUJD/w==");
        assert_eq!(s.streamed, Streamed::Text);
        assert_eq!(s.captured[0], Member::Text("ab/c".into()));
        assert_eq!(s.captured[1], Member::Text("déjà 😀 é".into()));
        agrees(doc, &[]).unwrap();
        // Every two-piece split, and one byte at a time.
        for k in 0..=doc.len() {
            agrees(doc, &[k]).unwrap();
        }
        let every: Vec<usize> = (0..doc.len()).collect();
        agrees(doc, &every).unwrap();
    }

    #[test]
    fn depth_limit_matches_serde_json() {
        for depth in [MAX_DEPTH - 1, MAX_DEPTH, MAX_DEPTH + 1] {
            for (open, close) in [("[", "]"), ("{\"k\":", "}")] {
                // `depth` containers in all, the top-level object included.
                let inner = depth - 1;
                let doc = format!("{{\"a\":{}0{}}}", open.repeat(inner), close.repeat(inner));
                assert_eq!(
                    serde_json::from_str::<Value>(&doc).is_ok(),
                    depth <= MAX_DEPTH,
                    "{depth}"
                );
                agrees(doc.as_bytes(), &[]).unwrap();
            }
        }
    }

    #[test]
    fn numbers_are_judged_by_serde_json() {
        for n in [
            "0",
            "-0",
            "1.5e3",
            "1E+2",
            "1e400",
            "-1e400",
            "1e-400",
            "01",
            "1.",
            ".5",
            "-",
            "1e",
            "+1",
            "1.5e3.2",
            "123456789012345678901234567890",
            "0e99999999999",
        ] {
            agrees(format!("{{\"n\":{n}}}").as_bytes(), &[]).unwrap();
            agrees(n.as_bytes(), &[]).unwrap();
            agrees(format!("[{n} ]").as_bytes(), &[1, 2]).unwrap();
        }
        let long = format!("[{}]", "1".repeat(MAX_NUMBER + 1));
        assert_eq!(
            scan(long.as_bytes(), &[3]).unwrap_err(),
            ScanError::TooLong { what: "numeral" }
        );
    }

    #[test]
    fn strings_are_validated_like_serde_json() {
        let cases: &[&[u8]] = &[
            br#""\ud83d\ude00""#,
            br#""\ud83d""#,
            br#""\ude00""#,
            br#""\ud83d\u0041""#,
            br#""\ud83d\ud83d\ude00""#,
            br#""\ud83dx""#,
            br#""\ud83d\n""#,
            br#""\u00""#,
            br#""\u00g0""#,
            br#""\x""#,
            b"\"a\x01b\"",
            b"\"\xc3\xa9\"",
            b"\"\xc3\"",
            b"\"\xc3\\u0041\"",
            b"\"\xe0\x80\x80\"",
            b"\"\xed\xa0\x80\"",
            b"\"\xf4\x90\x80\x80\"",
            b"\"\xff\"",
            b"\"\x7f\"",
            b"\"abc",
        ];
        for s in cases {
            for doc in [
                s.to_vec(),
                [&b"{\"payload\":"[..], s, b"}"].concat(),
                [&b"{\"fileHash\":"[..], s, b"}"].concat(),
                [&b"{"[..], s, b":1}"].concat(),
            ] {
                for k in 0..=doc.len() {
                    agrees(&doc, &[k]).unwrap();
                }
            }
        }
    }

    #[test]
    fn member_rules_follow_value() {
        for doc in [
            &br#"{"payload":"QUJD","payload":7}"#[..],
            br#"{"payload":7,"payload":"QUJD"}"#,
            br#"{"fileHash":"a","fileHash":["b"]}"#,
            br#"{"a":{"payload":"x","fileHash":"y"}}"#,
            br#"[{"payload":"x"}]"#,
            br#""payload""#,
            br#"{"payload":{"payload":"x"}}"#,
            br#"{"filePath":"0123456789012345678901234567890123456789012345678901234567890123456789"}"#,
            br#"{}"#,
            br#"  {"a":1}  "#,
            br#"{"a":1} x"#,
            br#"{"a":1,}"#,
            br#"[1,]"#,
            br#"{"a" 1}"#,
            br#"{1:1}"#,
            br#"{"a":tru}"#,
            br#"{"a":truex}"#,
            br#"{"a":nul"#,
            b"",
            b"   ",
            b"\xef\xbb\xbf{}",
        ] {
            agrees(doc, &[]).unwrap();
            agrees(doc, &[1, 5, 9]).unwrap();
        }
    }

    #[test]
    fn raw_value_marker_is_refused() {
        // With axum's `raw_value` feature serde_json would parse this member's string as
        // the object itself; no client sends serde_json's private marker, so refuse it.
        let marker = String::from_utf8(RAW_VALUE_TOKEN.to_vec()).unwrap();
        let doc = format!(r#"{{"{marker}":"{{}}"}}"#);
        assert!(matches!(
            scan(doc.as_bytes(), &[]),
            Err(ScanError::Syntax { .. })
        ));
        // Only as the first key is it special.
        let doc = format!(r#"{{"a":1,"{marker}":"{{}}"}}"#);
        agrees(doc.as_bytes(), &[]).unwrap();
    }

    /// Any JSON value, rendered by serde_json (so escapes are serde_json's own).
    fn json_value() -> impl Strategy<Value = Value> {
        let leaf = prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(Value::from),
            any::<f64>().prop_map(Value::from),
            "\\PC{0,12}".prop_map(Value::String),
        ];
        leaf.prop_recursive(4, 32, 6, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..6).prop_map(Value::Array),
                prop::collection::vec(
                    (
                        prop_oneof![
                            Just("payload".to_owned()),
                            Just("fileHash".to_owned()),
                            Just("filePath".to_owned()),
                            "\\PC{0,8}"
                        ],
                        inner
                    ),
                    0..6
                )
                .prop_map(|kv| Value::Object(kv.into_iter().collect())),
            ]
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2048))]

        /// Documents built from JSON-ish tokens, valid or not: accepted exactly when
        /// serde_json accepts them, with the same members, however they're split.
        #[test]
        fn token_soup_agrees_with_serde_json(
            toks in prop::collection::vec(prop_oneof![
                Just("{"), Just("}"), Just("["), Just("]"), Just(":"), Just(","), Just(" "),
                Just("\"payload\""), Just("\"fileHash\""), Just("\"filePath\""), Just("\"k\""),
                Just("\"QUJD\""), Just("\"a\\/b\""), Just("\"\\u0041\""), Just("\"\\ud83d\""),
                Just("\"\\ude00\""), Just("\""), Just("\\"), Just("true"), Just("null"),
                Just("fals"), Just("-1.5e3"), Just("0"), Just("1e999"), Just("\t\n"),
                Just("\u{e9}"), Just("x"),
            ], 0..24),
            cuts in prop::collection::vec(0usize..200, 0..6),
        ) {
            let doc = toks.concat();
            agrees(doc.as_bytes(), &cuts).map_err(TestCaseError::fail)?;
        }

        /// Any serialized value is accepted with the members `Value` sees.
        #[test]
        fn serialized_values_agree_with_serde_json(
            v in json_value(),
            pretty in any::<bool>(),
            cuts in prop::collection::vec(0usize..400, 0..6),
        ) {
            let doc = if pretty {
                serde_json::to_vec_pretty(&v).unwrap()
            } else {
                serde_json::to_vec(&v).unwrap()
            };
            agrees(&doc, &cuts).map_err(TestCaseError::fail)?;
        }

        /// Arbitrary bytes (mostly rejected) agree too.
        #[test]
        fn random_bytes_agree_with_serde_json(
            doc in prop::collection::vec(any::<u8>(), 0..32),
            cuts in prop::collection::vec(0usize..40, 0..4),
        ) {
            agrees(&doc, &cuts).map_err(TestCaseError::fail)?;
        }
    }
}
