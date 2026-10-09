//! The `regex` term predicate's pattern subset: SPARQL `REGEX` patterns from
//! an allow-list grammar on which Rust's `regex` crate and Python's `re`
//! (what rdflib runs) agree, translated to the Rust dialect.
//!
//! The grammar: literal characters; the escaped metacharacters
//! `\. \* \+ \? \( \) \[ \] \{ \} \| \^ \$ \\ \/ \-`; `.`; classes `[...]`
//! and `[^...]` of literals, ranges, escaped characters and `\d \w \s \D \W
//! \S` (no nested `[`, and none of the set operators `&&`, `--`, `~~` and
//! `||`, which Python warns about); the shorthands `\d \w \s \D \W \S \b
//! \B`; the anchors `^`, `$` and `\A`; groups `( )` and `(?: )` nested up to
//! `MAX_GROUP_DEPTH`; alternation; the quantifiers `* + ? {n} {n,} {n,m}`
//! and their lazy forms. Anything else — backreferences, lookaround,
//! `\p{...}`, `\Z`, inline flags, named groups, possessive quantifiers, a
//! brace that is no quantifier, a leading `]` in a class — leaves every text
//! undecided, as does flag `i` on a pattern holding a non-ASCII character.
//!
//! Three rules keep a decided verdict exact: a pattern using `\w \W \s \S \d
//! \D \b \B` or flag `i` decides ASCII texts only; a pattern with `$` and
//! without flag `m` leaves a text ending in `\n` undecided (Python's `$` also
//! matches before a final newline); and a pattern with `\B` leaves the empty
//! text undecided (Python 3.14 matches `\B` there, 3.11–3.13 do not).
//! Python's `\s` also covers `\x1c`–`\x1f` on ASCII text, which Rust's Unicode
//! `\s` leaves out, so it is written out.

use std::fmt::Write as _;

use super::predicates::Verdict;

/// Python's `\s` on ASCII text, as class items: `\t \n \x0b \x0c \r`, the
/// separators `\x1c`–`\x1f`, and the space.
const PY_SPACE: &str = r"\t-\r\x1C-\x20";

/// Python's `\S` on ASCII text, as a (nested) class.
const PY_NON_SPACE: &str = r"[^\t-\r\x1C-\x20]";

/// The grammar's escapable metacharacters.
const ESCAPABLE: &str = r".*+?()[]{}|^$\/-";

/// The Rust `regex` metacharacters, escaped wherever a literal lands.
const RUST_META: &str = r"\.+*?()|[]{}^$#&-~";

/// Largest counted repetition the subset takes.
const MAX_REPEAT: u32 = 1_000;

/// Deepest group nesting the subset takes: the translator recurses once per
/// level, and a pattern is untrusted input, so the depth is bounded well below
/// what Python (about 400 levels) and Rust (250) themselves take.
const MAX_GROUP_DEPTH: u32 = 64;

/// A `REGEX` test compiled for native evaluation, or a pattern outside the
/// subset (every text undecided).
#[derive(Debug, Clone)]
pub(crate) struct RegexTest {
    pattern: String,
    flags: String,
    compiled: Option<Compiled>,
}

#[derive(Debug, Clone)]
struct Compiled {
    regex: regex::Regex,
    /// A Unicode-sensitive construct or flag `i`: ASCII texts only.
    ascii_only: bool,
    /// `$` without flag `m`: a text ending in `\n` is undecided.
    final_newline_undecided: bool,
    /// `\B`: the empty text is undecided.
    empty_text_undecided: bool,
}

impl PartialEq for RegexTest {
    fn eq(&self, other: &Self) -> bool {
        self.pattern == other.pattern && self.flags == other.flags
    }
}

impl RegexTest {
    /// `pattern` under SPARQL `flags`: `i`, `s` and `m` honoured, any other
    /// flag ignored (rdflib's mapping).
    pub(crate) fn new(pattern: &str, flags: &str) -> Self {
        let (i, s, m) = (
            flags.contains('i'),
            flags.contains('s'),
            flags.contains('m'),
        );
        let compiled = Translation::of(pattern).and_then(|t| {
            if i && !pattern.is_ascii() {
                return None;
            }
            let regex = regex::RegexBuilder::new(&t.rust)
                .case_insensitive(i)
                .dot_matches_new_line(s)
                .multi_line(m)
                .size_limit(1 << 22)
                .dfa_size_limit(1 << 22)
                .build()
                .ok()?;
            Some(Compiled {
                regex,
                ascii_only: t.unicode_sensitive || i,
                final_newline_undecided: t.has_dollar && !m,
                empty_text_undecided: t.has_non_boundary,
            })
        });
        Self {
            pattern: pattern.to_owned(),
            flags: flags.to_owned(),
            compiled,
        }
    }

    /// Python's `re.search(pattern, text, flags)` for `text`, or `Unknown`.
    pub(crate) fn eval(&self, text: &str) -> Verdict {
        let Some(c) = &self.compiled else {
            return Verdict::Unknown;
        };
        if c.ascii_only && !text.is_ascii() {
            return Verdict::Unknown;
        }
        if c.final_newline_undecided && text.ends_with('\n') {
            return Verdict::Unknown;
        }
        if c.empty_text_undecided && text.is_empty() {
            return Verdict::Unknown;
        }
        Verdict::from(c.regex.is_match(text))
    }

    /// The pattern and flags, as the predicate renders them.
    pub(crate) fn render(&self) -> String {
        format!("{:?} flags={:?}", self.pattern, self.flags)
    }
}

/// A pattern of the subset in the Rust dialect, and what it uses.
struct Translation {
    rust: String,
    unicode_sensitive: bool,
    has_dollar: bool,
    has_non_boundary: bool,
}

impl Translation {
    fn of(pattern: &str) -> Option<Self> {
        let mut t = Translator {
            chars: pattern.chars().peekable(),
            out: String::with_capacity(pattern.len() * 2),
            unicode_sensitive: false,
            has_dollar: false,
            has_non_boundary: false,
            depth: 0,
        };
        t.alternation()?;
        if t.chars.next().is_some() {
            return None; // an unbalanced `)`
        }
        Some(Translation {
            rust: t.out,
            unicode_sensitive: t.unicode_sensitive,
            has_dollar: t.has_dollar,
            has_non_boundary: t.has_non_boundary,
        })
    }
}

struct Translator<'a> {
    chars: std::iter::Peekable<std::str::Chars<'a>>,
    out: String,
    unicode_sensitive: bool,
    has_dollar: bool,
    has_non_boundary: bool,
    /// Groups open at this point.
    depth: u32,
}

/// One class item.
enum ClassItem {
    Char(char),
    Shorthand(&'static str),
}

fn push_escaped(out: &mut String, c: char) {
    if RUST_META.contains(c) {
        out.push('\\');
    }
    out.push(c);
}

impl Translator<'_> {
    fn alternation(&mut self) -> Option<()> {
        self.sequence()?;
        while self.chars.peek() == Some(&'|') {
            self.chars.next();
            self.out.push('|');
            self.sequence()?;
        }
        Some(())
    }

    fn sequence(&mut self) -> Option<()> {
        while let Some(&c) = self.chars.peek() {
            if c == '|' || c == ')' {
                break;
            }
            let quantifiable = self.atom()?;
            self.quantifier(quantifiable)?;
        }
        Some(())
    }

    /// One atom; `Some(true)` when a quantifier may follow it.
    fn atom(&mut self) -> Option<bool> {
        match self.chars.next()? {
            '(' => {
                self.depth += 1;
                if self.depth > MAX_GROUP_DEPTH {
                    return None;
                }
                if self.chars.peek() == Some(&'?') {
                    self.chars.next();
                    if self.chars.next()? != ':' {
                        return None;
                    }
                    self.out.push_str("(?:");
                } else {
                    self.out.push('(');
                }
                self.alternation()?;
                if self.chars.next()? != ')' {
                    return None;
                }
                self.out.push(')');
                self.depth -= 1;
                Some(true)
            }
            '[' => {
                self.class()?;
                Some(true)
            }
            '.' => {
                self.out.push('.');
                Some(true)
            }
            '^' => {
                self.out.push('^');
                Some(false)
            }
            '$' => {
                self.out.push('$');
                self.has_dollar = true;
                Some(false)
            }
            '\\' => self.escape(),
            // Nothing to repeat, or a brace that is no quantifier.
            '*' | '+' | '?' | '{' => None,
            c => {
                push_escaped(&mut self.out, c);
                Some(true)
            }
        }
    }

    fn escape(&mut self) -> Option<bool> {
        let e = self.chars.next()?;
        match e {
            'd' | 'D' | 'w' | 'W' => {
                self.unicode_sensitive = true;
                self.out.push('\\');
                self.out.push(e);
                Some(true)
            }
            's' => {
                self.unicode_sensitive = true;
                write!(self.out, "[{PY_SPACE}]").ok()?;
                Some(true)
            }
            'S' => {
                self.unicode_sensitive = true;
                self.out.push_str(PY_NON_SPACE);
                Some(true)
            }
            'b' | 'B' => {
                self.unicode_sensitive = true;
                self.has_non_boundary |= e == 'B';
                self.out.push('\\');
                self.out.push(e);
                Some(false)
            }
            'A' => {
                self.out.push_str(r"\A");
                Some(false)
            }
            e if ESCAPABLE.contains(e) => {
                push_escaped(&mut self.out, e);
                Some(true)
            }
            _ => None,
        }
    }

    fn quantifier(&mut self, quantifiable: bool) -> Option<()> {
        match self.chars.peek() {
            Some(&q @ ('*' | '+' | '?')) => {
                if !quantifiable {
                    return None;
                }
                self.chars.next();
                self.out.push(q);
            }
            Some('{') => {
                if !quantifiable {
                    return None;
                }
                self.chars.next();
                self.braces()?;
            }
            _ => return Some(()),
        }
        if self.chars.peek() == Some(&'?') {
            self.chars.next();
            self.out.push('?');
        }
        // A second quantifier (`a**`, possessive `a*+`, `a{2}{3}`) is outside.
        if matches!(self.chars.peek(), Some('*' | '+' | '?' | '{')) {
            return None;
        }
        Some(())
    }

    /// `{n}`, `{n,}` or `{n,m}` after the `{`, written to the output.
    fn braces(&mut self) -> Option<()> {
        let lo = self.number()?;
        match self.chars.next()? {
            '}' => write!(self.out, "{{{lo}}}").ok(),
            ',' if self.chars.peek() == Some(&'}') => {
                self.chars.next();
                write!(self.out, "{{{lo},}}").ok()
            }
            ',' => {
                let hi = self.number()?;
                if self.chars.next()? != '}' || hi < lo {
                    return None;
                }
                write!(self.out, "{{{lo},{hi}}}").ok()
            }
            _ => None,
        }
    }

    fn number(&mut self) -> Option<u32> {
        let mut n: u32 = 0;
        let mut digits = 0;
        while let Some(d) = self.chars.peek().and_then(|c| c.to_digit(10)) {
            self.chars.next();
            n = n.checked_mul(10)?.checked_add(d)?;
            digits += 1;
        }
        (digits > 0 && n <= MAX_REPEAT).then_some(n)
    }

    fn class(&mut self) -> Option<()> {
        self.out.push('[');
        if self.chars.peek() == Some(&'^') {
            self.chars.next();
            self.out.push('^');
        }
        // A leading `]` is a literal in both dialects; the subset leaves it
        // out instead of ruling on where a class ends.
        if self.chars.peek() == Some(&']') {
            return None;
        }
        let mut items = 0usize;
        loop {
            let c = self.chars.next()?;
            match c {
                ']' if items > 0 => {
                    self.out.push(']');
                    return Some(());
                }
                ']' | '[' => return None,
                '&' | '-' | '~' | '|' if self.chars.peek() == Some(&c) => return None,
                _ => {}
            }
            let item = if c == '\\' {
                self.class_escape()?
            } else {
                ClassItem::Char(c)
            };
            match item {
                ClassItem::Shorthand(text) => {
                    // Python: a shorthand ends no range (`[\d-z]` is an error).
                    if self.dash_starts_range() {
                        return None;
                    }
                    self.out.push_str(text);
                }
                ClassItem::Char(lo) => {
                    if self.dash_starts_range() {
                        self.chars.next(); // the `-`
                        let hi = match self.chars.next()? {
                            '\\' => match self.class_escape()? {
                                ClassItem::Char(hi) => hi,
                                ClassItem::Shorthand(_) => return None,
                            },
                            // A range ending in `-` (`[+--]`) is Python's "possible set difference".
                            '[' | ']' | '-' => return None,
                            hi => hi,
                        };
                        if hi < lo {
                            return None;
                        }
                        push_escaped(&mut self.out, lo);
                        self.out.push('-');
                        push_escaped(&mut self.out, hi);
                    } else {
                        push_escaped(&mut self.out, lo);
                    }
                }
            }
            items += 1;
        }
    }

    /// Whether the next characters are a `-` opening a range: one not
    /// followed by the class's closing `]`.
    fn dash_starts_range(&self) -> bool {
        let mut ahead = self.chars.clone();
        ahead.next() == Some('-') && !matches!(ahead.next(), Some(']') | None)
    }

    fn class_escape(&mut self) -> Option<ClassItem> {
        let e = self.chars.next()?;
        let shorthand = |t: &mut Self, text: &'static str| {
            t.unicode_sensitive = true;
            ClassItem::Shorthand(text)
        };
        Some(match e {
            'd' => shorthand(self, r"\d"),
            'D' => shorthand(self, r"\D"),
            'w' => shorthand(self, r"\w"),
            'W' => shorthand(self, r"\W"),
            's' => shorthand(self, PY_SPACE),
            'S' => shorthand(self, PY_NON_SPACE),
            e if ESCAPABLE.contains(e) => ClassItem::Char(e),
            _ => return None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use Verdict::{False, True, Unknown};

    /// Python 3.13 `re.search` results (rdflib's call), checked on 2026-10-07.
    #[test]
    fn decided_texts_agree_with_python_re() {
        for (pattern, flags, text, want) in [
            ("ab", "", "xaby", True),
            ("^ab", "", "xab", False),
            ("ab$", "m", "xab\n", True),
            ("a.c", "", "a\nc", False),
            ("a.c", "s", "a\nc", True),
            (r"\d+", "", "x42", True),
            (r"\s", "", "a\x1cb", True),
            (r"\S+$", "", "\x1c", False),
            ("A", "i", "xa", True),
            ("é", "", "café", True),
            ("[a-c]{2,3}?x", "", "zbbx", True),
            ("(?:ab|cd)+", "", "xcdab", True),
            (r"\bfoo\b", "", "a foo b", True),
            (r"\bfoo\b", "", "afoob", False),
            ("[^a]", "", "aaa", False),
            (r"\.", "", "a.b", True),
            (r"a\/b", "", "a/b", True),
            ("", "", "anything", True),
            ("a|", "", "b", True),
            (r"\Aab", "m", "x\nab", False),
            ("^ab", "m", "x\nab", True),
            ("x", "q", "x", True),
            ("[a-]", "", "-", True),
            (r"a\b", "", "a!", True),
            ("a&&b", "", "a&&b", True),
            ("ab", "", "AB", False),
            (r"[\s,]+", "", "a\x1f,b", True),
            (r"[\S]", "", "\x1d", False),
            (r"\W", "", "\x1e", True),
            (r"^(a|b)*?c{1,}$", "", "ababcc", True),
            (r"[\w.]+@[\w.]+", "", "x y.z@a.b", True),
            (r"\D", "", "123", False),
            (r"\B", "", "ab", True),
            ("a{2}", "", "aa", True),
            ("a{2,}", "", "a", False),
            ("(a)(b)?", "", "a", True),
            (r"[\^\-\]\\]", "", "\\", True),
            (r"\|", "", "a|b", True),
            (r"\$", "", "a$", True),
            ("[$]", "", "$", True),
            ("^$", "m", "a\n", True),
            ("a.$", "", "a\r", True),
        ] {
            assert_eq!(
                RegexTest::new(pattern, flags).eval(text),
                want,
                "{pattern:?} {flags:?} on {text:?}"
            );
        }
    }

    /// More of the allow-list's boundaries, each row checked against Python
    /// 3.11, 3.13 and 3.14 `re.search` (rdflib's call).
    #[test]
    fn allow_list_boundaries_agree_with_python_re() {
        for (pattern, flags, text, want) in [
            // Counted repetition, empty groups and empty alternatives.
            ("a{0}", "", "b", True),
            ("^a{0}$", "", "b", False),
            ("a{2,3}?b", "", "aaab", True),
            ("a{02}", "", "aa", True),
            ("(?:)", "", "x", True),
            ("()", "", "x", True),
            ("|a", "", "b", True),
            ("^(?:a|)$", "", "", True),
            ("(?:^)*b", "", "ab", True),
            // Class items: a dash is a literal at either end and after a
            // range, a shorthand sits beside a literal dash, an escaped
            // metacharacter is itself.
            ("[a-c-e]", "", "-", True),
            ("[a-c-e]", "", "d", False),
            ("[a-c-e]", "", "e", True),
            (r"[\d-]", "", "-", True),
            (r"[-\d]", "", "-", True),
            (r"[\--a]", "", "5", True),
            ("[+,]", "", "-", False),
            (r"[\\]", "", "\\", True),
            (r"[\/]", "", "/", True),
            (r"[\.]", "", "x", False),
            (r"[^\d\s]", "", "a", True),
            (r"[^\S]", "", " ", True),
            ("[^^]", "", "a", True),
            ("[a^]", "", "^", True),
            ("[a-]]", "", "a]", True),
            ("[a-]]", "", "a", False),
            ("[a|b]", "", "|", True),
            ("a]", "", "a]", True),
            ("a}", "", "a}", True),
            // Escapes and anchors.
            (r"\\", "", "a\\b", True),
            (r"\^", "", "^", True),
            ("a^b", "", "a^b", False),
            ("a$b", "", "a$b", False),
            ("a$\nb", "m", "a\nb", True),
            ("a$\nb", "", "a\nb", False),
            (r"\Aa|b", "", "xb", True),
            ("a|b|c", "", "c", True),
            ("^(?:a|b)+$", "", "abba", True),
            ("x*", "", "", True),
            (r"\b", "", "", False),
            (r"\b", "", "a", True),
            (r"\w+\b$", "", "ab", True),
            // Non-ASCII characters in the pattern or the text, where no
            // Unicode-sensitive construct looks at them.
            ("é+", "", "caféé", True),
            (".", "", "é", True),
            ("^.$", "", "é", True),
            ("^.$", "", "😀", True),
            ("^..$", "", "😀", False),
            ("[é]", "", "é", True),
            ("[^é]", "", "é", False),
            ("[^é]", "", "e", True),
            ("[à-ÿ]", "", "é", True),
            ("a", "", "é", False),
            ("é", "", "e", False),
            // Flags: `i` on ASCII, `s` and `m` anywhere, any other letter
            // (or an upper-case one) ignored.
            ("[a-c]", "i", "B", True),
            ("[^a]", "i", "A", False),
            ("a.b", "s", "a\nb", True),
            ("a.b", "x", "a\nb", False),
            ("a.b", "S", "a\nb", False),
            ("A", "I", "a", False),
            ("^b", "m", "a\nb", True),
            ("^b", "M", "a\nb", False),
            ("A", "si", "a", True),
            ("A", "xi", "a", True),
            ("A", "ii", "a", True),
            ("a.b$", "ims", "A\nB\n", True),
            ("^b$", "m", "a\r\nb\r\n", False),
            ("^b\r$", "m", "a\r\nb\r\n", True),
            // The end of the text.
            ("$", "", "", True),
            ("^$", "", "", True),
            ("a$", "m", "a\n", True),
            (r"\Aa", "", "a\n", True),
            ("[$]", "", "a\n", False),
        ] {
            assert_eq!(
                RegexTest::new(pattern, flags).eval(text),
                want,
                "{pattern:?} {flags:?} on {text:?}"
            );
        }
    }

    /// A Unicode-sensitive construct (or flag `i`) on a non-ASCII text, and
    /// `$` without `m` on a text ending in `\n`, are left undecided.
    #[test]
    fn unicode_and_final_newline_rules() {
        assert_eq!(RegexTest::new(r"\d", "").eval("x٣"), Unknown);
        assert_eq!(RegexTest::new("A", "i").eval("xá"), Unknown);
        assert_eq!(
            RegexTest::new("é", "i").eval("É"),
            Unknown,
            "flag i on a non-ASCII pattern"
        );
        assert_eq!(RegexTest::new("ab$", "").eval("xab\n"), Unknown);
        assert_eq!(RegexTest::new("x$", "").eval("x\n\n"), Unknown);
        assert_eq!(
            RegexTest::new("ab", "").eval("xab\n"),
            True,
            "no $: decided"
        );
        assert_eq!(RegexTest::new("a\n$", "").eval("a\n"), Unknown);
        assert_eq!(
            RegexTest::new("a|b$", "").eval("a\n"),
            Unknown,
            "any $ in the pattern"
        );
        assert_eq!(
            RegexTest::new("ab$", "m").eval("xab\n"),
            True,
            "flag m: decided"
        );
        assert_eq!(RegexTest::new("ab$", "").eval("xab"), True);
        assert_eq!(RegexTest::new("ab$", "").eval("xab\r"), False);
    }

    /// Each Unicode-sensitive construct, alone and in a class, leaves a
    /// non-ASCII text undecided; without one the same text is decided.
    #[test]
    fn every_unicode_sensitive_construct_leaves_non_ascii_text_undecided() {
        for pattern in [
            r"\w", r"\W", r"\d", r"\D", r"\s", r"\S", r"\b", r"\B", r"[\w]", r"[\W]", r"[\d]",
            r"[\D]", r"[\s]", r"[\S]", r"[^\s]", r"a\b", r"x|\w",
        ] {
            assert_eq!(
                RegexTest::new(pattern, "").eval("é"),
                Unknown,
                "{pattern:?}"
            );
            assert_eq!(
                RegexTest::new(pattern, "").eval("aéb"),
                Unknown,
                "{pattern:?}"
            );
            assert_ne!(
                RegexTest::new(pattern, "").eval("ab"),
                Unknown,
                "{pattern:?} on ASCII"
            );
        }
        for pattern in ["a", ".", "[a-z]", "[^a]", "é", "^a$", "a|b", "(?:a)+"] {
            assert_ne!(
                RegexTest::new(pattern, "").eval("é"),
                Unknown,
                "{pattern:?}"
            );
        }
    }

    /// Python 3.14 matches `\B` on the empty text and 3.11–3.13 do not, so
    /// a pattern with `\B` leaves the empty text undecided (and nothing else).
    #[test]
    fn non_boundary_on_the_empty_text_is_undecided() {
        for pattern in [r"\B", r"x*\B", r"a|\B", r"^\B$", r"(?:\B)", r"a\B|b"] {
            assert_eq!(RegexTest::new(pattern, "").eval(""), Unknown, "{pattern:?}");
            assert_eq!(
                RegexTest::new(pattern, "m").eval(""),
                Unknown,
                "{pattern:?} m"
            );
        }
        assert_eq!(RegexTest::new(r"\B", "").eval("ab"), True);
        assert_eq!(RegexTest::new(r"\B", "").eval("!"), True);
        assert_eq!(RegexTest::new(r"a\B", "").eval("a"), False);
        assert_eq!(
            RegexTest::new(r"\b", "").eval(""),
            False,
            "\\b never matches the empty text"
        );
        assert_eq!(RegexTest::new(r"x*", "").eval(""), True);
    }

    /// Patterns outside the subset — including ones Python rejects or reads
    /// differently from Rust — leave every text undecided.
    #[test]
    fn undecidable_patterns_leave_every_text_undecided() {
        for pattern in [
            r"[\d-z]", "a{2,1}", "(?i)a", r"\Z", "{", "a**", "(a", "a)", "[]a]", "[[]", "[a&&b]",
            r"\x41", "a{,2}", "a*+", r"\1", "(?P<n>a)", "(?=a)", r"\p{L}", "^*", r"\b+", "a{1001}",
        ] {
            for text in ["a", "{", "]", "[", "&", "aa"] {
                assert_eq!(
                    RegexTest::new(pattern, "").eval(text),
                    Unknown,
                    "{pattern:?} on {text:?}"
                );
            }
        }
    }

    /// The rest of what Python rejects, reads differently, or the subset does
    /// not take: every text stays undecided, under any flags.
    #[test]
    fn the_rest_outside_the_subset_is_undecided() {
        for pattern in [
            // Escapes outside the grammar: letters, digits, other punctuation.
            r"\n",
            r"\t",
            r"\r",
            r"\f",
            r"\v",
            r"\a",
            r"\0",
            r"\u0041",
            r"\U00000041",
            r"\N{LATIN SMALL LETTER A}",
            r"\z",
            r"\G",
            r"\K",
            r"\h",
            r"\R",
            r"\X",
            r"\Q.\E",
            r"\q",
            r"\#",
            r"\&",
            r"\~",
            r"\ ",
            r"\,",
            r"\_",
            r"\<",
            r"\>",
            r"\=",
            r"\!",
            r"\:",
            r"\%",
            r"\'",
            "\\",
            r"\k<n>",
            r"\P{L}",
            r"\p{Greek}",
            r"\w\p{L}",
            r"[\n]",
            r"[\x41]",
            r"[\b]",
            r"[\p{L}]",
            r"[\0]",
            // Group syntax beyond `( )` and `(?: )`.
            "(?i:a)",
            "(?s)a",
            "(?-i:a)",
            "(?#c)",
            "(?<=a)",
            "(?<!a)",
            "(?!a)",
            "(?>a)",
            "(?(1)a|b)",
            "(?P=n)",
            "(?<n>a)",
            "(?'n'a)",
            "(?",
            "(?:",
            "(?:a",
            "(*)",
            "(?:*)",
            "(|*)",
            // Quantifiers: nothing to repeat, repeated twice, possessive,
            // braces that are no quantifier, a count past the cap.
            "*a",
            "+a",
            "?a",
            "a|*",
            "a{2}{3}",
            "a{2}+",
            "a{2}*",
            "a???",
            "a+*",
            "a?*",
            "a++",
            "a?+",
            "a{x}",
            "a{1, 2}",
            "a{}",
            "a{1",
            "a{1,",
            "a{1,2",
            "a{1,x}",
            "a{-1}",
            "a{1}}x{",
            "}{",
            "a{,}",
            "a{1001,}",
            "a{1,1001}",
            "a{99999999999}",
            "$*",
            "$+",
            r"\A*",
            r"\B*",
            r"\b?",
            "^{2}",
            "$?",
            "(?:^)**",
            // Classes: leading `]`, nested `[`, set operators, ranges Python
            // refuses or Rust reads differently, shorthands in ranges.
            "[]",
            "[^]",
            "[^]a]",
            "[[:alpha:]]",
            "[a[b]]",
            "[a--b]",
            "[a~~b]",
            "[a||b]",
            "[--a]",
            "[+--]",
            "[ --]",
            "[z-a]",
            r"[a-\d]",
            r"[\s-a]",
            r"[\w-.]",
            r"[a-\w]",
            r"[+-\d]",
            r"[\d-\w]",
            "[a-[]",
            "[a",
            "[a-",
            "[",
            "[^",
            "[a\\",
            r"[\",
            "[a-\\",
        ] {
            for flags in ["", "i", "s", "m", "ims"] {
                for text in ["", "a", "{", "]", "[", "&", "aa", "a\n", "é"] {
                    assert_eq!(
                        RegexTest::new(pattern, flags).eval(text),
                        Unknown,
                        "{pattern:?} {flags:?} on {text:?}"
                    );
                }
            }
        }
    }

    /// A test renders as its pattern and flags, and compares by them.
    #[test]
    fn tests_render_and_compare_by_pattern_and_flags() {
        assert_eq!(RegexTest::new("^A", "i").render(), r#""^A" flags="i""#);
        assert_eq!(
            RegexTest::new("a\"b\n", "").render(),
            "\"a\\\"b\\n\" flags=\"\""
        );
        assert_eq!(RegexTest::new("a", "i"), RegexTest::new("a", "i"));
        assert_ne!(RegexTest::new("a", "i"), RegexTest::new("a", ""));
        assert_ne!(RegexTest::new("a", ""), RegexTest::new("b", ""));
        // An undecidable pattern still has a name.
        assert_eq!(RegexTest::new("(?=a)", "").render(), r#""(?=a)" flags="""#);
    }

    /// Each escapable metacharacter is itself, outside a class and in one;
    /// the punctuation that is no metacharacter in either dialect needs no
    /// escape.
    #[test]
    fn escaped_metacharacters_and_bare_punctuation_are_literals() {
        for c in r".*+?()[]{}|^$\/-".chars() {
            let (outside, inside) = (format!("\\{c}"), format!("[\\{c}]"));
            for pattern in [&outside, &inside] {
                let test = RegexTest::new(pattern, "");
                assert_eq!(test.eval(&format!("x{c}y")), True, "{pattern:?}");
                assert_eq!(test.eval("xy"), False, "{pattern:?}");
            }
        }
        for c in "#&~<>=!:,'\"@%_ \t/-}]".chars() {
            let test = RegexTest::new(&c.to_string(), "");
            assert_eq!(test.eval(&format!("x{c}y")), True, "{c:?}");
            assert_eq!(test.eval("xy"), False, "{c:?}");
        }
    }

    /// A pattern that nests deeper than the translator reads, or whose
    /// compiled form would cost too much, is undecided — never a crash.
    #[test]
    fn oversized_patterns_are_undecided() {
        for depth in [300, 100_000] {
            let nested = format!("{}a{}", "(".repeat(depth), ")".repeat(depth));
            assert_eq!(
                RegexTest::new(&nested, "").eval("a"),
                Unknown,
                "{depth} groups"
            );
            let unclosed = "(?:".repeat(depth);
            assert_eq!(
                RegexTest::new(&unclosed, "").eval("a"),
                Unknown,
                "{depth} unclosed"
            );
        }
        assert_eq!(RegexTest::new("(?:a{1000}){1000}", "").eval("a"), Unknown);
        // A modest nesting and a long literal are still decided.
        let nested = format!("{}a{}", "(?:".repeat(20), ")".repeat(20));
        assert_eq!(RegexTest::new(&nested, "").eval("xa"), True);
        let long = "a".repeat(1_000);
        assert_eq!(RegexTest::new(&long, "").eval(&long), True);
        assert_eq!(RegexTest::new(&long, "").eval(&long[1..]), False);
        // A catastrophic pattern for a backtracking engine is linear here.
        let evil = RegexTest::new("^(a+)+$", "");
        assert_eq!(evil.eval(&format!("{}b", "a".repeat(5_000))), False);
    }
}
