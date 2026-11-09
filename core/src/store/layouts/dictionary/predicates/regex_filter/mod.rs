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
//! and their lazy forms. Anything else (backreferences, lookaround,
//! `\p{...}`, `\Z`, inline flags, named groups, possessive quantifiers, a
//! brace that is no quantifier, a leading `]` in a class) leaves every text
//! undecided, as do flag `i` on a pattern holding a non-ASCII character and
//! a compiled program past `NFA_SIZE_LIMIT`.
//!
//! Three rules keep a decided verdict exact: a pattern using `\w \W \s \S \d
//! \D \b \B` or flag `i` decides ASCII texts only; a pattern with `$` and
//! without flag `m` leaves a text ending in `\n` undecided (Python's `$` also
//! matches before a final newline); and a pattern with `\B` leaves the empty
//! text undecided (Python 3.14 matches `\B` there, 3.11–3.13 do not).

use std::fmt::Write as _;

use super::Verdict;

/// The class shorthands, spelled out on ASCII as Rust class items: wrapped in
/// `[ ]` where a shorthand stands alone (`escape`) and spliced into a class
/// (`class_escape`), from this one table so the two cannot drift apart. A
/// pattern using one decides ASCII texts only, so ASCII ranges suffice, and
/// they compile to a handful of states where Rust's Unicode `\w` or a negated
/// class (`[^0-9]`) is a large program. Python's `\s` also covers
/// `\x1c`–`\x1f` on ASCII text, which Rust's `\s` leaves out.
const SHORTHANDS: [(char, &str); 6] = [
    ('d', r"0-9"),
    ('D', r"\x00-\x2F\x3A-\x7F"),
    ('w', r"0-9A-Z_a-z"),
    ('W', r"\x00-\x2F\x3A-\x40\x5B-\x5E\x60\x7B-\x7F"),
    ('s', r"\t-\r\x1C-\x20"),
    ('S', r"\x00-\x08\x0E-\x1B\x21-\x7F"),
];

/// The class items of the shorthand `\letter`, if it is one.
fn shorthand(letter: char) -> Option<&'static str> {
    SHORTHANDS
        .iter()
        .find(|&&(l, _)| l == letter)
        .map(|&(_, items)| items)
}

/// The grammar's escapable metacharacters.
const ESCAPABLE: &str = r".*+?()[]{}|^$\/-";

/// The Rust `regex` metacharacters, escaped wherever a literal lands.
const RUST_META: &str = r"\.+*?()|[]{}^$#&-~";

/// Largest counted repetition the subset takes.
const MAX_REPEAT: u32 = 1_000;

/// Largest compiled program the subset takes, in bytes of NFA. Matching costs
/// O(states) per byte whenever the lazy DFA is not enough, so this bounds a
/// pattern's cost; the patterns the subset is for are a few dozen states.
/// Counted repetitions multiply (a nested `{n}{m}` has n times m states), so
/// the limit refuses those while a per-count cap cannot.
const NFA_SIZE_LIMIT: usize = 1 << 17;

/// The lazy DFA's cache, in bytes: enough to hold the DFA of the largest
/// program under `NFA_SIZE_LIMIT` on its worst text (every position of a long
/// literal live at once). A cache that is too small is slower, not safer: the
/// engine then runs the NFA simulation for every candidate. It is allocated
/// only as the texts need it.
const DFA_CACHE_LIMIT: usize = 1 << 24;

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
                .size_limit(NFA_SIZE_LIMIT)
                .dfa_size_limit(DFA_CACHE_LIMIT)
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
        if let Some(items) = shorthand(e) {
            self.unicode_sensitive = true;
            write!(self.out, "[{items}]").ok()?;
            return Some(true);
        }
        match e {
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
                            // A range ending in `-` (`[+--]`) is Python's
                            // "possible set difference".
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
        if let Some(items) = shorthand(e) {
            self.unicode_sensitive = true;
            return Some(ClassItem::Shorthand(items));
        }
        ESCAPABLE.contains(e).then_some(ClassItem::Char(e))
    }
}

#[cfg(test)]
mod tests;
