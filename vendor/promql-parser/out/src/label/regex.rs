//! Translate RE2's syntax differences before using Rust's linear-time regex engine.
use regex::{Regex, RegexBuilder};

pub fn compile_regex(pattern: &str) -> Result<Regex, String> {
    let translated = translate(pattern)?;
    let ast = regex_syntax::ast::parse::ParserBuilder::new()
        .octal(true)
        .build()
        .parse(&translated)
        .map_err(|e| e.to_string())?;
    if !valid_repeats(&ast, 1000) {
        return Err("invalid repeat count".into());
    }
    RegexBuilder::new(&format!("^(?:{translated})$"))
        .dot_matches_new_line(true)
        .octal(true)
        .build()
        .map_err(|e| e.to_string())
}
fn translate(pattern: &str) -> Result<String, String> {
    let chars = pattern.chars().collect::<Vec<_>>();
    let mut out = String::new();
    let mut i = 0;
    let mut class = false;
    let mut class_first = false;
    let mut named = 0usize;
    while i < chars.len() {
        let c = chars[i];
        let first = class_first;
        if class && c != '^' {
            class_first = false;
        }
        if c == '\\' {
            i += 1;
            let Some(&escaped) = chars.get(i) else {
                return Err("trailing backslash".into());
            };
            match escaped {
                'd' => out.push_str("[0-9]"),
                'D' => out.push_str("[^0-9]"),
                'w' => out.push_str("[A-Za-z0-9_]"),
                'W' => out.push_str("[^A-Za-z0-9_]"),
                's' => out.push_str(r"[\t\n\f\r ]"),
                'S' => out.push_str(r"[^\t\n\f\r ]"),
                'b' | 'B' if !class => out.push_str(&format!("(?-u:\\{escaped})")),
                'Q' => {
                    i += 1;
                    let mut literal = String::new();
                    while i < chars.len() && !(chars[i] == '\\' && chars.get(i + 1) == Some(&'E')) {
                        literal.push(chars[i]);
                        i += 1;
                    }
                    out.push_str(&regex::escape(&literal));
                    if i < chars.len() {
                        i += 1;
                    } else {
                        break;
                    }
                }
                'u' | 'U' => return Err("invalid RE2 escape".into()),
                'p' | 'P' => {
                    let start = i;
                    i += 1;
                    let property = if chars.get(i) == Some(&'{') {
                        i += 1;
                        let begin = i;
                        while i < chars.len() && chars[i] != '}' {
                            i += 1;
                        }
                        if i == chars.len() {
                            return Err("unclosed Unicode class".into());
                        }
                        chars[begin..i].iter().collect::<String>()
                    } else {
                        let Some(&c) = chars.get(i) else {
                            return Err("missing Unicode class".into());
                        };
                        c.to_string()
                    };
                    if !include_str!("unicode-properties.txt")
                        .lines()
                        .any(|p| p == property.trim_start_matches('^'))
                    {
                        return Err("invalid RE2 Unicode class".into());
                    }
                    out.push('\\');
                    out.extend(chars[start..=i].iter());
                }
                '1'..='7' if !chars.get(i + 1).is_some_and(|c| ('0'..='7').contains(c)) => {
                    return Err("invalid backreference".into());
                }
                _ => {
                    out.push('\\');
                    out.push(escaped);
                }
            }
        } else if c == '[' {
            if class && chars.get(i + 1) != Some(&':') {
                out.push_str(r"\[");
            } else {
                if !class {
                    class_first = true;
                }
                class = true;
                out.push(c);
            }
        } else if c == ']' {
            out.push(c);
            // The inner closing bracket in POSIX classes does not close the outer class.
            if !first && (i == 0 || chars[i - 1] != ':') {
                class = false;
            }
        } else if class && c == '-' && chars.get(i + 1) == Some(&'-') {
            if i > 0 && matches!(chars[i - 1], '[' | '^') {
                out.push_str(r"\-");
            } else {
                out.push_str(r"-\-");
                i += 1;
            }
        } else if class && matches!(c, '&' | '~') {
            out.push('\\');
            out.push(c);
        } else if !class && c == '(' && chars.get(i + 1) == Some(&'?') {
            let mut j = i + 2;
            if chars.get(j) == Some(&'P') && chars.get(j + 1) == Some(&'<') {
                j += 1;
            }
            if chars.get(j) == Some(&'<') {
                j += 1;
                let begin = j;
                while j < chars.len() && chars[j] != '>' {
                    j += 1;
                }
                if j == chars.len() {
                    return Err("invalid capture name".into());
                }
                let name = chars[begin..j].iter().collect::<String>();
                if name.is_empty() || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                {
                    return Err("invalid capture name".into());
                }
                let hex = name.bytes().map(|b| format!("{b:02x}")).collect::<String>();
                out.push_str(&format!("(?P<__promql_{hex}_{named}>"));
                named += 1;
                i = j;
            } else {
                while j < chars.len() && !matches!(chars[j], ':' | ')') {
                    if !matches!(chars[j], 'i' | 'm' | 's' | 'U' | '-') {
                        return Err("invalid RE2 flags".into());
                    }
                    j += 1;
                }
                out.push(c);
            }
        } else if !class && c == '{' {
            let mut j = i + 1;
            while j < chars.len() && chars[j].is_ascii_digit() {
                j += 1;
            }
            let first = chars[i + 1..j].iter().collect::<String>();
            let mut counts = vec![first.clone()];
            if chars.get(j) == Some(&',') {
                j += 1;
                let start = j;
                while j < chars.len() && chars[j].is_ascii_digit() {
                    j += 1;
                }
                counts.push(chars[start..j].iter().collect());
            }
            if !first.is_empty()
                && chars.get(j) == Some(&'}')
                && !counts.iter().any(|v| v.len() > 1 && v.starts_with('0'))
            {
                if counts
                    .iter()
                    .filter(|v| !v.is_empty())
                    .any(|v| v.parse::<u64>().map_or(true, |n| n > 1000))
                {
                    return Err("invalid repeat count".into());
                }
                out.push(c);
            } else {
                out.push_str(r"\{");
            }
        } else {
            out.push(c);
        }
        i += 1;
    }
    Ok(out)
}

fn valid_repeats(ast: &regex_syntax::ast::Ast, n: u32) -> bool {
    use regex_syntax::ast::{Ast, RepetitionKind, RepetitionRange};
    match ast {
        Ast::Repetition(r) => {
            let m = match &r.op.kind {
                RepetitionKind::Range(range) => match range {
                    RepetitionRange::Exactly(n)
                    | RepetitionRange::AtLeast(n)
                    | RepetitionRange::Bounded(_, n) => *n,
                },
                _ => 1,
            };
            m == 0 || (m <= n && valid_repeats(&r.ast, n / m))
        }
        Ast::Group(g) => valid_repeats(&g.ast, n),
        Ast::Alternation(a) => a.asts.iter().all(|a| valid_repeats(a, n)),
        Ast::Concat(a) => a.asts.iter().all(|a| valid_repeats(a, n)),
        _ => true,
    }
}
/// Go replacement expansion, including duplicate named groups and literal dollars.
pub fn expand_regex(regex: &Regex, input: &str, template: &str) -> Option<String> {
    let captures = regex.captures(input)?;
    let name_char = Regex::new(r"^[\pL\p{Nd}_]$").unwrap();
    let mut out = String::new();
    let mut rest = template;
    while let Some(dollar) = rest.find('$') {
        out.push_str(&rest[..dollar]);
        rest = &rest[dollar + 1..];
        if let Some(next) = rest.strip_prefix('$') {
            out.push('$');
            rest = next;
            continue;
        }
        let brace = rest.starts_with('{');
        let text = if brace { &rest[1..] } else { rest };
        let len = text
            .char_indices()
            .take_while(|(_, c)| name_char.is_match(&c.to_string()))
            .map(|(i, c)| i + c.len_utf8())
            .last()
            .unwrap_or(0);
        if len == 0 || (brace && !text[len..].starts_with('}')) {
            out.push('$');
            continue;
        }
        let name = &text[..len];
        rest = &text[len + usize::from(brace)..];
        let value = if name.bytes().all(|b| b.is_ascii_digit())
            && (name.len() == 1 || !name.starts_with('0'))
        {
            name.parse::<usize>().ok().and_then(|i| captures.get(i))
        } else {
            let hex = name.bytes().map(|b| format!("{b:02x}")).collect::<String>();
            let prefix = format!("__promql_{hex}_");
            regex.capture_names().enumerate().find_map(|(i, n)| {
                n.filter(|n| n.starts_with(&prefix))
                    .and_then(|_| captures.get(i))
            })
        };
        if let Some(value) = value {
            out.push_str(value.as_str());
        }
    }
    out.push_str(rest);
    Some(out)
}
