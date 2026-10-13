//! Resolve duration arithmetic with query context. Quoted label contents are
//! copied verbatim; offsets consume one grammar atom, preserving outer operators.
use anyhow::{Context, Result, bail};
use promql_parser::parser::{self, Expr};
pub(super) fn parse(query: &str, start: i64, end: i64, step: i64) -> Result<Expr> {
    if query.len() > 65536 {
        bail!("query exceeds 65536 byte limit");
    }
    let bytes = query.as_bytes();
    let mut out = String::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'"' | b'\'' | b'`' => {
                let next = quoted_end(bytes, i)?;
                out.push_str(&query[i..next]);
                i = next;
            }
            b'[' => {
                let end_index = query[i + 1..]
                    .find(']')
                    .context("unclosed range selector")?
                    + i
                    + 1;
                let parts = query[i + 1..end_index].split(':').collect::<Vec<_>>();
                if parts.len() > 2 {
                    bail!("unexpected second colon(:) in brackets")
                }
                out.push('[');
                for (j, part) in parts.iter().enumerate() {
                    if j > 0 {
                        out.push(':')
                    }
                    if part.trim().is_empty() && j > 0 {
                        continue;
                    }
                    let value = duration(part, start, end, step)?;
                    if value <= 0.0 {
                        bail!("duration must be greater than 0")
                    };
                    out.push_str(&format!("{}ms", (value * 1000.0).round() as u64));
                }
                out.push(']');
                i = end_index + 1;
            }
            b'o' if query[i..].starts_with("offset")
                && (i == 0 || !identifier(bytes[i - 1]))
                && bytes.get(i + 6).is_none_or(|b| !identifier(*b)) =>
            {
                out.push_str("offset ");
                i += 6;
                while bytes.get(i).is_some_and(u8::is_ascii_whitespace) {
                    i += 1
                }
                let begin = i;
                let mut atom = i;
                if bytes.get(atom).is_some_and(|c| *c == b'+' || *c == b'-') {
                    atom += 1;
                    while bytes.get(atom).is_some_and(u8::is_ascii_whitespace) {
                        atom += 1
                    }
                }
                let finish = if bytes.get(atom) == Some(&b'(') {
                    paren_end(bytes, atom)?
                } else {
                    let mut end = atom;
                    while bytes.get(end).is_some_and(|b| identifier(*b) || *b == b'.') {
                        end += 1;
                    }
                    let mut paren = end;
                    while bytes.get(paren).is_some_and(u8::is_ascii_whitespace) {
                        paren += 1
                    }
                    if bytes.get(paren) == Some(&b'(') {
                        paren_end(bytes, paren)?
                    } else {
                        end
                    }
                };
                if finish <= begin {
                    bail!("expected duration after offset")
                }
                let value = duration(&query[begin..finish], start, end, step)?;
                if value < 0.0 {
                    out.push('-')
                }
                if value == 0.0 {
                    out.push('0')
                } else {
                    out.push_str(&format!("{}ms", (value.abs() * 1000.0).round() as u64));
                }
                i = finish;
            }
            _ => {
                let c = query[i..].chars().next().unwrap();
                out.push(c);
                i += c.len_utf8();
            }
        }
    }
    parser::parse(&out).map_err(anyhow::Error::msg)
}
fn identifier(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}
fn quoted_end(bytes: &[u8], start: usize) -> Result<usize> {
    let quote = bytes[start];
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == quote {
            return Ok(i + 1);
        }
        if bytes[i] == b'\\' && quote != b'`' {
            i += 1
        }
        i += 1;
    }
    bail!("unterminated string")
}
fn paren_end(bytes: &[u8], start: usize) -> Result<usize> {
    let mut depth = 0;
    for (i, &b) in bytes.iter().enumerate().skip(start) {
        if b == b'(' {
            depth += 1
        }
        if b == b')' {
            depth -= 1;
            if depth == 0 {
                return Ok(i + 1);
            }
        }
    }
    bail!("unclosed duration expression")
}
fn duration(text: &str, start: i64, end: i64, step: i64) -> Result<f64> {
    fn evaluate(e: &Expr, start: i64, end: i64, step: i64) -> Result<f64> {
        Ok(match e {
            Expr::NumberLiteral(v) => v.val,
            Expr::Paren(p) => evaluate(&p.expr, start, end, step)?,
            Expr::Unary(u) => -evaluate(&u.expr, start, end, step)?,
            Expr::Binary(b) => {
                let a = evaluate(&b.lhs, start, end, step)?;
                let c = evaluate(&b.rhs, start, end, step)?;
                match b.op.to_string().as_str() {
                    "+" => a + c,
                    "-" => a - c,
                    "*" => a * c,
                    "/" => a / c,
                    "%" => a % c,
                    "^" => a.powf(c),
                    _ => bail!("invalid duration operator"),
                }
            }
            Expr::Call(c) => match c.func.name {
                "step" => step as f64 / 1000.0,
                "range" => (end as f64 - start as f64) / 1000.0,
                "min_of" | "max_of" => {
                    let a = evaluate(&c.args.args[0], start, end, step)?;
                    let b = evaluate(&c.args.args[1], start, end, step)?;
                    if a.is_nan() || b.is_nan() {
                        f64::NAN
                    } else if c.func.name == "min_of" {
                        a.min(b)
                    } else {
                        a.max(b)
                    }
                }
                _ => bail!("invalid duration function"),
            },
            _ => bail!("duration expression must be constant"),
        })
    }
    let text = regex::Regex::new(r"(?i)\bstep\(\)")
        .unwrap()
        .replace_all(text, "step()");
    let text = regex::Regex::new(r"\b0(?:ms|[smhdwy])\b")
        .unwrap()
        .replace_all(&text, "0");
    let expr = parser::parse(&text).map_err(anyhow::Error::msg)?;
    let value = evaluate(&expr, start, end, step)?;
    if !value.is_finite() || value.abs() > i64::MAX as f64 / 1000.0 {
        bail!("duration expression overflows milliseconds")
    }
    Ok(value)
}
