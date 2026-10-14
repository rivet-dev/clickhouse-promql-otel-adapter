//! Resolve duration arithmetic with query context. Quoted label contents are
//! copied verbatim; offsets consume one grammar atom, preserving outer operators.
use anyhow::{Context, Result, bail};
use promql_parser::parser::{self, Expr, Offset};
use std::{collections::BTreeMap, time::Duration};
const MARKER: u64 = 10_000_000_000_000;
pub(super) fn syntax(query: &str) -> Result<Expr> {
    parse_internal(query, None)
}
pub(super) fn parse(query: &str, start: i64, end: i64, step: i64) -> Result<Expr> {
    parse_internal(query, Some((start, end, step)))
}
fn parse_internal(query: &str, context: Option<(i64, i64, i64)>) -> Result<Expr> {
    if query.len() > 65536 {
        bail!("query exceeds 65536 byte limit");
    }
    // Remove comments before looking for duration brackets or offset atoms.
    // Keep byte positions and newlines so diagnostics still refer to the input.
    let mut clean = query.as_bytes().to_vec();
    let mut at = 0;
    while at < clean.len() {
        match clean[at] {
            b'"' | b'\'' | b'`' => at = quoted_end(&clean, at)?,
            b'#' => {
                while at < clean.len() && clean[at] != b'\n' {
                    clean[at] = b' ';
                    at += 1;
                }
            }
            _ => at += 1,
        }
    }
    let clean = String::from_utf8(clean).expect("comments replaced with ASCII");
    let query = clean.as_str();
    let bytes = query.as_bytes();
    let mut out = String::new();
    let mut i = 0;
    let mut braces = 0;
    let mut expressions = BTreeMap::new();
    while i < bytes.len() {
        match bytes[i] {
            b'#' => {
                let end = query[i..].find('\n').map_or(bytes.len(), |n| i + n);
                out.push_str(&query[i..end]);
                i = end;
            }
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
                    out.push_str(&replace_duration(part, true, &mut expressions, context)?);
                }
                out.push(']');
                i = end_index + 1;
            }
            b'{' => {
                braces += 1;
                out.push('{');
                i += 1;
            }
            b'}' => {
                braces -= 1;
                out.push('}');
                i += 1;
            }
            b'o' | b'O'
                if braces == 0
                    && i > 0
                    && query[i..]
                        .get(..6)
                        .is_some_and(|s| s.eq_ignore_ascii_case("offset"))
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
                out.push_str(&replace_duration(
                    &query[begin..finish],
                    false,
                    &mut expressions,
                    context,
                )?);
                i = finish;
            }
            _ => {
                let c = query[i..].chars().next().unwrap();
                out.push(c);
                i += c.len_utf8();
            }
        }
    }
    let mut parsed = parser::parse(&out).map_err(anyhow::Error::msg)?;
    restore(&mut parsed, &expressions)?;
    Ok(parsed)
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
fn evaluate(e: &Expr, start: i64, end: i64, step: i64) -> Result<f64> {
    Ok(match e {
        Expr::NumberLiteral(v) => v.val,
        Expr::Paren(p) => evaluate(&p.expr, start, end, step)?,
        Expr::Unary(u) => {
            evaluate(&u.expr, start, end, step)? * if u.positive { 1.0 } else { -1.0 }
        }
        Expr::Binary(b) => {
            if matches!(b.rhs.as_ref(),Expr::NumberLiteral(n) if n.val==0.0) {
                match b.op.to_string().as_str() {
                    "/" => bail!("division by zero"),
                    "%" => bail!("modulo by zero"),
                    _ => {}
                }
            }
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
fn duration_expr(text: &str) -> Result<Expr> {
    let text = regex::Regex::new(r"(?i)\bstep\(\)")
        .unwrap()
        .replace_all(text, "step()");
    let text = regex::Regex::new(r"\b0(?:ms|[smhdwy])\b")
        .unwrap()
        .replace_all(&text, "0");
    let mut expr = parser::parse(&text).map_err(anyhow::Error::msg)?;
    fn positive(expr: &mut Expr) {
        if let Expr::Unary(u) = expr
            && u.positive
        {
            *expr = *u.expr.clone();
            positive(expr);
            return;
        }
        match expr {
            Expr::Unary(u) => positive(&mut u.expr),
            Expr::Paren(p) => positive(&mut p.expr),
            Expr::Binary(b) => {
                positive(&mut b.lhs);
                positive(&mut b.rhs)
            }
            Expr::Call(c) => {
                for a in &mut c.args.args {
                    positive(a)
                }
            }
            _ => {}
        }
    }
    positive(&mut expr);
    // Validate the permitted expression grammar without imposing query-time values.
    evaluate(&expr, 0, 1_000, 1_000)?;
    Ok(expr)
}
fn replace_duration(
    text: &str,
    positive: bool,
    expressions: &mut BTreeMap<u64, (Expr, f64)>,
    context: Option<(i64, i64, i64)>,
) -> Result<String> {
    let expr = duration_expr(text)?;
    let literal = matches!(expr, Expr::NumberLiteral(_));
    let (start, end, step) = context.unwrap_or((0, 1000, 1000));
    let value = evaluate(&expr, start, end, step)?;
    if literal || context.is_some() {
        if !value.is_finite() || value.abs() >= i64::MAX as f64 / 1e9 {
            bail!("duration out of range");
        }
        if positive && value <= 0.0 {
            bail!("duration must be greater than 0");
        }
    }
    if matches!(&expr, Expr::NumberLiteral(n) if n.duration) {
        return Ok(text.trim().to_owned());
    }
    if literal && (value.abs() * 1000.0).fract() == 0.0 {
        if value == 0.0 {
            return Ok("0".into());
        }
        return Ok(format!(
            "{}{}ms",
            if value < 0.0 { "-" } else { "" },
            (value.abs() * 1000.0) as u64
        ));
    }
    let marker = MARKER + expressions.len() as u64;
    expressions.insert(
        marker,
        (
            expr,
            if context.is_some() || literal {
                value
            } else {
                0.0
            },
        ),
    );
    Ok(format!("{marker}ms"))
}
fn restore(expr: &mut Expr, expressions: &BTreeMap<u64, (Expr, f64)>) -> Result<()> {
    fn range(
        value: &mut Duration,
        expression: &mut Option<Box<Expr>>,
        map: &BTreeMap<u64, (Expr, f64)>,
    ) {
        if let Some((expr, seconds)) = map.get(&(value.as_millis() as u64)) {
            *expression = if matches!(expr, Expr::NumberLiteral(_)) {
                None
            } else {
                Some(Box::new(expr.clone()))
            };
            *value = Duration::from_nanos((seconds.max(0.0) * 1e9) as u64);
        }
    }
    fn offset(
        value: &mut Option<Offset>,
        expression: &mut Option<Box<Expr>>,
        map: &BTreeMap<u64, (Expr, f64)>,
    ) {
        if let Some(Offset::Pos(duration)) = value
            && let Some((expr, seconds)) = map.get(&(duration.as_millis() as u64))
        {
            *expression = if matches!(expr, Expr::NumberLiteral(_)) {
                None
            } else {
                Some(Box::new(expr.clone()))
            };
            let d = Duration::from_nanos((seconds.abs() * 1e9) as u64);
            *value = Some(if *seconds < 0.0 {
                Offset::Neg(d)
            } else {
                Offset::Pos(d)
            });
        }
    }
    match expr {
        Expr::VectorSelector(v) => {
            if !v.matchers.or_matchers.is_empty() {
                bail!("unexpected identifier \"or\" in label matching, expected \",\" or \"}}\"");
            }
            offset(&mut v.offset, &mut v.offset_expr, expressions)
        }
        Expr::MatrixSelector(m) => {
            if !m.vs.matchers.or_matchers.is_empty() {
                bail!(
                    "unexpected identifier \"or\" in label matching, expected comma or closing brace"
                );
            }
            range(&mut m.range, &mut m.range_expr, expressions);
            offset(&mut m.vs.offset, &mut m.vs.offset_expr, expressions);
        }
        Expr::Subquery(s) => {
            restore(&mut s.expr, expressions)?;
            range(&mut s.range, &mut s.range_expr, expressions);
            if let Some(step) = &mut s.step {
                range(step, &mut s.step_expr, expressions)
            }
            offset(&mut s.offset, &mut s.offset_expr, expressions);
        }
        Expr::Call(c) => {
            for a in &mut c.args.args {
                restore(a, expressions)?
            }
        }
        Expr::Aggregate(a) => {
            restore(&mut a.expr, expressions)?;
            if let Some(p) = &mut a.param {
                restore(p, expressions)?
            }
        }
        Expr::Binary(b) => {
            restore(&mut b.lhs, expressions)?;
            restore(&mut b.rhs, expressions)?
        }
        Expr::Unary(u) => restore(&mut u.expr, expressions)?,
        Expr::Paren(p) => restore(&mut p.expr, expressions)?,
        _ => {}
    }
    Ok(())
}
