//! Ahead-of-time compilation of dashboard PromQL into ClickHouse leaves plus
//! a small operator tree.
//!
//! Each selector becomes one leaf: a complete `SELECT` that buckets one
//! cluster's samples by the request step. An aggregation directly over a
//! selector (`sum`, `min`, `max`, `avg` or `count`, by a label list or over
//! everything) runs in its leaf, so the leaf returns one series per group.
//! The other operators stay in the plan's tree, for the caller to evaluate
//! over the leaf results.
//! Exact Prometheus semantics are not a goal: ranges are ignored, and
//! `rate`/`increase` are reset-aware growth per step bucket rather than
//! extrapolated sliding windows.
//!
//! Metric and label names match as the Prometheus receiver stores them: a
//! counter under its scraped name, with or without `_total`, and a classic
//! histogram under its base name.
mod sql;

use std::collections::BTreeMap;

use promql_parser::label::{MatchOp, Matcher};
use promql_parser::parser::{
    self, AggregateExpr, BinaryExpr, Call, Expr, LabelModifier, MatrixSelector, Offset,
    VectorMatchCardinality, VectorSelector,
};
use serde::{Deserialize, Serialize};

use sql::{Grouping, GrowthColumn, Reading, Table};
pub use sql::{MAX_SERIES_PER_LEAF, SERIES_LIMIT_ERROR, quote};

/// The label whose matchers are removed: the cluster comes only from the
/// leaf's `ProjectId` predicate.
const PROJECT_LABEL: &str = "rivet_project";

/// Grafana's duration variables. Every leaf buckets by the request step and
/// ignores ranges, so any duration stands in for them inside a range.
const DURATION_VARIABLES: [&str; 3] = ["__rate_interval", "__interval", "__range"];

/// Grafana's numeric interval variables. Their value depends on the request
/// range or step, which a compiled plan does not know.
const NUMERIC_INTERVAL_VARIABLES: [&str; 4] = [
    "__range_s",
    "__range_ms",
    "__interval_ms",
    "__rate_interval_ms",
];

/// One expression to compile, as read from a `compile` input line.
#[derive(Debug, Deserialize)]
pub struct Target {
    pub id: String,
    pub expr: String,
    /// Values for variables used outside label matchers, such as `$k` in
    /// `topk($k, ...)`.
    #[serde(default)]
    pub variables: BTreeMap<String, String>,
}

/// One `compile` output line.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum Compiled {
    Plan { id: String, plan: Plan },
    Error { id: String, error: CompileError },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CompileError {
    /// The PromQL construct that could not be compiled.
    pub construct: String,
    pub message: String,
}

impl CompileError {
    fn new(construct: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            construct: construct.into(),
            message: message.into(),
        }
    }

    fn unsupported(construct: impl Into<String>) -> Self {
        let construct = construct.into();
        let message = format!("{construct} is not supported");
        Self { construct, message }
    }
}

impl std::fmt::Display for CompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.construct, self.message)
    }
}

impl std::error::Error for CompileError {}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Plan {
    pub leaves: Vec<Leaf>,
    pub tree: Node,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Leaf {
    /// A complete `SELECT`. Its placeholders are `{project:String}`,
    /// `{from:UInt32}`, `{to:UInt32}`, `{query_from:UInt32}` and
    /// `{step:UInt32}`.
    pub sql: String,
    pub shape: Shape,
    /// The selector's `offset`, which the caller subtracts from the window.
    pub offset_seconds: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Shape {
    /// Rows of `(t, labels, value)`.
    Value,
    /// Rows of `(t, labels, counts, bounds)`.
    Buckets,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Node {
    Leaf {
        index: usize,
    },
    Number {
        value: f64,
    },
    Aggregate {
        #[serde(rename = "fn")]
        function: AggregateFunction,
        #[serde(skip_serializing_if = "Option::is_none")]
        by: Option<Vec<String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        without: Option<Vec<String>>,
        arg: Box<Node>,
    },
    Topk {
        k: f64,
        #[serde(skip_serializing_if = "Option::is_none")]
        by: Option<Vec<String>>,
        arg: Box<Node>,
    },
    Bottomk {
        k: f64,
        #[serde(skip_serializing_if = "Option::is_none")]
        by: Option<Vec<String>>,
        arg: Box<Node>,
    },
    Quantile {
        q: f64,
        #[serde(skip_serializing_if = "Option::is_none")]
        by: Option<Vec<String>>,
        arg: Box<Node>,
    },
    HistogramQuantile {
        q: f64,
        arg: Box<Node>,
    },
    Binary {
        operator: String,
        bool: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        on: Option<Vec<String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        ignoring: Option<Vec<String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        group: Option<Group>,
        #[serde(skip_serializing_if = "Option::is_none")]
        include: Option<Vec<String>>,
        lhs: Box<Node>,
        rhs: Box<Node>,
    },
    Call {
        #[serde(rename = "fn")]
        function: CallFunction,
        args: Vec<Argument>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregateFunction {
    Sum,
    Min,
    Max,
    Avg,
    Count,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Group {
    Left,
    Right,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CallFunction {
    ClampMin,
    ClampMax,
    Abs,
    Scalar,
    Vector,
    Time,
    LabelReplace,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum Argument {
    Node(Node),
    String(String),
    Number(f64),
}

/// Whether an expression yields ordinary samples or classic histogram
/// buckets, which only `histogram_quantile` (optionally through `sum by
/// (le, ...)`) may consume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Value,
    Buckets,
}

/// Compiles one dashboard expression. `variables` fill variables used
/// outside label matchers; a matcher whose value references any variable is
/// removed, so the panel covers every value of that label.
pub fn compile(expr: &str, variables: &BTreeMap<String, String>) -> Result<Plan, CompileError> {
    let source = substitute(expr, variables)?;
    let parsed = parser::parse(&source).map_err(|e| CompileError::new("syntax", e))?;
    let mut compiler = Compiler::default();
    let (tree, kind) = compiler.node(&parsed)?;
    if kind == Kind::Buckets {
        return Err(CompileError::new(
            "histogram buckets",
            "classic histogram buckets must feed histogram_quantile",
        ));
    }
    Ok(Plan {
        leaves: compiler.leaves,
        tree,
    })
}

/// Compiles one input line's target.
pub fn compile_target(target: Target) -> Compiled {
    match compile(&target.expr, &target.variables) {
        Ok(plan) => Compiled::Plan {
            id: target.id,
            plan,
        },
        Err(error) => Compiled::Error {
            id: target.id,
            error,
        },
    }
}

/// Replaces Grafana variables outside string literals: duration variables
/// inside a range by a placeholder duration and the others by `variables`.
/// Variables inside string literals stay, so their matchers can be removed
/// after parsing.
fn substitute(expr: &str, variables: &BTreeMap<String, String>) -> Result<String, CompileError> {
    let chars = expr.chars().collect::<Vec<_>>();
    let mut out = String::with_capacity(expr.len());
    let mut quote: Option<char> = None;
    let mut bracket_depth = 0usize;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if let Some(open) = quote {
            out.push(c);
            if c == '\\' && open != '`' {
                if let Some(next) = chars.get(i + 1) {
                    out.push(*next);
                    i += 1;
                }
            } else if c == open {
                quote = None;
            }
            i += 1;
            continue;
        }
        if matches!(c, '"' | '\'' | '`') {
            quote = Some(c);
            out.push(c);
            i += 1;
            continue;
        }
        let bracket_reference = match c {
            '[' if chars.get(i + 1) == Some(&'[') => bracket_variable(&chars, i),
            _ => None,
        };
        let reference = if c == '$' {
            dollar_variable(&chars, i)
        } else if bracket_reference.is_some() {
            bracket_reference
        } else {
            if c == '[' {
                bracket_depth += 1;
            } else if c == ']' {
                bracket_depth = bracket_depth.saturating_sub(1);
            }
            out.push(c);
            i += 1;
            continue;
        };
        let Some(reference) = reference else {
            return Err(CompileError::new(
                "variable",
                format!("malformed variable in {expr:?}"),
            ));
        };
        out.push_str(&variable_value(&reference, bracket_depth > 0, variables)?);
        i = reference.end;
    }
    Ok(out)
}

/// A Grafana variable reference: `$name`, `${name}`, `${name:format}`,
/// `[[name]]` or `[[name:format]]`.
struct VariableReference {
    name: String,
    format: Option<String>,
    /// The index just past the reference.
    end: usize,
}

/// Parses a `$name`, `${name}` or `${name:format}` reference at `start`.
fn dollar_variable(chars: &[char], start: usize) -> Option<VariableReference> {
    if chars.get(start + 1) != Some(&'{') {
        let end = identifier_end(chars, start + 1)?;
        return Some(VariableReference {
            name: chars[start + 1..end].iter().collect(),
            format: None,
            end,
        });
    }
    enclosed_variable(chars, start + 2, "}")
}

/// Parses a `[[name]]` or `[[name:format]]` reference at `start`.
fn bracket_variable(chars: &[char], start: usize) -> Option<VariableReference> {
    enclosed_variable(chars, start + 2, "]]")
}

/// Parses `name` or `name:format` starting at `start` and closed by `close`.
fn enclosed_variable(chars: &[char], start: usize, close: &str) -> Option<VariableReference> {
    let name_end = identifier_end(chars, start)?;
    let name = chars[start..name_end].iter().collect::<String>();
    let rest = chars[name_end..].iter().collect::<String>();
    let (format, closed_at) = match rest.strip_prefix(':') {
        Some(after_colon) => {
            let format_len = after_colon.find(close)?;
            let format = &after_colon[..format_len];
            if format.is_empty() {
                return None;
            }
            (
                Some(format.to_owned()),
                name_end + 1 + format.chars().count(),
            )
        }
        None if rest.starts_with(close) => (None, name_end),
        None => return None,
    };
    Some(VariableReference {
        name,
        format,
        end: closed_at + close.chars().count(),
    })
}

/// The index just past an identifier starting at `start`, or `None` when no
/// identifier starts there.
fn identifier_end(chars: &[char], start: usize) -> Option<usize> {
    let first = chars.get(start)?;
    if !(first.is_ascii_alphabetic() || *first == '_') {
        return None;
    }
    let mut end = start + 1;
    while end < chars.len() && (chars[end].is_ascii_alphanumeric() || chars[end] == '_') {
        end += 1;
    }
    Some(end)
}

/// The text a variable outside string literals is replaced by.
fn variable_value(
    reference: &VariableReference,
    in_range: bool,
    variables: &BTreeMap<String, String>,
) -> Result<String, CompileError> {
    let name = reference.name.as_str();
    let construct = format!("${name}");
    if NUMERIC_INTERVAL_VARIABLES.contains(&name) {
        return Err(CompileError::new(
            construct,
            format!(
                "${name} depends on the request range or step, which a compiled plan does not know"
            ),
        ));
    }
    if DURATION_VARIABLES.contains(&name) {
        if !in_range {
            return Err(CompileError::new(
                construct,
                format!("${name} is supported only as a range duration"),
            ));
        }
        return Ok("1m".to_owned());
    }
    if let Some(format) = reference.format.as_deref().filter(|f| *f != "raw") {
        return Err(CompileError::new(
            construct,
            format!("variable format :{format} is supported only inside label matchers"),
        ));
    }
    variables.get(name).cloned().ok_or_else(|| {
        CompileError::new(
            construct,
            format!("variable ${name} is used outside a label matcher and has no value"),
        )
    })
}

#[derive(Default)]
struct Compiler {
    leaves: Vec<Leaf>,
}

impl Compiler {
    fn node(&mut self, expr: &Expr) -> Result<(Node, Kind), CompileError> {
        match expr {
            Expr::NumberLiteral(n) => Ok((Node::Number { value: n.val }, Kind::Value)),
            Expr::Paren(p) => self.node(&p.expr),
            Expr::Unary(u) => {
                let (node, kind) = self.value(&u.expr)?;
                if u.positive {
                    return Ok((node, kind));
                }
                Ok((
                    Node::Binary {
                        operator: "*".into(),
                        bool: false,
                        on: None,
                        ignoring: None,
                        group: None,
                        include: None,
                        lhs: Box::new(node),
                        rhs: Box::new(Node::Number { value: -1.0 }),
                    },
                    Kind::Value,
                ))
            }
            Expr::VectorSelector(v) => self.selector(v, Table::Gauge, Reading::Last, None),
            Expr::Aggregate(a) => self.aggregate(a),
            Expr::Binary(b) => self.binary(b),
            Expr::Call(c) => self.call(c),
            Expr::MatrixSelector(_) => Err(CompileError::new(
                "range vector",
                "a range vector must be the argument of a supported function",
            )),
            Expr::Subquery(_) => Err(CompileError::unsupported("subquery")),
            Expr::StringLiteral(_) => Err(CompileError::unsupported("string literal")),
            Expr::Extension(_) => Err(CompileError::unsupported("extension")),
        }
    }

    /// Compiles an expression that must yield ordinary samples.
    fn value(&mut self, expr: &Expr) -> Result<(Node, Kind), CompileError> {
        let (node, kind) = self.node(expr)?;
        if kind == Kind::Buckets {
            return Err(CompileError::new(
                "histogram buckets",
                "classic histogram buckets must feed histogram_quantile",
            ));
        }
        Ok((node, kind))
    }

    fn aggregate(&mut self, a: &AggregateExpr) -> Result<(Node, Kind), CompileError> {
        let op = a.op.to_string();
        let (by, without) = match &a.modifier {
            None => (None, None),
            Some(LabelModifier::Include(labels)) => (Some(labels.labels.clone()), None),
            Some(LabelModifier::Exclude(labels)) => (None, Some(labels.labels.clone())),
        };
        if let Some(grouped) = self.grouped_leaf(&op, by.as_deref(), without.as_deref(), &a.expr) {
            return grouped;
        }
        let (arg, kind) = self.node(&a.expr)?;
        if kind == Kind::Buckets {
            // Summing buckets keeps them buckets as long as `le` stays a
            // grouping label.
            let by = match (op.as_str(), by, without) {
                ("sum", Some(by), None) if by.iter().any(|l| l == "le") => {
                    by.into_iter().filter(|l| l != "le").collect::<Vec<_>>()
                }
                _ => {
                    return Err(CompileError::new(
                        format!("{op} over histogram buckets"),
                        "classic histogram buckets may only be summed by a label list containing le",
                    ));
                }
            };
            return Ok((
                Node::Aggregate {
                    function: AggregateFunction::Sum,
                    by: Some(by),
                    without: None,
                    arg: Box::new(arg),
                },
                Kind::Buckets,
            ));
        }
        let arg = Box::new(arg);
        let node = match op.as_str() {
            "sum" | "min" | "max" | "avg" | "count" => Node::Aggregate {
                function: match op.as_str() {
                    "sum" => AggregateFunction::Sum,
                    "min" => AggregateFunction::Min,
                    "max" => AggregateFunction::Max,
                    "avg" => AggregateFunction::Avg,
                    _ => AggregateFunction::Count,
                },
                by,
                without,
                arg,
            },
            "topk" | "bottomk" | "quantile" => {
                if without.is_some() {
                    return Err(CompileError::unsupported(format!("{op} without (...)")));
                }
                let parameter = literal_parameter(&op, a.param.as_deref())?;
                match op.as_str() {
                    "topk" => Node::Topk {
                        k: parameter,
                        by,
                        arg,
                    },
                    "bottomk" => Node::Bottomk {
                        k: parameter,
                        by,
                        arg,
                    },
                    _ => Node::Quantile {
                        q: parameter,
                        by,
                        arg,
                    },
                }
            }
            op => return Err(CompileError::unsupported(op)),
        };
        Ok((node, Kind::Value))
    }

    /// Folds an aggregation directly over a selector into that selector's
    /// leaf, so ClickHouse returns one series per group. `None` when the
    /// aggregation stays in the tree.
    fn grouped_leaf(
        &mut self,
        op: &str,
        by: Option<&[String]>,
        without: Option<&[String]>,
        expr: &Expr,
    ) -> Option<Result<(Node, Kind), CompileError>> {
        if without.is_some() {
            return None;
        }
        let function = match op {
            "sum" => AggregateFunction::Sum,
            "min" => AggregateFunction::Min,
            "max" => AggregateFunction::Max,
            "avg" => AggregateFunction::Avg,
            "count" => AggregateFunction::Count,
            _ => return None,
        };
        let by = by.unwrap_or_default();
        match expr {
            Expr::Paren(p) => self.grouped_leaf(op, Some(by), None, &p.expr),
            Expr::VectorSelector(v) => {
                let grouping = Grouping {
                    function,
                    by: by.to_vec(),
                };
                Some(self.selector(v, Table::Gauge, Reading::Last, Some(grouping)))
            }
            Expr::Call(c) if is_range_function(c.func.name) => {
                let matrix = match matrix_argument(c.func.name, &c.args.args) {
                    Ok(matrix) => matrix,
                    Err(error) => return Some(Err(error)),
                };
                let reads_buckets =
                    is_counter_function(c.func.name) && counter_source(matrix).1.is_none();
                let by = if reads_buckets {
                    // Summing buckets keeps them buckets only while `le` stays
                    // a grouping label; anything else is refused by the tree path.
                    if function != AggregateFunction::Sum || !by.iter().any(|l| l == "le") {
                        return None;
                    }
                    by.iter().filter(|l| *l != "le").cloned().collect()
                } else {
                    by.to_vec()
                };
                let grouping = Grouping { function, by };
                Some(self.range_call(c.func.name, matrix, Some(grouping)))
            }
            _ => None,
        }
    }

    fn binary(&mut self, b: &BinaryExpr) -> Result<(Node, Kind), CompileError> {
        let operator = b.op.to_string();
        let (lhs, _) = self.value(&b.lhs)?;
        let (rhs, _) = self.value(&b.rhs)?;
        let (mut on, mut ignoring, mut group, mut include, mut bool) =
            (None, None, None, None, false);
        if let Some(modifier) = &b.modifier {
            if modifier.fill_values.lhs.is_some() || modifier.fill_values.rhs.is_some() {
                return Err(CompileError::unsupported("fill modifier"));
            }
            bool = modifier.return_bool;
            match &modifier.matching {
                None => {}
                Some(LabelModifier::Include(labels)) => on = Some(labels.labels.clone()),
                Some(LabelModifier::Exclude(labels)) => ignoring = Some(labels.labels.clone()),
            }
            match &modifier.card {
                VectorMatchCardinality::OneToOne | VectorMatchCardinality::ManyToMany => {}
                VectorMatchCardinality::ManyToOne(labels) => {
                    group = Some(Group::Left);
                    include = Some(labels.labels.clone());
                }
                VectorMatchCardinality::OneToMany(labels) => {
                    group = Some(Group::Right);
                    include = Some(labels.labels.clone());
                }
            }
        }
        if !matches!(
            operator.as_str(),
            "+" | "-" | "*" | "/" | ">" | "<" | ">=" | "<=" | "==" | "!=" | "and" | "or" | "unless"
        ) {
            return Err(CompileError::unsupported(format!("operator {operator}")));
        }
        Ok((
            Node::Binary {
                operator,
                bool,
                on,
                ignoring,
                group,
                include,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            },
            Kind::Value,
        ))
    }

    fn call(&mut self, c: &Call) -> Result<(Node, Kind), CompileError> {
        let name = c.func.name;
        let args = &c.args.args;
        match name {
            name if is_range_function(name) => {
                let matrix = matrix_argument(name, args)?;
                self.range_call(name, matrix, None)
            }
            "histogram_quantile" => {
                let q = literal_parameter(name, args.first().map(|a| a.as_ref()))?;
                let Some(arg) = args.get(1) else {
                    return Err(CompileError::new(
                        name,
                        "histogram_quantile needs two arguments",
                    ));
                };
                let (arg, kind) = self.node(arg)?;
                if kind != Kind::Buckets {
                    return Err(CompileError::new(
                        name,
                        "histogram_quantile needs the rate of a classic histogram's _bucket series",
                    ));
                }
                Ok((
                    Node::HistogramQuantile {
                        q,
                        arg: Box::new(arg),
                    },
                    Kind::Value,
                ))
            }
            "clamp_min" | "clamp_max" | "abs" | "scalar" | "vector" | "time" | "label_replace" => {
                let function = match name {
                    "clamp_min" => CallFunction::ClampMin,
                    "clamp_max" => CallFunction::ClampMax,
                    "abs" => CallFunction::Abs,
                    "scalar" => CallFunction::Scalar,
                    "vector" => CallFunction::Vector,
                    "time" => CallFunction::Time,
                    _ => CallFunction::LabelReplace,
                };
                let args = args
                    .iter()
                    .map(|arg| match arg.as_ref() {
                        Expr::NumberLiteral(n) => Ok(Argument::Number(n.val)),
                        Expr::StringLiteral(s) => Ok(Argument::String(s.val.clone())),
                        arg => self.value(arg).map(|(node, _)| Argument::Node(node)),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok((Node::Call { function, args }, Kind::Value))
            }
            name => Err(CompileError::unsupported(format!("{name}()"))),
        }
    }

    /// Compiles a range function over a selector into one leaf.
    fn range_call(
        &mut self,
        name: &str,
        matrix: &MatrixSelector,
        grouping: Option<Grouping>,
    ) -> Result<(Node, Kind), CompileError> {
        if is_counter_function(name) {
            let per_second = name != "increase";
            let (table, column) = counter_source(matrix);
            let reading = match column {
                None => Reading::Buckets,
                Some(column) => Reading::Growth { column, per_second },
            };
            return self.selector(&matrix.vs, table, reading, grouping);
        }
        let reading = match name {
            "max_over_time" => Reading::Max,
            "min_over_time" => Reading::Min,
            "avg_over_time" => Reading::Avg,
            "changes" => Reading::Changes,
            _ => Reading::Deriv,
        };
        self.selector(&matrix.vs, Table::Gauge, reading, grouping)
    }

    fn selector(
        &mut self,
        selector: &VectorSelector,
        table: Table,
        reading: Reading,
        grouping: Option<Grouping>,
    ) -> Result<(Node, Kind), CompileError> {
        if selector.at.is_some() {
            return Err(CompileError::unsupported("@ modifier"));
        }
        if selector.offset_expr.is_some() || selector.anchored || selector.smoothed {
            return Err(CompileError::unsupported("selector modifier"));
        }
        if !selector.matchers.or_matchers.is_empty() {
            return Err(CompileError::unsupported("or matchers"));
        }
        let offset_seconds = match &selector.offset {
            None => 0,
            Some(Offset::Pos(d)) => d.as_secs() as i64,
            Some(Offset::Neg(d)) => -(d.as_secs() as i64),
        };
        let (name, matchers) = selection(selector)?;
        if table == Table::Histogram && matchers.iter().any(|m| m.name == "le") {
            return Err(CompileError::unsupported("le matcher on a histogram"));
        }
        // A selector read without a counter function may name a gauge or a counter.
        let sources = match table {
            Table::Gauge => vec![
                (Table::Gauge, stored_names(&name, Table::Gauge)),
                (Table::Sum, stored_names(&name, Table::Sum)),
            ],
            Table::Sum | Table::Histogram => vec![(table, stored_names(&name, table))],
        };
        let (shape, kind) = match reading {
            Reading::Buckets => (Shape::Buckets, Kind::Buckets),
            _ => (Shape::Value, Kind::Value),
        };
        let leaf = Leaf {
            sql: sql::leaf(&sources, &matchers, reading, grouping.as_ref()),
            shape,
            offset_seconds,
        };
        let index = match self.leaves.iter().position(|existing| *existing == leaf) {
            Some(index) => index,
            None => {
                self.leaves.push(leaf);
                self.leaves.len() - 1
            }
        };
        Ok((Node::Leaf { index }, kind))
    }
}

fn is_counter_function(name: &str) -> bool {
    matches!(name, "rate" | "irate" | "increase")
}

/// Functions over a range selector that compile into the selector's leaf.
fn is_range_function(name: &str) -> bool {
    is_counter_function(name)
        || matches!(
            name,
            "max_over_time" | "min_over_time" | "avg_over_time" | "changes" | "deriv"
        )
}

fn matrix_argument<'a>(
    name: &str,
    args: &'a [Box<Expr>],
) -> Result<&'a MatrixSelector, CompileError> {
    match args.first().map(|a| a.as_ref()) {
        Some(Expr::MatrixSelector(m)) => Ok(m),
        _ => Err(CompileError::new(
            format!("{name}()"),
            format!("{name} needs a range vector selector argument"),
        )),
    }
}

fn literal_parameter(op: &str, param: Option<&Expr>) -> Result<f64, CompileError> {
    match param {
        Some(Expr::NumberLiteral(n)) => Ok(n.val),
        Some(Expr::Paren(p)) => literal_parameter(op, Some(&p.expr)),
        _ => Err(CompileError::new(
            op,
            format!("the parameter of {op} must be a number literal"),
        )),
    }
}

/// The table and growth column a counter selector reads: a classic
/// histogram's `_bucket`, `_count` or `_sum` series, or a counter.
fn counter_source(matrix: &MatrixSelector) -> (Table, Option<GrowthColumn>) {
    let name = matrix.vs.name.as_deref().unwrap_or_default();
    if name.ends_with("_bucket") {
        (Table::Histogram, None)
    } else if name.ends_with("_count") {
        (Table::Histogram, Some(GrowthColumn::Count))
    } else if name.ends_with("_sum") {
        (Table::Histogram, Some(GrowthColumn::Sum))
    } else {
        (Table::Sum, Some(GrowthColumn::Value))
    }
}

/// The metric name and the label matchers a selector keeps: cluster and
/// variable matchers are removed.
fn selection(selector: &VectorSelector) -> Result<(String, Vec<Matcher>), CompileError> {
    let mut name = selector.name.clone();
    let mut kept = Vec::new();
    for matcher in &selector.matchers.matchers {
        if matcher.name == "__name__" {
            match matcher.op {
                MatchOp::Equal => name = Some(matcher.value.clone()),
                MatchOp::NotEqual | MatchOp::Re(_) | MatchOp::NotRe(_) => {
                    return Err(CompileError::unsupported("matcher on __name__"));
                }
            }
            continue;
        }
        if matcher.name == PROJECT_LABEL || references_variable(&matcher.value) {
            continue;
        }
        kept.push(matcher.clone());
    }
    let name =
        name.ok_or_else(|| CompileError::new("selector", "a selector must name its metric"))?;
    Ok((name, kept))
}

/// Whether a matcher value references a Grafana variable. A `$` or `[[`
/// that does not start a reference, such as a regex anchor or a POSIX class
/// like `[[:alpha:]]`, keeps the matcher.
fn references_variable(value: &str) -> bool {
    let chars = value.chars().collect::<Vec<_>>();
    (0..chars.len()).any(|i| match chars[i] {
        '$' => dollar_variable(&chars, i).is_some(),
        '[' if chars.get(i + 1) == Some(&'[') => bracket_variable(&chars, i).is_some(),
        _ => false,
    })
}

/// The `MetricName`s the Prometheus receiver may have stored a series under.
fn stored_names(name: &str, table: Table) -> Vec<String> {
    match table {
        Table::Histogram => {
            let base = ["_bucket", "_count", "_sum"]
                .iter()
                .find_map(|suffix| name.strip_suffix(suffix))
                .unwrap_or(name);
            vec![base.to_owned()]
        }
        Table::Sum => match name.strip_suffix("_total") {
            Some(base) => vec![name.to_owned(), base.to_owned()],
            None => vec![name.to_owned()],
        },
        Table::Gauge => vec![name.to_owned()],
    }
}

#[cfg(test)]
#[path = "../../tests/unit/compile.rs"]
mod tests;
