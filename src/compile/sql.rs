//! ClickHouse SQL for compiled plan leaves.
//!
//! Every statement reads one cluster's rows: `scope` is the only builder of a
//! leaf's `WHERE`, and it always begins with the `{project:String}` predicate.
//! Literal label names and values are quoted with `quote`; the only
//! placeholders are `{project:String}`, `{from:UInt32}`, `{to:UInt32}`,
//! `{query_from:UInt32}` and `{step:UInt32}`.
use promql_parser::label::{MatchOp, Matcher};

/// The most series one leaf may return before the statement fails.
pub const MAX_SERIES_PER_LEAF: u32 = 1_000;

/// The message a leaf fails with when it selects more than
/// `MAX_SERIES_PER_LEAF` series.
pub const SERIES_LIMIT_ERROR: &str = "rivet_series_limit";

const BUCKET: &str =
    "toUnixTimestamp(toStartOfInterval(TimeUnix, toIntervalSecond({step:UInt32})))";

const SERIES: &str = "cityHash64(ResourceAttributes['service.instance.id'], Attributes)";

const LABELS: &str = "mapFilter((k, v) -> v != '', mapUpdate(map('job', ResourceAttributes['service.name'], 'instance', ResourceAttributes['service.instance.id']), Attributes))";

const PREVIOUS: &str =
    "OVER (PARTITION BY series ORDER BY TimeUnix ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)";

/// Prometheus's lookback. A counter sample further than this after the one
/// before it starts a new series and adds no growth, so a step's value never
/// depends on how far back the query reads.
const LOOKBACK_SECONDS: u32 = 300;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Table {
    Gauge,
    Sum,
    Histogram,
}

impl Table {
    fn name(self) -> &'static str {
        match self {
            Table::Gauge => "otel_metrics_gauge",
            Table::Sum => "otel_metrics_sum",
            Table::Histogram => "otel_metrics_histogram",
        }
    }
}

/// What a leaf computes for each series in each step bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reading {
    /// The last sample.
    Last,
    Max,
    Min,
    Avg,
    /// How often the value changed from the sample before it.
    Changes,
    /// The least-squares slope per second.
    Deriv,
    /// The reset-aware growth of a counter column, per second when `per_second`.
    Growth {
        column: GrowthColumn,
        per_second: bool,
    },
    /// The reset-aware growth of every histogram bucket.
    Buckets,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrowthColumn {
    Value,
    Count,
    Sum,
}

impl GrowthColumn {
    fn name(self) -> &'static str {
        match self {
            GrowthColumn::Value => "Value",
            GrowthColumn::Count => "Count",
            GrowthColumn::Sum => "Sum",
        }
    }
}

/// Builds one leaf's statement. `sources` are the tables that may hold the
/// series, each with the stored `MetricName`s it may hold them under; several
/// sources are read as one, which only `Value` readings support.
pub fn leaf(sources: &[(Table, Vec<String>)], matchers: &[Matcher], reading: Reading) -> String {
    let (inner, outer) = match reading {
        Reading::Last => ("Value".to_owned(), "argMax(Value, TimeUnix) AS value".to_owned()),
        Reading::Max => ("Value".to_owned(), "max(Value) AS value".to_owned()),
        Reading::Min => ("Value".to_owned(), "min(Value) AS value".to_owned()),
        Reading::Avg => ("Value".to_owned(), "avg(Value) AS value".to_owned()),
        Reading::Changes => (
            format!("Value, lagInFrame(Value, 1, Value) {PREVIOUS} AS before"),
            "toFloat64(countIf(Value != before)) AS value".to_owned(),
        ),
        Reading::Deriv => (
            "Value".to_owned(),
            "tupleElement(simpleLinearRegression(toFloat64(toUnixTimestamp64Nano(toDateTime64(TimeUnix, 9))) / 1e9, Value), 1) AS value".to_owned(),
        ),
        Reading::Growth { column, per_second } => {
            let column = column.name();
            (
                format!(
                    "lagInFrame({column}, 1, {column}) {PREVIOUS} AS before, lagInFrame(TimeUnix, 1, TimeUnix) {PREVIOUS} AS before_at, if(dateDiff('second', before_at, TimeUnix) > {LOOKBACK_SECONDS}, 0, if({column} >= before, toFloat64({column} - before), toFloat64({column}))) AS growth"
                ),
                if per_second {
                    "sum(growth) / {step:UInt32} AS value".to_owned()
                } else {
                    "sum(growth) AS value".to_owned()
                },
            )
        }
        Reading::Buckets => (
            format!(
                "ExplicitBounds, lagInFrame(BucketCounts, 1, BucketCounts) {PREVIOUS} AS before, lagInFrame(TimeUnix, 1, TimeUnix) {PREVIOUS} AS before_at, if(dateDiff('second', before_at, TimeUnix) > {LOOKBACK_SECONDS}, arrayMap(now -> toUInt64(0), BucketCounts), if(length(before) = length(BucketCounts) AND arrayAll((now, was) -> now >= was, BucketCounts, before), arrayMap((now, was) -> toUInt64(now - was), BucketCounts, before), BucketCounts)) AS growth"
            ),
            "sumForEach(growth) AS counts, any(ExplicitBounds) AS bounds".to_owned(),
        ),
    };
    let source = match sources {
        [(table, names)] => format!("{}\n\tWHERE {}", table.name(), scope(names, matchers)),
        sources => {
            let branches = sources
                .iter()
                .map(|(table, names)| {
                    format!(
                        "SELECT ResourceAttributes, Attributes, TimeUnix, Value\n\t\tFROM {}\n\t\tWHERE {}",
                        table.name(),
                        scope(names, matchers).replace("\n\t\t", "\n\t\t\t")
                    )
                })
                .collect::<Vec<_>>()
                .join("\n\t\tUNION ALL\n\t\t");
            format!("(\n\t\t{branches}\n\t)")
        }
    };
    format!(
        "SELECT t, any(labels) AS labels, {outer}, throwIf(uniqExact(series) OVER () > {MAX_SERIES_PER_LEAF}, '{SERIES_LIMIT_ERROR}') AS series_limit
FROM (
	SELECT {BUCKET} AS t, {SERIES} AS series, {LABELS} AS labels, TimeUnix, {inner}
	FROM {source}
)
WHERE t >= {{from:UInt32}}
GROUP BY t, series
ORDER BY series, t"
    )
}

/// The only builder of a leaf's `WHERE`. The cluster predicate comes first
/// and cannot be left out; it is also the sort key's first column, so it
/// bounds the scan to the cluster's range.
fn scope(names: &[String], matchers: &[Matcher]) -> String {
    let mut terms = vec![
        "ProjectId = {project:String}".to_owned(),
        "TimeUnix >= toDateTime({query_from:UInt32})".to_owned(),
        "TimeUnix < toDateTime({to:UInt32})".to_owned(),
        // OTel `NoRecordedValue`: a staleness marker stored as a zero sample.
        "bitAnd(Flags, 1) = 0".to_owned(),
        name_term(names),
    ];
    terms.extend(matchers.iter().map(matcher_term));
    terms.join("\n\t\tAND ")
}

fn name_term(names: &[String]) -> String {
    match names {
        [name] => format!("MetricName = {}", quote(name)),
        names => format!(
            "MetricName IN ({})",
            names
                .iter()
                .map(|n| quote(n))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

fn label(name: &str) -> String {
    match name {
        "job" => "ResourceAttributes['service.name']".to_owned(),
        "instance" => "ResourceAttributes['service.instance.id']".to_owned(),
        name => format!("Attributes[{}]", quote(name)),
    }
}

fn matcher_term(matcher: &Matcher) -> String {
    let label = label(&matcher.name);
    match &matcher.op {
        MatchOp::Equal => format!("{label} = {}", quote(&matcher.value)),
        MatchOp::NotEqual => format!("{label} != {}", quote(&matcher.value)),
        // Prometheus regexes are fully anchored RE2, as are ClickHouse's.
        MatchOp::Re(_) => format!("match({label}, {})", quote(&anchored(&matcher.value))),
        MatchOp::NotRe(_) => format!("NOT match({label}, {})", quote(&anchored(&matcher.value))),
    }
}

fn anchored(regex: &str) -> String {
    format!("^(?:{regex})$")
}

/// A ClickHouse string literal.
pub fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}
