//! ClickHouse SQL for compiled plan leaves.
//!
//! Every statement reads one cluster's rows: `scope` is the only builder of a
//! leaf's `WHERE`, and it always begins with the `{project:String}` predicate.
//! Literal label names and values are quoted with `quote`; the only
//! placeholders are `{project:String}`, `{from:UInt32}`, `{to:UInt32}`,
//! `{query_from:UInt32}` and `{step:UInt32}`.
use promql_parser::label::{MatchOp, Matcher};

use super::AggregateFunction;

/// The most series one leaf may return before the statement fails.
pub const MAX_SERIES_PER_LEAF: u32 = 1_000;

/// The message a leaf fails with when it selects more than
/// `MAX_SERIES_PER_LEAF` series.
pub const SERIES_LIMIT_ERROR: &str = "rivet_series_limit";

/// The step a sample belongs to, labelled by its end on the grid from
/// `{from}`: step `t` holds the samples in `(t - step, t]`, as a Prometheus
/// evaluation at `t` sees them. Sub-second precision decides boundary samples.
const BUCKET: &str = "toUInt32({from:UInt32} + {step:UInt32} * ceil((toUnixTimestamp64Nano(toDateTime64(TimeUnix, 9)) - toInt64({from:UInt32}) * 1000000000) / ({step:UInt32} * 1000000000)))";

/// A series' identity within one table. `MetricName` is left out: the
/// names one leaf reads in a table are one metric stored with and without
/// `_total`, whose samples continue one series.
const SERIES: &str = "cityHash64(ResourceAttributes['service.name'], ResourceAttributes['service.instance.id'], Attributes)";

/// `SERIES` across the branches of a union, which carry their table in
/// `SourceTable`.
const UNION_SERIES: &str = "cityHash64(SourceTable, ResourceAttributes['service.name'], ResourceAttributes['service.instance.id'], Attributes)";

const PREVIOUS: &str =
    "OVER (PARTITION BY series ORDER BY TimeUnix ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)";

const STEP_BEFORE: &str =
    "OVER (PARTITION BY series ORDER BY t ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)";

const BOUNDED_STEP_BEFORE: &str =
    "OVER (PARTITION BY series, bounds ORDER BY t ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)";

/// A series' Prometheus labels, for a row grouped by series: `job` and
/// `instance` from the resource, then the attributes, without empty values.
const STEP_LABELS: &str = "mapFilter((k, v) -> v != '', mapUpdate(map('job', any(ResourceAttributes['service.name']), 'instance', any(ResourceAttributes['service.instance.id'])), any(Attributes)))";

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

/// A table a leaf reads, the stored `MetricName`s it may hold the series
/// under and the column holding its samples.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source {
    pub table: Table,
    pub names: Vec<String>,
    pub column: Column,
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
    /// The reset-aware growth of a counter, per second when `per_second`.
    Growth {
        per_second: bool,
    },
    /// The reset-aware growth of every histogram bucket.
    Buckets,
}

/// A source's sample column. Bucket readings ignore it and read
/// `BucketCounts`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Column {
    Value,
    Count,
    Sum,
}

impl Column {
    fn name(self) -> &'static str {
        match self {
            Column::Value => "Value",
            Column::Count => "Count",
            Column::Sum => "Sum",
        }
    }
}

/// An aggregation folded into a leaf, so the leaf returns one series per group
/// instead of every selected series.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grouping {
    pub function: AggregateFunction,
    pub by: Vec<String>,
}

/// Builds one leaf's statement. `sources` are the tables that may hold the
/// series; several sources are read as one, each projecting its column as
/// `Value`, which bucket readings do not support. `metric` is the selector's
/// metric name, which a `Last` reading keeps as `__name__` for grouping.
pub fn leaf(
    sources: &[Source],
    matchers: &[Matcher],
    metric: &str,
    reading: Reading,
    grouping: Option<&Grouping>,
) -> String {
    let (source, series, column) = match sources {
        [source] => (
            format!(
                "{}\n\tWHERE {}",
                source.table.name(),
                scope(&source.names, matchers)
            ),
            SERIES,
            source.column,
        ),
        sources => {
            let branches = sources
                .iter()
                .map(|source| {
                    let value = match source.column {
                        Column::Value => "Value".to_owned(),
                        column => format!("toFloat64({}) AS Value", column.name()),
                    };
                    format!(
                        "SELECT {} AS SourceTable, ResourceAttributes, Attributes, TimeUnix, {value}\n\t\tFROM {}\n\t\tWHERE {}",
                        quote(source.table.name()),
                        source.table.name(),
                        scope(&source.names, matchers).replace("\n\t\t", "\n\t\t\t")
                    )
                })
                .collect::<Vec<_>>()
                .join("\n\t\tUNION ALL\n\t\t");
            (
                format!("(\n\t\t{branches}\n\t)"),
                UNION_SERIES,
                Column::Value,
            )
        }
    };
    let labels = match grouping {
        None => "all_labels".to_owned(),
        Some(grouping) => {
            let metric = match reading {
                Reading::Last => Some(metric),
                _ => None,
            };
            group_labels(&grouping.by, metric)
        }
    };
    let rows = indent(&series_rows(&source, series, column, reading, &labels), 1);
    let columns = match reading {
        Reading::Buckets => "counts, bounds",
        _ => "value",
    };

    let Some(grouping) = grouping else {
        return format!(
            "SELECT t, labels, {columns}, throwIf(uniqExact(series) OVER () > {MAX_SERIES_PER_LEAF}, '{SERIES_LIMIT_ERROR}') AS series_limit
FROM (
	{rows}
)
ORDER BY series, t"
        );
    };
    let limit = format!(
        "throwIf(uniqExact(labels) OVER () > {MAX_SERIES_PER_LEAF}, '{SERIES_LIMIT_ERROR}') AS series_limit"
    );
    match reading {
        // Series whose bounds differ cannot be summed; the bounds most series
        // in the group share win. Counts and bounds come from one `argMax` so
        // a tie cannot pair one layout's counts with another's bounds.
        Reading::Buckets => format!(
            "SELECT t, labels, tupleElement(picked, 1) AS counts, tupleElement(picked, 2) AS bounds, {limit}
FROM (
	SELECT t, labels, argMax((counts, bounds), (members, bounds)) AS picked
	FROM (
		SELECT t, labels, bounds, sumForEach(counts) AS counts, count() AS members
		FROM (
			{rows}
		)
		GROUP BY t, labels, bounds
	)
	GROUP BY t, labels
)
ORDER BY labels, t",
            rows = indent(&rows, 2),
        ),
        _ => {
            let value = match grouping.function {
                AggregateFunction::Sum => "sum(value)",
                AggregateFunction::Min => "min(value)",
                AggregateFunction::Max => "max(value)",
                AggregateFunction::Avg => "avg(value)",
                AggregateFunction::Count => "toFloat64(count())",
            };
            format!(
                "SELECT t, labels, {value} AS value, {limit}
FROM (
	{rows}
)
GROUP BY t, labels
ORDER BY labels, t"
            )
        }
    }
}

/// One row per series and step from `{from}` through `{to}`: `(t, series, labels,
/// value)`, or `(t, series, labels, counts, bounds)` for buckets. `labels` is
/// an expression over `all_labels`, the series' full label map, built once
/// per row rather than per sample.
fn series_rows(
    source: &str,
    series: &str,
    column: Column,
    reading: Reading,
    labels: &str,
) -> String {
    let (inner, outer) = match reading {
        Reading::Growth { per_second } => {
            return growth_rows(source, series, column, per_second, labels);
        }
        Reading::Buckets => return bucket_rows(source, series, labels),
        Reading::Last => ("Value".to_owned(), "argMax(Value, TimeUnix) AS value"),
        Reading::Max => ("Value".to_owned(), "max(Value) AS value"),
        Reading::Min => ("Value".to_owned(), "min(Value) AS value"),
        Reading::Avg => ("Value".to_owned(), "avg(Value) AS value"),
        Reading::Changes => (
            format!("Value, lagInFrame(Value, 1, Value) {PREVIOUS} AS before"),
            "toFloat64(countIf(Value != before)) AS value",
        ),
        Reading::Deriv => (
            "Value".to_owned(),
            "tupleElement(simpleLinearRegression(toFloat64(toUnixTimestamp64Nano(toDateTime64(TimeUnix, 9))) / 1e9, Value), 1) AS value",
        ),
    };
    format!(
        "SELECT t, series, {STEP_LABELS} AS all_labels, {labels} AS labels, {outer}
FROM (
	SELECT {BUCKET} AS t, {series} AS series, ResourceAttributes, Attributes, TimeUnix, {inner}
	FROM {source}
)
WHERE t >= {{from:UInt32}} AND t <= {{to:UInt32}}
GROUP BY t, series"
    )
}

/// Reset-aware counter growth per step. Each series is first reduced to one
/// row per step (first, last and peak sample), so the window that joins a
/// step to the one before it runs over steps, not raw samples. Within a step,
/// `last < peak` means the counter reset, and the growth is the rise to the
/// peak plus everything after the reset; a second reset in one step, or a
/// counter that passes its old peak again in the step it reset, is
/// undercounted. Between steps the growth follows Prometheus: a decrease is a
/// reset that counts the new value, and a gap past the lookback adds nothing.
fn growth_rows(
    source: &str,
    series: &str,
    column: Column,
    per_second: bool,
    labels: &str,
) -> String {
    let column = format!("toFloat64({})", column.name());
    let per_second = match per_second {
        true => " / {step:UInt32}",
        false => "",
    };
    format!(
        "SELECT t, series, labels, (if(dateDiff('second', before_at, first_at) > {LOOKBACK_SECONDS}, 0, if(first >= before, first - before, first)) + if(last >= peak, last - first, peak - first + last)){per_second} AS value
FROM (
	SELECT t, series, labels, first, last, peak, first_at, lagInFrame(last, 1, first) {STEP_BEFORE} AS before, lagInFrame(last_at, 1, first_at) {STEP_BEFORE} AS before_at
	FROM (
		SELECT {BUCKET} AS t, {series} AS series, {STEP_LABELS} AS all_labels, {labels} AS labels, argMin({column}, TimeUnix) AS first, argMax({column}, TimeUnix) AS last, max({column}) AS peak, min(TimeUnix) AS first_at, max(TimeUnix) AS last_at
		FROM {source}
		GROUP BY t, series
	)
)
WHERE t >= {{from:UInt32}} AND t <= {{to:UInt32}}",
        source = indent(source, 2),
    )
}

/// `growth_rows` for every histogram bucket. A series' steps are split by
/// bounds, so a series whose buckets change starts over.
fn bucket_rows(source: &str, series: &str, labels: &str) -> String {
    format!(
        "SELECT t, series, labels, arrayMap((between, within) -> between + within, if(dateDiff('second', before_at, first_at) > {LOOKBACK_SECONDS}, arrayMap(now -> toUInt64(0), first), if(arrayAll((now, was) -> now >= was, first, before), arrayMap((now, was) -> toUInt64(now - was), first, before), first)), arrayMap((start, top, end) -> toUInt64(if(end >= top, end - start, top - start + end)), first, peak, last)) AS counts, bounds
FROM (
	SELECT t, series, labels, bounds, first, last, peak, first_at, lagInFrame(last, 1, first) {BOUNDED_STEP_BEFORE} AS before, lagInFrame(last_at, 1, first_at) {BOUNDED_STEP_BEFORE} AS before_at
	FROM (
		SELECT {BUCKET} AS t, {series} AS series, ExplicitBounds AS bounds, {STEP_LABELS} AS all_labels, {labels} AS labels, argMin(BucketCounts, TimeUnix) AS first, argMax(BucketCounts, TimeUnix) AS last, maxForEach(BucketCounts) AS peak, min(TimeUnix) AS first_at, max(TimeUnix) AS last_at
		FROM {source}
		GROUP BY t, series, bounds
	)
)
WHERE t >= {{from:UInt32}} AND t <= {{to:UInt32}}",
        source = indent(source, 2),
    )
}

fn indent(sql: &str, tabs: usize) -> String {
    sql.replace('\n', &format!("\n{}", "\t".repeat(tabs)))
}

/// A series' labels reduced to the grouping labels, matching how the plan
/// evaluator picks them: absent and empty values are left out. `__name__` is
/// `metric` when given; series past a function have no name.
fn group_labels(by: &[String], metric: Option<&str>) -> String {
    let pairs = by
        .iter()
        .map(|name| match (name.as_str(), metric) {
            ("__name__", Some(metric)) => format!("{}, {}", quote(name), quote(metric)),
            _ => format!("{}, all_labels[{}]", quote(name), quote(name)),
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("mapFilter((k, v) -> v != '', CAST(map({pairs}), 'Map(String, String)'))")
}

/// The only builder of a leaf's `WHERE`. The cluster predicate comes first
/// and cannot be left out; it is also the sort key's first column, so it
/// bounds the scan to the cluster's range.
fn scope(names: &[String], matchers: &[Matcher]) -> String {
    let mut terms = vec![
        "ProjectId = {project:String}".to_owned(),
        "TimeUnix >= toDateTime({query_from:UInt32})".to_owned(),
        "TimeUnix <= toDateTime({to:UInt32})".to_owned(),
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
