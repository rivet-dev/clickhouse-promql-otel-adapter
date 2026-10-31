//! Exemplar reads use exemplar timestamps, which can differ from point timestamps.
use crate::{
    engine::matches,
    model::{Labels, float_string},
    storage::{Store, TABLES, number, prometheus_scope, row_labels, row_metric_name},
    translation::label_name,
};
use anyhow::{Context, Result, bail};
use promql_parser::parser::VectorSelector;
use serde_json::{Value, json};
use std::collections::BTreeMap;
impl Store {
    pub async fn exemplars(
        &self,
        start: i64,
        end: i64,
        selectors: &[VectorSelector],
    ) -> Result<Vec<Value>> {
        let mut groups: BTreeMap<Labels, Vec<Value>> = BTreeMap::new();
        let mut total = 0;
        for kind in TABLES.into_iter().filter(|k| *k != "summary") {
            let filter = self.metric_filter(kind, selectors).await?;
            if filter.as_ref().is_some_and(|v| v.is_empty()) {
                continue;
            }
            let predicate = if filter.is_some() {
                " AND MetricName IN {names:Array(String)}"
            } else {
                ""
            };
            let sql = format!(
                "SELECT *, arrayMap(t -> toUnixTimestamp64Milli(toDateTime64(t,3)), `Exemplars.TimeUnix`) AS _times FROM {}.otel_metrics_{kind} WHERE arrayExists(t -> t >= {{start:Int64}} AND t <= {{end:Int64}}, _times){predicate} LIMIT {} SETTINGS output_format_json_quote_denormals=1 FORMAT JSONEachRow",
                self.database,
                self.max_rows + 1
            );
            let text = self
                .query(
                    &sql,
                    &[
                        ("param_start", start.to_string()),
                        ("param_end", end.to_string()),
                        (
                            "param_names",
                            crate::storage::string_array(&filter.unwrap_or_default()),
                        ),
                    ],
                )
                .await?;
            for line in text.lines() {
                if total % 1024 == 0 {
                    tokio::task::yield_now().await;
                }
                total += 1;
                if total > self.max_rows {
                    bail!("ClickHouse exemplar scan limit exceeded");
                }
                let row: Value = serde_json::from_str(line)?;
                let name = row_metric_name(&row, kind)?;
                let base = row_labels(&row, &self.promoted_resource_attributes)?;
                for (i, t) in row["_times"]
                    .as_array()
                    .context("missing exemplar timestamps")?
                    .iter()
                    .enumerate()
                {
                    let t = t
                        .as_i64()
                        .or_else(|| t.as_str().and_then(|t| t.parse().ok()))
                        .context("invalid exemplar timestamp")?;
                    if t < start || t > end {
                        continue;
                    }
                    let value = number(&row["Exemplars.Value"][i])?;
                    let mut series = base.clone();
                    if kind == "histogram" {
                        let bound = row["ExplicitBounds"]
                            .as_array()
                            .context("missing histogram bounds")?
                            .iter()
                            .map(number)
                            .collect::<Result<Vec<_>>>()?
                            .into_iter()
                            .find(|b| value <= *b)
                            .unwrap_or(f64::INFINITY);
                        series.insert("__name__".into(), format!("{name}_bucket"));
                        series.insert("le".into(), float_string(bound));
                    } else {
                        series.insert("__name__".into(), name.clone());
                    }
                    if !selectors.iter().any(|s| matches(s, &series)) {
                        continue;
                    }
                    let mut labels = Labels::new();
                    if let Some(attrs) = row["Exemplars.FilteredAttributes"][i].as_object() {
                        for (k, v) in attrs {
                            let key = if prometheus_scope(&row) {
                                k.clone()
                            } else {
                                label_name(k)?
                            };
                            let value = v.as_str().context("invalid exemplar attribute")?;
                            if !value.is_empty()
                                && labels.insert(key.clone(), value.into()).is_some()
                            {
                                bail!("exemplar attribute collision at {key}");
                            }
                        }
                    }
                    for (field, key) in [
                        ("Exemplars.TraceId", "trace_id"),
                        ("Exemplars.SpanId", "span_id"),
                    ] {
                        if let Some(value) = row[field][i]
                            .as_str()
                            .filter(|v| !v.is_empty() && !v.bytes().all(|b| b == b'0'))
                        {
                            labels.insert(key.into(), value.into());
                        }
                    }
                    groups.entry(series).or_default().push(json!({"labels":labels,"value":float_string(value),"timestamp":t as f64/1000.0}));
                }
            }
        }
        Ok(groups
            .into_iter()
            .map(|(labels, mut exemplars)| {
                exemplars.sort_by(|a, b| {
                    a["timestamp"]
                        .as_f64()
                        .unwrap()
                        .total_cmp(&b["timestamp"].as_f64().unwrap())
                        .then_with(|| a.to_string().cmp(&b.to_string()))
                });
                exemplars.dedup();
                json!({"seriesLabels":labels,"exemplars":exemplars})
            })
            .collect())
    }
}
