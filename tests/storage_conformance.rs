mod common;
#[path = "common/corpus.rs"]
mod corpus;
#[path = "common/reference.rs"]
mod reference;
use clickhouse_promql::{
    engine::Engine,
    model::Series,
    storage::{Store, plan},
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

fn limitations(data: &[Series], precise: bool) -> BTreeSet<&'static str> {
    let mut reasons = BTreeSet::new();
    for s in data {
        for p in &s.points {
            if !precise && p.timestamp % 1000 != 0 {
                reasons.insert("DateTime loses subsecond sample timestamps");
            }
            if !precise && p.start_timestamp % 1000 != 0 {
                reasons.insert("DateTime loses subsecond start timestamps");
            }
            if !precise && (p.timestamp < 0 || p.timestamp > 4_294_967_295_000) {
                reasons.insert("DateTime timestamp range");
            }
            if let Some(h) = &p.histogram {
                if h.schema == -53 {
                    reasons.insert("OTLP exponential histograms cannot represent Prometheus custom native buckets");
                }
                if !precise && h.zero_threshold != 0.0 {
                    reasons.insert("Collector schema omits ZeroThreshold");
                }
                if h.reset_hint != 0 {
                    reasons.insert("OTLP has no explicit per-sample counter reset hint");
                }
                if [h.count, h.zero_count]
                    .into_iter()
                    .chain(h.positive.values().copied())
                    .chain(h.negative.values().copied())
                    .any(|x| x < 0.0 || x.fract() != 0.0)
                {
                    reasons.insert("OTLP histogram counts must be nonnegative integers");
                }
            }
        }
    }
    reasons
}
async fn insert(store: &Store, data: &[Series], precise: bool) {
    let mut tables: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for s in data {
        for p in &s.points {
            let mut attrs = s.labels.clone();
            let name = attrs.remove("__name__").unwrap_or_default();
            let mut row = json!({"MetricName":name,"ScopeName":"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver","Attributes":attrs,"TimeUnix":p.timestamp/1000,"StartTimeUnix":p.start_timestamp/1000,"Flags":if p.stale(){1}else{0}});
            if precise {
                row["TimeUnix"] = json!(p.timestamp as f64 / 1000.0);
                row["StartTimeUnix"] = json!(p.start_timestamp as f64 / 1000.0);
            }
            let kind = if let Some(h) = &p.histogram {
                if precise {
                    row["ZeroThreshold"] = json!(h.zero_threshold);
                }
                row["Scale"] = json!(h.schema);
                row["Count"] = json!(h.count as u64);
                row["Sum"] = json!(h.sum.to_string());
                row["ZeroCount"] = json!(h.zero_count as u64);
                row["AggregationTemporality"] = json!(2);
                for (name, buckets) in [("Positive", &h.positive), ("Negative", &h.negative)] {
                    let first = buckets.keys().next().copied().unwrap_or(1);
                    let last = buckets.keys().next_back().copied().unwrap_or(first - 1);
                    row[format!("{name}Offset")] = json!(first - 1);
                    row[format!("{name}BucketCounts")] = json!(
                        (first..=last)
                            .map(|i| buckets.get(&i).copied().unwrap_or(0.0) as u64)
                            .collect::<Vec<_>>()
                    );
                }
                "exponential_histogram"
            } else {
                row["Value"] = json!(p.value.to_string());
                "gauge"
            };
            tables.entry(kind).or_default().push(row.to_string());
        }
    }
    for (kind, rows) in tables {
        for batch in rows.chunks(10000) {
            store
                .query(
                    &format!(
                        "INSERT INTO {}.otel_metrics_{kind} FORMAT JSONEachRow\n{}",
                        store.database,
                        batch.join("\n")
                    ),
                    &[],
                )
                .await
                .unwrap();
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upstream_queries_through_default_collector_schema() {
    run(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upstream_queries_through_precision_preserving_schema() {
    run(true).await;
}
async fn run(precise: bool) {
    let (cases, datasets) = corpus::load(|data| reference::load(data).unwrap());
    let total = cases.len();
    let mut grouped: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for case in cases {
        grouped
            .entry(case["dataset"].as_str().unwrap().into())
            .or_default()
            .push(case);
    }
    let report = Arc::new(Mutex::new(
        json!({"total":total,"passed":0,"excluded":0,"failures":[],"datasets":[]}),
    ));
    let semaphore = Arc::new(tokio::sync::Semaphore::new(4));
    let mut tasks = tokio::task::JoinSet::new();
    for (id, data) in datasets {
        let cases = grouped.remove(&id).unwrap();
        let reasons = limitations(&data, precise);
        if !reasons.is_empty() {
            let mut r = report.lock().unwrap();
            r["excluded"] = json!(r["excluded"].as_u64().unwrap() + cases.len() as u64);
            r["datasets"]
                .as_array_mut()
                .unwrap()
                .push(json!({"id":id,"executions":cases.len(),"limitations":reasons}));
            continue;
        }
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let report = report.clone();
        tasks.spawn(async move {let _permit=permit;common::empty_database_test(precise,move |store|async move{
            if precise {
                store.query(&format!("ALTER TABLE {}.otel_metrics_exponential_histogram ADD COLUMN ZeroThreshold Float64",store.database),&[]).await.unwrap();
            }
            insert(&store,&data,precise).await;
            let mut cache=BTreeMap::new();let mut failures=Vec::new();let mut passed=0;
            for case in &cases {
                let start=case["start"].as_i64().unwrap();let end=case["end"].as_i64().unwrap();let step=case["step"].as_i64().unwrap();let lookback=case["lookback"].as_i64().unwrap();
                let expr=Engine::parse_at(case["query"].as_str().unwrap(),start,end,step).unwrap();let (a,b,selectors)=plan(&expr,start,end,lookback).unwrap();
                let key=format!("{a}/{b}/{}",selectors.iter().map(ToString::to_string).collect::<Vec<_>>().join(";"));
                if !cache.contains_key(&key) {let value=if selectors.is_empty(){Ok(Vec::new())}else{store.load(a,b,&selectors).await.map(|v|v.series.to_vec()).map_err(|e|e.to_string())};cache.insert(key.clone(),value);}
                match reference::compare(case,cache[&key].as_ref()) {Ok(())=>passed+=1,Err(e)=>failures.push(json!({"dataset":id,"query":case["query"],"fixture":case["fixture"],"reason":e}))}
            }
            let mut r=report.lock().unwrap();r["passed"]=json!(r["passed"].as_u64().unwrap()+passed);r["failures"].as_array_mut().unwrap().extend(failures);r["datasets"].as_array_mut().unwrap().push(json!({"id":id,"executions":cases.len(),"passed":passed}));
        }).await;});
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
    let mut report = report.lock().unwrap();
    report["datasets"]
        .as_array_mut()
        .unwrap()
        .sort_by_key(|d| d["id"].as_str().unwrap().to_owned());
    std::fs::create_dir_all(".state").unwrap();
    std::fs::write(
        if precise {
            ".state/storage-conformance-precise.json"
        } else {
            ".state/storage-conformance.json"
        },
        serde_json::to_vec_pretty(&*report).unwrap(),
    )
    .unwrap();
    assert!(
        report["failures"].as_array().unwrap().is_empty(),
        "{} storage query failures; see .state/storage-conformance.json",
        report["failures"].as_array().unwrap().len()
    );
    assert_eq!(
        report["passed"].as_u64().unwrap() + report["excluded"].as_u64().unwrap(),
        total as u64
    );
}
