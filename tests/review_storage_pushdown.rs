mod common;
use clickhouse_promql::storage::Store;
use promql_parser::parser::{self, Expr, VectorSelector};
use serde_json::{Value, json};
const T: i64 = 1_700_000_000_000;
const PROM: &str =
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver";
fn selector(s: &str) -> VectorSelector {
    match parser::parse(s).unwrap() {
        Expr::VectorSelector(s) => s,
        _ => panic!("selector"),
    }
}
async fn insert(store: &Store, kind: &str, rows: Vec<Value>) {
    let body = rows
        .iter()
        .map(Value::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    store
        .query(
            &format!(
                "INSERT INTO {}.otel_metrics_{kind} FORMAT JSONEachRow\n{body}",
                store.database
            ),
            &[],
        )
        .await
        .unwrap();
}
fn gauge(attrs: Value) -> Value {
    json!({"MetricName":"requests", "ScopeName":PROM, "Attributes":attrs, "TimeUnix":T/1000, "Value":1})
}
#[tokio::test]
async fn selective_labels_are_filtered_before_row_limits_for_all_read_paths() {
    common::empty_database_test(false, |mut store| async move {
        let rows = (0..100)
            .map(|i| {
                let mut r = gauge(json!({"tenant":i.to_string()}));
                r["Exemplars.TimeUnix"] = json!([T / 1000]);
                r["Exemplars.Value"] = json!([1]);
                r["Exemplars.FilteredAttributes"] = json!([{}]);
                r["Exemplars.TraceId"] = json!([""]);
                r["Exemplars.SpanId"] = json!([""]);
                r
            })
            .collect();
        insert(&store, "gauge", rows).await;
        store.max_rows = 2;
        let selectors = [selector("requests{tenant=\"42\"}")];
        assert_eq!(store.load(T, T, &selectors).await.unwrap().series.len(), 1);
        assert_eq!(
            store.discover(T, T, &selectors).await.unwrap().series.len(),
            1
        );
        assert_eq!(store.exemplars(T, T, &selectors).await.unwrap().len(), 1);
    })
    .await;
}
#[tokio::test]
async fn missing_empty_normalized_labels_and_resource_fallback_match_rust() {
    common::empty_database_test(true, |store| async move {
        let mut otel=gauge(json!({"http.é":"ok", "9id":"digit", "job":"", "instance":""}));
        otel["ScopeName"]=json!("sdk");
        otel["ResourceAttributes"]=json!({"service.name":"svc","service.namespace":"ns","service.instance.id":"node", "tenant":"resource"});
        let mut explicit=gauge(json!({"job":"explicit", "instance":"explicit", "missing":""}));
        explicit["ResourceAttributes"]=json!({"service.name":"svc","service.instance.id":"node"});
        insert(&store,"gauge",vec![otel,explicit,gauge(json!({"missing":"present"}))]).await;
        for (q,n) in [("requests{http__=\"ok\",key_9id=\"digit\",job=\"ns/svc\",instance=\"node\"}",1),
          ("requests{missing=\"\"}",2),("requests{missing!=\"\"}",1),("requests{missing!=\"present\"}",2),
          ("requests{job=\"explicit\",instance=\"explicit\"}",1),("target_info{tenant=\"resource\"}",1)] {
            assert_eq!(store.load(T,T,&[selector(q)]).await.unwrap().series.len(),n,"{q}");
        }
    }).await;
}
#[tokio::test]
async fn or_branches_and_unfilterable_alternatives_preserve_matches() {
    common::empty_database_test(false, |store| async move {
        insert(
            &store,
            "gauge",
            vec![
                gauge(json!({"tenant":"a"})),
                gauge(json!({"tenant":"b"})),
                gauge(json!({"tenant":"c"})),
            ],
        )
        .await;
        let mut s = selector("requests{tenant=\"a\"}");
        s.matchers.or_matchers = vec![
            s.matchers.matchers.clone(),
            selector("requests{tenant=\"b\"}").matchers.matchers,
        ];
        assert_eq!(store.load(T, T, &[s]).await.unwrap().series.len(), 2);
        assert_eq!(
            store
                .load(
                    T,
                    T,
                    &[
                        selector("requests{tenant=\"a\"}"),
                        selector("requests{tenant=~\"b|c\"}")
                    ]
                )
                .await
                .unwrap()
                .series
                .len(),
            3
        );
    })
    .await;
}
#[tokio::test]
async fn sample_projection_does_not_download_exemplar_payloads() {
    common::empty_database_test(false, |mut store| async move {
        let mut r = gauge(json!({"tenant":"a'\\\"; DROP TABLE nope"}));
        r["Exemplars.TimeUnix"] = json!([T / 1000]);
        r["Exemplars.Value"] = json!([1]);
        r["Exemplars.FilteredAttributes"] = json!([{"large":"x".repeat(128_000)}]);
        r["Exemplars.TraceId"] = json!([""]);
        r["Exemplars.SpanId"] = json!([""]);
        insert(&store, "gauge", vec![r]).await;
        store.max_bytes = 4096;
        let mut quoted = selector("requests{tenant=\"placeholder\"}");
        quoted.matchers.matchers[0].value = "a'\\\"; DROP TABLE nope".into();
        assert_eq!(store.load(T, T, &[quoted]).await.unwrap().series.len(), 1);
        assert_eq!(
            store
                .load(T, T, &[selector("requests")])
                .await
                .unwrap()
                .series
                .len(),
            1
        );
        assert!(
            store
                .exemplars(T, T, &[selector("requests")])
                .await
                .is_err()
        );
    })
    .await;
}

#[tokio::test]
async fn normalized_collisions_remain_errors_even_if_first_value_does_not_match() {
    common::empty_database_test(false, |store| async move {
        let mut r = gauge(json!({"a.b":"first", "a_b":"second"}));
        r["ScopeName"] = json!("sdk");
        insert(&store, "gauge", vec![r]).await;
        let error = store
            .load(T, T, &[selector("requests{a_b=\"second\"}")])
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("collision"), "{error}");
    })
    .await;
}

#[tokio::test]
async fn generated_samples_are_bounded_without_rejecting_selective_bucket_reads() {
    common::empty_database_test(false, |mut store| async move {
        let row=json!({"MetricName":"latency", "ScopeName":PROM,"TimeUnix":T/1000,"Count":4,"Sum":10,"BucketCounts":[1,1,1,1],"ExplicitBounds":[1,2,3]});
        insert(&store,"histogram",vec![row]).await;
        store.max_samples=1;
        for discover in [false,true] {
            let s=[selector("latency_bucket{le=\"2\"}")];
            let result=if discover {store.discover(T,T,&s).await} else {store.load(T,T,&s).await};
            assert_eq!(result.unwrap().series.len(),1);
            let s=[selector("latency_bucket")];
            let result=if discover {store.discover(T,T,&s).await} else {store.load(T,T,&s).await};
            assert!(result.err().unwrap().to_string().contains("expanded sample limit"));
        }
    }).await;
}

#[tokio::test]
async fn response_byte_budget_is_shared_across_tables() {
    common::empty_database_test(false, |mut store| async move {
        let mut r = gauge(json!({}));
        r["MetricDescription"] = json!("x".repeat(600));
        insert(&store, "gauge", vec![r.clone()]).await;
        r["MetricName"] = json!("other");
        insert(&store, "sum", vec![r]).await;
        store.max_bytes = 1500;
        for q in ["requests", "other"] {
            assert!(store.load(T, T, &[selector(q)]).await.is_ok());
        }
        let error = store
            .load(T, T, &[selector("{__name__=~\"requests|other\"}")])
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("byte limit"), "{error}");
    })
    .await;
}

#[tokio::test]
async fn generated_label_or_branches_and_native_buckets_obey_budgets() {
    common::empty_database_test(true, |mut store| async move {
        insert(&store,"histogram",vec![json!({"MetricName":"latency","ScopeName":PROM,"TimeUnix":T/1000,"Count":3,"Sum":3,"BucketCounts":[1,1,1],"ExplicitBounds":[1,2]})]).await;
        let mut s=selector("latency_bucket{le=\"1\"}");
        s.matchers.or_matchers=vec![s.matchers.matchers.clone(),selector("latency_bucket{le=\"2\"}").matchers.matchers];
        assert_eq!(store.load(T,T,&[s]).await.unwrap().series.len(),2);
        insert(&store,"exponential_histogram",vec![json!({"MetricName":"native","ScopeName":PROM,"TimeUnix":T/1000,"Count":2,"Sum":2,"PositiveBucketCounts":[1,1]})]).await;
        store.max_samples=2;
        assert!(store.load(T,T,&[selector("native")]).await.err().unwrap().to_string().contains("expanded sample limit"));
        store.max_samples=3;
        assert_eq!(store.load(T,T,&[selector("native")]).await.unwrap().series.len(),1);
    }).await;
}

#[tokio::test]
async fn literal_regex_alternatives_push_down_with_missing_and_negative_semantics() {
    common::empty_database_test(false, |mut store| async move {
        let mut rows = (0..100)
            .map(|i| gauge(json!({"tenant":i.to_string()})))
            .collect::<Vec<_>>();
        rows.extend([
            gauge(json!({"other":"empty","tenant":""})),
            gauge(json!({"other":"missing"})),
        ]);
        insert(&store, "gauge", rows).await;
        store.max_rows = 3;
        for (q, n) in [
            ("requests{tenant=~\"42\"}", 1),
            ("requests{tenant=~\"42|43\"}", 2),
            ("requests{tenant=~\"\"}", 2),
            ("requests{tenant=~\"|42\"}", 3),
            ("requests{tenant=~\"42|43\",tenant!~\"42\"}", 1),
            ("requests{tenant=~\"|42\",tenant!~\"42\"}", 2),
        ] {
            assert_eq!(
                store.load(T, T, &[selector(q)]).await.unwrap().series.len(),
                n,
                "{q}"
            );
        }
        // Metacharacters must not be treated as literal sets.
        store.max_rows = 200;
        assert_eq!(
            store
                .load(T, T, &[selector("requests{tenant=~\"4.\"}")])
                .await
                .unwrap()
                .series
                .len(),
            10
        );
    })
    .await;
}

#[tokio::test]
async fn label_filters_are_scoped_to_their_own_selectors_metric_names() {
    common::empty_database_test(false, |mut store| async move {
        let mut rows = Vec::new();
        for name in ["a", "b"] {
            for tenant in ["1", "2", "3"] {
                let mut row = gauge(json!({"tenant": tenant}));
                row["MetricName"] = json!(name);
                rows.push(row);
            }
        }
        insert(&store, "gauge", rows).await;
        // a{tenant="1"} reads one row and b reads three; a selector without
        // pushable matchers must not admit every row of a.
        store.max_rows = 4;
        let loaded = store
            .load(T, T, &[selector("a{tenant=\"1\"}"), selector("b")])
            .await
            .unwrap();
        assert_eq!(loaded.series.len(), 4);
        store.max_rows = 3;
        let loaded = store
            .load(
                T,
                T,
                &[selector("a{tenant=\"1\"}"), selector("b{tenant=~\"2|3\"}")],
            )
            .await
            .unwrap();
        assert_eq!(loaded.series.len(), 3);
        // The two-name dictionary shares the row budget.
        store.max_rows = 2;
        let loaded = store
            .load(T, T, &[selector("a{tenant=\"1\"}"), selector("missing")])
            .await
            .unwrap();
        assert_eq!(loaded.series.len(), 1);
        // A selector nothing narrows still admits the whole table.
        let error = store
            .load(
                T,
                T,
                &[selector("a{tenant=\"1\"}"), selector("{__name__=~\"b.*\"}")],
            )
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("sample scan limit"), "{error}");
    })
    .await;
}
