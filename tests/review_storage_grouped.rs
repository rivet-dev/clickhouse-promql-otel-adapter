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
#[tokio::test]
async fn all_sample_kinds_send_long_labels_once_without_losing_points() {
    common::empty_database_test(true,|mut store|async move {
        for kind in ["gauge","sum","histogram","summary","exponential_histogram"] {
            let rows=(0..100).map(|i| {
                let mut row=json!({"MetricName":kind,"ScopeName":PROM,"TimeUnix":T/1000+i,"Attributes":{"long_label":"x".repeat(2000)}});
                for (key,value) in match kind {
                    "gauge"|"sum"=>json!({"Value":i}),
                    "histogram"=>json!({"Count":i,"Sum":i,"BucketCounts":[i],"ExplicitBounds":[]}),
                    "summary"=>json!({"Count":i,"Sum":i,"ValueAtQuantiles.Quantile":[0.5],"ValueAtQuantiles.Value":[i]}),
                    _=>json!({"Count":i,"Sum":i,"PositiveBucketCounts":[i]})
                }.as_object().unwrap() { row[key]=value.clone(); }
                row
            }).collect();
            insert(&store,kind,rows).await;
        }
        // Uncompressed row-oriented responses repeat >200KB of labels/table.
        store.max_bytes=16_000;
        for query in ["gauge","sum","histogram_count","summary_count","exponential_histogram"] {
            let loaded=store.load(T,T+100_000,&[selector(query)]).await.unwrap();
            assert_eq!(loaded.series.len(),1,"{query}");
            assert_eq!(loaded.series[0].points.len(),100,"{query}");
            assert_eq!(loaded.series[0].points[99].timestamp,T+99_000);
        }
    }).await;
}
#[tokio::test]
async fn grouped_transport_preserves_raw_row_limits_and_conflicting_samples() {
    common::empty_database_test(false,|mut store|async move {
        insert(&store,"gauge",(0..3).map(|i|json!({"MetricName":"bounded","ScopeName":PROM,"TimeUnix":T/1000+i,"Value":i})).collect()).await;
        store.max_rows=2;
        let error=store.load(T,T+3_000,&[selector("bounded")]).await.err().unwrap().to_string();
        assert!(error.contains("sample scan limit"),"{error}");
        store.max_rows=100;
        insert(&store,"gauge",vec![json!({"MetricName":"conflict","ScopeName":PROM,"TimeUnix":T/1000,"Value":2}),json!({"MetricName":"conflict","ScopeName":PROM,"TimeUnix":T/1000,"Value":1})]).await;
        assert!(store.load(T,T,&[selector("conflict")]).await.err().unwrap().to_string().contains("conflicting samples"));
    }).await;
}
#[tokio::test]
async fn metadata_variants_flags_start_time_and_target_info_survive_grouping() {
    common::empty_database_test(true,|store|async move {
        insert(&store,"sum",vec![
            json!({"MetricName":"counter","ScopeName":PROM,"TimeUnix":T/1000,"StartTimeUnix":T/1000-10,"Value":2,"IsMonotonic":true,"MetricDescription":"first","ResourceAttributes":{"service.name":"svc"}}),
            json!({"MetricName":"counter","ScopeName":PROM,"TimeUnix":T/1000+1,"StartTimeUnix":T/1000-5,"Value":3,"Flags":1,"IsMonotonic":true,"MetricDescription":"second","ResourceAttributes":{"service.name":"svc"}}),
        ]).await;
        let loaded=store.load(T,T+1000,&[selector("counter"),selector("target_info")]).await.unwrap();
        assert_eq!(loaded.metadata["counter"].len(),2);
        let counter=loaded.series.iter().find(|s|s.labels["__name__"]=="counter").unwrap();
        assert_eq!(counter.points[0].start_timestamp,T-10_000);
        assert_eq!(counter.points[1].start_timestamp,T-5_000);
        assert!(counter.points[1].stale());
        let target=loaded.series.iter().find(|s|s.labels["__name__"]=="target_info").unwrap();
        assert_eq!(target.points.len(),1);
    }).await;
}

#[tokio::test]
async fn output_cache_keeps_changing_bounds_and_quantiles_distinct() {
    common::empty_database_test(true, |store| async move {
        insert(&store,"histogram",vec![
            json!({"MetricName":"changing","ScopeName":PROM,"TimeUnix":T/1000,"Count":3,"Sum":6,"BucketCounts":[1,1,1],"ExplicitBounds":[1,2]}),
            json!({"MetricName":"changing","ScopeName":PROM,"TimeUnix":T/1000+1,"Count":6,"Sum":12,"BucketCounts":[2,2,2],"ExplicitBounds":[2,3]}),
        ]).await;
        let all=store.load(T,T+1000,&[selector("changing_bucket")]).await.unwrap();
        for (le,expected) in [("1",vec![1.0]),("2",vec![2.0,2.0]),("3",vec![4.0]),("+Inf",vec![3.0,6.0])] {
            let series=all.series.iter().find(|s|s.labels["le"]==le).unwrap();
            assert_eq!(series.points.iter().map(|p|p.value).collect::<Vec<_>>(),expected);
        }
        insert(&store,"summary",vec![
            json!({"MetricName":"quantiles","ScopeName":PROM,"TimeUnix":T/1000,"ValueAtQuantiles.Quantile":[0.5],"ValueAtQuantiles.Value":[5]}),
            json!({"MetricName":"quantiles","ScopeName":PROM,"TimeUnix":T/1000+1,"ValueAtQuantiles.Quantile":[0.9],"ValueAtQuantiles.Value":[9]}),
        ]).await;
        let selected=store.load(T,T+1000,&[selector("quantiles{quantile=\"0.9\"}")]).await.unwrap();
        assert_eq!(selected.series.len(),1);
        assert_eq!(selected.series[0].points.len(),1);
        assert_eq!(selected.series[0].points[0].value,9.0);
    }).await;
}

#[tokio::test]
async fn unsupported_column_types_fall_back_to_json_transport() {
    common::empty_database_test(false, |store| async move {
        insert(&store,"histogram",(0..3).map(|i|json!({"MetricName":"latency","ScopeName":PROM,"TimeUnix":T/1000+i,"Attributes":{"pod":"a"},"Count":i+1,"Sum":i,"BucketCounts":[i,1],"ExplicitBounds":[0.5]})).collect()).await;
        let load = |store: Store| async move {
            let loaded = store.load(T, T + 3_000, &[selector("latency_bucket")]).await.unwrap();
            loaded.series.into_iter().map(|s| (s.labels.into_map(), s.points.iter().map(|p| (p.timestamp, p.value, p.start_timestamp)).collect::<Vec<_>>())).collect::<Vec<_>>()
        };
        let typed = load(store.clone()).await;
        assert_eq!(typed.len(), 2);
        assert_eq!(typed[0].1.len(), 3);
        // Geometry has no RowBinary reader, so the same rows arrive as JSON.
        store.query(&format!("ALTER TABLE {}.otel_metrics_histogram ADD COLUMN geo Point DEFAULT (1, 2)", store.database), &[]).await.unwrap();
        assert_eq!(load(store.clone()).await, typed);
    }).await;
}
