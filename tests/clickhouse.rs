mod common;
use clickhouse_promql::{engine::Engine, storage::plan};
const START: i64 = 1_700_000_000_000;
#[tokio::test]
async fn collector_counter_query() {
    common::database_test(|store| async move {
        let query = "sum(rate(requests_total[1m]))";
        let expr = Engine::parse(query).unwrap();
        let (a, b, s) = plan(&expr, START + 75_000, START + 75_000, 300_000).unwrap();
        let data = store.load(a, b, &s).await.unwrap();
        let result = Engine::new(&data.series, START + 75_000, START + 75_000)
            .instant(query, START + 75_000)
            .unwrap()
            .vector()
            .unwrap();
        assert_eq!(result.len(), 1);
        assert!((result[0].value - 2.0 / 3.0).abs() < 1e-12);
    })
    .await
}
#[tokio::test]
async fn collector_histogram_query() {
    common::database_test(|store| async move {
        let query = "histogram_quantile(0.5,latency_bucket)";
        let expr = Engine::parse(query).unwrap();
        let (a, b, s) = plan(&expr, START, START, 300_000).unwrap();
        let data = store.load(a, b, &s).await.unwrap();
        let result = Engine::new(&data.series, START, START)
            .instant(query, START)
            .unwrap()
            .vector()
            .unwrap();
        assert_eq!(result[0].value, 1.0);
        assert_eq!(result[0].labels["job"], "tests/fixture");
    })
    .await
}
#[tokio::test]
async fn collector_summary_and_flags() {
    common::database_test(|store| async move {
        for (query, count) in [("rpc_duration{quantile=\"0.5\"}", 1), ("missing", 0)] {
            let expr = Engine::parse(query).unwrap();
            let (a, b, s) = plan(&expr, START, START, 300_000).unwrap();
            let data = store.load(a, b, &s).await.unwrap();
            let result = Engine::new(&data.series, START, START)
                .instant(query, START)
                .unwrap()
                .vector()
                .unwrap();
            assert_eq!(result.len(), count);
        }
    })
    .await
}
#[tokio::test]
async fn rejects_lossy_and_delta_data() {
    common::database_test(|store| async move {
        for (query, error) in [
            ("subsecond", "conflicting samples"),
            ("delta_requests_total", "delta temporality"),
        ] {
            let expr = Engine::parse(query).unwrap();
            let (a, b, s) = plan(&expr, START, START, 300_000).unwrap();
            let result = store.load(a, b, &s).await;
            assert!(result.is_err(), "{query} should fail");
            assert!(result.err().unwrap().to_string().contains(error));
        }
    })
    .await
}
#[tokio::test]
async fn discovers_all_five_types_without_evaluating_them() {
    common::database_test(|store| async move {
        let data = store.discover(START, START + 75_000, &[]).await.unwrap();
        assert!(data.series.iter().any(|s| s.labels["__name__"] == "size"));
        assert!(
            data.series
                .iter()
                .any(|s| s.labels["__name__"] == "latency_bucket" && s.labels["le"] == "+Inf")
        );
        assert!(data.metadata.contains_key("requests_total"));
    })
    .await
}

#[tokio::test]
async fn native_histogram_from_default_schema() {
    common::database_test(|store| async move {
        let expr = Engine::parse("histogram_count(size)").unwrap();
        let (a, b, selectors) = plan(&expr, START, START, 300_000).unwrap();
        let data = store.load(a, b, &selectors).await.unwrap();
        assert_eq!(data.warnings.len(), 1);
        let result = Engine::new(&data.series, START, START)
            .evaluate(&expr, START)
            .unwrap()
            .vector()
            .unwrap();
        assert_eq!(result[0].value, 6.0);
    })
    .await
}

#[tokio::test]
async fn collector_special_values_attributes_and_resets() {
    common::database_test(|store| async move {
        for (query, expected) in [
            ("resets(reset_requests_total[1m])", 1.0),
            ("start_timestamp(reset_requests_total)", 1700000030.0),
            ("large_integer", 9007199254740992.0),
            ("hist_absent_sum", 0.0),
            ("hist_zero_sum", 0.0),
        ] {
            let t = START + 45000;
            let expr = Engine::parse_at(query, t, t, 0).unwrap();
            let (a, b, selectors) = plan(&expr, t, t, 300000).unwrap();
            let loaded = store.load(a, b, &selectors).await.unwrap();
            let value = Engine::new(&loaded.series, t, t)
                .evaluate(&expr, t)
                .unwrap()
                .vector()
                .unwrap();
            assert_eq!(value.len(), 1, "{query}");
            assert_eq!(value[0].value, expected, "{query}");
        }
        let expr = Engine::parse("typed_attributes").unwrap();
        let (a, b, s) = plan(&expr, START, START, 300000).unwrap();
        let data = store.load(a, b, &s).await.unwrap();
        let labels = &data.series[0].labels;
        assert_eq!(labels["bool"], "true");
        assert_eq!(labels["int"], "42");
        assert_eq!(labels["double"], "1.25");
        assert_eq!(labels["array"], "[1,\"two\"]");
        assert_eq!(labels["map"], "{\"key\":\"value\"}");
        assert_eq!(labels["otel_scope_name"], "fixture-generator");
        let expr = Engine::parse("special").unwrap();
        let (a, b, s) = plan(&expr, START, START, 300000).unwrap();
        let data = store.load(a, b, &s).await.unwrap();
        let values = Engine::new(&data.series, START, START)
            .evaluate(&expr, START)
            .unwrap()
            .vector()
            .unwrap();
        assert_eq!(values.len(), 3);
        for s in values {
            match s.labels["kind"].as_str() {
                "nan" => assert!(s.value.is_nan()),
                "positive" => assert_eq!(s.value, f64::INFINITY),
                "negative" => assert_eq!(s.value, f64::NEG_INFINITY),
                _ => panic!(),
            }
        }
        let expr = Engine::parse("target_info").unwrap();
        let (a, b, s) = plan(&expr, START, START, 300000).unwrap();
        let data = store.load(a, b, &s).await.unwrap();
        assert_eq!(data.series.len(), 1);
        assert_eq!(data.series[0].labels["job"], "tests/fixture");
        assert_eq!(data.series[0].points[0].value, 1.0);
    })
    .await
}
#[tokio::test]
async fn collector_exemplars_preserve_trace_ids_and_filtered_attributes() {
    common::database_test(|store| async move {
        let expr = Engine::parse("requests_total").unwrap();
        let (_, _, s) = plan(&expr, START, START + 15000, 300000).unwrap();
        let values = store.exemplars(START, START + 15000, &s).await.unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0]["exemplars"].as_array().unwrap().len(), 2);
        let e = &values[0]["exemplars"][0];
        assert_eq!(e["labels"]["trace_id"], "0123456789abcdef0123456789abcdef");
        assert_eq!(e["labels"]["span_id"], "0123456789abcdef");
        assert_eq!(e["labels"]["sample"], "yes");
        assert_eq!(e["value"], "0.5");
        let expr = Engine::parse("latency_bucket{le=\"1\"}").unwrap();
        let (_, _, s) = plan(&expr, START, START, 300000).unwrap();
        let values = store.exemplars(START, START, &s).await.unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0]["seriesLabels"]["le"], "1");
    })
    .await
}

#[tokio::test]
async fn fine_exponential_scales_are_downsampled_for_prometheus() {
    common::empty_database_test(true,|store|async move{
        store.query(&format!("INSERT INTO {}.otel_metrics_exponential_histogram (MetricName,TimeUnix,Count,Sum,Scale,ZeroCount,PositiveOffset,PositiveBucketCounts,NegativeOffset,NegativeBucketCounts,AggregationTemporality) VALUES ('fine',1700000000,3,3,9,0,0,[1,2],0,[],2)",store.database),&[]).await.unwrap();
        let expr=Engine::parse("fine").unwrap();let(a,b,s)=plan(&expr,START,START,300000).unwrap();let data=store.load(a,b,&s).await.unwrap();let h=data.series[0].points[0].histogram.as_ref().unwrap();assert_eq!(h.schema,8);assert_eq!(h.positive.len(),1);assert_eq!(h.positive[&1],3.0);
    }).await
}

#[tokio::test]
async fn metric_filter_preserves_normalization_and_escapes() {
    common::empty_database_test(false, |mut store| async move {
        let weird="a'b\\c";
        let scope="github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver";
        let mut rows=(0..100).map(|i|serde_json::json!({"MetricName":"unrelated","TimeUnix":1700000000+i,"Value":i}).to_string()).collect::<Vec<_>>();
        rows.push(serde_json::json!({"MetricName":"http.request.duration","MetricUnit":"s","TimeUnix":1700000000,"Value":3}).to_string());
        rows.push(serde_json::json!({"MetricName":weird,"ScopeName":scope,"TimeUnix":1700000000,"Value":4}).to_string());
        store.query(&format!("INSERT INTO {}.otel_metrics_gauge FORMAT JSONEachRow\n{}",store.database,rows.join("\n")),&[]).await.unwrap();
        store.max_rows=10;
        for (query,expected) in [("http_request_duration_seconds".to_owned(),3.0),(format!("{{{}}}",serde_json::to_string(weird).unwrap()),4.0)] {
            let expr=Engine::parse(&query).unwrap(); let (a,b,selectors)=plan(&expr,START,START,300_000).unwrap();
            let data=store.load(a,b,&selectors).await.unwrap();
            assert_eq!(data.series.len(),1);assert_eq!(data.series[0].points[0].value,expected);
        }
    }).await;
}

#[tokio::test]
async fn review_storage_regressions() {
    common::empty_database_test(false, |mut store| async move {
        let scope="github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver";
        let rows=[
            serde_json::json!({"MetricName":"foo","ScopeName":scope,"Attributes":{"job":"x","instance":"y"},"TimeUnix":950,"Value":10}),
            serde_json::json!({"MetricName":"foo","ScopeName":scope,"Attributes":{"job":"x","instance":"y"},"TimeUnix":980,"Value":20}),
            serde_json::json!({"MetricName":"target_info","ScopeName":scope,"Attributes":{"job":"x","instance":"y","region":"west"},"TimeUnix":800,"Value":1}),
            serde_json::json!({"MetricName":"???","TimeUnix":1,"Value":1}),
        ];
        store.query(&format!("INSERT INTO {}.otel_metrics_gauge FORMAT JSONEachRow\n{}",store.database,rows.iter().map(ToString::to_string).collect::<Vec<_>>().join("\n")),&[]).await.unwrap();
        let q="info(rate(foo[1m] @ 1000))";let expr=Engine::parse_at(q,2_000_000,2_000_000,0).unwrap();let (a,b,selectors)=plan(&expr,2_000_000,2_000_000,300_000).unwrap();
        assert!(a<=700_000);
        let data=store.load(a,b,&selectors).await.unwrap();let v=Engine::new(&data.series,2_000_000,2_000_000).instant(q,2_000_000).unwrap().vector().unwrap();
        assert_eq!(v[0].labels["region"],"west");
        store.query(&format!("INSERT INTO {}.otel_metrics_gauge (MetricName,TimeUnix,Value) SELECT 'dense', 2000+number, number FROM numbers(100)",store.database),&[]).await.unwrap();
        store.max_rows=10;
        let data=store.discover(2_000_000,2_100_000,&[]).await.unwrap();assert_eq!(data.series.len(),1);
        store.query(&format!("INSERT INTO {}.otel_metrics_gauge (MetricName,TimeUnix,StartTimeUnix,Value) VALUES ('same',2000,1800,5),('same',2000,1900,5)",store.database),&[]).await.unwrap();
        let expr=Engine::parse("same").unwrap();let (a,b,selectors)=plan(&expr,2_000_000,2_000_000,300_000).unwrap();
        assert!(store.load(a,b,&selectors).await.err().unwrap().to_string().contains("conflicting samples"));
    }).await;
}

#[tokio::test]
async fn collector_histograms_without_optional_buckets() {
    common::database_test(|store| async move {
        for (q, value) in [
            ("hist_no_buckets_count", 5.0),
            ("hist_no_buckets_sum", 10.0),
            ("hist_no_buckets_bucket", 5.0),
        ] {
            let expr = Engine::parse(q).unwrap();
            let (a, b, selectors) = plan(&expr, START, START, 300_000).unwrap();
            let data = store.load(a, b, &selectors).await.unwrap();
            let v = Engine::new(&data.series, START, START)
                .instant(q, START)
                .unwrap()
                .vector()
                .unwrap();
            assert_eq!(v.len(), 1);
            assert_eq!(v[0].value, value);
        }
    })
    .await;
}
#[tokio::test]
async fn promoted_resource_attribute_separates_targets() {
    common::empty_database_test(false, |mut store| async move {
        // Two hosts report one series and differ only in a resource attribute
        // that is not service.instance.id.
        let rows = ["host-a", "host-b"]
            .iter()
            .enumerate()
            .map(|(i, host)| {
                serde_json::json!({
                    "MetricName": "heartbeats_total", "Value": i as f64 + 1.0,
                    "TimeUnix": "2023-11-14 22:13:20", "StartTimeUnix": "2023-11-14 22:00:00",
                    "IsMonotonic": true, "AggregationTemporality": 2,
                    "ResourceAttributes": {"service.name": "engine", "rivet.instance_id": host},
                    "ScopeName": "fixture-generator",
                })
                .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n");
        store
            .query(
                &format!(
                    "INSERT INTO {}.otel_metrics_sum FORMAT JSONEachRow\n{rows}",
                    store.database
                ),
                &[],
            )
            .await
            .unwrap();
        let query = "heartbeats_total";
        let expr = Engine::parse(query).unwrap();
        let (a, b, s) = plan(&expr, START, START, 300_000).unwrap();
        let collision = store.load(a, b, &s).await;
        assert!(
            collision
                .err()
                .unwrap()
                .to_string()
                .contains("conflicting samples")
        );

        store.promoted_resource_attributes = vec![("rivet.instance_id".into(), "pod".into())];
        let result = Engine::new(&store.load(a, b, &s).await.unwrap().series, START, START)
            .instant(query, START)
            .unwrap()
            .vector()
            .unwrap();
        let mut pods = result
            .iter()
            .map(|s| s.labels["pod"].clone())
            .collect::<Vec<_>>();
        pods.sort();
        assert_eq!(pods, ["host-a", "host-b"]);

        // Label pushdown must read a promoted label from the resource attributes.
        for (query, expected) in [
            ("heartbeats_total{pod=\"host-b\"}", vec!["host-b"]),
            (
                "heartbeats_total{pod=~\"host-a|host-b\"}",
                vec!["host-a", "host-b"],
            ),
            ("heartbeats_total{pod!=\"host-a\"}", vec!["host-b"]),
        ] {
            let expr = Engine::parse(query).unwrap();
            let (a, b, s) = plan(&expr, START, START, 300_000).unwrap();
            let result = Engine::new(&store.load(a, b, &s).await.unwrap().series, START, START)
                .instant(query, START)
                .unwrap()
                .vector()
                .unwrap();
            let mut pods = result
                .iter()
                .map(|s| s.labels["pod"].clone())
                .collect::<Vec<_>>();
            pods.sort();
            assert_eq!(pods, expected, "{query}");
        }
    })
    .await
}
#[tokio::test]
async fn label_columns_filter_the_mirrored_column() {
    common::empty_database_test(false, |mut store| async move {
        // ServiceName stands in for a sort-key tenant column. The last row
        // breaks the mirroring contract on purpose, to prove the column
        // filter is applied in SQL.
        let rows = [("a", "a"), ("b", "b"), ("a", "other")]
            .iter()
            .map(|(tenant, column)| {
                serde_json::json!({
                    "MetricName": "requests", "Value": 1,
                    "TimeUnix": "2023-11-14 22:13:20", "ServiceName": column,
                    "Attributes": {"tenant": tenant, "column": column},
                    "ScopeName": "fixture-generator",
                })
                .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n");
        store
            .query(
                &format!(
                    "INSERT INTO {}.otel_metrics_gauge FORMAT JSONEachRow\n{rows}",
                    store.database
                ),
                &[],
            )
            .await
            .unwrap();
        store.label_columns = vec![("tenant".into(), "ServiceName".into())];
        for (query, expected) in [
            ("requests{tenant=\"a\"}", vec!["a"]),
            ("requests{tenant=~\"a|b\"}", vec!["a", "b"]),
            ("requests{tenant!=\"b\"}", vec!["a", "other"]),
            ("requests{tenant=~\"a.*\"}", vec!["a", "other"]),
        ] {
            let expr = Engine::parse(query).unwrap();
            let (a, b, s) = plan(&expr, START, START, 300_000).unwrap();
            let result = Engine::new(&store.load(a, b, &s).await.unwrap().series, START, START)
                .instant(query, START)
                .unwrap()
                .vector()
                .unwrap();
            let mut columns = result
                .iter()
                .map(|s| s.labels["column"].clone())
                .collect::<Vec<_>>();
            columns.sort();
            assert_eq!(columns, expected, "{query}");
        }
    })
    .await
}
#[tokio::test]
async fn histogram_count_and_sum_match_without_reading_buckets() {
    common::database_test(|store| async move {
        let value = |query: &'static str| {
            let store = store.clone();
            async move {
                let expr = Engine::parse(query).unwrap();
                let (a, b, s) = plan(&expr, START, START, 300_000).unwrap();
                let data = store.load(a, b, &s).await.unwrap();
                let mut values = Engine::new(&data.series, START, START)
                    .instant(query, START)
                    .unwrap()
                    .vector()
                    .unwrap()
                    .into_iter()
                    .map(|s| s.value)
                    .collect::<Vec<_>>();
                values.sort_by(f64::total_cmp);
                values
            }
        };
        // The first query names only _count, so its rows omit bucket arrays.
        let count = value("latency_count").await;
        assert!(!count.is_empty());
        assert_eq!(count, value("latency_bucket{le=\"+Inf\"}").await);
        // Naming a _bucket series alongside makes the same rows carry buckets.
        let sum = value("latency_sum").await;
        assert!(!sum.is_empty());
        assert_eq!(
            sum,
            value("latency_sum or latency_bucket{le=\"absent\"}").await
        );
    })
    .await
}
