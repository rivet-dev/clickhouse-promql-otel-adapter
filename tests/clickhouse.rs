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
