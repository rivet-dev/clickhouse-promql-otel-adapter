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
