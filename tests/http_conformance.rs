mod common;
use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use clickhouse_promql::api::{App, router};
use serde_json::{Value, json};
use tower::ServiceExt;

fn normalize(value: &mut Value) {
    match value {
        Value::Number(n) => *value = json!(n.as_f64().unwrap()),
        Value::Array(v) => {
            for v in v {
                normalize(v)
            }
        }
        Value::Object(map) => {
            for (key, v) in map {
                if key == "timings" {
                    if let Some(timings) = v.as_object_mut() {
                        for v in timings.values_mut() {
                            *v = json!(0.0);
                        }
                    }
                } else {
                    normalize(v)
                }
            }
        }
        _ => {}
    }
}
#[tokio::test]
async fn pinned_prometheus_http_contract() {
    common::database_test(|store| async move {
        for kind in ["gauge","sum","histogram","summary","exponential_histogram"] {
            store.query(&format!("TRUNCATE TABLE {}.otel_metrics_{kind}",store.database),&[]).await.unwrap();
        }
        let mut rows=Vec::new();
        for instance in ["a","b"] {for i in 0..6 {rows.push(json!({"MetricName":"test_gauge","TimeUnix":1700000000+i*15,"Value":i+1,"Attributes":{"job":"api","instance":instance},"Exemplars.FilteredAttributes":[{"sample":"yes"}],"Exemplars.TimeUnix":[1699999999+i*15],"Exemplars.Value":[(i+1) as f64/10.0],"Exemplars.TraceId":["0123456789abcdef0123456789abcdef"],"Exemplars.SpanId":[""]}).to_string());}}
        store.query(&format!("INSERT INTO {}.otel_metrics_gauge (MetricName,TimeUnix,Value,Attributes,`Exemplars.FilteredAttributes`,`Exemplars.TimeUnix`,`Exemplars.Value`,`Exemplars.TraceId`,`Exemplars.SpanId`) FORMAT JSONEachRow\n{}",store.database,rows.join("\n")),&[]).await.unwrap();
        let app=router(App::new(store));
        let cases:Vec<Value>=serde_json::from_str(include_str!("conformance/http.json")).unwrap();
        let mut failures=Vec::new();let total=cases.len();
        for (index,case) in cases.into_iter().enumerate() {
            let mut request=Request::builder().method(case["method"].as_str().unwrap()).uri(case["uri"].as_str().unwrap());
            if let Some(content_type)=case["content_type"].as_str().filter(|s|!s.is_empty()) {request=request.header("content-type",content_type);}
            let response=app.clone().oneshot(request.body(Body::from(case["body"].as_str().unwrap().to_owned())).unwrap()).await.unwrap();
            let ndjson=response.headers().get("content-type").is_some_and(|v|v.to_str().unwrap().contains("ndjson"));
            let status=response.status().as_u16();let bytes=to_bytes(response.into_body(),2_000_000).await.unwrap();
            let mut actual=if ndjson {json!(String::from_utf8_lossy(&bytes).lines().map(|l|serde_json::from_str::<Value>(l).unwrap()).collect::<Vec<_>>())} else {serde_json::from_slice::<Value>(&bytes).unwrap_or_else(|_|json!(String::from_utf8_lossy(&bytes)))};let mut expected=case["response"].clone();normalize(&mut actual);normalize(&mut expected);
            if status as u64 != case["status"].as_u64().unwrap() || actual != expected {
                failures.push(json!({"index":index,"request":case,"actual_status":status,"actual":actual}));
            }
        }
        std::fs::create_dir_all(".state").unwrap();std::fs::write(".state/http-conformance.json",serde_json::to_vec_pretty(&json!({"total":total,"passed":total-failures.len(),"failures":failures})).unwrap()).unwrap();
        assert!(failures.is_empty(),"{} of {total} HTTP cases failed; see .state/http-conformance.json",failures.len());
    }).await
}
