use clickhouse_promql::engine::Engine;
use serde_json::{Value, json};
#[test]
fn pinned_prometheus_parser_contract() {
    let cases: Vec<Value> = serde_json::from_str(include_str!("conformance/parser.json")).unwrap();
    let mut failures = Vec::new();
    for (i, c) in cases.iter().enumerate() {
        let hex = c["input_hex"].as_str().unwrap();
        let bytes = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect::<Vec<_>>();
        let result = std::str::from_utf8(&bytes)
            .map_err(anyhow::Error::from)
            .and_then(Engine::parse);
        let mismatch = match &result {
            Ok(expr) => {
                c.get("error").is_some() || json!(expr.value_type().to_string()) != c["type"]
            }
            Err(_) => c.get("error").is_none(),
        };
        if mismatch {
            failures.push(json!({"index":i,"expected":c,"actual":result.map(|e|format!("{}: {e}",e.value_type())).unwrap_or_else(|e|format!("ERROR: {e}"))}));
        }
    }
    std::fs::create_dir_all(".state").unwrap();
    std::fs::write(
        ".state/parser-conformance.json",
        serde_json::to_vec_pretty(
            &json!({"total":cases.len(),"passed":cases.len()-failures.len(),"failures":failures}),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(
        failures.is_empty(),
        "{} of {} parser acceptance/type cases failed; see .state/parser-conformance.json",
        failures.len(),
        cases.len()
    );
}
