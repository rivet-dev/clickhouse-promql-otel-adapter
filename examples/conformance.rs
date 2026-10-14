//! Compare the Rust evaluator with results recorded by the pinned upstream suite.
//! Every case is counted; unsupported cases fail and are included in the report.
use clickhouse_promql::model::Series;
#[path = "../tests/common/reference.rs"]
mod reference;
use reference::{compare, load};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs,
    io::{BufRead, BufReader},
    path::PathBuf,
};
fn main() -> anyhow::Result<()> {
    let root = PathBuf::from(std::env::args().nth(1).unwrap_or(".state/oracle".into()));
    let mut datasets = BTreeMap::<String, Result<Vec<Series>, String>>::new();
    let mut totals = BTreeMap::<String, [usize; 2]>::new();
    let mut failures = Vec::new();
    let filter = std::env::var("CONFORMANCE_FILTER").unwrap_or_default();
    for (index, line) in BufReader::new(fs::File::open(root.join("cases.jsonl"))?)
        .lines()
        .enumerate()
    {
        let case: Value = serde_json::from_str(&line?)?;
        let fixture = case["fixture"].as_str().unwrap();
        if !fixture.contains(&filter) {
            continue;
        }
        let id = case["dataset"].as_str().unwrap();
        if !datasets.contains_key(id) {
            let data: Value = serde_json::from_slice(&fs::read(
                root.join("datasets").join(format!("{id}.json")),
            )?)?;
            datasets.insert(id.into(), load(&data));
        }
        let result = compare(&case, datasets[id].as_ref());
        totals.entry(fixture.into()).or_default()[if result.is_ok() { 0 } else { 1 }] += 1;
        if let Err(error) = result {
            failures.push(json!({"line":index+1,"fixture":fixture,"query":case["query"],"reason":error,"dataset":id}));
        }
    }
    let passed: usize = totals.values().map(|v| v[0]).sum();
    let failed: usize = totals.values().map(|v| v[1]).sum();
    for (name, [pass, fail]) in &totals {
        println!("{name:32} {pass:6} passed {fail:6} failed");
    }
    println!("Total: {passed} passed, {failed} failed");
    fs::write(
        root.join("report.json"),
        serde_json::to_vec_pretty(
            &json!({"passed":passed,"failed":failed,"fixtures":totals,"failures":failures}),
        )?,
    )?;
    if failed > 0 {
        std::process::exit(1)
    }
    Ok(())
}
