//! The committed Prometheus reference corpus: every recorded execution and
//! the datasets they run against.
use clickhouse_promql::model::Series;
use serde_json::Value;
use std::{collections::BTreeMap, io::Read};

/// Cases in corpus order and datasets by id, each parsed with `dataset`.
pub fn load(
    dataset: impl Fn(&Value) -> Vec<Series>,
) -> (Vec<Value>, BTreeMap<String, Vec<Series>>) {
    let gz =
        flate2::read::GzDecoder::new(include_bytes!("../conformance/prometheus.tar.gz").as_slice());
    let mut datasets = BTreeMap::new();
    let mut cases = Vec::new();
    for entry in tar::Archive::new(gz).entries().unwrap() {
        let mut entry = entry.unwrap();
        let path = entry.path().unwrap().to_string_lossy().into_owned();
        let mut content = String::new();
        entry.read_to_string(&mut content).unwrap();
        if path == "cases.jsonl" {
            cases = content
                .lines()
                .map(|s| serde_json::from_str(s).unwrap())
                .collect();
        } else {
            let id = path
                .strip_prefix("datasets/")
                .unwrap()
                .strip_suffix(".json")
                .unwrap();
            datasets.insert(id.into(), dataset(&serde_json::from_str(&content).unwrap()));
        }
    }
    (cases, datasets)
}
