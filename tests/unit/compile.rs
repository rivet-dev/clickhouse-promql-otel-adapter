use super::*;

const CORPUS: &[&str] = &[
    r#"sum by (pod) (rate(rivet_guard_requests_total{rivet_project="$project", rivet_datacenter=~"$datacenter", pod=~"$host"}[$__rate_interval]))"#,
    r#"increase(rivet_guard_requests_total{kind="http"}[5m])"#,
    r#"histogram_quantile(0.99, sum by (le, machine) (rate(rivet_state_machine_effect_seconds_bucket{rivet_project="$project", machine=~"$machine"}[$__rate_interval])))"#,
    r#"sum(rate(rivet_epoxy_work_duration_seconds_count[1m])) / sum(rate(rivet_epoxy_work_duration_seconds_sum[1m]))"#,
    r#"max by (pod) (rivet_depot_client_volume_slots{pod=~"$host"}) / max by (pod) (rivet_depot_client_volume_capacity)"#,
    r#"1 - max by (pod) (node_memory_MemAvailable_bytes{role=~"engine|runtime", pod=~"$host|$runtime"}) / max by (pod) (node_memory_MemTotal_bytes)"#,
    r#"topk(15, max_over_time(rivet_state_machine_effects_in_flight[1h]))"#,
    r#"clamp_max(avg by (pod) (rate(rivet_tokio_worker_busy_duration_total[1m])), 1)"#,
    r#"label_replace(changes(rivet_topology_epoch{job!="x"}[5m]), "host", "$1", "pod", "(.*)")"#,
    r#"(sum(rate(a_total[1m])) > 0.5) and on () (sum(rate(a_total[1m])) > 0.5) or on () (0 * sum(b))"#,
    r#"scalar(sum(b)) * vector(2) + time()"#,
    r#"sum by (machine) (rate(c_total[1m])) * on (machine) group_left (owner) max by (machine, owner) (d)"#,
    r#"-deriv(g[10m] offset 5m)"#,
];

fn compiled(expr: &str) -> Plan {
    compile(expr, &BTreeMap::new()).unwrap_or_else(|e| panic!("{expr}: {e}"))
}

fn error(expr: &str) -> CompileError {
    compile(expr, &BTreeMap::new()).expect_err(expr)
}

#[test]
fn every_leaf_is_scoped_to_one_project() {
    let variable = regex::Regex::new(r"\$\{?[A-Za-z_]").unwrap();
    let scoped_read =
        regex::Regex::new(r"FROM otel_metrics_\w+\n\t*WHERE ProjectId = \{project:String\}\n")
            .unwrap();
    for expr in CORPUS {
        for leaf in compiled(expr).leaves {
            let reads = leaf.sql.matches("FROM otel_metrics_").count();
            assert!(reads >= 1, "{expr}");
            assert_eq!(
                scoped_read.find_iter(&leaf.sql).count(),
                reads,
                "{expr}: {}",
                leaf.sql
            );
            assert_eq!(leaf.sql.matches("ProjectId").count(), reads, "{expr}");
            assert!(!leaf.sql.contains("rivet_project"), "{expr}");
            assert!(!variable.is_match(&leaf.sql), "{expr}");
        }
    }
}

#[test]
fn placeholders_are_the_documented_ones() {
    let placeholder = regex::Regex::new(r"\{([a-z_]+):([A-Za-z0-9]+)\}").unwrap();
    for expr in CORPUS {
        for leaf in compiled(expr).leaves {
            for capture in placeholder.captures_iter(&leaf.sql) {
                let found = format!("{}:{}", &capture[1], &capture[2]);
                assert!(
                    [
                        "project:String",
                        "from:UInt32",
                        "to:UInt32",
                        "query_from:UInt32",
                        "step:UInt32"
                    ]
                    .contains(&found.as_str()),
                    "{expr}: {found}"
                );
            }
        }
    }
}

#[test]
fn plans_snapshot() {
    let plans = CORPUS
        .iter()
        .map(|expr| serde_json::json!({ "expr": expr, "plan": compiled(expr) }))
        .collect::<Vec<_>>();
    insta::assert_json_snapshot!(plans);
}

#[test]
fn repeated_selectors_share_a_leaf() {
    let plan = compiled(r#"sum(rate(a_total[1m])) / sum(rate(a_total[5m]))"#);
    assert_eq!(plan.leaves.len(), 1);
}

#[test]
fn variables_outside_matchers_take_their_value() {
    let variables = BTreeMap::from([("k".to_owned(), "5".to_owned())]);
    let plan = compile("topk($k, g)", &variables).unwrap();
    assert!(matches!(plan.tree, Node::Topk { k, .. } if k == 5.0));
    assert_eq!(error("topk(${k}, g)").construct, "$k");
}

#[test]
fn matcher_values_are_quoted() {
    let plan = compiled(r#"g{path="a'b\\c"}"#);
    assert!(
        plan.leaves[0]
            .sql
            .contains(r#"Attributes['path'] = 'a\'b\\c'"#),
        "{}",
        plan.leaves[0].sql
    );
}

#[test]
fn counters_match_with_and_without_total() {
    let plan = compiled("rate(a_total[1m])");
    assert!(
        plan.leaves[0]
            .sql
            .contains("MetricName IN ('a_total', 'a')")
    );
}

#[test]
fn unsupported_constructs_name_themselves() {
    for (expr, construct) in [
        ("rate(a_total[5m] @ end())", "@ modifier"),
        ("sort_desc(g)", "sort_desc()"),
        ("rate(a_total[5m:1m])", "rate()"),
        ("max_over_time(g[5m:1m])", "max_over_time()"),
        ("absent(g)", "absent()"),
        ("predict_linear(g[5m], 60)", "predict_linear()"),
        ("count_values(\"v\", g)", "count_values"),
        ("{__name__=~\"a.*\"}", "matcher on __name__"),
        ("rate(h_bucket[1m])", "histogram buckets"),
        (
            "max by (le) (rate(h_bucket[1m]))",
            "max over histogram buckets",
        ),
        (
            "sum by (pod) (rate(h_bucket[1m]))",
            "sum over histogram buckets",
        ),
        ("rate(h_bucket{le=\"1\"}[1m])", "le matcher on a histogram"),
        ("histogram_quantile(0.9, g)", "histogram_quantile"),
        ("g[5m]", "range vector"),
    ] {
        assert_eq!(error(expr).construct, construct, "{expr}");
    }
}

#[test]
fn regex_anchors_keep_their_matcher() {
    for (expr, kept) in [
        (r#"g{path=~"/api/.*$"}"#, "match(Attributes['path']"),
        (r#"g{path!~"/api/.*$"}"#, "NOT match(Attributes['path']"),
        (r#"g{path=~"^a$|^b$"}"#, "match(Attributes['path']"),
    ] {
        let sql = &compiled(expr).leaves[0].sql;
        assert!(sql.contains(kept), "{expr}: {sql}");
    }
}

#[test]
fn variable_matchers_are_removed() {
    for expr in [
        r#"g{pod=~"$host"}"#,
        r#"g{pod=~"${host}"}"#,
        r#"g{pod=~"$host|$runtime"}"#,
        r#"g{pod!~"/api/$path.*$"}"#,
    ] {
        let sql = &compiled(expr).leaves[0].sql;
        assert!(!sql.contains("Attributes['pod']"), "{expr}: {sql}");
    }
}

#[test]
fn duration_variables_stand_in_only_inside_ranges() {
    for expr in [
        "rate(a_total[$__rate_interval])",
        "increase(a_total[${__range}])",
        "max_over_time(g[[[__interval]]])",
    ] {
        compiled(expr);
    }
    for (expr, construct) in [
        ("increase(a_total[$__range]) / $__range_s", "$__range_s"),
        ("g * ${__range_ms}", "$__range_ms"),
        ("g / $__interval_ms", "$__interval_ms"),
        ("g / [[__rate_interval_ms]]", "$__rate_interval_ms"),
        ("rate(a_total[$__range_s])", "$__range_s"),
        ("g offset $__interval", "$__interval"),
        ("g / $__range", "$__range"),
    ] {
        assert_eq!(error(expr).construct, construct, "{expr}");
    }
}

#[test]
fn variable_syntaxes_outside_matchers() {
    let variables = BTreeMap::from([("k".to_owned(), "5".to_owned())]);
    for expr in ["topk(${k}, g)", "topk(${k:raw}, g)", "topk([[k]], g)"] {
        let plan = compile(expr, &variables).unwrap_or_else(|e| panic!("{expr}: {e}"));
        assert!(
            matches!(plan.tree, Node::Topk { k, .. } if k == 5.0),
            "{expr}"
        );
    }
    let error = compile("topk(${k:regex}, g)", &variables).unwrap_err();
    assert_eq!(error.construct, "$k");
    assert_eq!(
        compile("topk([[k], g)", &variables).unwrap_err().construct,
        "syntax"
    );
}

#[test]
fn variable_syntaxes_inside_matchers_are_removed() {
    for expr in [
        r#"g{pod=~"${host:regex}"}"#,
        r#"g{pod=~"[[host]]"}"#,
        r#"g{pod=~"[[host:pipe]]"}"#,
    ] {
        let sql = &compiled(expr).leaves[0].sql;
        assert!(!sql.contains("Attributes['pod']"), "{expr}: {sql}");
    }
    let sql = &compiled(r#"g{pod=~"[[:alpha:]]+"}"#).leaves[0].sql;
    assert!(sql.contains("match(Attributes['pod']"), "{sql}");
}
