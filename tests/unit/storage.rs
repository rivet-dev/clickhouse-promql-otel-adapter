use super::*;
use promql_parser::parser;

fn selector(text: &str) -> VectorSelector {
    match parser::parse(text).unwrap() {
        Expr::VectorSelector(selector) => selector,
        _ => panic!("selector"),
    }
}

fn table_filter(
    selectors: &[&str],
    names: &[Option<&[&str]>],
    columns: &[(&str, &str)],
) -> Option<TableFilter> {
    let selectors = selectors.iter().map(|s| selector(s)).collect::<Vec<_>>();
    let names = names
        .iter()
        .map(|names| names.map(|names| names.iter().map(|n| n.to_string()).collect()))
        .collect::<Vec<_>>();
    let columns = columns
        .iter()
        .map(|(label, column)| (label.to_string(), column.to_string()))
        .collect::<Vec<_>>();
    selector_filter("gauge", &selectors, &names, &[], &columns)
}

fn filter(
    selectors: &[&str],
    names: &[Option<&[&str]>],
    columns: &[(&str, &str)],
) -> Option<(String, Vec<(String, String)>)> {
    table_filter(selectors, names, columns).map(|filter| (filter.predicate, filter.params))
}

#[test]
fn only_selectors_the_table_can_serve_are_kept_for_decoding() {
    let kept = |selectors: &[&str], names: &[Option<&[&str]>]| {
        table_filter(selectors, names, &[]).map(|filter| {
            filter
                .selectors
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
    };
    assert_eq!(
        kept(
            &["a", "missing", "b{x=\"1\"}"],
            &[Some(&["a"]), Some(&[]), Some(&["b"])]
        ),
        Some(vec!["a".to_string(), "b{x=\"1\"}".to_string()])
    );
    assert_eq!(
        kept(&["missing", "{__name__=~\"c.*\"}"], &[Some(&[]), None]),
        Some(vec!["{__name__=~\"c.*\"}".to_string()])
    );
    assert_eq!(kept(&["missing"], &[Some(&[])]), None);
    assert_eq!(kept(&[], &[]), Some(Vec::new()));
    // The same set in another order or with repeats is the same request.
    let names: &[Option<&[&str]>] = &[Some(&["b"]), Some(&["a"]), Some(&["b"])];
    let a_then_b = table_filter(&["b", "a", "b"], names, &[]).unwrap();
    let b_then_a = table_filter(&["a", "b"], &names[1..], &[]).unwrap();
    assert_eq!(
        a_then_b
            .selectors
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
    assert_eq!(a_then_b.predicate, b_then_a.predicate);
    assert_eq!(a_then_b.params, b_then_a.params);
}

#[test]
fn each_selector_is_scoped_to_its_own_metric_names() {
    let (predicate, params) = filter(
        &["a{tenant=\"1\"}", "b"],
        &[Some(&["a"]), Some(&["b"])],
        &[],
    )
    .unwrap();
    assert!(
        predicate.starts_with(" AND ((MetricName IN {names_0:Array(String)} AND (((("),
        "{predicate}"
    );
    assert!(
        predicate.ends_with(") OR (MetricName IN {names_3:Array(String)}))"),
        "{predicate}"
    );
    assert_eq!(
        params[0],
        ("param_names_0".to_string(), "['a']".to_string())
    );
    assert_eq!(params[1].0, "param_label_key_1");
    assert_eq!(params[2].0, "param_label_value_1");
    assert_eq!(
        params[3],
        ("param_names_3".to_string(), "['b']".to_string())
    );
}

#[test]
fn selectors_matching_no_stored_metric_are_dropped() {
    let (predicate, params) = filter(
        &["missing", "b{tenant!=\"\"}"],
        &[Some(&[]), Some(&["b", "b2"])],
        &[],
    )
    .unwrap();
    assert!(
        predicate.starts_with(" AND ((MetricName IN {names_0:Array(String)} AND"),
        "{predicate}"
    );
    assert_eq!(predicate.matches("MetricName IN").count(), 1, "{predicate}");
    assert_eq!(params[0].1, "['b','b2']");
    assert!(filter(&["missing"], &[Some(&[])], &[]).is_none());
}

#[test]
fn unfilterable_selectors_admit_every_row() {
    // Names that cannot be narrowed, with a label term, filter on labels only.
    let (predicate, params) = filter(&["{__name__=~\"a.*\",tenant=\"1\"}"], &[None], &[]).unwrap();
    assert!(
        predicate.starts_with(" AND (((((if(ScopeName"),
        "{predicate}"
    );
    assert!(!predicate.contains("MetricName IN"), "{predicate}");
    assert_eq!(params.len(), 2);
    // Neither names nor labels narrow this one, which admits the whole table,
    // including the rows another selector had narrowed.
    assert_eq!(
        filter(
            &["a{tenant=\"1\"}", "{__name__=~\"b.*\"}"],
            &[Some(&["a"]), None],
            &[]
        ),
        Some((String::new(), Vec::new()))
    );
    assert_eq!(
        filter(&["a{tenant=~\"1.*\"}"], &[Some(&["a"]), None], &[])
            .unwrap()
            .0,
        " AND ((MetricName IN {names_0:Array(String)}))"
    );
    assert_eq!(filter(&[], &[], &[]), Some((String::new(), Vec::new())));
}

#[test]
fn or_branches_filter_independently_within_their_selector() {
    let mut selector = selector("a{tenant=\"1\"}");
    selector.matchers.or_matchers = vec![
        selector.matchers.matchers.clone(),
        self::selector("a{tenant=~\"x.*\"}").matchers.matchers,
    ];
    let names = [Some(vec!["a".to_string()])];
    let filter = selector_filter("gauge", &[selector], &names, &[], &[]).unwrap();
    let (predicate, params) = (filter.predicate, filter.params);
    assert_eq!(predicate, " AND ((MetricName IN {names_0:Array(String)}))");
    assert_eq!(
        params.len(),
        1,
        "branch terms of an unfilterable selector are dropped"
    );
}

#[test]
fn mirrored_columns_replace_the_attribute_term_for_positive_matchers() {
    let columns = [("tenant", "ProjectId")];
    let (predicate, params) = filter(&["a{tenant=\"1\"}"], &[Some(&["a"])], &columns).unwrap();
    assert_eq!(
        predicate,
        " AND ((MetricName IN {names_0:Array(String)} AND (((ProjectId IN {label_column_1:Array(String)})))))"
    );
    assert_eq!(
        params[1],
        ("param_label_column_1".to_string(), "['1']".to_string())
    );
    let (predicate, _) = filter(&["a{tenant=~\"1|2\"}"], &[Some(&["a"])], &columns).unwrap();
    assert!(
        predicate.contains("ProjectId IN") && !predicate.contains("Attributes"),
        "{predicate}"
    );
    // Matchers the column cannot decide keep the attribute term.
    for query in ["a{tenant!=\"1\"}", "a{tenant=~\"1.*\"}", "a{tenant=\"\"}"] {
        let (predicate, _) = filter(&[query], &[Some(&["a"])], &columns).unwrap();
        assert!(!predicate.contains("ProjectId"), "{query}: {predicate}");
        assert!(
            predicate.contains("Attributes[") || predicate.ends_with("String)}))"),
            "{query}: {predicate}"
        );
    }
}

#[test]
fn clickhouse_summary_headers_feed_round_trip_logs() {
    let mut headers = reqwest::header::HeaderMap::new();
    assert_eq!(Summary::from_headers(&headers), Summary::default());
    headers.insert(
        "x-clickhouse-summary",
        "{\"read_rows\":\"131217\",\"read_bytes\":\"1\",\"elapsed_ns\":\"83456789\"}"
            .parse()
            .unwrap(),
    );
    assert_eq!(
        Summary::from_headers(&headers),
        Summary {
            elapsed_ms: Some(83),
            read_rows: Some(131217),
        }
    );
    headers.insert("x-clickhouse-summary", "not json".parse().unwrap());
    assert_eq!(Summary::from_headers(&headers), Summary::default());
}
