use super::*;
use serde_json::json;

/// Builds RowBinary payloads by hand.
#[derive(Default)]
struct Payload(Vec<u8>);

impl Payload {
    fn length(mut self, mut value: usize) -> Self {
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            if value == 0 {
                self.0.push(byte);
                return self;
            }
            self.0.push(byte | 0x80);
        }
    }

    fn string(self, text: &str) -> Self {
        let mut payload = self.length(text.len());
        payload.0.extend_from_slice(text.as_bytes());
        payload
    }

    fn raw(mut self, bytes: &[u8]) -> Self {
        self.0.extend_from_slice(bytes);
        self
    }

    fn header(self, columns: &[(&str, &str)]) -> Self {
        let mut payload = self.length(columns.len());
        for (name, _) in columns {
            payload = payload.string(name);
        }
        for (_, ty) in columns {
            payload = payload.string(ty);
        }
        payload
    }
}

fn parse(text: &str) -> Ty {
    Ty::parse(text).unwrap()
}

#[test]
fn header_parses_names_and_nested_types() {
    let payload = Payload::default().header(&[
        ("Attributes", "Map(LowCardinality(String), String)"),
        (
            "_samples",
            "Array(Tuple(`_timestamp` Int64, Flags UInt32, BucketCounts Array(UInt64), Float64))",
        ),
        ("ZeroThreshold", "Nullable(Float64)"),
        ("When", "DateTime64(3, 'UTC')"),
        ("Kind", "Enum8('a' = 1, 'b\\'c' = -2)"),
        ("Money", "Decimal(18, 4)"),
        ("Total", "SimpleAggregateFunction(sum, UInt64)"),
    ]);
    let columns = Reader::new(&payload.0).header().unwrap();
    let types = columns.iter().map(|c| c.ty.clone()).collect::<Vec<_>>();
    assert_eq!(
        columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
        [
            "Attributes",
            "_samples",
            "ZeroThreshold",
            "When",
            "Kind",
            "Money",
            "Total"
        ]
    );
    assert_eq!(
        types,
        [
            Ty::Map(Box::new(Ty::String), Box::new(Ty::String)),
            Ty::Array(Box::new(Ty::Tuple(vec![
                Ty::Int64,
                Ty::UInt32,
                Ty::Array(Box::new(Ty::UInt64)),
                Ty::Float64,
            ]))),
            Ty::Nullable(Box::new(Ty::Float64)),
            Ty::DateTime64,
            Ty::Enum8(vec![(1, "a".into()), (-2, "b'c".into())]),
            Ty::Decimal { bytes: 8, scale: 4 },
            Ty::UInt64,
        ]
    );
}

#[test]
fn unsupported_types_are_reported_as_such() {
    for text in [
        "Variant(String, UInt64)",
        "Array(JSON)",
        "Point",
        "AggregateFunction(uniq, String)",
        "Tuple(",
        "FixedString(x)",
    ] {
        let error = Ty::parse(text).unwrap_err();
        assert!(error.to_string().contains("unsupported"), "{text}: {error}");
    }
    let payload = Payload::default().header(&[("geo", "Polygon")]);
    let error = Reader::new(&payload.0).header().unwrap_err();
    assert!(error.is::<Unsupported>(), "{error:#}");
    assert!(error.to_string().contains("geo"), "{error:#}");
}

#[test]
fn cells_decode_every_supported_type() {
    let uuid_half = 0x0123456789abcdefu64;
    let payload = Payload::default()
        .raw(&[7])
        .raw(&65535u16.to_le_bytes())
        .raw(&4_000_000_000u32.to_le_bytes())
        .raw(&u64::MAX.to_le_bytes())
        .raw(&(-1i8).to_le_bytes())
        .raw(&(-2i16).to_le_bytes())
        .raw(&(-3i32).to_le_bytes())
        .raw(&i64::MIN.to_le_bytes())
        .raw(&1.5f32.to_le_bytes())
        .raw(&f64::NAN.to_le_bytes())
        .raw(&f64::INFINITY.to_le_bytes())
        .raw(&f64::NEG_INFINITY.to_le_bytes())
        .raw(&2.5f64.to_le_bytes())
        .raw(&[0xc0, 0x3f])
        .raw(&[1])
        .string("text")
        .raw(b"ab\0\0")
        .raw(&19724u16.to_le_bytes())
        .raw(&(-1i32).to_le_bytes())
        .raw(&1_704_164_645u32.to_le_bytes())
        .raw(&1_704_164_645_678i64.to_le_bytes())
        .raw(&uuid_half.to_le_bytes())
        .raw(&uuid_half.to_le_bytes())
        .raw(&0x01020304u32.to_le_bytes())
        .raw(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
        .raw(&(-2i8).to_le_bytes())
        .raw(&300i16.to_le_bytes())
        .raw(&125i64.to_le_bytes())
        .raw(&[1])
        .raw(&[0])
        .raw(&2.5f64.to_le_bytes())
        .length(2)
        .raw(&1u64.to_le_bytes())
        .raw(&2u64.to_le_bytes())
        .length(1)
        .string("k")
        .string("v")
        .raw(&3i64.to_le_bytes())
        .string("t")
        .raw(&(-5i128).to_le_bytes())
        .raw(&[0xff; 32]);
    let types = [
        ("UInt8", json!(7)),
        ("UInt16", json!(65535)),
        ("UInt32", json!(4_000_000_000u32)),
        ("UInt64", json!(u64::MAX)),
        ("Int8", json!(-1)),
        ("Int16", json!(-2)),
        ("Int32", json!(-3)),
        ("Int64", json!(i64::MIN)),
        ("Float32", json!(1.5)),
        ("Float64", json!("nan")),
        ("Float64", json!("inf")),
        ("Float64", json!("-inf")),
        ("LowCardinality(Float64)", json!(2.5)),
        ("BFloat16", json!(1.5)),
        ("Bool", json!(true)),
        ("String", json!("text")),
        ("FixedString(4)", json!("ab")),
        ("Date", json!(19724)),
        ("Date32", json!(-1)),
        ("DateTime('UTC')", json!(1_704_164_645u32)),
        ("DateTime64(3)", json!(1_704_164_645_678i64)),
        ("UUID", json!("01234567-89ab-cdef-0123-456789abcdef")),
        ("IPv4", json!("1.2.3.4")),
        ("IPv6", json!("::1")),
        ("Enum8('x' = -2)", json!("x")),
        ("Enum16('y' = 300)", json!("y")),
        ("Decimal64(2)", json!(1.25)),
        ("Nullable(String)", json!(null)),
        ("Nullable(Float64)", json!(2.5)),
        ("Array(UInt64)", json!([1, 2])),
        ("Map(LowCardinality(String), String)", json!({"k": "v"})),
        ("Tuple(a Int64, b String)", json!([3, "t"])),
        ("Int128", json!("-5")),
        ("UInt256", json!(format!("0x{}", "ff".repeat(32)))),
    ];
    let mut reader = Reader::new(&payload.0);
    for (ty, expected) in types {
        let value = reader.cell(&parse(ty)).unwrap();
        assert_eq!(value, expected, "{ty}");
    }
    assert!(reader.is_empty());
}

#[test]
fn numeric_reads_accept_integer_widths_and_reject_other_types() {
    let payload = Payload::default()
        .raw(&[9])
        .raw(&7u64.to_le_bytes())
        .raw(&(-4i32).to_le_bytes())
        .length(2)
        .raw(&[1, 2])
        .string("no");
    let mut reader = Reader::new(&payload.0);
    assert_eq!(reader.integer(&Ty::UInt8).unwrap(), 9);
    assert_eq!(reader.float(&Ty::UInt64).unwrap(), 7.0);
    assert_eq!(reader.float(&Ty::Int32).unwrap(), -4.0);
    let mut values = Vec::new();
    reader.floats(&parse("Array(UInt8)"), &mut values).unwrap();
    assert_eq!(values, [1.0, 2.0]);
    assert!(reader.integer(&Ty::String).is_err());
    assert!(!Ty::String.is_number());
    assert!(!parse("Array(String)").is_number_array());
    assert!(parse("Array(LowCardinality(Float32))").is_number_array());
    assert!(!parse("Nullable(Int8)").is_integer());

    let payload = Payload::default().raw(&u64::MAX.to_le_bytes());
    let error = Reader::new(&payload.0).integer(&Ty::UInt64).unwrap_err();
    assert!(error.to_string().contains("out of range"), "{error}");
}

#[test]
fn truncated_and_oversized_payloads_fail_cleanly() {
    let payload = Payload::default().length(5).raw(b"abc");
    let error = Reader::new(&payload.0).string().unwrap_err();
    assert!(error.to_string().contains("truncated"), "{error}");

    let payload = Payload::default().raw(&1u32.to_le_bytes());
    assert!(Reader::new(&payload.0).cell(&Ty::Int64).is_err());

    let payload = Payload::default().raw(&[0xff; 11]);
    let error = Reader::new(&payload.0).length().unwrap_err();
    assert!(error.to_string().contains("length prefix"), "{error}");

    let payload = Payload::default().length(1_000_000).raw(&[1]);
    assert!(
        Reader::new(&payload.0)
            .cell(&parse("Array(UInt8)"))
            .is_err()
    );
    assert!(Reader::new(&[]).header().is_err());
}

#[test]
fn tuple_element_names_are_stripped() {
    assert_eq!(tuple_element_type("Int64"), "Int64");
    assert_eq!(tuple_element_type("name Int64"), "Int64");
    assert_eq!(
        tuple_element_type("`Exemplars.Value` Array(Float64)"),
        "Array(Float64)"
    );
    assert_eq!(
        tuple_element_type("Map(String, String)"),
        "Map(String, String)"
    );
    assert_eq!(
        tuple_element_type("x Map(String, String)"),
        "Map(String, String)"
    );
    assert_eq!(
        split_arguments("Enum8('a,b' = 1, 'c)' = 2)"),
        ["Enum8('a,b' = 1, 'c)' = 2)"]
    );
    assert_eq!(
        split_arguments("'a,b' = 1, 'c\\'d' = 2"),
        ["'a,b' = 1", "'c\\'d' = 2"]
    );
    assert_eq!(split_arguments(""), Vec::<&str>::new());
}

#[test]
fn streamed_exceptions_are_found_at_the_end_of_a_payload() {
    let mut bytes = vec![0u8; 10_000];
    bytes.extend_from_slice(
        b"Code: 241. DB::Exception: Memory limit exceeded. (MEMORY_LIMIT_EXCEEDED)\n",
    );
    assert_eq!(
        trailing_exception(&bytes).unwrap(),
        "Code: 241. DB::Exception: Memory limit exceeded. (MEMORY_LIMIT_EXCEEDED)"
    );
    assert_eq!(trailing_exception(b"Code: 1 but just data"), None);
    assert_eq!(trailing_exception(&[0xff, 0xfe]), None);
}

/// Decodes a wide row from a real ClickHouse both ways and expects the same
/// JSON, except for the types whose JSON text is a date or padded string.
#[tokio::test]
async fn real_clickhouse_values_decode_like_json() {
    let store = crate::storage::Store::new(
        std::env::var("CLICKHOUSE_URL").unwrap_or("http://127.0.0.1:18123".into()),
        "system".into(),
        "default".into(),
        "local-test-only".into(),
    )
    .unwrap();
    let sql = "SELECT 7::UInt8 AS u8, 65535::UInt16 AS u16, 4000000000::UInt32 AS u32, 18446744073709551615::UInt64 AS u64, -1::Int8 AS i8, -2::Int16 AS i16, -3::Int32 AS i32, -9223372036854775808::Int64 AS i64, 1.5::Float32 AS f32, 2.5::Float64 AS f64, nan AS nan_value, inf AS inf_value, -inf AS negative_inf, 'text' AS s, toLowCardinality('lc') AS lc, CAST(NULL, 'Nullable(String)') AS absent, CAST('x', 'Nullable(String)') AS present, [1, 2, 3]::Array(UInt64) AS arr, map('a', 'b', 'c', 'd') AS m, CAST(map('k', 'v'), 'Map(LowCardinality(String), String)') AS lcm, tuple(1::Int64, 'a') AS tup, true AS b, toUUID('01234567-89ab-cdef-0123-456789abcdef') AS u, CAST('a', 'Enum8(\\'a\\' = 1, \\'b\\' = 2)') AS e, toDecimal64(1.25, 2) AS dec, toIPv4('1.2.3.4') AS ip4, toIPv6('::1') AS ip6, [[1.5]]::Array(Array(Float64)) AS nested, groupArray(tuple(number::Int64, 2::UInt32, [3::UInt64])) AS samples, toDate('2024-01-02') AS d, toDateTime('2024-01-02 03:04:05', 'UTC') AS dt, toDateTime64('2024-01-02 03:04:05.678', 3, 'UTC') AS dt64, toFixedString('ab', 4) AS fs FROM numbers(2)";
    let json = store
        .query(
            &format!(
                "{sql} SETTINGS output_format_json_quote_denormals=1, output_format_json_quote_64bit_integers=0 FORMAT JSONEachRow"
            ),
            &[],
        )
        .await
        .expect("real ClickHouse required: just clickhouse-up");
    let expected: Json = serde_json::from_str(json.trim()).unwrap();
    let bytes = store
        .query_bytes(&format!("{sql} FORMAT RowBinaryWithNamesAndTypes"), &[])
        .await
        .unwrap();
    let mut reader = Reader::new(&bytes);
    let columns = reader.header().unwrap();
    let mut row = serde_json::Map::new();
    for column in &columns {
        row.insert(column.name.clone(), reader.cell(&column.ty).unwrap());
    }
    assert!(reader.is_empty());
    for (name, value) in &row {
        match name.as_str() {
            "d" => assert_eq!(value, &json!(19724)),
            "dt" => assert_eq!(value, &json!(1_704_164_645u32)),
            "dt64" => assert_eq!(value, &json!(1_704_164_645_678i64)),
            "fs" => assert_eq!(value, &json!("ab")),
            _ => assert_eq!(value, &expected[name], "{name}"),
        }
    }
    assert_eq!(row.len(), expected.as_object().unwrap().len());
}
