#!/usr/bin/env python3
"""Deterministic OTLP/HTTP JSON covering the five standard metrics table types."""
import json

BASE_NS = 1_700_000_000_000_000_000


def attrs(**values):
    return [{"key": k, "value": {"stringValue": str(v)}} for k, v in values.items()]


def dataset():
    metrics = []
    for name, kind, extra in [
        ("temperature", "gauge", {}),
        ("requests", "sum", {"aggregationTemporality": 2, "isMonotonic": True}),
        ("inflight", "sum", {"aggregationTemporality": 2, "isMonotonic": False}),
        ("delta_requests", "sum", {"aggregationTemporality": 1, "isMonotonic": True}),
        ("latency", "histogram", {"aggregationTemporality": 2}),
        ("size", "exponentialHistogram", {"aggregationTemporality": 2}),
        ("rpc_duration", "summary", {}),
    ]:
        points = []
        for i in range(6):
            point = {"attributes": attrs(route="/api", region="west"),
                     "startTimeUnixNano": str(BASE_NS),
                     "timeUnixNano": str(BASE_NS + i * 15_000_000_000), "flags": 0}
            if kind in ("gauge", "sum"):
                point["asDouble"] = 20.5 + i if kind == "gauge" else float(i * 10)
            elif kind == "histogram":
                point.update(count=str(6*(i+1)), sum=4.5*(i+1),
                             bucketCounts=[str(n*(i+1)) for n in (1,2,3)],
                             explicitBounds=[0.1, 1.0], min=0.05, max=2.0)
            elif kind == "exponentialHistogram":
                point.update(count=str(6*(i+1)), sum=4.0*(i+1), scale=1,
                             zeroCount=str(i+1), zeroThreshold=0.01,
                             positive={"offset": 0, "bucketCounts": [str(2*(i+1)), str(2*(i+1))]},
                             negative={"offset": -1, "bucketCounts": [str(i+1)]}, min=-1.0, max=2.0)
            else:
                point.update(count=str(10*(i+1)), sum=3.0*(i+1),
                             quantileValues=[{"quantile": 0.5, "value": 0.2}, {"quantile": 0.99, "value": 0.9}])
            if kind != "summary":
                point["exemplars"] = [{"filteredAttributes": attrs(sample="yes"),
                                       "timeUnixNano": point["timeUnixNano"], "asDouble": 0.5,
                                       "spanId": "0123456789abcdef", "traceId": "0123456789abcdef0123456789abcdef"}]
            points.append(point)
        metrics.append({"name": name, "description": f"Fixture {name}",
                        "unit": "", kind: {**extra, "dataPoints": points}})
    metrics.append({"name": "subsecond", "gauge": {"dataPoints": [
        {"timeUnixNano": str(BASE_NS + offset), "asDouble": value}
        for offset, value in [(100_000_000, 1), (900_000_000, 2)]]}})
    metrics.append({"name": "missing", "gauge": {"dataPoints": [
        {"timeUnixNano": str(BASE_NS), "asDouble": 0, "flags": 1}]}})
    metrics.extend([
        {"name": "hist_no_buckets", "histogram": {"aggregationTemporality": 2, "dataPoints": [{"timeUnixNano": str(BASE_NS), "count": "5", "sum": 10}]}},
        {"name": "hist_absent", "histogram": {"aggregationTemporality": 2, "dataPoints": [{"timeUnixNano": str(BASE_NS), "count": "0", "bucketCounts": ["0"], "explicitBounds": []}]}},
        {"name": "hist_zero", "histogram": {"aggregationTemporality": 2, "dataPoints": [{"timeUnixNano": str(BASE_NS), "count": "0", "sum": 0, "min": 0, "max": 0, "bucketCounts": ["0"], "explicitBounds": []}]}},
        {"name": "large_integer", "gauge": {"dataPoints": [{"timeUnixNano": str(BASE_NS), "asInt": "9007199254740993"}]}},
        {"name": "special", "gauge": {"dataPoints": [{"timeUnixNano": str(BASE_NS), "attributes": attrs(kind=kind), "asDouble": value} for kind, value in [("nan", "NaN"), ("positive", "Infinity"), ("negative", "-Infinity")]]}},
        {"name": "reset_requests", "sum": {"aggregationTemporality": 2, "isMonotonic": True, "dataPoints": [{"timeUnixNano": str(BASE_NS+i*15_000_000_000), "startTimeUnixNano": str(BASE_NS+(30_000_000_000 if i>=2 else 0)), "asInt": str(value)} for i,value in enumerate([10,20,3,13])]}},
        {"name":"typed_attributes", "gauge":{"dataPoints":[{"timeUnixNano":str(BASE_NS),"asDouble":1,"attributes":[
            {"key":"bool","value":{"boolValue":True}}, {"key":"int","value":{"intValue":"42"}}, {"key":"double","value":{"doubleValue":1.25}},
            {"key":"array","value":{"arrayValue":{"values":[{"intValue":"1"},{"stringValue":"two"}]}}},
            {"key":"map","value":{"kvlistValue":{"values":[{"key":"key","value":{"stringValue":"value"}}]}}}
        ]}]}}
    ])
    return {"resourceMetrics": [{"resource": {"attributes": attrs(**{
        "service.name": "fixture", "service.namespace": "tests", "service.instance.id": "one"})},
        "scopeMetrics": [{"scope": {"name": "fixture-generator", "version": "1"}, "metrics": metrics}]}]}

if __name__ == "__main__":
    print(json.dumps(dataset(), sort_keys=True, indent=2))
