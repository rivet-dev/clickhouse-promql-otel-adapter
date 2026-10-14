package oracle

import (
	"encoding/json"
	"github.com/prometheus/otlptranslator"
	"os"
	"testing"
)

func TestOTelNames(t *testing.T) {
	out := os.Getenv("OTEL_NAMES_OUTPUT")
	if out == "" {
		t.Skip("set OTEL_NAMES_OUTPUT")
	}
	namer := otlptranslator.MetricNamer{WithMetricSuffixes: true}
	labeler := otlptranslator.LabelNamer{PreserveMultipleUnderscores: true}
	cases := []map[string]any{}
	for _, name := range []string{"http.requests", "requests_total", "total_requests_total", "cpu__usage", "record:metric:", "1metric", "meter", "métric", "", "_", "__", "!!!", "seconds", "bytes_per_second", "ratio_cpu_ratio", "with space", "already_valid"} {
		for _, unit := range []string{"", "s", "ms", "us", "ns", "min", "h", "d", "By", "KiBy", "MiBy", "GiBy", "TiBy", "KBy", "MBy", "GBy", "TBy", "m", "V", "A", "J", "W", "g", "Cel", "Hz", "1", "%", "By/s", "m/s", "1/s", "{requests}/s", "foo-bar/baz qux", "per_", "{thing}", "s/"} {
			for _, kind := range []otlptranslator.MetricType{otlptranslator.MetricTypeGauge, otlptranslator.MetricTypeMonotonicCounter, otlptranslator.MetricTypeNonMonotonicCounter, otlptranslator.MetricTypeHistogram} {
				result, err := namer.Build(otlptranslator.Metric{Name: name, Unit: unit, Type: kind})
				c := map[string]any{"name": name, "unit": unit, "kind": kind, "result": result}
				if err != nil {
					c["error"] = err.Error()
				}
				cases = append(cases, c)
			}
		}
	}
	labels := []map[string]any{}
	for _, name := range []string{"job", "http.method", "_private", "__name__", "1label", "x__y", "foo...bar", "métric", "", "_", "!!!", ":foo:"} {
		result, err := labeler.Build(name)
		c := map[string]any{"name": name, "result": result}
		if err != nil {
			c["error"] = err.Error()
		}
		labels = append(labels, c)
	}
	data, err := json.MarshalIndent(map[string]any{"metrics": cases, "labels": labels}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(out, append(data, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}
