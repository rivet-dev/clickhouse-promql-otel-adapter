package oracle

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/util/teststorage"
	v1 "github.com/prometheus/prometheus/web/api/v1"
)

func TestAPIReference(t *testing.T) {
	out := os.Getenv("API_OUTPUT")
	if out == "" {
		t.Skip("set API_OUTPUT to generate HTTP references")
	}
	db := teststorage.New(t)
	defer db.Close()
	app := db.Appender(context.Background())
	for _, instance := range []string{"a", "b"} {
		for i := 0; i < 6; i++ {
			_, err := app.Append(0, labels.FromStrings("__name__", "test_gauge", "job", "api", "instance", instance), 1700000000000+int64(i)*15000, float64(i+1))
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := app.Commit(); err != nil {
		t.Fatal(err)
	}
	registry := prometheus.NewRegistry()
	api := v1.NewAPI(promqltest.NewTestEngine(t, false, 5*time.Minute, 1000000), db, nil, nil, db,
		nil, nil, nil, func() config.Config { return config.Config{} }, map[string]string{}, v1.GlobalURLOptions{}, func(h http.HandlerFunc) http.HandlerFunc { return h }, nil, "", false, false, 1000, slog.Default(), nil,
		1000000, 10, 1048576, false, nil, nil, &v1.PrometheusVersion{Version: "3.15.0"}, nil, nil, registry, registry, nil, false, nil, false, false, false, false, 5*time.Minute, false, false, nil, nil, v1.OpenAPIOptions{}, parser.NewParser(parser.Options{EnableExperimentalFunctions: true, ExperimentalDurationExpr: true, EnableExtendedRangeSelectors: true, EnableBinopFillModifiers: true}))
	router := route.New()
	api.Register(router.WithPrefix("/api/v1"))
	cases := []map[string]any{}
	add := func(path string, params url.Values, method string, body string, contentType string) {
		uri := "/api/v1/" + path + "?" + params.Encode()
		req := httptest.NewRequest(method, uri, strings.NewReader(body))
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, req)
		var value any
		if err := json.Unmarshal(recorder.Body.Bytes(), &value); err != nil {
			value = recorder.Body.String()
		}
		// Durations are measurements, not stable protocol fixtures.
		if root, ok := value.(map[string]any); ok {
			if data, ok := root["data"].(map[string]any); ok {
				if stats, ok := data["stats"].(map[string]any); ok {
					if timings, ok := stats["timings"].(map[string]any); ok {
						for key := range timings {
							timings[key] = float64(0)
						}
					}
				}
			}
		}
		cases = append(cases, map[string]any{"method": method, "uri": uri, "body": body, "content_type": contentType, "status": recorder.Code, "response": value})
	}
	base := url.Values{"query": {"test_gauge"}, "time": {"1700000075"}}
	queries := []string{"1+2", "test_gauge", `test_gauge{instance="a"}`, "sum(test_gauge)", "test_gauge[1m]", "\"hello\"", "1/0", "0/0", "", "(", "unknown(test_gauge)", "rate(test_gauge)", "sum(1)", "{}", "1 == 1"}
	for _, q := range queries {
		for _, method := range []string{"GET", "POST"} {
			p := url.Values{"query": {q}, "time": {"1700000075"}}
			body := ""
			if method == "POST" {
				body = p.Encode()
				p = url.Values{}
			}
			add("query", p, method, body, "application/x-www-form-urlencoded")
		}
	}
	for _, name := range []string{"time", "timeout", "lookback_delta", "limit", "stats"} {
		for _, value := range []string{"", "-1", "0", "1", "1.5", "NaN", "Inf", "garbage", "1m", "all", "samples"} {
			p := url.Values{}
			for k, v := range base {
				p[k] = v
			}
			p[name] = []string{value}
			add("query", p, "GET", "", "")
		}
	}
	for _, q := range []string{"test_gauge", "sum(test_gauge)", "test_gauge[1m]", "\"hello\"", "1"} {
		for _, step := range []string{"15", "0", "-1", "NaN", "15s", "0.0001"} {
			add("query_range", url.Values{"query": {q}, "start": {"1700000000"}, "end": {"1700000075"}, "step": {step}}, "GET", "", "")
		}
	}
	for _, path := range []string{"series", "labels", "label/instance/values", "label/__name__/values", "label/invalid-name/values"} {
		for _, match := range []string{"", "test_gauge", `{job="api"}`, `{job=~".*"}`, "sum(test_gauge)", "invalid{"} {
			p := url.Values{"start": {"1700000000"}, "end": {"1700000075"}}
			if match != "" {
				p["match[]"] = []string{match}
			}
			add(path, p, "GET", "", "")
		}
	}
	for _, path := range []string{"format_query", "parse_query"} {
		for _, q := range []string{"1+2", "sum by (job)(rate(test_gauge[1m]))", "test_gauge offset 1m @ 1700000000", ""} {
			add(path, url.Values{"query": {q}}, "GET", "", "")
		}
	}
	add("query", base, "PUT", "", "")
	add("query", base, "POST", "", "")
	add("query", base, "POST", "{}", "application/json")
	parserData, err := os.ReadFile("../../tests/conformance/parser.json")
	if err != nil {
		t.Fatal(err)
	}
	var parserCases []map[string]any
	if err = json.Unmarshal(parserData, &parserCases); err != nil {
		t.Fatal(err)
	}
	for _, c := range parserCases {
		if _, ok := c["error"]; !ok {
			for _, path := range []string{"format_query", "parse_query"} {
				add(path, url.Values{"query": {c["input"].(string)}}, "GET", "", "")
			}
		}
	}
	data, err := json.MarshalIndent(cases, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(out, append(data, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}
