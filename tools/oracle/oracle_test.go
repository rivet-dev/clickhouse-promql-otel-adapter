// This is test-only tooling. The adapter and evaluator are entirely Rust.
package oracle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/teststorage"
)

type engine struct {
	base    promql.QueryEngine
	out     string
	fixture string
	cases   *os.File
}
type query struct {
	promql.Query
	owner            *engine
	data             storage.Queryable
	text             string
	start, end, step int64
	lookback         int64
}

func (e *engine) NewInstantQuery(ctx context.Context, s storage.Queryable, o promql.QueryOpts, text string, t time.Time) (promql.Query, error) {
	q, err := e.base.NewInstantQuery(ctx, s, o, text, t)
	if err != nil {
		e.recordCreation(ctx, s, text, t.UnixMilli(), t.UnixMilli(), 0, lookback(o), err)
		return nil, err
	}
	return &query{q, e, s, text, t.UnixMilli(), t.UnixMilli(), 0, lookback(o)}, nil
}
func (e *engine) NewRangeQuery(ctx context.Context, s storage.Queryable, o promql.QueryOpts, text string, start, end time.Time, step time.Duration) (promql.Query, error) {
	q, err := e.base.NewRangeQuery(ctx, s, o, text, start, end, step)
	if err != nil {
		e.recordCreation(ctx, s, text, start.UnixMilli(), end.UnixMilli(), step.Milliseconds(), lookback(o), err)
		return nil, err
	}
	return &query{q, e, s, text, start.UnixMilli(), end.UnixMilli(), step.Milliseconds(), lookback(o)}, nil
}
func (q *query) Exec(ctx context.Context) *promql.Result {
	result := q.Query.Exec(ctx)
	data, err := q.snapshot(ctx)
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(data)
	id := hex.EncodeToString(digest[:])
	path := filepath.Join(q.owner.out, "datasets", id+".json")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err = os.WriteFile(path, data, 0644); err != nil {
			panic(err)
		}
	}
	record := map[string]any{"fixture": q.owner.fixture, "dataset": id, "query": q.text, "start": q.start, "end": q.end, "step": q.step, "lookback": q.lookback}
	if result.Err != nil {
		record["error"] = result.Err.Error()
	} else {
		record["expected"] = map[string]any{"resultType": result.Value.Type(), "result": result.Value}
	}
	record["warnings"], record["infos"] = result.Warnings.AsStrings(q.text, 0, 0)
	if err := json.NewEncoder(q.owner.cases).Encode(record); err != nil {
		panic(err)
	}
	return result
}
func (q *query) snapshot(ctx context.Context) ([]byte, error) {
	querier, err := q.data.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		return nil, err
	}
	defer querier.Close()
	matcher := labels.MustNewMatcher(labels.MatchRegexp, "__name__", ".*")
	set := querier.Select(ctx, true, nil, matcher)
	series := []any{}
	for set.Next() {
		s := set.At()
		ls := map[string]string{}
		s.Labels().Range(func(l labels.Label) { ls[l.Name] = l.Value })
		points := []any{}
		it := s.Iterator(nil)
		for kind := it.Next(); kind != chunkenc.ValNone; kind = it.Next() {
			switch kind {
			case chunkenc.ValFloat:
				t, v := it.At()
				text := fmt.Sprint(v)
				if value.IsStaleNaN(v) {
					text = "stale"
				}
				points = append(points, map[string]any{"timestamp": t, "start_timestamp": it.AtST(), "value": text})
			case chunkenc.ValHistogram, chunkenc.ValFloatHistogram:
				t, h := it.AtFloatHistogram(nil)
				// Retain all internal fields, including reset hint and custom buckets.
				points = append(points, map[string]any{"timestamp": t, "start_timestamp": it.AtST(), "histogram": map[string]any{"CounterResetHint": h.CounterResetHint, "Schema": h.Schema, "ZeroThreshold": fmt.Sprint(h.ZeroThreshold), "ZeroCount": fmt.Sprint(h.ZeroCount), "Count": fmt.Sprint(h.Count), "Sum": fmt.Sprint(h.Sum), "PositiveSpans": h.PositiveSpans, "NegativeSpans": h.NegativeSpans, "PositiveBuckets": floatStrings(h.PositiveBuckets), "NegativeBuckets": floatStrings(h.NegativeBuckets), "CustomValues": floatStrings(h.CustomValues)}})
			}
		}
		if err := it.Err(); err != nil {
			return nil, err
		}
		series = append(series, map[string]any{"labels": ls, "points": points})
	}
	if err := set.Err(); err != nil {
		return nil, err
	}
	return json.Marshal(series)
}
func TestGenerate(t *testing.T) {
	out := os.Getenv("ORACLE_OUTPUT")
	if out == "" {
		t.Skip("set ORACLE_OUTPUT to explicitly generate reference corpus")
	}
	if err := os.MkdirAll(filepath.Join(out, "datasets"), 0755); err != nil {
		t.Fatal(err)
	}
	cases, err := os.Create(filepath.Join(out, "cases.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer cases.Close()
	e := &engine{base: promqltest.NewTestEngine(t, false, 5*time.Minute, 100000000), out: out, cases: cases}
	source := os.Getenv("PROMETHEUS_SOURCE")
	if source == "" {
		source = "../../.research/prometheus"
	}
	files, err := filepath.Glob(filepath.Join(source, "promql/promqltest/testdata/*.test"))
	if err != nil || len(files) == 0 {
		t.Fatal("missing upstream test fixtures", err)
	}
	for _, file := range files {
		t.Run(filepath.Base(file), func(t *testing.T) {
			e.fixture = filepath.Base(file)
			data, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			promqltest.RunTestWithStorage(t, string(data), e, func(t testing.TB) storage.Storage {
				return teststorage.New(t, func(o *tsdb.Options) {
					o.EnableSTStorage = true
					o.FloatChunkEncoding = chunkenc.EncXOR2
					o.EnableHistogramSTEncoding = true
				})
			})
		})
	}
}

func lookback(o promql.QueryOpts) int64 {
	if o == nil || o.LookbackDelta() == 0 {
		return 300000
	}
	return o.LookbackDelta().Milliseconds()
}

func floatStrings(values []float64) []string {
	out := make([]string, len(values))
	for i, v := range values {
		out[i] = fmt.Sprint(v)
	}
	return out
}

func (e *engine) recordCreation(ctx context.Context, s storage.Queryable, text string, start, end, step, lookback int64, creationErr error) {
	q := &query{owner: e, data: s}
	data, err := q.snapshot(ctx)
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(data)
	id := hex.EncodeToString(digest[:])
	if err = os.WriteFile(filepath.Join(e.out, "datasets", id+".json"), data, 0644); err != nil {
		panic(err)
	}
	record := map[string]any{"fixture": e.fixture, "dataset": id, "query": text, "start": start, "end": end, "step": step, "lookback": lookback, "error": creationErr.Error(), "creation_error": true, "warnings": []string{}, "infos": []string{}}
	if err = json.NewEncoder(e.cases).Encode(record); err != nil {
		panic(err)
	}
}
