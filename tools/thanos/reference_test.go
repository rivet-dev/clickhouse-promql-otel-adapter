// Copied into the pinned Thanos checkout only when regenerating reference data.
package dedup

import (
	"encoding/json"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"math/rand"
	"os"
	"testing"
)

func TestAdapterReference(t *testing.T) {
	out := os.Getenv("THANOS_REFERENCE_OUTPUT")
	if out == "" {
		t.Skip("set THANOS_REFERENCE_OUTPUT")
	}
	rng := rand.New(rand.NewSource(3817))
	cases := []map[string]any{}
	for n := 0; n < 400; n++ {
		replicas := [][]sample{}
		input := [][][2]float64{}
		for r := 0; r < 2+n%3; r++ {
			ss := []sample{}
			values := [][2]float64{}
			value := float64(r * 20)
			for i := 0; i < 40; i++ {
				if rng.Intn(7) == 0 {
					continue
				}
				if rng.Intn(13) == 0 {
					value = 0
				}
				value += float64(rng.Intn(20))
				ts := int64(i*15000 + r*100 + rng.Intn(80))
				ss = append(ss, sample{t: ts, f: value})
				values = append(values, [2]float64{float64(ts), value})
			}
			replicas = append(replicas, ss)
			input = append(input, values)
		}
		for _, counter := range []bool{false, true} {
			var it adjustableSeriesIterator
			for _, ss := range replicas {
				var next adjustableSeriesIterator
				chunk := chunkenc.NewXORChunk()
				app, err := chunk.Appender()
				if err != nil {
					t.Fatal(err)
				}
				for _, s := range ss {
					app.Append(s.t, s.f)
				}
				if counter {
					next = &counterErrAdjustSeriesIterator{Iterator: chunk.Iterator(nil)}
				} else {
					next = noopAdjustableSeriesIterator{Iterator: chunk.Iterator(nil)}
				}
				if it == nil {
					it = next
				} else {
					it = newDedupSeriesIterator(it, next)
				}
			}
			expected := [][2]float64{}
			for typ := it.Next(); typ != chunkenc.ValNone; typ = it.Next() {
				ts, v := it.At()
				expected = append(expected, [2]float64{float64(ts), v})
			}
			cases = append(cases, map[string]any{"replicas": input, "counter": counter, "expected": expected})
		}
	}
	data, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(out, append(data, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}
