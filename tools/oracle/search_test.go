package oracle

import (
	"encoding/json"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"math/rand"
	"os"
	"testing"
)

func TestSearchScores(t *testing.T) {
	out := os.Getenv("SEARCH_OUTPUT")
	if out == "" {
		t.Skip("set SEARCH_OUTPUT")
	}
	rng := rand.New(rand.NewSource(6723))
	alphabet := []rune("abcde_123Σ世😊")
	cases := []map[string]any{}
	for n := 0; n < 400; n++ {
		word := func(length int) string {
			r := make([]rune, length)
			for i := range r {
				r[i] = alphabet[rng.Intn(len(alphabet))]
			}
			return string(r)
		}
		pattern, text := word(rng.Intn(6)), word(rng.Intn(12))
		if n%4 == 0 {
			text = pattern + text
		}
		if n%4 == 1 {
			text = text + pattern
		}
		for _, alg := range []string{"subsequence", "jarowinkler"} {
			for _, threshold := range []float64{0, .5, 1} {
				var ok bool
				var score float64
				if alg == "subsequence" {
					ok, score = v1.NewSubsequenceFilter(pattern, threshold).Accept(text)
				} else {
					ok, score = v1.NewSubstringFilter(pattern).Accept(text)
					if !ok && threshold > 0 {
						ok, score = v1.NewFuzzyFilter(pattern, threshold).Accept(text)
					}
				}
				cases = append(cases, map[string]any{"query": pattern, "text": text, "algorithm": alg, "threshold": threshold, "accepted": ok, "score": score})
			}
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
