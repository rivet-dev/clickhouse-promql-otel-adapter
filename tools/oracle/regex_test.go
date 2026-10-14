package oracle

import (
	"encoding/json"
	"github.com/prometheus/prometheus/model/labels"
	"os"
	"sort"
	"strings"
	"testing"
	"unicode"
)

func TestRegexReference(t *testing.T) {
	out := os.Getenv("REGEX_OUTPUT")
	if out == "" {
		t.Skip("set REGEX_OUTPUT")
	}
	if path := os.Getenv("UNICODE_PROPERTIES_OUTPUT"); path != "" {
		names := []string{"Any"}
		for name := range unicode.Categories {
			names = append(names, name)
		}
		for name := range unicode.Scripts {
			names = append(names, name)
		}
		sort.Strings(names)
		if err := os.WriteFile(path, []byte(strings.Join(names, "\n")+"\n"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	patterns := []string{`[]a&&b]`, `[^]a&&b]`, `a{01}`, `a{1,02}`, `[a[:digit:]&&b]`, `.*`, `.+`, `\d+`, `\D+`, `\w+`, `\W+`, `\s+`, `\S+`, `\babc\b`, `\Babc\B`, `[a&&b]`, `[a~~b]`, `[a--b]`, `[a[b]`, `[[:alpha:]]`, `[[:digit:]]`, `\Q[a].*\E`, `\Qopen`, `\123`, `\12`, `\0`, `\00`, `\1`, `\u0061`, `\pL`, `\p{Greek}`, `\p{Script=Greek}`, `\p{Any}`, `\p{ASCII}`, `(?x)a b`, `(?u)a`, `(?-u)a`, `(?R)a`, `(?s:.)`, `(?-s:.)`, `(?m:^abc$)`, `(?i:abc)`, `a{1001}`, `a{1,1001}`, `a{1000}`, `a{bad}`, `a{,2}`, `(?P<x>a)(?P<x>b)`, `(?P<x>a)|(?P<x>b)`, `(a{100}){100}`, `[\D]`, `[\W]`, `[\S]`, `[a\-b]`, `[]a]`, `[^]a]`}
	values := []string{"a{01}", "a{1,02}", "]", "", "a", "b", "ab", "abc", "ABC", "a\nb", "\n", " ", "\v", "\t", "٣", "世", "Σ", "_", "123", "&", "~", "[", "S", "\x00", "[a].*", "open", "aaaaaaaaaa"}
	cases := []map[string]any{}
	for _, p := range patterns {
		m, e := labels.NewMatcher(labels.MatchRegexp, "l", p)
		if e != nil {
			cases = append(cases, map[string]any{"pattern": p, "error": true})
			continue
		}
		for _, v := range values {
			cases = append(cases, map[string]any{"pattern": p, "value": v, "matches": m.Matches(v)})
		}
	}
	data, e := json.Marshal(cases)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(out, append(data, '\n'), 0644); e != nil {
		t.Fatal(e)
	}
}
