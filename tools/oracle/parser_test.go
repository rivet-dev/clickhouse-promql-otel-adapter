package oracle

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	goparser "go/parser"
	"go/token"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
)

func parserInput(expr ast.Expr) (string, error) {
	switch e := expr.(type) {
	case *ast.BasicLit:
		return strconv.Unquote(e.Value)
	case *ast.BinaryExpr:
		if e.Op != token.ADD {
			return "", fmt.Errorf("unsupported input operator")
		}
		a, err := parserInput(e.X)
		if err != nil {
			return "", err
		}
		b, err := parserInput(e.Y)
		return a + b, err
	case *ast.CallExpr:
		if sel, ok := e.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Sprintf" {
			pattern, err := parserInput(e.Args[0])
			if err != nil {
				return "", err
			}
			var rendered bytes.Buffer
			format.Node(&rendered, token.NewFileSet(), e.Args[1])
			switch rendered.String() {
			case "float64(math.MaxInt64) + 1":
				return fmt.Sprintf(pattern, float64(math.MaxInt64)+1), nil
			case "float64(math.MinInt64) - 1":
				return fmt.Sprintf(pattern, float64(math.MinInt64)-1), nil
			}
		}
		if sel, ok := e.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Repeat" {
			s, err := parserInput(e.Args[0])
			if err != nil {
				return "", err
			}
			n, err := strconv.Atoi(e.Args[1].(*ast.BasicLit).Value)
			return strings.Repeat(s, n), err
		}
	}
	var buffer bytes.Buffer
	format.Node(&buffer, token.NewFileSet(), expr)
	return "", fmt.Errorf("unsupported input expression %s", buffer.String())
}
func TestParserReference(t *testing.T) {
	out := os.Getenv("PARSER_OUTPUT")
	if out == "" {
		t.Skip("set PARSER_OUTPUT to record parser reference")
	}
	source := os.Getenv("PROMETHEUS_SOURCE")
	if source == "" {
		source = "../../.research/prometheus"
	}
	file, err := goparser.ParseFile(token.NewFileSet(), filepath.Join(source, "promql/parser/parse_test.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var inputs *ast.CompositeLit
	ast.Inspect(file, func(n ast.Node) bool {
		if v, ok := n.(*ast.ValueSpec); ok && len(v.Names) == 1 && v.Names[0].Name == "testExpr" {
			inputs = v.Values[0].(*ast.CompositeLit)
			return false
		}
		return true
	})
	if inputs == nil {
		t.Fatal("missing upstream expression cases")
	}
	p := parser.NewParser(parser.Options{EnableExperimentalFunctions: true, ExperimentalDurationExpr: true, EnableExtendedRangeSelectors: true, EnableBinopFillModifiers: true})
	records := []map[string]any{}
	for _, node := range inputs.Elts {
		row := node.(*ast.CompositeLit)
		for _, field := range row.Elts {
			kv := field.(*ast.KeyValueExpr)
			if kv.Key.(*ast.Ident).Name != "input" {
				continue
			}
			input, err := parserInput(kv.Value)
			if err != nil {
				t.Fatal(err)
			}
			expr, err := p.ParseExpr(input)
			record := map[string]any{"input": input, "input_hex": fmt.Sprintf("%x", []byte(input))}
			if err != nil {
				record["error"] = err.Error()
			} else {
				record["formatted"] = expr.String()
				record["type"] = expr.Type()
			}
			records = append(records, record)
		}
	}
	data, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(out, append(data, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}
