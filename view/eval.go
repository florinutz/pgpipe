package view

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/parser/test_driver"
)

// resolveField looks up a field value from event metadata or payload.
// Bare fields: "channel", "operation", "source" → event metadata.
// Dotted fields: "payload.amount" → payload["amount"].
func resolveField(field string, meta EventMeta, payload map[string]any) any {
	switch field {
	case "channel":
		return meta.Channel
	case "operation":
		return meta.Operation
	case "source":
		return meta.Source
	}

	// payload.X → access payload["X"]
	if strings.HasPrefix(field, "payload.") {
		key := field[len("payload."):]
		return resolveNestedField(payload, key)
	}

	// Try direct lookup in payload for convenience.
	if v, ok := payload[field]; ok {
		return v
	}
	return nil
}

// resolveNestedField walks dot-separated keys into nested maps.
func resolveNestedField(m map[string]any, key string) any {
	parts := strings.Split(key, ".")
	current := any(m)
	for _, p := range parts {
		mm, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current = mm[p]
	}
	return current
}

// exprToValue extracts a literal value from an AST expression.
func exprToValue(expr ast.ExprNode) (any, error) {
	switch e := expr.(type) {
	case *test_driver.ValueExpr:
		return datumToGo(e)
	case *ast.UnaryOperationExpr:
		// Handle negative numbers: -5 is parsed as UnaryMinus(5).
		if e.Op == opcode.Minus {
			val, err := exprToValue(e.V)
			if err != nil {
				return nil, err
			}
			return negateNumber(val)
		}
		return nil, fmt.Errorf("unsupported unary op: %s", e.Op)
	default:
		return nil, fmt.Errorf("unsupported value expression: %T", expr)
	}
}

// datumToGo converts a TiDB datum to a Go value.
func datumToGo(v *test_driver.ValueExpr) (any, error) {
	d := v.Datum
	switch d.Kind() {
	case test_driver.KindInt64:
		return d.GetInt64(), nil
	case test_driver.KindUint64:
		return d.GetUint64(), nil
	case test_driver.KindFloat32:
		return float64(d.GetFloat32()), nil
	case test_driver.KindFloat64:
		return d.GetFloat64(), nil
	case test_driver.KindString:
		return d.GetString(), nil
	case test_driver.KindNull:
		return nil, nil
	default:
		return d.GetString(), nil
	}
}

func negateNumber(v any) (any, error) {
	switch n := v.(type) {
	case int64:
		return -n, nil
	case uint64:
		return -int64(n), nil
	case float64:
		return -n, nil
	default:
		return nil, fmt.Errorf("cannot negate %T", v)
	}
}

// makeComparisonPred creates a predicate that compares a field value against a literal.
func makeComparisonPred(field string, op opcode.Op, literal any) Predicate {
	return func(m EventMeta, p map[string]any) bool {
		val := resolveField(field, m, p)
		cmp := compareValues(val, literal)
		switch op {
		case opcode.EQ:
			return cmp == 0
		case opcode.NE:
			return cmp != 0
		case opcode.GT:
			return cmp > 0
		case opcode.GE:
			return cmp >= 0
		case opcode.LT:
			return cmp < 0
		case opcode.LE:
			return cmp <= 0
		default:
			return false
		}
	}
}

// compareValues compares two values with type coercion.
// Returns -1, 0, or 1. Returns -2 on incompatible types (treated as not-equal).
func compareValues(a, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -2
	}
	if b == nil {
		return -2
	}

	// Try numeric comparison.
	af, aOk := toFloat64(a)
	bf, bOk := toFloat64(b)
	if aOk && bOk {
		switch {
		case af < bf:
			return -1
		case af > bf:
			return 1
		default:
			return 0
		}
	}

	// String comparison.
	as := fmt.Sprint(a)
	bs := fmt.Sprint(b)
	switch {
	case as < bs:
		return -1
	case as > bs:
		return 1
	default:
		return 0
	}
}

// toFloat64 attempts to convert a value to float64.
// Handles numeric types and string-encoded numbers (common in WAL events
// where column values are always string-typed).
func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	case string:
		f, err := strconv.ParseFloat(n, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}
