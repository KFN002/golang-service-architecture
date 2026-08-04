package expression

import (
	"math"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// evalAST walks the AST directly — the reference implementation the
// distributed pipeline must agree with. Note: this is NOT code evaluation —
// it folds a closed set of four arithmetic operators over parsed literals;
// no dynamic code paths exist.
func evalAST(n *Node) float64 {
	if n.IsLeaf() {
		return n.Value
	}
	l, r := evalAST(n.Left), evalAST(n.Right)
	switch n.Op {
	case "+":
		return l + r
	case "-":
		return l - r
	case "*":
		return l * r
	case "/":
		return l / r
	}
	panic("unknown op " + n.Op)
}

func TestParseAndEval(t *testing.T) {
	t.Parallel()

	cases := map[string]float64{
		"1+1":               2,
		"2 + 2 * 3":         8,
		"(2 + 2) * 3":       12,
		"10 / 4":            2.5,
		"-5 + 3":            -2,
		"2 * -3":            -6,
		"-(2 + 3)":          -5,
		"3.5 * 2":           7,
		"1 + 2 * 3 - 4 / 2": 5,
		"((1+2)*(3+4))/7":   3,
		"42":                42,
		"2*3*4*5":           120,
		"100 - 10 - 5":      85, // left associativity
		"64 / 8 / 2":        4,  // left associativity
	}
	for raw, want := range cases {
		ast, err := Parse(raw)
		if err != nil {
			t.Errorf("Parse(%q) error: %v", raw, err)
			continue
		}
		if got := evalAST(ast); math.Abs(got-want) > 1e-9 {
			t.Errorf("evalAST(%q) = %v, want %v", raw, got, want)
		}
	}
}

func TestParseErrors(t *testing.T) {
	t.Parallel()

	bad := []string{"", "1 +", "* 2", "(1+2", "1+2)", "1 ++ 2", "abc", "1 + +*", "()"}
	for _, raw := range bad {
		if _, err := Parse(raw); err == nil {
			t.Errorf("Parse(%q) = nil error, want failure", raw)
		}
	}
}

func TestPlan(t *testing.T) {
	t.Parallel()

	t.Run("single literal short-circuits", func(t *testing.T) {
		ast, _ := Parse("42")
		tasks, immediate, err := Plan(uuid.New(), ast)
		if err != nil || tasks != nil || immediate == nil || *immediate != 42 {
			t.Fatalf("Plan(42) = (%v, %v, %v)", tasks, immediate, err)
		}
	})

	t.Run("DAG shape for 2+2*3", func(t *testing.T) {
		ast, _ := Parse("2 + 2 * 3")
		tasks, immediate, err := Plan(uuid.New(), ast)
		if err != nil || immediate != nil {
			t.Fatalf("unexpected: %v %v", immediate, err)
		}
		if len(tasks) != 2 {
			t.Fatalf("len(tasks) = %d, want 2", len(tasks))
		}
		mul, add := tasks[0], tasks[1]
		if mul.Op != "*" || mul.UnmetDeps != 0 || !mul.Ready() {
			t.Errorf("mul task wrong: %+v", mul)
		}
		if add.Op != "+" || add.UnmetDeps != 1 || add.Arg2TaskID == nil || *add.Arg2TaskID != mul.ID {
			t.Errorf("add task wrong: %+v", add)
		}
		if !add.IsRoot || mul.IsRoot {
			t.Error("root flag misplaced")
		}
	})

	t.Run("deep nesting limited", func(t *testing.T) {
		raw := strings.Repeat("1+", 300) + "1"
		ast, err := Parse(raw)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if _, _, err := Plan(uuid.New(), ast); err == nil {
			t.Error("Plan should reject > MaxTasksPerExpr operations")
		}
	})
}
