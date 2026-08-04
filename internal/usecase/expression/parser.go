package expression

import (
	"strconv"
	"strings"

	"github.com/KFN002/perfect-go-service/pkg/apperrors"
)

// Node is one AST node: either a literal Value (leaf) or an operator with two
// children. The parser produces it; the planner consumes it.
type Node struct {
	Op    string  // "" for leaves
	Value float64 // meaningful only for leaves
	Left  *Node
	Right *Node
}

// IsLeaf reports whether the node is a literal.
func (n *Node) IsLeaf() bool { return n.Op == "" }

type tokenKind int

const (
	tokNumber tokenKind = iota
	tokOp
	tokLParen
	tokRParen
)

type token struct {
	kind  tokenKind
	op    string
	value float64
}

// Parse turns raw text into an AST using the shunting-yard algorithm.
// Unary minus is handled by rewriting to (0 - x) at token level.
func Parse(raw string) (*Node, error) {
	tokens, err := tokenize(raw)
	if err != nil {
		return nil, err
	}
	return buildAST(tokens)
}

// opNeg is the internal unary-minus operator. It binds tighter than * and /
// and lowers into a binary (0 - x) node, keeping the task model purely binary.
const opNeg = "neg"

func precedence(op string) int {
	switch op {
	case opNeg:
		return 3
	case "*", "/":
		return 2
	default:
		return 1
	}
}

func tokenize(raw string) ([]token, error) {
	var out []token
	s := strings.TrimSpace(raw)
	i := 0
	expectOperand := true // start of input and after '(' or an operator

	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t':
			i++
		case c >= '0' && c <= '9' || c == '.':
			j := i
			for j < len(s) && (s[j] >= '0' && s[j] <= '9' || s[j] == '.') {
				j++
			}
			v, err := strconv.ParseFloat(s[i:j], 64)
			if err != nil {
				return nil, apperrors.Newf(apperrors.CodeInvalidInput, "bad number %q", s[i:j])
			}
			out = append(out, token{kind: tokNumber, value: v})
			i = j
			expectOperand = false
		case c == '(':
			out = append(out, token{kind: tokLParen})
			i++
			expectOperand = true
		case c == ')':
			out = append(out, token{kind: tokRParen})
			i++
			expectOperand = false
		case c == '+' || c == '-' || c == '*' || c == '/':
			if expectOperand {
				if c != '-' {
					return nil, apperrors.Newf(apperrors.CodeInvalidInput,
						"operator %q where a value was expected", string(c))
				}
				// Unary minus: a dedicated high-precedence operator so that
				// "2 * -3" parses as 2 * (-3), not (2 * 0) - 3.
				out = append(out, token{kind: tokOp, op: opNeg})
			} else {
				out = append(out, token{kind: tokOp, op: string(c)})
			}
			i++
			expectOperand = true
		default:
			return nil, apperrors.Newf(apperrors.CodeInvalidInput, "illegal character %q", string(c))
		}
	}
	if len(out) == 0 {
		return nil, apperrors.New(apperrors.CodeInvalidInput, "expression is empty")
	}
	return out, nil
}

// buildAST is shunting-yard directly into an AST (operand stack of *Node).
func buildAST(tokens []token) (*Node, error) {
	var operands []*Node
	var ops []token // operators and left parens

	popOp := func() error {
		if len(ops) == 0 {
			return apperrors.New(apperrors.CodeInvalidInput, "malformed expression")
		}
		op := ops[len(ops)-1]
		ops = ops[:len(ops)-1]

		if op.op == opNeg {
			// Unary: lower into binary (0 - x) so tasks stay two-argument.
			if len(operands) < 1 {
				return apperrors.New(apperrors.CodeInvalidInput, "malformed expression")
			}
			r := operands[len(operands)-1]
			operands = operands[:len(operands)-1]
			operands = append(operands, &Node{Op: "-", Left: &Node{}, Right: r})
			return nil
		}

		if len(operands) < 2 {
			return apperrors.New(apperrors.CodeInvalidInput, "malformed expression")
		}
		r := operands[len(operands)-1]
		l := operands[len(operands)-2]
		operands = operands[:len(operands)-2]
		operands = append(operands, &Node{Op: op.op, Left: l, Right: r})
		return nil
	}

	// shouldPop implements associativity: binary ops are left-associative
	// (pop on >=); unary neg is right-associative (pop only on >).
	shouldPop := func(top, incoming token) bool {
		if top.kind != tokOp {
			return false
		}
		if incoming.op == opNeg {
			return precedence(top.op) > precedence(incoming.op)
		}
		return precedence(top.op) >= precedence(incoming.op)
	}

	for _, t := range tokens {
		switch t.kind {
		case tokNumber:
			operands = append(operands, &Node{Value: t.value})
		case tokOp:
			for len(ops) > 0 && shouldPop(ops[len(ops)-1], t) {
				if err := popOp(); err != nil {
					return nil, err
				}
			}
			ops = append(ops, t)
		case tokLParen:
			ops = append(ops, t)
		case tokRParen:
			for len(ops) > 0 && ops[len(ops)-1].kind != tokLParen {
				if err := popOp(); err != nil {
					return nil, err
				}
			}
			if len(ops) == 0 {
				return nil, apperrors.New(apperrors.CodeInvalidInput, "unbalanced parentheses")
			}
			ops = ops[:len(ops)-1] // discard '('
		}
	}
	for len(ops) > 0 {
		if ops[len(ops)-1].kind == tokLParen {
			return nil, apperrors.New(apperrors.CodeInvalidInput, "unbalanced parentheses")
		}
		if err := popOp(); err != nil {
			return nil, err
		}
	}
	if len(operands) != 1 {
		return nil, apperrors.New(apperrors.CodeInvalidInput, "malformed expression")
	}
	return operands[0], nil
}
