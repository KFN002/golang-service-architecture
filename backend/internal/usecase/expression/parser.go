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

// Parse turns raw text into an AST using the shunting-yard algorithm.
// Unary minus becomes a high-precedence internal operator that lowers into a
// binary (0 - x) node, keeping the task model purely binary.
func Parse(raw string) (*Node, error) {
	tokens, err := tokenize(raw)
	if err != nil {
		return nil, err
	}
	b := &astBuilder{}
	return b.build(tokens)
}

// opNeg is the internal unary-minus operator. It binds tighter than * and /.
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

// scanner walks the raw expression rune by rune.
type scanner struct {
	src string
	pos int
	out []token
	// expectOperand is true at input start and after '(' or an operator —
	// the position where a '-' means negation, not subtraction.
	expectOperand bool
}

func tokenize(raw string) ([]token, error) {
	s := &scanner{src: strings.TrimSpace(raw), expectOperand: true}
	for s.pos < len(s.src) {
		if err := s.step(); err != nil {
			return nil, err
		}
	}
	if len(s.out) == 0 {
		return nil, apperrors.New(apperrors.CodeInvalidInput, "expression is empty")
	}
	return s.out, nil
}

// step consumes one token (or skips whitespace).
func (s *scanner) step() error {
	switch c := s.src[s.pos]; {
	case c == ' ' || c == '\t':
		s.pos++
		return nil
	case c >= '0' && c <= '9' || c == '.':
		return s.scanNumber()
	case c == '(':
		s.out = append(s.out, token{kind: tokLParen})
		s.pos++
		s.expectOperand = true
		return nil
	case c == ')':
		s.out = append(s.out, token{kind: tokRParen})
		s.pos++
		s.expectOperand = false
		return nil
	case c == '+' || c == '-' || c == '*' || c == '/':
		return s.scanOperator(c)
	default:
		return apperrors.Newf(apperrors.CodeInvalidInput, "illegal character %q", string(c))
	}
}

func (s *scanner) scanNumber() error {
	start := s.pos
	for s.pos < len(s.src) &&
		(s.src[s.pos] >= '0' && s.src[s.pos] <= '9' || s.src[s.pos] == '.') {
		s.pos++
	}
	v, err := strconv.ParseFloat(s.src[start:s.pos], 64)
	if err != nil {
		return apperrors.Newf(apperrors.CodeInvalidInput, "bad number %q", s.src[start:s.pos])
	}
	s.out = append(s.out, token{kind: tokNumber, value: v})
	s.expectOperand = false
	return nil
}

func (s *scanner) scanOperator(c byte) error {
	if s.expectOperand {
		if c != '-' {
			return apperrors.Newf(apperrors.CodeInvalidInput,
				"operator %q where a value was expected", string(c))
		}
		// Unary minus: a dedicated high-precedence operator so that
		// "2 * -3" parses as 2 * (-3), not (2 * 0) - 3.
		s.out = append(s.out, token{kind: tokOp, op: opNeg})
	} else {
		s.out = append(s.out, token{kind: tokOp, op: string(c)})
	}
	s.pos++
	s.expectOperand = true
	return nil
}

// astBuilder is shunting-yard directly into an AST.
type astBuilder struct {
	operands []*Node
	ops      []token // operators and left parens
}

func (b *astBuilder) build(tokens []token) (*Node, error) {
	for _, t := range tokens {
		if err := b.feed(t); err != nil {
			return nil, err
		}
	}
	for len(b.ops) > 0 {
		if b.top().kind == tokLParen {
			return nil, apperrors.New(apperrors.CodeInvalidInput, "unbalanced parentheses")
		}
		if err := b.popOp(); err != nil {
			return nil, err
		}
	}
	if len(b.operands) != 1 {
		return nil, apperrors.New(apperrors.CodeInvalidInput, "malformed expression")
	}
	return b.operands[0], nil
}

func (b *astBuilder) feed(t token) error {
	switch t.kind {
	case tokNumber:
		b.operands = append(b.operands, &Node{Value: t.value})
		return nil
	case tokOp:
		for len(b.ops) > 0 && shouldPop(b.top(), t) {
			if err := b.popOp(); err != nil {
				return err
			}
		}
		b.ops = append(b.ops, t)
		return nil
	case tokLParen:
		b.ops = append(b.ops, t)
		return nil
	default: // tokRParen
		return b.closeParen()
	}
}

func (b *astBuilder) closeParen() error {
	for len(b.ops) > 0 && b.top().kind != tokLParen {
		if err := b.popOp(); err != nil {
			return err
		}
	}
	if len(b.ops) == 0 {
		return apperrors.New(apperrors.CodeInvalidInput, "unbalanced parentheses")
	}
	b.ops = b.ops[:len(b.ops)-1] // discard '('
	return nil
}

func (b *astBuilder) top() token { return b.ops[len(b.ops)-1] }

// popOp folds the top operator over the operand stack.
func (b *astBuilder) popOp() error {
	op := b.ops[len(b.ops)-1]
	b.ops = b.ops[:len(b.ops)-1]

	if op.op == opNeg {
		// Unary: lower into binary (0 - x) so tasks stay two-argument.
		if len(b.operands) < 1 {
			return apperrors.New(apperrors.CodeInvalidInput, "malformed expression")
		}
		r := b.operands[len(b.operands)-1]
		b.operands = b.operands[:len(b.operands)-1]
		b.operands = append(b.operands, &Node{Op: "-", Left: &Node{}, Right: r})
		return nil
	}

	if len(b.operands) < 2 {
		return apperrors.New(apperrors.CodeInvalidInput, "malformed expression")
	}
	r := b.operands[len(b.operands)-1]
	l := b.operands[len(b.operands)-2]
	b.operands = b.operands[:len(b.operands)-2]
	b.operands = append(b.operands, &Node{Op: op.op, Left: l, Right: r})
	return nil
}

// shouldPop implements associativity: binary ops are left-associative
// (pop on >=); unary neg is right-associative (pop only on >).
func shouldPop(top, incoming token) bool {
	if top.kind != tokOp {
		return false
	}
	if incoming.op == opNeg {
		return precedence(top.op) > precedence(incoming.op)
	}
	return precedence(top.op) >= precedence(incoming.op)
}
