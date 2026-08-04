// Package validator holds the expression validation utilities.
//
// Validation is a hard security boundary: everything submitted from the
// outside passes through here before touching the parser. Rules are strict
// allow-list based — anything not explicitly permitted is rejected.
package validator

import (
	"strings"

	"github.com/KFN002/perfect-go-service/pkg/apperrors"
	"github.com/KFN002/perfect-go-service/pkg/constants"
)

// ValidateExpression checks raw for syntactic plausibility and abuse limits.
// It does NOT parse — the parser performs the authoritative grammar check.
// This layer exists to reject garbage cheaply before any allocation-heavy work.
func ValidateExpression(raw string) error {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return apperrors.New(apperrors.CodeInvalidInput, "expression is empty")
	}
	if len(trimmed) > constants.MaxExpressionLength {
		return apperrors.Newf(apperrors.CodeInvalidInput,
			"expression exceeds %d characters", constants.MaxExpressionLength)
	}

	scan := &exprScan{}
	for _, r := range trimmed {
		if err := scan.check(r); err != nil {
			return err
		}
	}
	return scan.finish()
}

// exprScan accumulates the character-walk state of one validation.
type exprScan struct {
	depth, maxDepth int
	prev            rune
	digits          bool
}

func (s *exprScan) check(r rune) error {
	switch {
	case r >= '0' && r <= '9', r == '.':
		s.digits = true
	case r == '(':
		s.depth++
		s.maxDepth = max(s.maxDepth, s.depth)
	case r == ')':
		s.depth--
		if s.depth < 0 {
			return apperrors.New(apperrors.CodeInvalidInput, "unbalanced parentheses")
		}
	case r == '+' || r == '-' || r == '*' || r == '/':
		if err := s.checkOperator(r); err != nil {
			return err
		}
	case r == ' ' || r == '\t':
		return nil // allowed whitespace; do not update prev
	default:
		return apperrors.Newf(apperrors.CodeInvalidInput, "illegal character %q", r)
	}
	s.prev = r
	return nil
}

func (s *exprScan) finish() error {
	switch {
	case s.depth != 0:
		return apperrors.New(apperrors.CodeInvalidInput, "unbalanced parentheses")
	case s.maxDepth > constants.MaxParenDepth:
		return apperrors.Newf(apperrors.CodeInvalidInput,
			"nesting deeper than %d", constants.MaxParenDepth)
	case !s.digits:
		return apperrors.New(apperrors.CodeInvalidInput, "expression contains no numbers")
	case s.prev == '+' || s.prev == '-' || s.prev == '*' || s.prev == '/':
		return apperrors.New(apperrors.CodeInvalidInput, "expression ends with an operator")
	default:
		return nil
	}
}

// checkOperator rejects operator runs; '-' is allowed anywhere (negation).
func (s *exprScan) checkOperator(r rune) error {
	if (s.prev == '+' || s.prev == '*' || s.prev == '/') && r != '-' {
		return apperrors.Newf(apperrors.CodeInvalidInput,
			"operator %q cannot follow %q", r, s.prev)
	}
	return nil
}
