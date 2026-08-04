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

	depth, maxDepth := 0, 0
	prev := rune(0)
	digits := false

	for _, r := range trimmed {
		switch {
		case r >= '0' && r <= '9', r == '.':
			digits = true
		case r == '(':
			depth++
			if depth > maxDepth {
				maxDepth = depth
			}
		case r == ')':
			depth--
			if depth < 0 {
				return apperrors.New(apperrors.CodeInvalidInput, "unbalanced parentheses")
			}
		case r == '+' || r == '-' || r == '*' || r == '/':
			if (prev == '+' || prev == '*' || prev == '/') && r != '-' {
				return apperrors.Newf(apperrors.CodeInvalidInput,
					"operator %q cannot follow %q", r, prev)
			}
		case r == ' ' || r == '\t':
			// allowed whitespace
		default:
			return apperrors.Newf(apperrors.CodeInvalidInput, "illegal character %q", r)
		}
		if r != ' ' && r != '\t' {
			prev = r
		}
	}

	if depth != 0 {
		return apperrors.New(apperrors.CodeInvalidInput, "unbalanced parentheses")
	}
	if maxDepth > constants.MaxParenDepth {
		return apperrors.Newf(apperrors.CodeInvalidInput,
			"nesting deeper than %d", constants.MaxParenDepth)
	}
	if !digits {
		return apperrors.New(apperrors.CodeInvalidInput, "expression contains no numbers")
	}
	switch prev {
	case '+', '-', '*', '/':
		return apperrors.New(apperrors.CodeInvalidInput, "expression ends with an operator")
	}
	return nil
}
