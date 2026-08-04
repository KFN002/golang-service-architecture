package validator

import (
	"strings"
	"testing"

	"github.com/KFN002/perfect-go-service/pkg/apperrors"
)

func TestValidateExpression(t *testing.T) {
	t.Parallel()

	valid := []string{
		"1+1",
		"2 + 2 * 3",
		"(1 + 2) * (3 - 4) / 5",
		"-5 + 3",
		"2 * -3",
		"3.14 * 2",
		"((((1))))",
	}
	for _, expr := range valid {
		if err := ValidateExpression(expr); err != nil {
			t.Errorf("ValidateExpression(%q) = %v, want nil", expr, err)
		}
	}

	invalid := map[string]string{
		"":                        "empty",
		"   ":                     "whitespace only",
		"1 + a":                   "illegal character",
		"1; DROP TABLE tasks":     "injection attempt",
		"(1 + 2":                  "unbalanced open",
		"1 + 2)":                  "unbalanced close",
		"1 +":                     "trailing operator",
		"+* 2":                    "operator run",
		"()":                      "no numbers",
		strings.Repeat("1+", 300) + "1": "too long",
		strings.Repeat("(", 70) + "1" + strings.Repeat(")", 70): "too deep",
	}
	for expr, name := range invalid {
		err := ValidateExpression(expr)
		if err == nil {
			t.Errorf("%s: ValidateExpression(%q) = nil, want error", name, expr)
			continue
		}
		if apperrors.CodeOf(err) != apperrors.CodeInvalidInput {
			t.Errorf("%s: code = %v, want INVALID_INPUT", name, apperrors.CodeOf(err))
		}
	}
}
