// Package apperrors defines the typed error taxonomy of the system.
//
// Every error that crosses a layer boundary is (or wraps) an *Error carrying a
// stable machine-readable Code. Controllers map codes onto transport statuses
// (gRPC codes, HTTP statuses) in exactly one place, so the mapping can never
// drift between transports.
package apperrors

import (
	"errors"
	"fmt"
)

// Code is a stable, machine-readable error class.
type Code string

const (
	CodeInvalidInput  Code = "INVALID_INPUT"   // validation failed; caller's fault
	CodeNotFound      Code = "NOT_FOUND"       // entity does not exist
	CodeConflict      Code = "CONFLICT"        // duplicate / concurrent modification
	CodeUnavailable   Code = "UNAVAILABLE"     // dependency down, breaker open
	CodeRateLimited   Code = "RATE_LIMITED"    // throttled; retry later
	CodeOverloaded    Code = "OVERLOADED"      // load shed; retry later
	CodeDivisionByZero Code = "DIVISION_BY_ZERO" // permanent computation failure
	CodeInternal      Code = "INTERNAL"        // bug or unexpected state
)

// Error is the single error type that crosses layer boundaries.
type Error struct {
	Code    Code
	Message string
	cause   error
}

func (e *Error) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error { return e.cause }

// New creates a fresh typed error.
func New(code Code, msg string) *Error { return &Error{Code: code, Message: msg} }

// Newf creates a fresh typed error with formatting.
func Newf(code Code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// Wrap attaches a typed classification to an underlying cause.
func Wrap(code Code, msg string, cause error) *Error {
	return &Error{Code: code, Message: msg, cause: cause}
}

// CodeOf extracts the Code from any error chain; unknown errors are INTERNAL.
func CodeOf(err error) Code {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return CodeInternal
}

// IsRetryable reports whether the error class is transient — safe to retry.
func IsRetryable(err error) bool {
	switch CodeOf(err) {
	case CodeUnavailable, CodeRateLimited, CodeOverloaded:
		return true
	default:
		return false
	}
}

// Sentinel helpers for the most common classes.
var (
	ErrNotFound = New(CodeNotFound, "not found")
)
