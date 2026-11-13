package model

import (
	"errors"
	"fmt"
	"net/http"
)

// Domain sentinel errors mapped to HTTP problem+json responses.
var (
	ErrNotFound = errors.New("resource not found")
	ErrConflict = errors.New("resource conflict")
)

// FieldError describes a single validation or request error item inside ProblemDetail.
type FieldError struct {
	Field   string `json:"field,omitempty"`
	Message string `json:"message"`
	Code    string `json:"code,omitempty"`
}

// ValidationError carries structured field-level validation failures.
type ValidationError struct {
	Detail string
	Errors []FieldError
}

func (e *ValidationError) Error() string {
	if e.Detail != "" {
		return e.Detail
	}
	if len(e.Errors) > 0 {
		return fmt.Sprintf("validation failed: %s", e.Errors[0].Message)
	}
	return "validation failed"
}

// ConflictError carries a conflict reason and optional field details.
type ConflictError struct {
	Detail string
	Field  string
}

func (e *ConflictError) Error() string {
	if e.Detail != "" {
		return e.Detail
	}
	return ErrConflict.Error()
}

func (e *ConflictError) Unwrap() error {
	return ErrConflict
}

// ProblemDetail represents an RFC 7807 / RFC 9457 problem+json response body.
type ProblemDetail struct {
	Type     string       `json:"type"`
	Title    string       `json:"title"`
	Status   int          `json:"status"`
	Detail   string       `json:"detail,omitempty"`
	Instance string       `json:"instance,omitempty"`
	Errors   []FieldError `json:"errors"`
}

// NewProblemDetail builds a ProblemDetail ensuring Errors is always a non-nil slice.
func NewProblemDetail(status int, problemType, title, detail, instance string, errs []FieldError) ProblemDetail {
	if title == "" {
		title = http.StatusText(status)
	}
	if errs == nil {
		if detail != "" {
			errs = []FieldError{{Message: detail}}
		} else {
			errs = []FieldError{}
		}
	}
	return ProblemDetail{
		Type:     problemType,
		Title:    title,
		Status:   status,
		Detail:   detail,
		Instance: instance,
		Errors:   errs,
	}
}
