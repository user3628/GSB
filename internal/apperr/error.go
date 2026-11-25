package apperr

import (
	"encoding/json"
	"fmt"
)

const (
	CodeInput           = "E_INPUT"
	CodeNumeric         = "E_NUMERIC"
	CodeDecodeMismatch  = "E_DECODE_MISMATCH"
	CodeLatencyBudget   = "E_LATENCY_BUDGET"
	CodeStaleCheckpoint = "E_STALE_CHECKPOINT"
	CodePrereqMissing   = "E_PREREQ_MISSING"
	CodeEmptyQuery      = "E_EMPTY_QUERY"
	CodeNoMatch         = "E_NO_MATCH"
	CodeZeroWeight      = "E_ZERO_WEIGHT"
)

type Error struct {
	Code           string `json:"code"`
	Message        string `json:"message"`
	QueryID        string `json:"query_id,omitempty"`
	Term           string `json:"term,omitempty"`
	DocumentID     int64  `json:"doc_id,omitempty"`
	Stage          string `json:"stage,omitempty"`
	FirstDiffBlock int    `json:"first_diff_block,omitempty"`
	ElapsedMS      int64  `json:"elapsed_ms,omitempty"`
	ExpectedSeq    int64  `json:"expected_seq,omitempty"`
	ActualSeq      int64  `json:"actual_seq,omitempty"`
	Predecessor    string `json:"predecessor,omitempty"`
	Err            error  `json:"-"`
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error { return e.Err }

func (e *Error) JSONBytes() []byte {
	b, _ := json.Marshal(e)
	return b
}

func E(code, message string, err error) *Error {
	return &Error{Code: code, Message: message, Err: err}
}

func As(err error) *Error {
	if err == nil {
		return nil
	}
	if e, ok := err.(*Error); ok {
		return e
	}
	return &Error{Code: CodeInput, Message: err.Error(), Err: err}
}

func IsCode(err error, code string) bool {
	e, ok := err.(*Error)
	return ok && e.Code == code
}
