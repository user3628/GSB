package edgeconfig

import (
	"encoding/json"
	"errors"
	"net/http"
)

const (
	CodeInvalidJSON      = "E_INVALID_JSON"
	CodeValidation       = "E_VALIDATION"
	CodeDuplicateNode    = "E_DUPLICATE_NODE"
	CodeInvalidKeyPath   = "E_INVALID_KEY_PATH"
	CodeUndeclaredKey    = "E_UNDECLARED_KEY"
	CodeDependencyCycle  = "E_DEPENDENCY_CYCLE"
	CodeStaleSnapshot    = "E_STALE_SNAPSHOT"
	CodeStaleBase        = "E_STALE_BASE"
	CodeStaleRead        = "E_STALE_READ"
	CodeChecksumMismatch = "E_CHECKSUM_MISMATCH"
	CodeConflictRequest  = "E_CONFLICTING_REQUEST"
	CodeNotFound         = "E_NOT_FOUND"
	CodeProbeTimeout     = "E_PROBE_TIMEOUT"
	CodeProbeRejected    = "E_PROBE_REJECTED"
	CodeProbeFailed      = "E_PROBE_FAILED"
	CodeIncompleteIntent = "E_INCOMPLETE_INTENT"
	CodeInternal         = "E_INTERNAL"
)

type APIError struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	Details    any    `json:"details,omitempty"`
	HTTPStatus int    `json:"-"`
}

func (e *APIError) Error() string {
	if e == nil {
		return ""
	}
	return e.Code + ": " + e.Message
}

func NewAPIError(status int, code, message string, details any) *APIError {
	return &APIError{Code: code, Message: message, Details: details, HTTPStatus: status}
}

func AsAPIError(err error) *APIError {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr
	}
	return NewAPIError(http.StatusInternalServerError, CodeInternal, err.Error(), nil)
}

type errorEnvelope struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

func WriteJSONError(w http.ResponseWriter, err error) {
	apiErr := AsAPIError(err)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	status := apiErr.HTTPStatus
	if status == 0 {
		status = http.StatusInternalServerError
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorEnvelope{Error: errorBody{
		Code:    apiErr.Code,
		Message: apiErr.Message,
		Details: apiErr.Details,
	}})
}
