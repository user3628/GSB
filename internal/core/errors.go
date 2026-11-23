package core

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// BizError is the stable error contract exposed by the HTTP API. Context must
// contain facts (identifiers, versions, observations), never guessed state.
type BizError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
}

func (e *BizError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func NewBizError(code, message string, context map[string]any) *BizError {
	return &BizError{Code: code, Message: message, Context: context}
}

func BizErrorOf(err error) *BizError {
	if err == nil {
		return nil
	}
	if e, ok := err.(*BizError); ok {
		return e
	}
	return NewBizError("E_INTERNAL", err.Error(), nil)
}

func ErrorResponse(err error) (int, map[string]any) {
	e := BizErrorOf(err)
	status := http.StatusInternalServerError
	switch e.Code {
	case "E_BAD_REQUEST", "E_INVALID_REQUEST":
		status = http.StatusBadRequest
	case "E_SNAPSHOT_INVALID", "E_ENDPOINT_NOT_ALLOWED", "E_CONFIG_INVALID", "E_PICKUP_POLICY_MISMATCH", "E_NO_CLIENTS":
		status = http.StatusUnprocessableEntity
	case "E_CLIENT_NOT_FOUND", "E_SNAPSHOT_NOT_FOUND", "E_PUSH_NOT_FOUND", "E_RECOVERY_NOT_FOUND", "E_ROUTE_NOT_FOUND":
		status = http.StatusNotFound
	case "E_METHOD_NOT_ALLOWED":
		status = http.StatusMethodNotAllowed
	case "E_STALE_SNAPSHOT", "E_INVALID_TRANSITION", "E_CONFLICTING_PUSH", "E_CONFLICTING_ACK", "E_STALE_ACK", "E_ACK_VERSION_MISMATCH", "E_RESULT_VERSION_MISMATCH", "E_CLIENT_EXISTS":
		status = http.StatusConflict
	case "E_PUSH_QUEUE_FULL":
		status = http.StatusTooManyRequests
	case "E_PUSH_TIMEOUT", "E_PUSH_BUDGET_EXCEEDED":
		status = http.StatusGatewayTimeout
	case "E_PUSH_REFUSED", "E_PUSH_UPSTREAM", "E_CLIENT_RESPONSE_INVALID":
		status = http.StatusBadGateway
	case "E_RESULT_MISMATCH", "E_ACK_REQUIRED":
		status = http.StatusUnprocessableEntity
	}
	return status, map[string]any{"error": e}
}

func MarshalErrorJSON(err error) []byte {
	_, body := ErrorResponse(err)
	b, _ := json.Marshal(body)
	return b
}
