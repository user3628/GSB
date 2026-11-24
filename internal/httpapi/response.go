package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"local/sqlitepool/internal/domain"
)

type apiResult struct {
	status int
	body   []byte
}

type errorPayload struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context"`
}

type envelope struct {
	OK        bool          `json:"ok"`
	RequestID string        `json:"request_id,omitempty"`
	Data      any           `json:"data,omitempty"`
	Error     *errorPayload `json:"error,omitempty"`
}

func successResult(status int, requestID string, data any) apiResult {
	body, _ := json.Marshal(envelope{OK: true, RequestID: requestID, Data: data})
	return apiResult{status: status, body: body}
}

func faultResult(code, message string, context map[string]any, requestID string) apiResult {
	if context == nil {
		context = map[string]any{}
	}
	context["business_code"] = code
	context["request_id"] = requestID
	body, _ := json.Marshal(envelope{OK: false, RequestID: requestID, Error: &errorPayload{
		Code: code, Message: message, Context: context,
	}})
	return apiResult{status: statusForCode(code), body: body}
}

func resultFromError(err error, requestID string) apiResult {
	var be *domain.BusinessError
	if errors.As(err, &be) {
		return faultResult(be.Code, be.Message, be.Context, requestID)
	}
	return faultResult(domain.CodeInternal, "unexpected internal error", map[string]any{
		"error": err.Error(),
	}, requestID)
}

func statusForCode(code string) int {
	switch code {
	case domain.CodeAcquireTimeout, domain.CodeRequestCancelled:
		return http.StatusRequestTimeout
	case domain.CodePoolExhausted:
		return http.StatusTooManyRequests
	case domain.CodeDialFailed:
		return http.StatusServiceUnavailable
	case domain.CodeQueryTimeout:
		return http.StatusGatewayTimeout
	case domain.CodeConnReset:
		return http.StatusBadGateway
	case domain.CodeConflictingRequest, domain.CodeStaleConfig:
		return http.StatusConflict
	case domain.CodeBadRequest, domain.CodeConfigInvalid:
		return http.StatusBadRequest
	case domain.CodeNotFound:
		return http.StatusNotFound
	case domain.CodeQueryFailed, domain.CodeConnNotLeased, domain.CodeInternal, domain.CodeShutdown:
		return http.StatusInternalServerError
	default:
		return http.StatusInternalServerError
	}
}

func writeResult(w http.ResponseWriter, result apiResult) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(result.status)
	_, _ = w.Write(result.body)
}
