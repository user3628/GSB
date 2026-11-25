package httpapi

import (
	"encoding/json"
	"net/http"

	"gpusched/internal/domain"
)

type envelope struct {
	OK      bool   `json:"ok"`
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
	Data    any    `json:"data,omitempty"`
	Details any    `json:"details,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeOK(w http.ResponseWriter, data any) {
	writeJSON(w, http.StatusOK, envelope{OK: true, Data: data})
}

// statusFor maps domain error codes to HTTP statuses while preserving the
// stable business code in the response body.
func statusFor(code domain.ErrorCode) int {
	switch code {
	case domain.ESpecInvalid, domain.EInvalidTransition, domain.EPolicyVersion:
		return http.StatusBadRequest
	case domain.EQuotaExceeded, domain.EQuotaLeak:
		return http.StatusConflict
	case domain.EExecQueueFull:
		return http.StatusServiceUnavailable
	case domain.EConflictingRequest:
		return http.StatusConflict
	case domain.ENotFound:
		return http.StatusNotFound
	default:
		return http.StatusInternalServerError
	}
}

func writeErr(w http.ResponseWriter, err error) {
	if be, ok := domain.AsBizError(err); ok {
		writeJSON(w, statusFor(be.Code), envelope{
			OK: false, Code: string(be.Code), Message: be.Message, Details: be.Details,
		})
		return
	}
	writeJSON(w, http.StatusInternalServerError, envelope{
		OK: false, Code: string(domain.EInternal), Message: err.Error(),
	})
}
