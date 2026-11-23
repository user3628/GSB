package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"localinfer/workbench/internal/domain"
)

func errorStatus(code string) int {
	switch code {
	case domain.CodeSpecInvalid, domain.CodeTraceInvalid, domain.CodeBadRequest:
		return http.StatusBadRequest
	case domain.CodeNotFound:
		return http.StatusNotFound
	case domain.CodeConflicting, domain.CodeInsufficientCap:
		return http.StatusConflict
	case domain.CodeInsufficientSample, domain.CodePrecisionBreach, domain.CodeLatencyLimit, domain.CodeBatchLimit:
		return http.StatusUnprocessableEntity
	default:
		return http.StatusInternalServerError
	}
}

func writeError(w http.ResponseWriter, err error) {
	biz := domain.AsBizError(err)
	if biz == nil {
		biz = domain.NewBizError(domain.CodeInternal, "unknown error", nil)
	}
	ctx := biz.Context
	if ctx == nil {
		ctx = map[string]any{}
	}
	body := map[string]any{
		"code": biz.Code, "message": biz.Message, "context": ctx,
		"error": map[string]any{"code": biz.Code, "message": biz.Message, "context": ctx},
	}
	writeJSON(w, errorStatus(biz.Code), body)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(value)
}

func methodNotAllowed(w http.ResponseWriter, allowed ...string) {
	for _, method := range allowed {
		w.Header().Add("Allow", method)
	}
	writeError(w, domain.NewBizError(domain.CodeBadRequest, "method not allowed", map[string]any{"allowed": allowed}))
}

func decodeJSON(data []byte, target any, code string) error {
	if len(data) == 0 {
		return domain.NewBizError(code, "request body is empty", nil)
	}
	if err := json.Unmarshal(data, target); err != nil {
		return domain.NewBizError(code, "request body is not valid JSON", map[string]any{"reason": err.Error()})
	}
	return nil
}

func isBizCode(err error, code string) bool {
	var biz *domain.BizError
	return errors.As(err, &biz) && biz.Code == code
}
