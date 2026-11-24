package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
)

type HTTPServer struct {
	store       *Store
	allowCrash  bool
	maxBodySize int64
}

func NewHTTPHandler(store *Store, allowCrash bool) http.Handler {
	return &HTTPServer{store: store, allowCrash: allowCrash, maxBodySize: 8 << 20}
}

func (h *HTTPServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if rec := recover(); rec != nil {
			h.writeBiz(w, "", 0, NewBizError(CodeInternal, "panic recovered", http.StatusInternalServerError, map[string]any{"panic": rec}))
		}
	}()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	switch {
	case r.URL.Path == "/healthz" && r.Method == http.MethodGet:
		state, err := h.store.LastState(r.Context())
		if err != nil {
			h.writeStoreError(w, "", 0, err)
			return
		}
		h.writeJSON(w, http.StatusOK, map[string]any{"ok": true, "ledger_version": state.LedgerVersion, "state_digest": state.StateDigest})
	case r.URL.Path == "/v1/snapshots" && r.Method == http.MethodPost:
		var req SnapshotSubmission
		if err := h.decode(r, &req); err != nil {
			h.writeBiz(w, req.RequestID, 0, NewBizError(CodeLedgerInvalid, "invalid snapshot request JSON", http.StatusBadRequest, map[string]any{"error": err.Error()}))
			return
		}
		resp, err := h.store.SubmitSnapshot(r.Context(), req)
		h.finish(w, req.RequestID, resp, err)
	case r.URL.Path == "/v1/events" && r.Method == http.MethodPost:
		var req EventSubmission
		if err := h.decode(r, &req); err != nil {
			h.writeBiz(w, req.RequestID, 0, NewBizError(CodeEventInvalid, "invalid event batch JSON", http.StatusBadRequest, map[string]any{"error": err.Error()}))
			return
		}
		resp, err := h.store.SubmitEvents(r.Context(), req)
		h.finish(w, req.RequestID, resp, err)
	case r.URL.Path == "/v1/settle" && r.Method == http.MethodPost:
		var req SettlementRequest
		if err := h.decode(r, &req); err != nil {
			h.writeBiz(w, req.RequestID, 0, NewBizError(CodeBadRequest, "invalid settlement request JSON", http.StatusBadRequest, map[string]any{"error": err.Error()}))
			return
		}
		resp, err := h.store.Settle(r.Context(), req)
		h.finish(w, req.RequestID, resp, err)
	case r.URL.Path == "/v1/borrow-requests" && r.Method == http.MethodPost:
		var req BorrowRequest
		if err := h.decode(r, &req); err != nil {
			h.writeBiz(w, req.RequestID, 0, NewBizError(CodeBadRequest, "invalid borrow request JSON", http.StatusBadRequest, map[string]any{"error": err.Error()}))
			return
		}
		resp, err := h.store.Borrow(r.Context(), req)
		h.finish(w, req.RequestID, resp, err)
	case r.URL.Path == "/v1/state" && r.Method == http.MethodGet:
		var version *int64
		if v := r.URL.Query().Get("version"); v != "" {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n < 0 {
				h.writeBiz(w, "", 0, Biz(CodeBadRequest, "version must be a non-negative integer", map[string]any{"version": v}))
				return
			}
			version = &n
		}
		state, err := h.store.GetState(r.Context(), version)
		if err != nil {
			h.writeStoreError(w, "", 0, err)
			return
		}
		h.writeJSON(w, http.StatusOK, APIResponse{OK: true, LedgerVersion: state.LedgerVersion, Data: state})
	case strings.HasPrefix(r.URL.Path, "/v1/settlements/") && r.Method == http.MethodGet:
		requestID := strings.TrimPrefix(r.URL.Path, "/v1/settlements/")
		if requestID == "" || strings.Contains(requestID, "/") {
			h.writeBiz(w, "", 0, Biz(CodeNotFound, "settlement report not found", nil))
			return
		}
		report, err := h.store.GetSettlement(r.Context(), requestID)
		if err != nil {
			h.writeStoreError(w, requestID, 0, err)
			return
		}
		h.writeJSON(w, http.StatusOK, APIResponse{OK: true, RequestID: requestID, LedgerVersion: report.LedgerVersion, Data: report})
	case r.URL.Path == "/v1/recovery" && r.Method == http.MethodGet:
		report, err := h.store.LatestRecovery(r.Context())
		if err != nil {
			h.writeStoreError(w, "", 0, err)
			return
		}
		h.writeJSON(w, http.StatusOK, APIResponse{OK: true, LedgerVersion: report.LastCompleteVersion, Data: report})
	case r.URL.Path == "/v1/admin/crash-during-settle" && r.Method == http.MethodPost:
		if !h.allowCrash {
			h.writeBiz(w, "", 0, Biz(CodeNotFound, "crash injection endpoint is disabled", nil))
			return
		}
		var req CrashRequest
		if err := h.decode(r, &req); err != nil {
			h.writeBiz(w, req.RequestID, 0, NewBizError(CodeBadRequest, "invalid crash request JSON", http.StatusBadRequest, map[string]any{"error": err.Error()}))
			return
		}
		if err := h.store.ForceCrashDuringSettlement(r.Context(), req); err != nil {
			h.writeStoreError(w, req.RequestID, 0, err)
		}
		return
	case r.URL.Path == "/v1/state" || strings.HasPrefix(r.URL.Path, "/v1/"):
		h.writeBiz(w, "", 0, NewBizError(CodeNotFound, "route not found or method not allowed", http.StatusNotFound, map[string]any{"path": r.URL.Path, "method": r.Method}))
	default:
		h.writeBiz(w, "", 0, NewBizError(CodeNotFound, "route not found", http.StatusNotFound, map[string]any{"path": r.URL.Path, "method": r.Method}))
	}
}

func (h *HTTPServer) decode(r *http.Request, dst any) error {
	if r.Body == nil {
		return errors.New("request body is required")
	}
	defer r.Body.Close()
	limited := io.LimitReader(r.Body, h.maxBodySize+1)
	dec := json.NewDecoder(limited)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("exactly one JSON object is required")
		}
		return err
	}
	return nil
}

func (h *HTTPServer) finish(w http.ResponseWriter, requestID string, resp StoredResponse, err error) {
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		h.writeStoreError(w, requestID, 0, err)
		return
	}
	w.WriteHeader(resp.Status)
	_, _ = w.Write(resp.Body)
}

func (h *HTTPServer) writeStoreError(w http.ResponseWriter, requestID string, version int64, err error) {
	be := AsBiz(err)
	h.writeBiz(w, requestID, version, be)
}

func (h *HTTPServer) writeBiz(w http.ResponseWriter, requestID string, version int64, be *BizError) {
	status := be.HTTPStatus
	if status == 0 {
		status = statusForBizCode(be.Code)
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(APIResponse{OK: false, RequestID: requestID, LedgerVersion: version, Error: &ErrorPayload{Code: be.Code, Message: be.Message, Context: be.Context}})
}

func (h *HTTPServer) writeJSON(w http.ResponseWriter, status int, v any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
