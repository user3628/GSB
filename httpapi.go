package walgroup

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// NewHTTPHandler exposes the engine over the local HTTP API documented in
// README.md. It uses only net/http and does not make outbound connections.
func NewHTTPHandler(e *Engine) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":              true,
			"recovery_status": e.RecoveryReport().Status,
		})
	})
	mux.HandleFunc("GET /v1/recovery", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, e.RecoveryReport())
	})
	mux.HandleFunc("GET /v1/segments", func(w http.ResponseWriter, r *http.Request) {
		segs, err := e.Segments(r.Context())
		if err != nil {
			writeAPIError(w, err)
			return
		}
		cp, err := e.CurrentCheckpoint(r.Context())
		if err != nil {
			writeAPIError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"segments": segs, "checkpoint": cp})
	})
	mux.HandleFunc("GET /v1/transactions/{id}", func(w http.ResponseWriter, r *http.Request) {
		txn, err := e.Transaction(r.Context(), r.PathValue("id"))
		if err != nil {
			writeAPIError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, txn)
	})
	mux.HandleFunc("GET /v1/segments/{no}/parse", func(w http.ResponseWriter, r *http.Request) {
		no, err := strconv.ParseUint(r.PathValue("no"), 10, 64)
		if err != nil {
			writeAPIError(w, newError(CodeInvalidArgument, "invalid segment number"))
			return
		}
		parsed, err := e.ParseSegment(r.Context(), no)
		if err != nil {
			writeAPIError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, parsed)
	})
	mux.HandleFunc("POST /v1/transactions", func(w http.ResponseWriter, r *http.Request) {
		var in TransactionInput
		if err := decodeJSON(r, &in); err != nil {
			writeAPIError(w, err)
			return
		}
		txn, err := e.StageTransaction(r.Context(), in)
		if err != nil {
			writeAPIError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, txn)
	})
	mux.HandleFunc("POST /v1/commit-groups", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Transactions   []TransactionInput `json:"transactions,omitempty"`
			TransactionIDs []string           `json:"transaction_ids,omitempty"`
		}
		if err := decodeJSON(r, &req); err != nil {
			writeAPIError(w, err)
			return
		}
		var (
			res GroupResult
			err error
		)
		if len(req.Transactions) != 0 && len(req.TransactionIDs) != 0 {
			writeAPIError(w, newError(CodeInvalidArgument, "supply either transactions or transaction_ids, not both"))
			return
		}
		switch {
		case len(req.Transactions) != 0:
			res, err = e.CommitGroup(r.Context(), req.Transactions)
		case len(req.TransactionIDs) != 0:
			res, err = e.CommitStagedGroup(r.Context(), req.TransactionIDs)
		default:
			err = newError(CodeInvalidArgument, "transactions or transaction_ids is required")
		}
		if err != nil {
			writeAPIError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	})
	mux.HandleFunc("POST /v1/checkpoints", func(w http.ResponseWriter, r *http.Request) {
		var req CheckpointRequest
		if err := decodeJSON(r, &req); err != nil {
			writeAPIError(w, err)
			return
		}
		cp, err := e.AdvanceCheckpoint(r.Context(), req)
		if err != nil {
			writeAPIError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, cp)
	})
	mux.HandleFunc("GET /v1/checkpoints/current", func(w http.ResponseWriter, r *http.Request) {
		cp, err := e.CurrentCheckpoint(r.Context())
		if err != nil {
			writeAPIError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"checkpoint": cp})
	})
	mux.HandleFunc("POST /v1/reclaim", func(w http.ResponseWriter, r *http.Request) {
		var req ReclaimRequest
		if err := decodeJSON(r, &req); err != nil {
			writeAPIError(w, err)
			return
		}
		res, err := e.ReclaimSegments(r.Context(), req)
		if err != nil {
			writeAPIError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	})
	mux.HandleFunc("POST /v1/segments/{no}/seal", func(w http.ResponseWriter, r *http.Request) {
		no, err := strconv.ParseUint(r.PathValue("no"), 10, 64)
		if err != nil {
			writeAPIError(w, newError(CodeInvalidArgument, "invalid segment number"))
			return
		}
		seg, err := e.TransitionSegment(r.Context(), no, SegmentSealed)
		if err != nil {
			writeAPIError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, seg)
	})
	mux.HandleFunc("GET /v1/pages/{page}", func(w http.ResponseWriter, r *http.Request) {
		pageNo, err := strconv.ParseInt(r.PathValue("page"), 10, 64)
		if err != nil {
			writeAPIError(w, newError(CodeInvalidArgument, "invalid page number"))
			return
		}
		lsnText := r.URL.Query().Get("lsn")
		if lsnText == "" {
			writeAPIError(w, newError(CodeInvalidArgument, "lsn query parameter is required"))
			return
		}
		lsn, err := strconv.ParseUint(lsnText, 10, 64)
		if err != nil {
			writeAPIError(w, newError(CodeInvalidArgument, "invalid lsn query parameter"))
			return
		}
		page, err := e.ReadPage(r.Context(), pageNo, lsn)
		if err != nil {
			writeAPIError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, page)
	})
	mux.HandleFunc("POST /v1/replay", func(w http.ResponseWriter, r *http.Request) {
		res, err := e.ReplayFromCheckpoint(r.Context())
		if err != nil {
			writeAPIError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	})
	mux.HandleFunc("GET /v1/min-serviceable-lsn", func(w http.ResponseWriter, r *http.Request) {
		lsn, err := e.MinServiceableLSN(r.Context())
		if err != nil {
			writeAPIError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]uint64{"min_serviceable_lsn": lsn})
	})
	return mux
}

func decodeJSON(r *http.Request, dst any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(io.LimitReader(r.Body, 16<<20))
	dec.UseNumber()
	if err := dec.Decode(dst); err != nil {
		return wrapError(CodeInvalidArgument, err, "decode JSON request")
	}
	var extra any
	if err := dec.Decode(&extra); err == nil {
		return newError(CodeInvalidArgument, "request body must contain exactly one JSON value")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeAPIError(w http.ResponseWriter, err error) {
	var e *Error
	if !errors.As(err, &e) {
		e = &Error{Code: CodeIO, Message: err.Error()}
	}
	status := http.StatusInternalServerError
	switch e.Code {
	case CodeInvalidArgument, CodeConflictingTxn, CodeInvalidTransition:
		status = http.StatusBadRequest
	case CodeNotFound:
		status = http.StatusNotFound
	case CodeSegmentCorrupt, CodeRotateFailed, CodeReclaimUnsafe, CodeReclaimFailed,
		CodeCheckpointInvalid, CodeSnapshotRetired, CodeCheckpointLag, CodeGroupCommitFailed:
		status = http.StatusConflict
	case CodeClosed, CodeRecoveryRequired:
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]any{"error": e})
}

// HealthcheckHandler is a small convenience for demos and tests.
func HealthcheckHandler(e *Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if strings.TrimSpace(e.Dir()) == "" {
			writeAPIError(w, newError(CodeIO, "engine has no data directory"))
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "recovery_status": e.RecoveryReport().Status})
	}
}
