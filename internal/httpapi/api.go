package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"checkpointengine/internal/engine"
	"checkpointengine/internal/model"
)

type API struct {
	Engine *engine.Engine
	Log    *slog.Logger
}

func New(eng *engine.Engine, logger *slog.Logger) *API {
	if logger == nil {
		logger = slog.Default()
	}
	return &API{Engine: eng, Log: logger}
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", a.health)
	mux.HandleFunc("GET /", a.index)
	mux.HandleFunc("POST /api/v1/snapshots", a.submitSnapshot)
	mux.HandleFunc("GET /api/v1/snapshots", a.listSnapshots)
	mux.HandleFunc("GET /api/v1/policy", a.currentPolicy)
	mux.HandleFunc("POST /api/v1/checkpoints", a.submitCheckpoint)
	mux.HandleFunc("POST /api/v1/increments", a.submitCheckpoint)
	mux.HandleFunc("GET /api/v1/checkpoints/{id}", a.getSubmission)
	mux.HandleFunc("GET /api/v1/submissions/{id}", a.getSubmission)
	mux.HandleFunc("GET /api/v1/state", a.state)
	mux.HandleFunc("GET /api/v1/shards", a.shards)
	mux.HandleFunc("GET /api/v1/pools", a.pools)
	mux.HandleFunc("GET /api/v1/rounds", a.rounds)
	mux.HandleFunc("GET /api/v1/derivatives", a.derivatives)
	mux.HandleFunc("POST /api/v1/precision/verify", a.verifyPrecision)
	mux.HandleFunc("GET /api/v1/precision", a.precisionResults)
	mux.HandleFunc("GET /api/v1/write-queue", a.writeQueue)
	mux.HandleFunc("POST /api/v1/schedule", a.schedulePending)
	mux.HandleFunc("GET /api/v1/write-records", a.writeRecords)
	mux.HandleFunc("GET /api/v1/intents", a.intents)
	mux.HandleFunc("GET /api/v1/recovery-report", a.recoveryReport)
	mux.HandleFunc("POST /api/v1/recovery/scan", a.recoveryScan)
	return a.recoverPanics(a.logRequests(mux))
}

func (a *API) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		writeError(w, model.NewAPIError(model.CodeNotFound, "route not found"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"service": "single-host mixed-precision checkpoint engine",
		"version": "1.0",
		"endpoints": []string{
			"POST /api/v1/snapshots", "POST /api/v1/checkpoints", "GET /api/v1/state",
			"GET /api/v1/shards", "GET /api/v1/pools", "POST /api/v1/precision/verify",
			"GET /api/v1/write-queue", "GET /api/v1/recovery-report",
		},
	})
}

func (a *API) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "time": time.Now().UTC().Format(time.RFC3339Nano)})
}

func (a *API) submitSnapshot(w http.ResponseWriter, r *http.Request) {
	var req model.SnapshotRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, err)
		return
	}
	snap, err := a.Engine.SubmitSnapshot(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, snap)
}

func (a *API) listSnapshots(w http.ResponseWriter, r *http.Request) {
	items, err := a.Engine.Snapshots(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"snapshots": items})
}

func (a *API) currentPolicy(w http.ResponseWriter, r *http.Request) {
	policy, revision, err := a.Engine.CurrentPolicy(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	if policy == nil {
		writeError(w, model.NewAPIError(model.CodeNoPolicy, "no policy snapshot has been committed"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"revision": revision, "policy": policy})
}

func (a *API) submitCheckpoint(w http.ResponseWriter, r *http.Request) {
	var req model.IncrementRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, err)
		return
	}
	response, err := a.Engine.SubmitIncrement(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	status := http.StatusCreated
	if response.IdempotentReplay {
		status = http.StatusOK
	}
	writeJSON(w, status, response)
}

func (a *API) getSubmission(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	response, err := a.Engine.Submission(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (a *API) state(w http.ResponseWriter, r *http.Request) {
	view, err := a.Engine.State(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (a *API) shards(w http.ResponseWriter, r *http.Request) {
	round, err := intQuery(r, "round")
	if err != nil {
		writeError(w, err)
		return
	}
	result, err := a.Engine.ListShards(r.Context(), round, r.URL.Query().Get("pool"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (a *API) pools(w http.ResponseWriter, r *http.Request) {
	pools, revision, err := a.Engine.Pools(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"revision": revision, "pools": pools})
}

func (a *API) rounds(w http.ResponseWriter, r *http.Request) {
	round, err := intQuery(r, "round")
	if err != nil {
		writeError(w, err)
		return
	}
	items, err := a.Engine.RoundSummaries(r.Context(), round)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rounds": items})
}

func (a *API) derivatives(w http.ResponseWriter, r *http.Request) {
	round, err := intQuery(r, "round")
	if err != nil {
		writeError(w, err)
		return
	}
	items, err := a.Engine.Derivatives(r.Context(), round)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"derivatives": items})
}

func (a *API) verifyPrecision(w http.ResponseWriter, r *http.Request) {
	var req model.VerifyPrecisionRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, err)
		return
	}
	report, err := a.Engine.VerifyPrecision(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (a *API) precisionResults(w http.ResponseWriter, r *http.Request) {
	round, err := intQuery(r, "round")
	if err != nil {
		writeError(w, err)
		return
	}
	results, err := a.Engine.PrecisionResults(r.Context(), round)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

func (a *API) writeQueue(w http.ResponseWriter, r *http.Request) {
	items, err := a.Engine.WriteQueue(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"queue": items, "depth": len(items)})
}

func (a *API) schedulePending(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SubmissionID string `json:"submission_id"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, err)
		return
	}
	response, err := a.Engine.SchedulePending(r.Context(), req.SubmissionID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (a *API) writeRecords(w http.ResponseWriter, r *http.Request) {
	items, err := a.Engine.WriteRecords(r.Context(), strings.TrimSpace(r.URL.Query().Get("submission_id")))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"records": items})
}

func (a *API) intents(w http.ResponseWriter, r *http.Request) {
	pending := r.URL.Query().Get("pending") == "1" || strings.EqualFold(r.URL.Query().Get("pending"), "true")
	items, err := a.Engine.Intents(r.Context(), pending)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"intents": items})
}

func (a *API) recoveryReport(w http.ResponseWriter, r *http.Request) {
	report, err := a.Engine.RecoveryReport(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (a *API) recoveryScan(w http.ResponseWriter, r *http.Request) {
	report, err := a.Engine.Recover(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func decodeJSON(r *http.Request, dst any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(io.LimitReader(r.Body, 64<<20))
	if err := dec.Decode(dst); err != nil {
		return model.WrapAPIError(model.CodeBadRequest, "invalid JSON request body", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return model.WrapAPIError(model.CodeBadRequest, "request must contain exactly one JSON object", err)
	}
	return nil
}

func intQuery(r *http.Request, name string) (int, error) {
	text := strings.TrimSpace(r.URL.Query().Get(name))
	if text == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(text)
	if err != nil || n < 0 {
		return 0, model.WithDetails(model.NewAPIError(model.CodeBadRequest, fmt.Sprintf("%s must be a non-negative integer", name)), map[string]any{name: text})
	}
	return n, nil
}

func writeError(w http.ResponseWriter, err error) {
	apiErr := model.AsAPIError(err)
	if apiErr.Status == 0 {
		apiErr.Status = http.StatusInternalServerError
	}
	writeJSON(w, apiErr.Status, model.ErrorEnvelope{Error: apiErr})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (a *API) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		a.Log.Info("http_request", "method", r.Method, "path", r.URL.Path,
			"status", rec.status, "duration_ms", time.Since(start).Milliseconds())
	})
}

func (a *API) recoverPanics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				a.Log.Error("http_panic", "path", r.URL.Path, "panic", recovered)
				writeError(w, model.NewAPIError(model.CodeInternal, "unexpected server panic"))
			}
		}()
		next.ServeHTTP(w, r)
	})
}
