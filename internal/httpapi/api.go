package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"hostacl-workbench/internal/apperr"
	"hostacl-workbench/internal/engine"
	"hostacl-workbench/internal/model"
)

type API struct {
	Engine *engine.Engine
}

func New(eng *engine.Engine) http.Handler {
	api := &API{Engine: eng}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", api.health)
	mux.HandleFunc("/v1/state", api.state)
	mux.HandleFunc("/v1/versions", api.versions)
	mux.HandleFunc("/v1/snapshots", api.snapshots)
	mux.HandleFunc("/v1/increments", api.increments)
	mux.HandleFunc("/v1/apply", api.apply)
	mux.HandleFunc("/v1/cascade", api.cascade)
	mux.HandleFunc("/v1/verify", api.verify)
	mux.HandleFunc("/v1/verifications", api.verification)
	mux.HandleFunc("/v1/reads", api.reads)
	mux.HandleFunc("/v1/views", api.views)
	mux.HandleFunc("/v1/recovery", api.recovery)
	mux.HandleFunc("/v1/sample", api.sample)
	return mux
}

func (a *API) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, apperr.New(apperr.SpecInvalid, "method not allowed"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "time": time.Now().UTC().Format(time.RFC3339Nano)})
}

func (a *API) state(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, apperr.New(apperr.SpecInvalid, "method not allowed"))
		return
	}
	head, err := a.Engine.Head(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	policy, err := a.Engine.CurrentPolicy(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	views, err := a.Engine.CurrentViews(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	resp := map[string]any{"head": head, "view_count": len(views), "platform": a.Engine.Platform()}
	if policy != nil {
		resp["subject_count"] = len(policy.Subjects)
		resp["path_count"] = len(policy.Paths)
		resp["generated_at"] = policy.GeneratedAt
	}
	writeJSON(w, http.StatusOK, resp)
}

func (a *API) versions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, apperr.New(apperr.SpecInvalid, "method not allowed"))
		return
	}
	versions, err := a.Engine.Store().ListVersions(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"versions": versions})
}

func (a *API) snapshots(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, apperr.New(apperr.SpecInvalid, "method not allowed"))
		return
	}
	var snapshot model.Policy
	if err := decodeJSON(w, r, &snapshot); err != nil {
		writeError(w, err)
		return
	}
	result, err := a.Engine.SubmitSnapshot(r.Context(), snapshot)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (a *API) increments(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, apperr.New(apperr.SpecInvalid, "method not allowed"))
		return
	}
	var increment model.Increment
	if err := decodeJSON(w, r, &increment); err != nil {
		writeError(w, err)
		return
	}
	result, err := a.Engine.SubmitIncrement(r.Context(), increment)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (a *API) apply(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, apperr.New(apperr.SpecInvalid, "method not allowed"))
		return
	}
	report, tree, err := a.Engine.Rematerialize(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "applied", "tree_path": tree, "report": report})
}

func (a *API) cascade(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, apperr.New(apperr.SpecInvalid, "method not allowed"))
		return
	}
	report, err := a.Engine.RebuildCascade(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

type verifyRequest struct {
	MaxConcurrency int      `json:"max_concurrency"`
	QueueSize      int      `json:"queue_size"`
	BudgetMS       int64    `json:"budget_ms"`
	SubjectIDs     []string `json:"subject_ids"`
	Paths          []string `json:"paths"`
}

func (a *API) verify(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, apperr.New(apperr.SpecInvalid, "method not allowed"))
		return
	}
	var request verifyRequest
	if r.ContentLength != 0 {
		if err := decodeJSON(w, r, &request); err != nil {
			writeError(w, err)
			return
		}
	}
	if request.BudgetMS < 0 || request.MaxConcurrency < 0 || request.QueueSize < 0 {
		writeError(w, apperr.New(apperr.SpecInvalid, "verification limits must be >= 0"))
		return
	}
	report, err := a.Engine.Verify(r.Context(), model.VerificationOptions{
		MaxConcurrency: request.MaxConcurrency, QueueSize: request.QueueSize,
		Budget:     time.Duration(request.BudgetMS) * time.Millisecond,
		SubjectIDs: request.SubjectIDs, Paths: request.Paths,
	})
	if err != nil {
		if report.RunID != "" {
			writeJSON(w, apperr.Status(err), map[string]any{
				"code": codeOf(err), "message": errorMessage(err), "context": errorContext(err), "verification": report,
			})
			return
		}
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

type readRequest struct {
	SubjectID       string `json:"subject_id"`
	Path            string `json:"path"`
	ObservedVersion int64  `json:"observed_version"`
}

func (a *API) reads(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, apperr.New(apperr.SpecInvalid, "method not allowed"))
		return
	}
	var request readRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, err)
		return
	}
	result, err := a.Engine.Read(r.Context(), request.SubjectID, request.Path, request.ObservedVersion)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (a *API) verification(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, apperr.New(apperr.SpecInvalid, "method not allowed"))
		return
	}
	runID := r.URL.Query().Get("run_id")
	if runID == "" {
		writeError(w, apperr.New(apperr.SpecInvalid, "run_id is required"))
		return
	}
	status, err := a.Engine.Store().VerificationRunStatus(r.Context(), runID)
	if err != nil {
		writeError(w, err)
		return
	}
	if status == nil {
		writeError(w, apperr.With(apperr.NotFound, "verification run not found", map[string]any{"run_id": runID}))
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (a *API) views(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, apperr.New(apperr.SpecInvalid, "method not allowed"))
		return
	}
	views, err := a.Engine.CurrentViews(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	subjectID := r.URL.Query().Get("subject_id")
	path := r.URL.Query().Get("path")
	if subjectID != "" || path != "" {
		filtered := views[:0]
		for _, view := range views {
			if subjectID != "" && view.SubjectID != subjectID {
				continue
			}
			if path != "" && view.Path != path {
				continue
			}
			filtered = append(filtered, view)
		}
		views = filtered
	}
	writeJSON(w, http.StatusOK, map[string]any{"views": views})
}

func (a *API) recovery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, apperr.New(apperr.SpecInvalid, "method not allowed"))
		return
	}
	report, err := a.Engine.LatestRecovery(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	if report == nil {
		writeError(w, apperr.New(apperr.NotFound, "no recovery report exists"))
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (a *API) sample(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, apperr.New(apperr.SpecInvalid, "method not allowed"))
		return
	}
	writeJSON(w, http.StatusOK, model.DeterministicSample())
}

func decodeJSON(w http.ResponseWriter, r *http.Request, out any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, 4<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return apperr.With(apperr.SpecInvalid, "request body is not valid JSON for this endpoint", map[string]any{"error": err.Error()})
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return apperr.New(apperr.SpecInvalid, "request body must contain exactly one JSON value")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(value)
}

func writeError(w http.ResponseWriter, err error) {
	status := apperr.Status(err)
	body := map[string]any{
		"code": codeOf(err), "message": errorMessage(err), "context": errorContext(err),
	}
	writeJSON(w, status, body)
}

func codeOf(err error) string {
	var appErr *apperr.Error
	if errors.As(err, &appErr) {
		return string(appErr.Code)
	}
	return string(apperr.Internal)
}

func errorMessage(err error) string {
	var appErr *apperr.Error
	if errors.As(err, &appErr) {
		return appErr.Message
	}
	return "internal error"
}

func errorContext(err error) map[string]any {
	var appErr *apperr.Error
	if errors.As(err, &appErr) {
		return appErr.Context
	}
	return map[string]any{"error": err.Error()}
}

func Serve(ctx context.Context, addr string, handler http.Handler) error {
	server := &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServe() }()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func NormalizeAddr(addr string) string {
	if strings.HasPrefix(addr, ":") {
		return "127.0.0.1" + addr
	}
	return addr
}
