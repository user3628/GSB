package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"elastic-scale/internal/app"
	"elastic-scale/internal/model"
)

type Server struct {
	app *app.App
	mux *http.ServeMux
}

func New(a *app.App) *Server {
	s := &Server{app: a, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.health)
	s.mux.HandleFunc("GET /v1/services", s.listServices)
	s.mux.HandleFunc("PUT /v1/catalog", s.replaceCatalog)
	s.mux.HandleFunc("POST /v1/catalog", s.replaceCatalog)

	s.mux.HandleFunc("POST /v1/snapshots", s.loadSnapshot)
	s.mux.HandleFunc("PUT /v1/snapshots/current", s.loadSnapshot)
	s.mux.HandleFunc("POST /v1/snapshots/replace", s.loadSnapshot)
	s.mux.HandleFunc("GET /v1/snapshots/current", s.currentSnapshot)

	s.mux.HandleFunc("POST /v1/increments", s.applyIncrement)
	s.mux.HandleFunc("POST /v1/rebuilds", s.rebuild)
	s.mux.HandleFunc("GET /v1/rebuilds/latest", s.latestRebuild)
	s.mux.HandleFunc("GET /v1/rebuilds/{id}", s.getRebuild)

	s.mux.HandleFunc("GET /v1/metrics/{service}", s.readMetrics)
	s.mux.HandleFunc("GET /v1/decisions/{service}", s.readDecision)
	s.mux.HandleFunc("GET /v1/impact/{service}", s.readImpact)
	s.mux.HandleFunc("GET /v1/recovery", s.recovery)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	var be *model.BusinessError
	if !errors.As(err, &be) {
		be = model.NewError(model.CodeInternal, err.Error(), nil)
	}
	status := http.StatusInternalServerError
	switch be.Code {
	case model.CodeInvalidRequest:
		status = http.StatusBadRequest
	case model.CodeNotFound:
		status = http.StatusNotFound
	case model.CodeSnapshotInvalid, model.CodeInsufficientSamples, model.CodeModelInvalid:
		status = http.StatusUnprocessableEntity
	case model.CodeStaleBase, model.CodeStaleRead, model.CodeConflictRequest:
		status = http.StatusConflict
	}
	writeJSON(w, status, map[string]any{"error": be})
}

func decode(r *http.Request, dst any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return model.NewError(model.CodeInvalidRequest, "invalid JSON request", map[string]any{"reason": err.Error()})
	}
	return nil
}

func idempotencyKey(r *http.Request, current string) string {
	if current != "" {
		return current
	}
	return r.Header.Get("Idempotency-Key")
}

func observedVersion(r *http.Request) (*int64, error) {
	raw := r.URL.Query().Get("observed_version")
	if raw == "" {
		return nil, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return nil, model.NewError(model.CodeInvalidRequest, "observed_version must be an integer", map[string]any{"value": raw})
	}
	return &v, nil
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.app.Health(r.Context()))
}

func (s *Server) listServices(w http.ResponseWriter, r *http.Request) {
	services, err := s.app.ListServices(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"services": services})
}

func (s *Server) replaceCatalog(w http.ResponseWriter, r *http.Request) {
	var req app.CatalogRequest
	if err := decode(r, &req); err != nil {
		writeError(w, err)
		return
	}
	req.CommitID = idempotencyKey(r, req.CommitID)
	resp, err := s.app.ReplaceCatalog(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) loadSnapshot(w http.ResponseWriter, r *http.Request) {
	var req model.SnapshotRequest
	if err := decode(r, &req); err != nil {
		writeError(w, err)
		return
	}
	req.CommitID = idempotencyKey(r, req.CommitID)
	resp, err := s.app.LoadSnapshot(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) currentSnapshot(w http.ResponseWriter, r *http.Request) {
	snap, err := s.app.CurrentSnapshot(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) applyIncrement(w http.ResponseWriter, r *http.Request) {
	var req model.IncrementRequest
	if err := decode(r, &req); err != nil {
		writeError(w, err)
		return
	}
	req.CommitID = idempotencyKey(r, req.CommitID)
	resp, err := s.app.ApplyIncrement(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) rebuild(w http.ResponseWriter, r *http.Request) {
	var req model.RebuildRequest
	if err := decode(r, &req); err != nil {
		writeError(w, err)
		return
	}
	req.CommitID = idempotencyKey(r, req.CommitID)
	resp, err := s.app.Rebuild(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) latestRebuild(w http.ResponseWriter, r *http.Request) {
	run, err := s.app.LatestRebuild(r.Context())
	if err != nil {
		writeError(w, model.NewError(model.CodeNotFound, "no rebuild run found", nil))
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) getRebuild(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, model.NewError(model.CodeInvalidRequest, "invalid rebuild id", map[string]any{"value": r.PathValue("id")}))
		return
	}
	run, err := s.app.GetRebuild(r.Context(), id)
	if err != nil {
		writeError(w, model.NewError(model.CodeNotFound, "rebuild run not found", map[string]any{"id": id}))
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) readMetrics(w http.ResponseWriter, r *http.Request) {
	observed, err := observedVersion(r)
	if err != nil {
		writeError(w, err)
		return
	}
	resp, err := s.app.ReadMetrics(r.Context(), r.PathValue("service"), observed)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) readDecision(w http.ResponseWriter, r *http.Request) {
	observed, err := observedVersion(r)
	if err != nil {
		writeError(w, err)
		return
	}
	var minSamples int64
	if raw := r.URL.Query().Get("min_samples"); raw != "" {
		minSamples, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || minSamples < 0 {
			writeError(w, model.NewError(model.CodeInvalidRequest, "min_samples must be a non-negative integer", map[string]any{"value": raw}))
			return
		}
	}
	resp, err := s.app.ReadDecision(r.Context(), r.PathValue("service"), observed, minSamples)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) readImpact(w http.ResponseWriter, r *http.Request) {
	observed, err := observedVersion(r)
	if err != nil {
		writeError(w, err)
		return
	}
	resp, err := s.app.ReadImpact(r.Context(), r.PathValue("service"), observed)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) recovery(w http.ResponseWriter, r *http.Request) {
	report, err := s.app.LatestRecovery(r.Context())
	if err != nil {
		writeError(w, model.NewError(model.CodeNotFound, "no recovery report found", nil))
		return
	}
	writeJSON(w, http.StatusOK, report)
}
