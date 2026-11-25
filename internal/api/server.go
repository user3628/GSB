package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"driftengine/internal/engine"
	"driftengine/internal/errs"
)

type Server struct {
	Engine *engine.Engine
	Mux    *http.ServeMux
}

func NewServer(eng *engine.Engine) *Server {
	s := &Server{Engine: eng, Mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.Mux.HandleFunc("/healthz", s.health)
	s.Mux.HandleFunc("/v1/summary", s.summary)
	s.Mux.HandleFunc("/v1/objects", s.objects)
	s.Mux.HandleFunc("/v1/objects/", s.object)
	s.Mux.HandleFunc("/v1/drift", s.drift)
	s.Mux.HandleFunc("/v1/plan", s.plan)
	s.Mux.HandleFunc("/v1/batches", s.batches)
	s.Mux.HandleFunc("/v1/batches/", s.batchAction)
	s.Mux.HandleFunc("/v1/snapshots", s.snapshotSubmit)
	s.Mux.HandleFunc("/v1/increments", s.incrementSubmit)
	s.Mux.HandleFunc("/v1/rounds", s.runRound)
}

func (s *Server) Handler() http.Handler { return s.Mux }

func (s *Server) ListenAndServe(addr string) error {
	server := &http.Server{Addr: addr, Handler: s.Mux, ReadHeaderTimeout: 5 * time.Second}
	return server.ListenAndServe()
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "version": s.Engine.CurrentVersion()})
}

func (s *Server) summary(w http.ResponseWriter, _ *http.Request) {
	view := s.Engine.View()
	if view == nil {
		writeError(w, errs.New(errs.Internal, "no committed snapshot"), http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, view.Summary)
}

type objectView struct {
	Object any `json:"object"`
	Drift  any `json:"drift"`
	State  any `json:"state"`
}

func (s *Server) objects(w http.ResponseWriter, r *http.Request) {
	view := s.Engine.View()
	if view == nil {
		writeError(w, errs.New(errs.Internal, "no committed snapshot"), http.StatusNotFound)
		return
	}
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	out := []objectView{}
	for _, object := range view.Snapshot.Objects {
		out = append(out, objectView{Object: object, Drift: view.Drift[object.ID], State: view.ObjectStates[object.ID]})
	}
	writeJSON(w, http.StatusOK, map[string]any{"version": view.Version, "objects": out})
}

func (s *Server) object(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/v1/objects/")
	if id == "" || strings.Contains(id, "/") {
		http.NotFound(w, r)
		return
	}
	view := s.Engine.View()
	if view == nil {
		writeError(w, errs.New(errs.Internal, "no committed snapshot"), http.StatusNotFound)
		return
	}
	object, ok := objectByID(view, id)
	if !ok {
		writeError(w, errs.New(errs.SpecInvalid, "object not found", id), http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, objectView{Object: object, Drift: view.Drift[id], State: view.ObjectStates[id]})
}

func (s *Server) drift(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	view := s.Engine.View()
	if view == nil {
		writeError(w, errs.New(errs.Internal, "no committed snapshot"), http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"version": view.Version, "reports": view.Drift})
}

func (s *Server) plan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	view := s.Engine.View()
	if view == nil {
		writeError(w, errs.New(errs.Internal, "no committed snapshot"), http.StatusNotFound)
		return
	}
	status := http.StatusOK
	var apiErr *errs.Error
	if view.Plan != nil && view.Plan.DependencyCycle {
		apiErr = errs.New(errs.DependencyCycle, "dependency cycle detected", map[string]any{"cycle_ids": view.Plan.CycleIDs})
		status = http.StatusUnprocessableEntity
	}
	writeJSON(w, status, map[string]any{"plan": view.Plan, "error": apiErr})
}

func (s *Server) batches(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	view := s.Engine.View()
	if view == nil {
		writeError(w, errs.New(errs.Internal, "no committed snapshot"), http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"version": view.Version, "plan": view.Plan, "states": view.BatchStates})
}

type batchRequest struct {
	RequestHash string `json:"request_hash"`
}

func (s *Server) batchAction(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/batches/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	batchID := parts[0]
	action := "run"
	if len(parts) == 2 {
		action = parts[1]
	}
	if action != "run" && action != "trigger" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var req batchRequest
	if r.ContentLength > 0 {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	result, err := s.Engine.TriggerBatch(r.Context(), batchID, req.RequestHash)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) snapshotSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	data, err := readBody(r)
	if err != nil {
		writeError(w, errs.New(errs.SpecInvalid, err.Error()), http.StatusBadRequest)
		return
	}
	view, err := s.Engine.SubmitSnapshotData(r.Context(), data)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"version": view.Version, "digest": view.Snapshot.Digest, "summary": view.Summary})
}

func (s *Server) incrementSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	data, err := readBody(r)
	if err != nil {
		writeError(w, errs.New(errs.SpecInvalid, err.Error()), http.StatusBadRequest)
		return
	}
	view, err := s.Engine.SubmitIncrementData(r.Context(), data)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"version": view.Version, "base_version": view.BaseVersion, "summary": view.Summary})
}

func (s *Server) runRound(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()
	result, err := s.Engine.RunRound(ctx, engine.RoundOptions{})
	if err != nil {
		status := http.StatusInternalServerError
		if errs.Code(err) == errs.DependencyCycle {
			status = http.StatusUnprocessableEntity
		}
		writeJSON(w, status, map[string]any{"result": result, "error": errs.AsError(err)})
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func objectByID(view *engine.View, id string) (any, bool) {
	for _, object := range view.Snapshot.Objects {
		if object.ID == id {
			return object, true
		}
	}
	return nil, false
}

func readBody(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	return io.ReadAll(r.Body)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(value)
}

func writeError(w http.ResponseWriter, err *errs.Error, status int) {
	writeJSON(w, status, map[string]any{"error": err})
}

func writeEngineError(w http.ResponseWriter, err error) {
	apiErr := errs.AsError(err)
	status := http.StatusInternalServerError
	switch apiErr.Code {
	case errs.SpecInvalid:
		status = http.StatusUnprocessableEntity
	case errs.StaleBase, errs.StaleVersion, errs.ConflictRequest:
		status = http.StatusConflict
	case errs.WriteQueueFull:
		status = http.StatusTooManyRequests
	case errs.WriteTimeout:
		status = http.StatusGatewayTimeout
	case errs.DependencyCycle, errs.RetryExhausted:
		status = http.StatusUnprocessableEntity
	}
	writeJSON(w, status, map[string]any{"error": apiErr})
}

func methodNotAllowed(w http.ResponseWriter) {
	w.Header().Set("Allow", http.MethodGet)
	writeError(w, errs.New("E_METHOD_NOT_ALLOWED", "method not allowed"), http.StatusMethodNotAllowed)
}
