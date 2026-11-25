package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"

	"qosshaper/internal/qos"
)

type Server struct {
	Store *qos.Store
	Log   *log.Logger
}

func New(store *qos.Store, logger *log.Logger) *Server {
	if logger == nil {
		logger = log.Default()
	}
	return &Server{Store: store, Log: logger}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("POST /v1/policy/snapshot", s.submitSnapshot)
	mux.HandleFunc("POST /v1/policy/increment", s.submitIncrement)
	mux.HandleFunc("GET /v1/policy/queues", s.listQueueViews)
	mux.HandleFunc("GET /v1/policy/queues/{queue_id}", s.getQueueView)
	mux.HandleFunc("GET /v1/policy", s.listQueueViews)
	mux.HandleFunc("GET /v1/commits", s.commits)
	mux.HandleFunc("POST /v1/simulations", s.submitSimulation)
	mux.HandleFunc("GET /v1/simulations/{simulation_id}", s.getSimulation)
	mux.HandleFunc("GET /v1/simulations/{simulation_id}/queues/{queue_id}", s.getQueueDiagnostic)
	mux.HandleFunc("GET /v1/diagnostics/queues/{queue_id}", s.latestQueueDiagnostic)
	mux.HandleFunc("GET /v1/recovery/report", s.recoveryReport)
	mux.HandleFunc("GET /v1/sample", s.sample)
	mux.HandleFunc("GET /v1/requests/{request_id}", s.requestStatus)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, qos.NewError(qos.CodeNotFound, "HTTP route not found", http.StatusNotFound, map[string]any{
			"method": r.Method, "path": r.URL.Path,
		}))
	})
	return s.logging(mux)
}

func (s *Server) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
		s.Log.Printf("%s %s", r.Method, r.URL.RequestURI())
	})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	version, err := s.Store.CurrentVersion(r.Context())
	if err != nil {
		writeError(w, qos.NewError(qos.CodeInternal, "cannot read current version", 500, map[string]any{"error": err.Error()}))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"code": qos.CodeOK, "service": "qosshaper", "current_version": version})
}

func (s *Server) submitSnapshot(w http.ResponseWriter, r *http.Request) {
	var req qos.SnapshotRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	resp, biz := s.Store.SubmitSnapshot(r.Context(), req)
	if biz != nil {
		writeError(w, biz)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) submitIncrement(w http.ResponseWriter, r *http.Request) {
	var req qos.IncrementRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	resp, biz := s.Store.SubmitIncrement(r.Context(), req)
	if biz != nil {
		writeError(w, biz)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func requestedVersion(r *http.Request) (int64, bool) {
	raw := r.URL.Query().Get("observe_version")
	if raw == "" {
		raw = r.URL.Query().Get("base_version")
	}
	if raw == "" {
		raw = r.URL.Query().Get("version")
	}
	if raw == "" {
		return 0, true
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v <= 0 {
		return 0, false
	}
	return v, true
}

func (s *Server) getQueueView(w http.ResponseWriter, r *http.Request) {
	version, ok := requestedVersion(r)
	if !ok {
		writeError(w, qos.NewError(qos.CodePolicyInvalid, "observe_version must be a positive integer", 422, map[string]any{
			"observe_version": r.URL.Query().Get("observe_version"),
		}))
		return
	}
	resp, biz := s.Store.GetQueueView(r.Context(), r.PathValue("queue_id"), version)
	if biz != nil {
		writeError(w, biz)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) listQueueViews(w http.ResponseWriter, r *http.Request) {
	version, ok := requestedVersion(r)
	if !ok {
		writeError(w, qos.NewError(qos.CodePolicyInvalid, "observe_version must be a positive integer", 422, nil))
		return
	}
	resp, biz := s.Store.ListQueueViews(r.Context(), version)
	if biz != nil {
		writeError(w, biz)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) commits(w http.ResponseWriter, r *http.Request) {
	commits, err := s.Store.Commits(r.Context())
	if err != nil {
		writeError(w, qos.NewError(qos.CodeInternal, "cannot list commits", 500, map[string]any{"error": err.Error()}))
		return
	}
	version, _ := s.Store.CurrentVersion(r.Context())
	writeJSON(w, http.StatusOK, map[string]any{"code": qos.CodeOK, "current_version": version, "commits": commits})
}

func (s *Server) submitSimulation(w http.ResponseWriter, r *http.Request) {
	var req qos.SimulationRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	resp, biz := s.Store.SubmitSimulation(r.Context(), req)
	if biz != nil {
		writeError(w, biz)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) getSimulation(w http.ResponseWriter, r *http.Request) {
	version, ok := requestedVersion(r)
	if !ok {
		writeError(w, qos.NewError(qos.CodePolicyInvalid, "observe_version must be a positive integer", 422, nil))
		return
	}
	resp, biz := s.Store.GetSimulation(r.Context(), r.PathValue("simulation_id"), version)
	if biz != nil {
		writeError(w, biz)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) getQueueDiagnostic(w http.ResponseWriter, r *http.Request) {
	version, ok := requestedVersion(r)
	if !ok {
		writeError(w, qos.NewError(qos.CodePolicyInvalid, "observe_version must be a positive integer", 422, nil))
		return
	}
	resp, biz := s.Store.GetQueueDiagnostic(r.Context(), r.PathValue("simulation_id"), r.PathValue("queue_id"), version)
	if biz != nil {
		writeError(w, biz)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) latestQueueDiagnostic(w http.ResponseWriter, r *http.Request) {
	version, ok := requestedVersion(r)
	if !ok {
		writeError(w, qos.NewError(qos.CodePolicyInvalid, "observe_version must be a positive integer", 422, nil))
		return
	}
	resp, biz := s.Store.LatestQueueDiagnostic(r.Context(), r.PathValue("queue_id"), version)
	if biz != nil {
		writeError(w, biz)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) recoveryReport(w http.ResponseWriter, r *http.Request) {
	resp, biz := s.Store.GetRecoveryReport(r.Context())
	if biz != nil {
		writeError(w, biz)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) sample(w http.ResponseWriter, r *http.Request) {
	meta, biz := s.Store.SampleMeta(r.Context())
	if biz != nil {
		writeError(w, biz)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"code": qos.CodeOK, "sample": meta})
}

func (s *Server) requestStatus(w http.ResponseWriter, r *http.Request) {
	resp, biz := s.Store.GetIntent(r.Context(), r.PathValue("request_id"))
	if biz != nil {
		writeError(w, biz)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	defer r.Body.Close()
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20))
	if err := dec.Decode(dst); err != nil {
		msg := "request body is not valid JSON"
		if errors.Is(err, io.EOF) {
			msg = "request body is empty"
		}
		writeError(w, qos.NewError(qos.CodePolicyInvalid, msg, http.StatusUnprocessableEntity, map[string]any{"error": err.Error()}))
		return false
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		writeError(w, qos.NewError(qos.CodePolicyInvalid, "request body must contain exactly one JSON object", http.StatusUnprocessableEntity, nil))
		return false
	}
	return true
}

func writeError(w http.ResponseWriter, be *qos.BusinessError) {
	if be == nil {
		be = qos.NewError(qos.CodeInternal, "unknown error", 500, nil)
	}
	status := be.Status
	if status == 0 {
		status = http.StatusInternalServerError
	}
	writeJSON(w, status, be)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		_, _ = fmt.Fprint(w, `{"code":"E_INTERNAL","message":"response encoding failed"}`)
	}
}
