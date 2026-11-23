package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"example.com/joinshuffle/internal/model"
	"example.com/joinshuffle/internal/service"
)

type Server struct {
	service *service.Service
	mux     *http.ServeMux
}

func New(svc *service.Service) *Server {
	s := &Server{service: svc, mux: http.NewServeMux()}
	s.mux.HandleFunc("/healthz", s.handleHealth)
	s.mux.HandleFunc("/v1/status", s.handleStatus)
	s.mux.HandleFunc("/v1/snapshots/load", s.handleLoadSnapshots)
	s.mux.HandleFunc("/v1/snapshots", s.handleSnapshotStatus)
	s.mux.HandleFunc("/v1/jobs", s.handleJobs)
	s.mux.HandleFunc("/v1/jobs/", s.handleJobPath)
	s.mux.HandleFunc("/v1/recovery/report", s.handleRecoveryReport)
	s.mux.HandleFunc("/v1/recovery/run", s.handleRecoveryRun)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, model.NewError(model.CodeInvalidRequest, "method not allowed", "allowed", "GET"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"service": "join-shuffle-aggregation-engine",
	})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, model.NewError(model.CodeInvalidRequest, "method not allowed", "allowed", "GET"))
		return
	}
	report, err := s.service.RecoveryReport(r.Context())
	if err != nil {
		writeError(w, model.NewError(model.CodeInternal, "cannot read recovery status", "cause", err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":        true,
		"snapshots": s.service.SnapshotsStatus(),
		"recovery":  report,
	})
}

func (s *Server) handleLoadSnapshots(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, model.NewError(model.CodeInvalidRequest, "method not allowed", "allowed", "POST"))
		return
	}
	var req model.LoadSnapshotsRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, err)
		return
	}
	meta, err := s.service.LoadSnapshots(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":        true,
		"snapshots": meta,
	})
}

func (s *Server) handleSnapshotStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, model.NewError(model.CodeInvalidRequest, "method not allowed", "allowed", "GET"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":        true,
		"snapshots": s.service.SnapshotsStatus(),
	})
}

func (s *Server) handleJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, model.NewError(model.CodeInvalidRequest, "method not allowed", "allowed", "POST"))
		return
	}
	var req model.AggregateRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, err)
		return
	}
	response, err := s.service.Submit(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "job": response})
}

func (s *Server) handleJobPath(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, model.NewError(model.CodeInvalidRequest, "method not allowed", "allowed", "GET"))
		return
	}
	remainder := strings.TrimPrefix(r.URL.Path, "/v1/jobs/")
	parts := strings.Split(remainder, "/")
	if len(parts) < 1 || parts[0] == "" || len(parts) > 2 {
		writeError(w, model.NewError(model.CodeNotFound, "unknown job endpoint", "path", r.URL.Path))
		return
	}
	requestID, err := url.PathUnescape(parts[0])
	if err != nil {
		writeError(w, model.NewError(model.CodeInvalidRequest, "invalid request id encoding"))
		return
	}
	var payload any
	var serviceErr error
	if len(parts) == 1 {
		payload, serviceErr = s.service.GetJob(r.Context(), requestID)
	} else {
		switch parts[1] {
		case "partitions":
			payload, serviceErr = s.service.GetPartitions(r.Context(), requestID)
		case "results":
			payload, serviceErr = s.service.GetResults(r.Context(), requestID)
		case "partial-sums":
			payload, serviceErr = s.service.GetPartialSums(r.Context(), requestID)
		case "error-declaration":
			payload, serviceErr = s.service.GetErrorDeclaration(r.Context(), requestID)
		case "route":
			payload, serviceErr = s.service.GetRoute(r.Context(), requestID)
		default:
			serviceErr = model.NewError(model.CodeNotFound, "unknown job endpoint", "path", r.URL.Path)
		}
	}
	if serviceErr != nil {
		writeError(w, serviceErr)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "data": payload})
}

func (s *Server) handleRecoveryReport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, model.NewError(model.CodeInvalidRequest, "method not allowed", "allowed", "GET"))
		return
	}
	report, err := s.service.RecoveryReport(r.Context())
	if err != nil {
		writeError(w, model.NewError(model.CodeInternal, "recovery report failed", "cause", err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "report": report})
}

func (s *Server) handleRecoveryRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, model.NewError(model.CodeInvalidRequest, "method not allowed", "allowed", "POST"))
		return
	}
	report, err := s.service.Recover(r.Context())
	if err != nil {
		writeError(w, model.NewError(model.CodeInternal, "recovery scan failed", "cause", err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "report": report})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) error {
	defer r.Body.Close()
	limited := io.LimitReader(r.Body, 2<<20)
	decoder := json.NewDecoder(limited)
	if err := decoder.Decode(target); err != nil {
		if errors.Is(err, io.EOF) {
			return model.NewError(model.CodeInvalidRequest, "request body is required")
		}
		return model.NewError(model.CodeInvalidRequest, "invalid JSON request", "cause", err.Error())
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return model.NewError(model.CodeInvalidRequest, "request body must contain exactly one JSON value")
	}
	return nil
}

func writeError(w http.ResponseWriter, err error) {
	status, payload := model.ErrorPayload(err)
	writeJSON(w, status, payload)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(payload); err != nil {
		fmt.Fprintf(w, "{\"ok\":false,\"error\":{\"code\":\"E_INTERNAL\",\"message\":\"response encoding failed\"}}\n")
	}
}
