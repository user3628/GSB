package api

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"localinfer/workbench/internal/domain"
	"localinfer/workbench/internal/service"
)

const maxBodyBytes = 16 << 20

type Server struct {
	service *service.Service
	webDir  string
	logger  *log.Logger
}

func NewServer(svc *service.Service, webDir string, logger *log.Logger) http.Handler {
	if logger == nil {
		logger = log.New(os.Stderr, "workbench: ", log.LstdFlags|log.LUTC)
	}
	s := &Server{service: svc, webDir: webDir, logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.health)
	mux.HandleFunc("/api/v1/models", s.models)
	mux.HandleFunc("/api/v1/traces", s.traces)
	mux.HandleFunc("/api/v1/traces/", s.tracePath)
	mux.HandleFunc("/api/v1/runs", s.runs)
	mux.HandleFunc("/api/v1/runs/", s.runPath)
	mux.HandleFunc("/api/v1/batches/", s.batchPath)
	mux.HandleFunc("/api/v1/compare", s.compare)
	mux.HandleFunc("/api/v1/recovery/report", s.recovery)
	mux.HandleFunc("/api/v1/intents", s.intents)
	mux.HandleFunc("/api/v1/diagnostics", s.diagnostics)
	mux.HandleFunc("/static/", s.static)
	mux.HandleFunc("/", s.index)
	return s.middleware(mux)
}

func (s *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		defer func() {
			if recovered := recover(); recovered != nil {
				s.logger.Printf("panic serving %s %s: %v", r.Method, r.URL.Path, recovered)
				writeError(w, domain.NewBizError(domain.CodeInternal, "internal server panic", map[string]any{"reason": fmt.Sprint(recovered)}))
			}
			s.logger.Printf("%s %s %s", r.Method, r.URL.RequestURI(), time.Since(start).Round(time.Microsecond))
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "service": "local-inference-batch-workbench", "time": time.Now().UTC().Format(time.RFC3339Nano)})
}

func (s *Server) models(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		catalog := s.service.Catalog()
		writeJSON(w, http.StatusOK, catalog)
	case http.MethodPost:
		body, err := readBody(w, r)
		if err != nil {
			writeError(w, err)
			return
		}
		catalog, changed, err := s.service.InstallCatalog(body)
		if err != nil {
			writeError(w, err)
			return
		}
		status := http.StatusOK
		if changed {
			status = http.StatusCreated
		}
		writeJSON(w, status, map[string]any{"catalog": catalog, "changed": changed})
	default:
		methodNotAllowed(w, http.MethodGet, http.MethodPost)
	}
}

func (s *Server) traces(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		traces, err := s.service.Store().ListTraces()
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"traces": traces})
	case http.MethodPost:
		body, err := readBody(w, r)
		if err != nil {
			writeError(w, err)
			return
		}
		trace, created, err := s.service.SubmitTrace(body)
		if err != nil {
			writeError(w, err)
			return
		}
		status := http.StatusOK
		if created {
			status = http.StatusCreated
		}
		writeJSON(w, status, map[string]any{"trace": trace, "created": created})
	default:
		methodNotAllowed(w, http.MethodGet, http.MethodPost)
	}
}

func (s *Server) tracePath(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/api/v1/traces/")
	parts := strings.Split(rest, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		writeError(w, domain.NewBizError(domain.CodeNotFound, "unknown trace endpoint", map[string]any{"path": r.URL.Path}))
		return
	}
	traceID, action := parts[0], parts[1]
	version := r.URL.Query().Get("version")
	if version == "" {
		writeError(w, domain.NewBizError(domain.CodeBadRequest, "version query parameter is required", map[string]any{"trace_id": traceID}))
		return
	}
	switch action {
	case "organize":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		body, err := readBody(w, r)
		if err != nil {
			writeError(w, err)
			return
		}
		opts, err := parseOrganizationOptions(body)
		if err != nil {
			writeError(w, err)
			return
		}
		run, err := s.service.Organize(traceID, version, opts)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, run)
	case "comparison":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		comparison, err := s.service.CompareTraceModes(traceID, version)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, comparison)
	case "runs":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		runs, err := s.service.ListRuns(traceID, version)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"runs": runs})
	default:
		writeError(w, domain.NewBizError(domain.CodeNotFound, "unknown trace action", map[string]any{"action": action}))
	}
}

func (s *Server) runs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	runs, err := s.service.ListRuns(r.URL.Query().Get("trace_id"), r.URL.Query().Get("version"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": runs})
}

func (s *Server) runPath(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	runID := strings.TrimPrefix(r.URL.Path, "/api/v1/runs/")
	if runID == "" || strings.Contains(runID, "/") {
		writeError(w, domain.NewBizError(domain.CodeNotFound, "run not found", nil))
		return
	}
	run, err := s.service.GetRun(runID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) batchPath(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, "/api/v1/batches/")
	parts := strings.Split(rest, "/")
	if len(parts) == 0 || parts[0] == "" {
		writeError(w, domain.NewBizError(domain.CodeNotFound, "batch not found", nil))
		return
	}
	batchID := parts[0]
	action := ""
	if len(parts) > 1 {
		action = parts[1]
	}
	switch action {
	case "":
		envelope, err := s.service.GetBatch(batchID)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, envelope)
	case "quantiles":
		p := 0
		if raw := r.URL.Query().Get("p"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				writeError(w, domain.NewBizError(domain.CodeBadRequest, "p must be an integer", map[string]any{"p": raw}))
				return
			}
			p = parsed
		}
		quantiles, err := s.service.Quantiles(batchID, p)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"batch_id": batchID, "requested_quantile": p, "quantiles": quantiles})
	case "precision":
		envelope, err := s.service.GetBatch(batchID)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"batch_id": batchID, "run_status": envelope.RunStatus, "rounding_contract": "whole microseconds internally; half-away-from-zero at declared decimal place",
			"precision": envelope.Batch.Precision,
		})
	case "verify":
		verification, err := s.service.VerifyBatch(batchID)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, verification)
	default:
		writeError(w, domain.NewBizError(domain.CodeNotFound, "unknown batch action", map[string]any{"action": action}))
	}
}

func (s *Server) compare(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	left, right := r.URL.Query().Get("left"), r.URL.Query().Get("right")
	if left == "" || right == "" {
		writeError(w, domain.NewBizError(domain.CodeBadRequest, "left and right run ids are required", nil))
		return
	}
	comparison, err := s.service.CompareRuns(left, right)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, comparison)
}

func (s *Server) recovery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	if r.URL.Query().Get("all") == "1" {
		reports, err := s.service.RecoveryReports()
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"reports": reports})
		return
	}
	report, err := s.service.LatestRecoveryReport()
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) intents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			limit = parsed
		}
	}
	intents, err := s.service.Store().ListIntents(limit)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"intents": intents})
}

func (s *Server) diagnostics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	traces, err := s.service.Store().ListTraces()
	if err != nil {
		writeError(w, err)
		return
	}
	runs, err := s.service.ListRuns("", "")
	if err != nil {
		writeError(w, err)
		return
	}
	report, reportErr := s.service.LatestRecoveryReport()
	var reportValue any
	if reportErr == nil {
		reportValue = report
	} else if !isBizCode(reportErr, domain.CodeNotFound) {
		writeError(w, reportErr)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"catalog": s.service.Catalog(), "traces": traces, "runs": runs, "latest_recovery_report": reportValue,
	})
}

func (s *Server) static(w http.ResponseWriter, r *http.Request) {
	if s.webDir == "" {
		writeError(w, domain.NewBizError(domain.CodeNotFound, "static assets are disabled", nil))
		return
	}
	w.Header().Set("Cache-Control", "public, max-age=60")
	http.StripPrefix("/static/", http.FileServer(http.Dir(s.webDir))).ServeHTTP(w, r)
}

func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "/index.html" {
		writeError(w, domain.NewBizError(domain.CodeNotFound, "endpoint not found", map[string]any{"path": r.URL.Path}))
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		methodNotAllowed(w, http.MethodGet, http.MethodHead)
		return
	}
	if s.webDir == "" {
		writeError(w, domain.NewBizError(domain.CodeNotFound, "diagnostic page is disabled", nil))
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	http.ServeFile(w, r, filepath.Join(s.webDir, "index.html"))
}

func readBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, domain.NewBizError(domain.CodeBadRequest, "request body could not be read", map[string]any{"reason": err.Error(), "limit_bytes": maxBodyBytes})
	}
	return body, nil
}
