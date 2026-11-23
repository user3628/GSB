// Package httpapi exposes the release-gate domain through a local JSON API.
package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"releasegate/domain"
	"releasegate/service"
)

const maxBodyBytes = 32 << 20

type Options struct {
	EnableFaultInjection bool
	FaultHook            func()
}

type Server struct {
	svc  *service.Service
	opts Options
	mux  *http.ServeMux
}

func New(svc *service.Service, opts Options) *Server {
	s := &Server{svc: svc, opts: opts, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler {
	return s.recoverMiddleware(s.mux)
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.health)
	s.mux.HandleFunc("POST /v1/releases", s.createRelease)
	s.mux.HandleFunc("GET /v1/releases/{releaseID}", s.getRelease)
	s.mux.HandleFunc("POST /v1/releases/{releaseID}/start", s.startRelease)
	s.mux.HandleFunc("POST /v1/releases/{releaseID}/observations", s.ingestBatch)
	s.mux.HandleFunc("POST /v1/releases/{releaseID}/batches", s.ingestBatch)
	s.mux.HandleFunc("POST /v1/releases/{releaseID}/gate", s.gate)
	s.mux.HandleFunc("GET /v1/releases/{releaseID}/stats", s.stats)
	s.mux.HandleFunc("GET /v1/releases/{releaseID}/estimate", s.estimate)
	s.mux.HandleFunc("GET /v1/releases/{releaseID}/error-contract", s.errorContract)
	s.mux.HandleFunc("GET /v1/releases/{releaseID}/decisions", s.decisions)
	s.mux.HandleFunc("POST /v1/releases/{releaseID}/transitions", s.transition)
	s.mux.HandleFunc("GET /v1/releases/{releaseID}/recovery-report", s.recoveryReportForRelease)
	s.mux.HandleFunc("POST /v1/recovery/scan", s.recoveryScan)
	s.mux.HandleFunc("GET /v1/recovery/report", s.recoveryReport)
	if s.opts.EnableFaultInjection {
		s.mux.HandleFunc("POST /v1/releases/{releaseID}/faults/terminate-after-intent", s.terminateAfterIntent)
	}
	s.mux.HandleFunc("/", s.notFound)
}

func (s *Server) recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				writeBusinessError(w, domain.WithContext(domain.NewError(domain.CodeInternal, "panic recovered by HTTP boundary"), "panic", fmt.Sprint(recovered)))
			}
		}()
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "service": "releasegate"})
}

func (s *Server) createRelease(w http.ResponseWriter, r *http.Request) {
	var req service.CreateReleaseRequest
	if err := decodeJSON(r, &req); err != nil {
		writeBusinessError(w, invalidJSON(err))
		return
	}
	rel, err := s.svc.CreateRelease(r.Context(), req)
	if err != nil {
		writeBusinessError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, rel)
}

func (s *Server) getRelease(w http.ResponseWriter, r *http.Request) {
	rel, err := s.svc.GetRelease(r.Context(), r.PathValue("releaseID"))
	if err != nil {
		writeBusinessError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rel)
}

func (s *Server) startRelease(w http.ResponseWriter, r *http.Request) {
	rel, err := s.svc.Start(r.Context(), r.PathValue("releaseID"))
	if err != nil {
		writeBusinessError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rel)
}

func (s *Server) ingestBatch(w http.ResponseWriter, r *http.Request) {
	var in domain.BatchInput
	if err := decodeJSON(r, &in); err != nil {
		writeBusinessError(w, invalidJSON(err))
		return
	}
	releaseID := r.PathValue("releaseID")
	if in.ReleaseID == "" {
		in.ReleaseID = releaseID
	} else if in.ReleaseID != releaseID {
		writeBusinessError(w, domain.WithContext(domain.NewError(domain.CodeReleaseInvalid, "path and body release_id disagree"), "path_release_id", releaseID))
		return
	}
	result, err := s.svc.IngestBatch(r.Context(), in)
	if err != nil {
		writeBusinessError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

type gateRequest struct {
	StatsVersion int64 `json:"stats_version"`
}

func (s *Server) gate(w http.ResponseWriter, r *http.Request) {
	var req gateRequest
	if err := decodeOptionalJSON(r, &req); err != nil {
		writeBusinessError(w, invalidJSON(err))
		return
	}
	result, err := s.svc.Gate(r.Context(), r.PathValue("releaseID"), req.StatsVersion)
	if err != nil {
		writeBusinessError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	version, scope, stage, err := queryShape(r)
	if err != nil {
		writeBusinessError(w, err)
		return
	}
	detail, err := s.svc.StatsDetail(r.Context(), r.PathValue("releaseID"), version, scope, stage)
	if err != nil {
		writeBusinessError(w, err)
		return
	}
	response := map[string]any{
		"release_id":             detail.Release.ReleaseID,
		"stats_version":          detail.Version,
		"scope":                  detail.Scope,
		"stage":                  detail.Stage,
		"quantile_method":        detail.Release.Definition.QuantileMethod,
		"confidence_level_ppm":   detail.Release.Definition.ConfidenceLevelPPM,
		"snapshot_current_stage": detail.Release.CurrentStage,
		"data":                   detail.StageMetrics,
		"stage_metrics":          detail.StageMetrics,
		"estimate":               detail.Estimate,
		"error_contract":         detail.ErrorContract,
		"release":                detail.Release,
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) estimate(w http.ResponseWriter, r *http.Request) {
	version, scope, stage, err := queryShape(r)
	if err != nil {
		writeBusinessError(w, err)
		return
	}
	detail, err := s.svc.StatsDetail(r.Context(), r.PathValue("releaseID"), version, scope, stage)
	if err != nil {
		writeBusinessError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"release_id": detail.Release.ReleaseID, "stats_version": detail.Version,
		"scope": detail.Scope, "stage": detail.Stage, "estimate": detail.Estimate,
	})
}

func (s *Server) errorContract(w http.ResponseWriter, r *http.Request) {
	version, scope, stage, err := queryShape(r)
	if err != nil {
		writeBusinessError(w, err)
		return
	}
	detail, err := s.svc.StatsDetail(r.Context(), r.PathValue("releaseID"), version, scope, stage)
	if err != nil {
		writeBusinessError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, detail.ErrorContract)
}

func (s *Server) decisions(w http.ResponseWriter, r *http.Request) {
	decisions, err := s.svc.Decisions(r.Context(), r.PathValue("releaseID"))
	if err != nil {
		writeBusinessError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"release_id": r.PathValue("releaseID"), "decisions": decisions})
}

func (s *Server) transition(w http.ResponseWriter, r *http.Request) {
	var req domain.TransitionRequest
	if err := decodeJSON(r, &req); err != nil {
		writeBusinessError(w, invalidJSON(err))
		return
	}
	to := req.Destination()
	if to == "" {
		writeBusinessError(w, domain.NewError(domain.CodeReleaseInvalid, "to_state is required"))
		return
	}
	rel, err := s.svc.Transition(r.Context(), r.PathValue("releaseID"), strings.ToUpper(to), req.Reason)
	if err != nil {
		writeBusinessError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rel)
}

func (s *Server) recoveryReportForRelease(w http.ResponseWriter, r *http.Request) {
	report, err := s.svc.RecoveryReport(r.Context(), r.PathValue("releaseID"))
	if err != nil {
		writeBusinessError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) recoveryReport(w http.ResponseWriter, r *http.Request) {
	report, err := s.svc.RecoveryReport(r.Context(), "")
	if err != nil {
		writeBusinessError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) recoveryScan(w http.ResponseWriter, r *http.Request) {
	report, err := s.svc.ScanRecovery(r.Context())
	if err != nil {
		writeBusinessError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

type faultRequest struct {
	Kind string `json:"kind"`
}

func (s *Server) terminateAfterIntent(w http.ResponseWriter, r *http.Request) {
	var req faultRequest
	_ = decodeOptionalJSON(r, &req)
	intent, err := s.svc.PrepareFaultIntent(r.Context(), r.PathValue("releaseID"), req.Kind)
	if err != nil {
		writeBusinessError(w, err)
		return
	}
	if s.opts.FaultHook == nil {
		writeJSON(w, http.StatusAccepted, map[string]any{
			"simulated": false, "intent_id": intent.IntentID,
			"detail": "durable intent written; no fault hook configured so the process remains alive",
		})
		return
	}
	// The production demo hook exits with status 70 without writing a response.
	s.opts.FaultHook()
	writeBusinessError(w, domain.NewError(domain.CodeInternal, "fault hook returned without terminating"))
}

func (s *Server) notFound(w http.ResponseWriter, r *http.Request) {
	writeBusinessError(w, domain.WithContext(domain.NewError(domain.CodeNotFound, "route not found"), "path", r.URL.Path))
}

func queryShape(r *http.Request) (version int64, scope string, stage int, err error) {
	q := r.URL.Query()
	rawVersion := q.Get("version")
	if rawVersion == "" {
		rawVersion = q.Get("stats_version")
	}
	if rawVersion != "" {
		version, err = strconv.ParseInt(rawVersion, 10, 64)
		if err != nil || version < 0 {
			return 0, "", 0, domain.WithContext(domain.NewError(domain.CodeReleaseInvalid, "version must be a non-negative integer"), "version", rawVersion)
		}
	}
	scope = q.Get("scope")
	if scope == "" {
		scope = "stage"
	}
	if rawStage := q.Get("stage"); rawStage != "" {
		parsed, parseErr := strconv.Atoi(rawStage)
		if parseErr != nil || parsed <= 0 {
			return 0, "", 0, domain.WithContext(domain.NewError(domain.CodeReleaseInvalid, "stage must be a positive integer"), "stage", rawStage)
		}
		stage = parsed
	}
	return version, scope, stage, nil
}

func decodeJSON(r *http.Request, dst any) error {
	if r.Body == nil {
		return errors.New("request body is required")
	}
	defer r.Body.Close()
	dec := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes))
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return errors.New("request body must contain exactly one JSON value")
	}
	return nil
}

func decodeOptionalJSON(r *http.Request, dst any) error {
	if r.Body == nil || r.ContentLength == 0 {
		return nil
	}
	err := decodeJSON(r, dst)
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func invalidJSON(err error) error {
	return domain.WithContext(domain.NewError(domain.CodeReleaseInvalid, "invalid JSON request"), "detail", err.Error())
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(value)
}

func writeBusinessError(w http.ResponseWriter, err error) {
	be, ok := domain.AsBusinessError(err)
	if !ok {
		be = domain.WithContext(domain.NewError(domain.CodeInternal, "internal error"), "detail", err.Error())
	}
	writeJSON(w, statusForCode(be.Code), map[string]any{"error": be})
}

func statusForCode(code string) int {
	switch code {
	case domain.CodeReleaseInvalid:
		return http.StatusBadRequest
	case domain.CodeConflictingBatch, domain.CodeGateFailed, domain.CodeInvalidTransition, domain.CodeVersionRetired:
		return http.StatusConflict
	case domain.CodeInsufficientSamples, domain.CodeObserveMore:
		return http.StatusUnprocessableEntity
	case domain.CodeNotFound:
		return http.StatusNotFound
	case domain.CodeSimulatedCrash:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}
