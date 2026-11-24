package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"preprocesspipeline/internal/app"
	"preprocesspipeline/internal/domain"
)

type Server struct {
	service *app.Service
	mux     *http.ServeMux
}

type errorEnvelope struct {
	OK      bool       `json:"ok"`
	Code    string     `json:"code"`
	Message string     `json:"message"`
	Details any        `json:"details,omitempty"`
	Error   *errorBody `json:"error"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

func NewServer(service *app.Service) *Server {
	s := &Server{service: service, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler { return s.recoverPanic(s.mux) }

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.health)
	s.mux.HandleFunc("GET /v1/generator/policy", s.generatedPolicy)
	s.mux.HandleFunc("GET /v1/generator/raw", s.generatedRaw)
	s.mux.HandleFunc("POST /v1/policies/default", s.ensureDefaultPolicy)
	s.mux.HandleFunc("POST /v1/demo/run", s.demoRun)
	s.mux.HandleFunc("POST /v1/policies", s.submitPolicy)
	s.mux.HandleFunc("POST /v1/policies/patch", s.submitPolicyPatch)
	s.mux.HandleFunc("GET /v1/policies/{version}", s.getPolicy)
	s.mux.HandleFunc("POST /v1/batches", s.submitBatch)
	s.mux.HandleFunc("POST /v1/injections", s.inject)
	s.mux.HandleFunc("POST /v1/injections/{kind}", s.inject)
	s.mux.HandleFunc("GET /v1/batches/{batchID}", s.getBatch)
	s.mux.HandleFunc("GET /v1/batches/{batchID}/stages", s.listStages)
	s.mux.HandleFunc("GET /v1/batches/{batchID}/shards", s.listShards)
	s.mux.HandleFunc("GET /v1/recovery", s.latestRecovery)
	s.mux.HandleFunc("POST /v1/recovery", s.runRecovery)
	s.mux.HandleFunc("/", s.notFound)
}

func (s *Server) recoverPanic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				s.writeError(w, domain.NewError(domain.CodeInternal, "internal panic recovered"))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (s *Server) writeError(w http.ResponseWriter, err error) {
	var berr *domain.BusinessError
	if errors.As(err, &berr) {
		writeJSON(w, berr.HTTPStatus, errorEnvelope{
			OK: false, Code: berr.Code, Message: berr.Message, Details: berr.Details,
			Error: &errorBody{Code: berr.Code, Message: berr.Message, Details: berr.Details},
		})
		return
	}
	berr = domain.NewError(domain.CodeInternal, err.Error())
	writeJSON(w, http.StatusInternalServerError, errorEnvelope{
		OK: false, Code: berr.Code, Message: berr.Message,
		Error: &errorBody{Code: berr.Code, Message: berr.Message},
	})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "service": "preprocesspipeline"})
}

func (s *Server) generatedPolicy(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, domain.DefaultPolicy())
}

func (s *Server) generatedRaw(w http.ResponseWriter, r *http.Request) {
	count := queryInt(r, "count", domain.DefaultGenerateSpec().Count)
	seed := int64(queryInt(r, "seed", int(domain.DefaultPolicy().Seed)))
	namespace := r.URL.Query().Get("namespace")
	raw := domain.GenerateRawInput(domain.GenerateSpec{Count: count, Seed: seed, Namespace: namespace})
	if r.URL.Query().Get("format") == "text" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte(raw))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"raw_input": raw})
}

func (s *Server) ensureDefaultPolicy(w http.ResponseWriter, r *http.Request) {
	policy, err := s.service.EnsureDefaultPolicy(r.Context())
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, policy)
}

func (s *Server) demoRun(w http.ResponseWriter, r *http.Request) {
	if _, err := s.service.EnsureDefaultPolicy(r.Context()); err != nil {
		s.writeError(w, err)
		return
	}
	req := batchRequestFromQuery(r)
	if req.BatchID == "" {
		if req.Inject != "" {
			req.BatchID = "demo-" + normalizeInjectForID(req.Inject)
		} else {
			req.BatchID = "demo-batch-0001"
		}
	}
	resp, err := s.service.SubmitBatch(r.Context(), req)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) submitPolicy(w http.ResponseWriter, r *http.Request) {
	var policy domain.Policy
	if err := json.NewDecoder(r.Body).Decode(&policy); err != nil {
		s.writeError(w, domain.NewError(domain.CodeRecordInvalid, "invalid policy JSON: "+err.Error()))
		return
	}
	submitted, err := s.service.SubmitFullPolicy(r.Context(), policy, r.URL.Query().Get("inject"))
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, submitted)
}

func (s *Server) submitPolicyPatch(w http.ResponseWriter, r *http.Request) {
	var patch domain.PolicyPatch
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		s.writeError(w, domain.NewError(domain.CodeRecordInvalid, "invalid policy patch JSON: "+err.Error()))
		return
	}
	patched, err := s.service.SubmitPolicyPatch(r.Context(), patch, r.URL.Query().Get("inject"))
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, patched)
}

func (s *Server) getPolicy(w http.ResponseWriter, r *http.Request) {
	version, err := strconv.ParseInt(r.PathValue("version"), 10, 64)
	if err != nil {
		s.writeError(w, domain.NewError(domain.CodeRecordInvalid, "invalid policy version"))
		return
	}
	policy, err := s.service.Store().GetPolicy(r.Context(), version)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, policy)
}

func (s *Server) submitBatch(w http.ResponseWriter, r *http.Request) {
	req := batchRequestFromQuery(r)
	if r.Header.Get("Content-Type") != "" && jsonContentType(r) {
		var body app.BatchRequest
		if err := decodeOptionalJSON(r, &body); err != nil {
			s.writeError(w, domain.NewError(domain.CodeRecordInvalid, "invalid batch JSON: "+err.Error()))
			return
		}
		mergeBatchRequest(&req, body)
	}
	resp, err := s.service.SubmitBatch(r.Context(), req)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) getBatch(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("batchID")
	batch, err := s.service.Store().GetBatch(r.Context(), id)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, batch)
}

func (s *Server) listStages(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("batchID")
	version := queryInt64(r, "policy_version", 0)
	stages, err := s.service.Store().ListStageSummaries(r.Context(), id, version)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"batch_id": id, "stages": stages})
}

func (s *Server) listShards(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("batchID")
	version := queryInt64(r, "policy_version", 0)
	includeInvalid := queryBool(r, "include_invalid", false)
	shards, err := s.service.Store().ListShards(r.Context(), id, version, includeInvalid)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"batch_id": id, "shards": shards})
}

func (s *Server) latestRecovery(w http.ResponseWriter, r *http.Request) {
	report, err := s.service.LatestRecoveryReport(r.Context())
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) runRecovery(w http.ResponseWriter, r *http.Request) {
	report, err := s.service.Recover(r.Context(), "http_manual")
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) notFound(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":      true,
			"service": "preprocesspipeline",
			"endpoints": []string{
				"GET /healthz", "GET /v1/generator/policy", "GET /v1/generator/raw",
				"POST /v1/policies/default", "POST /v1/policies", "POST /v1/policies/patch",
				"POST /v1/batches", "GET /v1/batches/{id}/stages", "GET /v1/batches/{id}/shards",
				"GET /v1/recovery", "POST /v1/recovery",
			},
		})
		return
	}
	writeJSON(w, http.StatusNotFound, errorEnvelope{
		OK: false, Code: "E_NOT_FOUND", Message: "route not found: " + r.Method + " " + r.URL.Path,
		Error: &errorBody{Code: "E_NOT_FOUND", Message: "route not found"},
	})
}

func batchRequestFromQuery(r *http.Request) app.BatchRequest {
	q := r.URL.Query()
	req := app.BatchRequest{
		BatchID:        q.Get("batch_id"),
		Inject:         firstNonEmpty(q.Get("inject"), q.Get("injection"), q.Get("type"), q.Get("fault")),
		InterruptStage: q.Get("interrupt_stage"),
		PolicyVersion:  queryInt64(r, "policy_version", 0),
		Retry:          queryBool(r, "retry", false),
	}
	if q.Has("count") {
		req.Generate = &domain.GenerateSpec{Count: queryInt(r, "count", 0), Namespace: q.Get("namespace")}
	}
	return req
}

func normalizeInjectForID(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	value = strings.TrimPrefix(value, "e_")
	return strings.NewReplacer(" ", "-", "_", "-").Replace(value)
}

func mergeBatchRequest(target *app.BatchRequest, body app.BatchRequest) {
	if body.BatchID != "" {
		target.BatchID = body.BatchID
	}
	if body.PolicyVersion != 0 {
		target.PolicyVersion = body.PolicyVersion
	}
	if body.RawInput != "" {
		target.RawInput = body.RawInput
	}
	if body.Generate != nil {
		target.Generate = body.Generate
	}
	if body.Inject != "" {
		target.Inject = body.Inject
	}
	if body.Injection != "" {
		target.Injection = body.Injection
	}
	if body.Fault != "" {
		target.Fault = body.Fault
	}
	if body.InterruptStage != "" {
		target.InterruptStage = body.InterruptStage
	}
	if body.Retry {
		target.Retry = true
	}
}

func (s *Server) inject(w http.ResponseWriter, r *http.Request) {
	if _, err := s.service.EnsureDefaultPolicy(r.Context()); err != nil {
		s.writeError(w, err)
		return
	}
	req := batchRequestFromQuery(r)
	if r.Header.Get("Content-Type") != "" && jsonContentType(r) {
		var body app.BatchRequest
		if err := decodeOptionalJSON(r, &body); err != nil {
			s.writeError(w, domain.NewError(domain.CodeRecordInvalid, "invalid injection JSON: "+err.Error()))
			return
		}
		mergeBatchRequest(&req, body)
	}
	kind := r.PathValue("kind")
	if kind == "" {
		kind = r.URL.Query().Get("type")
	}
	if kind != "" {
		req.Inject = kind
	}
	req.Inject = firstNonEmpty(req.Inject, req.Injection, req.Fault)
	if req.BatchID == "" {
		req.BatchID = "injection-" + normalizeInjectForID(req.Inject)
	}
	req.Retry = queryBool(r, "retry", true)
	resp, err := s.service.SubmitBatch(r.Context(), req)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func jsonContentType(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Content-Type"), "application/json")
}

func decodeOptionalJSON(r *http.Request, target any) error {
	err := json.NewDecoder(r.Body).Decode(target)
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func queryInt(r *http.Request, key string, fallback int) int {
	value := r.URL.Query().Get(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func queryInt64(r *http.Request, key string, fallback int64) int64 {
	value := r.URL.Query().Get(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func queryBool(r *http.Request, key string, fallback bool) bool {
	value := r.URL.Query().Get(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}
