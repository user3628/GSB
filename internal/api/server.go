package api

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"

	"csrouter/internal/app"
	"csrouter/internal/domain"
	"csrouter/internal/generator"
	"csrouter/internal/store"
)

type Server struct {
	App    *app.App
	Mux    *http.ServeMux
	addr   string
	server *http.Server
}

func NewServer(application *app.App, addr string) *Server {
	mux := http.NewServeMux()
	s := &Server{App: application, Mux: mux, addr: addr}
	s.routes()
	s.server = &http.Server{Addr: addr, Handler: logging(mux)}
	return s
}

func (s *Server) ListenAndServe() error {
	host, _, err := net.SplitHostPort(s.addr)
	if err != nil {
		return err
	}
	if host != "127.0.0.1" && host != "localhost" && host != "::1" {
		return errors.New("HTTP server may bind only to loopback host 127.0.0.1, localhost, or [::1]")
	}
	return s.server.ListenAndServe()
}

func (s *Server) Close() error { return s.server.Close() }

func logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		next.ServeHTTP(w, r)
	})
}

func (s *Server) routes() {
	s.Mux.HandleFunc("/", s.index)
	s.Mux.HandleFunc("/api/health", s.health)
	s.Mux.HandleFunc("/api/generate", s.generate)
	s.Mux.HandleFunc("/api/manifest", s.manifest)
	s.Mux.HandleFunc("/api/validate", s.validate)
	s.Mux.HandleFunc("/api/run", s.run)
	s.Mux.HandleFunc("/api/resume", s.resume)
	s.Mux.HandleFunc("/api/stop", s.stop)
	s.Mux.HandleFunc("/api/status", s.status)
	s.Mux.HandleFunc("/api/fingerprints", s.fingerprints)
	s.Mux.HandleFunc("/api/ticket", s.ticket)
	s.Mux.HandleFunc("/api/report", s.report)
	s.Mux.HandleFunc("/api/reference", s.reference)
	s.Mux.HandleFunc("/api/strategy", s.strategy)
	s.Mux.HandleFunc("/api/priority-details", s.priorityDetails)
	s.Mux.HandleFunc("/api/shards/replace", s.replaceShard)
	s.Mux.HandleFunc("/api/config/change", s.changeConfig)
	s.Mux.HandleFunc("/api/config/rollback", s.rollbackConfig)
	s.Mux.HandleFunc("/api/policy/replace", s.replacePolicy)
	s.Mux.HandleFunc("/api/checkpoints/publish", s.publishCheckpoint)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeOK(w http.ResponseWriter, v any) { writeJSON(w, http.StatusOK, v) }

func writeBizError(w http.ResponseWriter, err *domain.BizError) {
	status := http.StatusInternalServerError
	switch err.Code {
	case domain.ErrEmptyText, domain.ErrInvalidLabel, domain.ErrShardMismatch, domain.ErrInvalidConfig:
		status = http.StatusUnprocessableEntity
	case domain.ErrStaleCheckpoint, domain.ErrIllegalStage:
		status = http.StatusConflict
	case domain.ErrNotFound:
		status = http.StatusNotFound
	}
	writeJSON(w, status, err)
}

func (s *Server) enrich(err *domain.BizError, tier string) *domain.BizError {
	if err == nil {
		return nil
	}
	if tier != "" {
		if policy, loadErr := s.App.DB.LatestPolicy(tier); loadErr == nil && policy != nil {
			if err.ConfigVersion == 0 {
				err.ConfigVersion = policy.EffectiveVersion()
			}
			if err.VersionFingerprint == "" {
				err.VersionFingerprint = policy.Fingerprint()
			}
		}
	}
	return err
}

func decodeBody(r *http.Request, target any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(io.LimitReader(r.Body, 64<<20))
	return dec.Decode(target)
}

func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(indexHTML))
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeOK(w, map[string]any{"code": domain.CodeOK, "service": "csrouter", "loopback_only": true})
}

func (s *Server) generate(w http.ResponseWriter, r *http.Request) {
	seed := generator.DefaultSeed
	if raw := r.URL.Query().Get("seed"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			writeBizError(w, domain.NewBizError(domain.ErrInvalidConfig, "", "", 0, "invalid seed"))
			return
		}
		seed = parsed
	}
	result, err := s.App.GenerateAndImport(seed)
	if err != nil {
		writeBizError(w, domain.NewBizError("E_GENERATE", "", "", 0, "%v", err))
		return
	}
	writeOK(w, result)
}

func (s *Server) manifest(w http.ResponseWriter, _ *http.Request) {
	manifest, err := s.App.LoadManifest()
	if err != nil {
		writeBizError(w, domain.NewBizError(domain.ErrNotFound, "", "", 0, "%v", err))
		return
	}
	writeOK(w, manifest)
}

func (s *Server) validate(w http.ResponseWriter, _ *http.Request) {
	result, err := s.App.ValidateBundle()
	if err != nil {
		writeJSON(w, http.StatusExpectationFailed, result)
		return
	}
	writeOK(w, result)
}

func (s *Server) run(w http.ResponseWriter, r *http.Request) {
	tier := r.URL.Query().Get("tier")
	if tier == "" {
		tier = "small"
	}
	var options app.RunOptions
	if r.Method == http.MethodPost && r.ContentLength > 0 {
		if err := decodeBody(r, &options); err != nil {
			writeBizError(w, domain.NewBizError(domain.ErrInvalidConfig, "", "", 0, "%v", err))
			return
		}
	}
	if stage := r.URL.Query().Get("stage"); stage != "" {
		options.ThroughStage = stage
	}
	options.ClearStop = true
	result, err := s.App.RunTier(tier, options)
	if bizErr, ok := err.(*domain.BizError); ok {
		writeBizError(w, s.enrich(bizErr, tier))
		return
	}
	if err != nil {
		writeBizError(w, domain.NewBizError("E_RUN", "", "", 0, "%v", err))
		return
	}
	writeOK(w, result)
}

func (s *Server) resume(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	q.Set("tier", firstNonEmpty(q.Get("tier"), "small"))
	r.URL.RawQuery = q.Encode()
	s.run(w, r)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func (s *Server) stop(w http.ResponseWriter, r *http.Request) {
	if err := s.App.DB.RequestStop(); err != nil {
		writeBizError(w, domain.NewBizError("E_STORE", "", "", 0, "%v", err))
		return
	}
	writeOK(w, map[string]string{"code": domain.CodeStopRequested, "message": "stop will take effect after current indivisible ticket"})
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	tier := firstNonEmpty(r.URL.Query().Get("tier"), "small")
	policy, err := s.App.DB.LatestPolicy(tier)
	if err != nil {
		writeBizError(w, domain.NewBizError("E_STORE", "", "", 0, "%v", err))
		return
	}
	statuses, err := s.App.DB.StageStatuses(tier)
	if err != nil {
		writeBizError(w, domain.NewBizError("E_STORE", "", "", versionOrZero(policy), "%v", err))
		return
	}
	fingerprints, _ := s.App.DB.FingerprintSnapshot(tier)
	stop, _ := s.App.DB.StopRequested()
	writeOK(w, map[string]any{
		"code":                domain.CodeOK,
		"tier":                tier,
		"policy":              policy,
		"stage_status":        statuses,
		"fingerprints":        fingerprints,
		"stop_requested":      stop,
		"current_version":     versionOrZero(policy),
		"version_fingerprint": fingerprintOrEmpty(policy),
	})
}

func versionOrZero(p *domain.Policy) int64 {
	if p == nil {
		return 0
	}
	return p.EffectiveVersion()
}

func fingerprintOrEmpty(p *domain.Policy) string {
	if p == nil {
		return ""
	}
	return p.Fingerprint()
}

func (s *Server) fingerprints(w http.ResponseWriter, r *http.Request) {
	tier := firstNonEmpty(r.URL.Query().Get("tier"), "small")
	result, err := s.App.DB.FingerprintSnapshot(tier)
	if err != nil {
		writeBizError(w, domain.NewBizError("E_STORE", "", "", 0, "%v", err))
		return
	}
	writeOK(w, result)
}

type ticketDetailResponse struct {
	Ticket      *store.TicketRow                 `json:"ticket"`
	Artifacts   map[string]any                   `json:"artifacts"`
	Checkpoints map[string]*store.ArtifactRecord `json:"checkpoint_records"`
}

func (s *Server) ticket(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		writeBizError(w, domain.NewBizError(domain.ErrInvalidConfig, "", "", 0, "id is required"))
		return
	}
	row, err := s.App.DB.GetTicket(id)
	if err != nil {
		writeBizError(w, domain.NewBizError("E_STORE", id, "", 0, "%v", err))
		return
	}
	if row == nil {
		writeBizError(w, domain.NewBizError(domain.ErrNotFound, id, "", 0, "ticket not found"))
		return
	}
	resp := ticketDetailResponse{Ticket: row, Artifacts: map[string]any{}, Checkpoints: map[string]*store.ArtifactRecord{}}
	for _, stage := range domain.Stages {
		rec, err := s.App.DB.GetArtifact(id, stage)
		if err != nil {
			writeBizError(w, domain.NewBizError("E_STORE", id, "", 0, "%v", err))
			return
		}
		if rec == nil {
			continue
		}
		resp.Checkpoints[stage] = rec
		var payload any
		_ = json.Unmarshal([]byte(rec.Payload), &payload)
		resp.Artifacts[stage] = payload
	}
	writeOK(w, resp)
}

func (s *Server) report(w http.ResponseWriter, r *http.Request) {
	tier := firstNonEmpty(r.URL.Query().Get("tier"), "small")
	reportData, err := s.App.DB.GetReport(tier)
	if err != nil {
		writeBizError(w, domain.NewBizError("E_STORE", "", "", 0, "%v", err))
		return
	}
	if reportData == nil {
		b, err := readFile(s.App.ReportPath(tier))
		if err != nil {
			writeBizError(w, domain.NewBizError(domain.ErrNotFound, "", "", 0, "report not found; run tier first"))
			return
		}
		var report domain.Report
		if err := json.Unmarshal(b, &report); err != nil {
			writeBizError(w, domain.NewBizError("E_REPORT", "", "", 0, "%v", err))
			return
		}
		reportData = &report
	}
	if class := r.URL.Query().Get("comparison"); class != "" {
		filtered := []domain.VerificationDetail{}
		for _, detail := range reportData.Details {
			if strings.EqualFold(detail.Comparison, class) {
				filtered = append(filtered, detail)
			}
		}
		writeOK(w, map[string]any{"tier": tier, "comparison": class, "count": len(filtered), "details": filtered})
		return
	}
	writeOK(w, reportData)
}

func readFile(path string) ([]byte, error) { return os.ReadFile(path) }

func (s *Server) reference(w http.ResponseWriter, r *http.Request) {
	tier := firstNonEmpty(r.URL.Query().Get("tier"), "small")
	b, err := os.ReadFile(s.App.ReferencePath(tier))
	if err != nil {
		writeBizError(w, domain.NewBizError(domain.ErrNotFound, "", "", 0, "reference not found; run tier first"))
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, _ = w.Write(b)
}

func (s *Server) strategy(w http.ResponseWriter, r *http.Request) {
	tier := firstNonEmpty(r.URL.Query().Get("tier"), "small")
	reportData, err := s.App.DB.GetReport(tier)
	if err == nil && reportData != nil {
		writeOK(w, reportData.Strategy)
		return
	}
	writeBizError(w, domain.NewBizError(domain.ErrNotFound, "", "", 0, "strategy unavailable; run tier first"))
}

func (s *Server) priorityDetails(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if q.Get("comparison") == "" {
		q.Set("comparison", "MISS")
	}
	r.URL.RawQuery = q.Encode()
	s.report(w, r)
}

type shardReplaceRequest struct {
	Tier   string `json:"tier"`
	Path   string `json:"path"`
	Source string `json:"source"`
	Demo   bool   `json:"demo"`
}

func (s *Server) replaceShard(w http.ResponseWriter, r *http.Request) {
	var req shardReplaceRequest
	if err := decodeBody(r, &req); err != nil {
		writeBizError(w, domain.NewBizError(domain.ErrInvalidConfig, "", "", 0, "%v", err))
		return
	}
	if req.Demo || req.Path == "" {
		result, bizErr := s.App.DemoReplaceShard(firstNonEmpty(req.Tier, "small"))
		if bizErr != nil {
			writeBizError(w, s.enrich(bizErr, req.Tier))
			return
		}
		writeOK(w, result)
		return
	}
	if req.Source == "" {
		req.Source = s.App.SamplePath(req.Path)
	}
	result, bizErr := s.App.ReplaceShardFile(req.Tier, req.Path, req.Source)
	if bizErr != nil {
		writeBizError(w, s.enrich(bizErr, req.Tier))
		return
	}
	writeOK(w, result)
}

func (s *Server) changeConfig(w http.ResponseWriter, r *http.Request) {
	var req app.ConfigChangeRequest
	if err := decodeBody(r, &req); err != nil {
		writeBizError(w, domain.NewBizError(domain.ErrInvalidConfig, "", "", 0, "%v", err))
		return
	}
	result, bizErr := s.App.ChangeConfig(req)
	if bizErr != nil {
		writeBizError(w, s.enrich(bizErr, req.Tier))
		return
	}
	writeOK(w, result)
}

type rollbackRequest struct {
	Tier    string `json:"tier"`
	Version int64  `json:"version"`
}

func (s *Server) rollbackConfig(w http.ResponseWriter, r *http.Request) {
	var req rollbackRequest
	if err := decodeBody(r, &req); err != nil {
		writeBizError(w, domain.NewBizError(domain.ErrInvalidConfig, "", "", 0, "%v", err))
		return
	}
	result, bizErr := s.App.RollbackConfig(req.Tier, req.Version)
	if bizErr != nil {
		writeBizError(w, s.enrich(bizErr, req.Tier))
		return
	}
	writeOK(w, result)
}

func (s *Server) replacePolicy(w http.ResponseWriter, r *http.Request) {
	var policy domain.Policy
	if err := decodeBody(r, &policy); err != nil {
		writeBizError(w, domain.NewBizError(domain.ErrInvalidConfig, "", "", 0, "%v", err))
		return
	}
	result, bizErr := s.App.ReplacePolicy(policy)
	if bizErr != nil {
		writeBizError(w, s.enrich(bizErr, policy.Tier))
		return
	}
	writeOK(w, result)
}

type publishRequest struct {
	TicketID          string          `json:"ticket_id"`
	Tier              string          `json:"tier"`
	Stage             string          `json:"stage"`
	InputFingerprint  string          `json:"input_fingerprint"`
	ConfigVersion     int64           `json:"config_version"`
	ConfigFingerprint string          `json:"config_fingerprint"`
	Artifact          json.RawMessage `json:"artifact"`
	Status            string          `json:"status"`
	ErrorCode         string          `json:"error_code"`
	ExpectedHead      *int64          `json:"expected_head"`
	ClientStageSeq    *int64          `json:"client_stage_seq"`
}

func (s *Server) publishCheckpoint(w http.ResponseWriter, r *http.Request) {
	var req publishRequest
	if err := decodeBody(r, &req); err != nil {
		writeBizError(w, domain.NewBizError(domain.ErrInvalidConfig, "", "", 0, "%v", err))
		return
	}
	result, bizErr := s.App.DB.PublishArtifact(store.PublishInput{
		TicketID: req.TicketID, Stage: req.Stage, InputFingerprint: req.InputFingerprint,
		ConfigVersion: req.ConfigVersion, ConfigFingerprint: req.ConfigFingerprint,
		Artifact: req.Artifact, Status: req.Status, ErrorCode: req.ErrorCode,
		ExpectedHead: req.ExpectedHead, ClientStageSeq: req.ClientStageSeq,
	})
	if bizErr != nil {
		tier := firstNonEmpty(req.Tier, r.URL.Query().Get("tier"))
		writeBizError(w, s.enrichPublishError(bizErr, req.TicketID, tier))
		return
	}
	writeOK(w, result)
}

func (s *Server) enrichPublishError(err *domain.BizError, ticketID, requestedTier string) *domain.BizError {
	if err == nil {
		return nil
	}
	tier := requestedTier
	if tier == "" && ticketID != "" {
		if row, loadErr := s.App.DB.GetTicket(ticketID); loadErr == nil && row != nil {
			tier = row.Tier
		}
	}
	if tier != "" {
		if policy, loadErr := s.App.DB.LatestPolicy(tier); loadErr == nil && policy != nil {
			err.ConfigVersion = policy.EffectiveVersion()
			if err.VersionFingerprint == "" {
				err.VersionFingerprint = policy.Fingerprint()
			}
		}
	}
	return s.enrich(err, tier)
}
