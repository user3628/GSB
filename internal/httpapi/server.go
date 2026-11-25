package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"nonceguard/internal/engine"
	"nonceguard/internal/model"
	"nonceguard/internal/peer"
	"nonceguard/internal/sample"
	"nonceguard/web"
)

type Server struct {
	Engine          *engine.Engine
	DefaultPeerAddr string
	StartedAtMs     int64
}

type errorEnvelope struct {
	OK      bool           `json:"ok"`
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
}

func NewServer(eng *engine.Engine, defaultPeerAddr string) *Server {
	return &Server{Engine: eng, DefaultPeerAddr: defaultPeerAddr, StartedAtMs: time.Now().UnixMilli()}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.root)
	mux.HandleFunc("/healthz", s.health)
	mux.HandleFunc("/v1/info", s.info)
	mux.HandleFunc("/v1/sample", s.getSample)
	mux.HandleFunc("/v1/config/current", s.currentConfig)
	mux.HandleFunc("/v1/config/versions/", s.configVersion)
	mux.HandleFunc("/v1/config/snapshots", s.configSnapshots)
	mux.HandleFunc("/v1/batches", s.batches)
	mux.HandleFunc("/v1/batches/", s.batchSubroutes)
	mux.HandleFunc("/v1/decisions", s.decisions)
	mux.HandleFunc("/v1/nonces", s.nonces)
	mux.HandleFunc("/v1/nonces/reclaim", s.reclaimNonces)
	mux.HandleFunc("/v1/recovery", s.recovery)
	mux.HandleFunc("/v1/recovery/run", s.recoveryRun)

	peerServer := peer.NewServer("gate-in-process-loopback-peer")
	mux.Handle("/peer/", peerServer.Mux)
	return logging(mux)
}

func logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		_ = start
	})
}

func (s *Server) root(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(web.IndexHTML)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeData(w, http.StatusOK, map[string]any{"status": "ok", "service": "nonceguard", "time_ms": s.Engine.Clock()})
}

func (s *Server) info(w http.ResponseWriter, r *http.Request) {
	cfg, err := s.Engine.ActiveConfig(r.Context())
	if err != nil {
		writeEngineFailure(w, engine.NewFailure(model.CodeInternal, "config unavailable", map[string]any{"error": err.Error()}))
		return
	}
	writeData(w, 200, map[string]any{
		"service":           "NonceGuard request freshness and nonce dedup engine",
		"active_config":     cfg,
		"boundary_policy":   model.BoundaryDescription(),
		"default_peer_addr": s.DefaultPeerAddr,
		"fixed_demo_seed":   "nonceguard.internal-proxy-gateway.seed.v1",
		"started_at_ms":     s.StartedAtMs,
		"server_clock_ms":   s.Engine.Clock(),
		"runtime_note":      "Use reference_ms for repeatable fixed-sample adjudication; monotonic high-water prevents rollback.",
	})
}

func (s *Server) getSample(w http.ResponseWriter, r *http.Request) {
	ref := int64Query(r, "reference_ms", sample.DemoReferenceMs)
	scenario := r.URL.Query().Get("scenario")
	if scenario == "" {
		scenario = "canonical"
	}
	switch scenario {
	case "canonical":
		batch := sample.CanonicalBatch(ref)
		writeData(w, 200, map[string]any{"scenario": scenario, "batch": batch, "expected_codes": expectedCanonicalCodes()})
	case "valid":
		count := intQuery(r, "count", 24)
		batchID := r.URL.Query().Get("batch_id")
		if batchID == "" {
			batchID = "demo-valid-batch"
		}
		writeData(w, 200, map[string]any{"scenario": scenario, "batch": sample.ValidBatch(batchID, count, ref)})
	case "invalid":
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(sample.InvalidBatchJSON())
	case "recycle":
		first, active, boundary, expired := sample.RecycleBatches(ref)
		writeData(w, 200, map[string]any{
			"scenario": scenario,
			"first":    first, "active_repeat": active, "boundary_repeat": boundary, "expired_repeat": expired,
			"expected": map[string]string{"active_repeat": model.CodeNonceReplayed, "boundary_repeat": model.CodeOK + " (recycled at right-exclusive expiry)", "expired_repeat": model.CodeOK + " (recycled)"},
		})
	case "config_v2":
		writeData(w, 200, sample.ConfigVersionTwo(ref))
	default:
		writeFailure(w, engine.NewFailure(model.CodeRequestInvalid, "unknown sample scenario", map[string]any{"scenario": scenario, "supported": []string{"canonical", "valid", "invalid", "recycle", "config_v2"}}))
	}
}

func expectedCanonicalCodes() []map[string]any {
	return []map[string]any{
		{"request_id": "req-ok-alpha", "code": model.CodeOK},
		{"request_id": "req-ok-beta", "code": model.CodeOK},
		{"request_id": "req-bad-signature", "code": model.CodeSignatureMismatch},
		{"request_id": "req-timestamp-early", "code": model.CodeTimestampEarly},
		{"request_id": "req-timestamp-late", "code": model.CodeTimestampLate},
		{"request_id": "req-nonce-replay-alpha", "code": model.CodeNonceReplayed},
		{"request_id": "req-ok-gamma", "code": model.CodeOK},
		{"request_id": "req-ok-alpha", "code": model.CodeOK, "idempotent_reuse": true},
		{"request_id": "req-ok-beta", "code": model.CodeConflictingRequest},
		{"request_id": "req-fast-delta", "code": model.CodeOK},
	}
}

func (s *Server) currentConfig(w http.ResponseWriter, r *http.Request) {
	cfg, err := s.Engine.ActiveConfig(r.Context())
	if err != nil {
		writeEngineFailure(w, engine.NewFailure(model.CodeInternal, "config unavailable", map[string]any{"error": err.Error()}))
		return
	}
	writeData(w, 200, cfg)
}

func (s *Server) configVersion(w http.ResponseWriter, r *http.Request) {
	versionText := strings.TrimPrefix(r.URL.Path, "/v1/config/versions/")
	version, err := strconv.ParseInt(versionText, 10, 64)
	if err != nil || version <= 0 {
		writeFailure(w, engine.NewFailure(model.CodeRequestInvalid, "invalid config version", map[string]any{"version": versionText}))
		return
	}
	cfg, ferr := s.Engine.GetConfig(r.Context(), version)
	if ferr != nil {
		writeFailure(w, engine.NewFailure(model.CodeNotFound, "config version not found", map[string]any{"config_version": version}))
		return
	}
	writeData(w, 200, cfg)
}

func (s *Server) configSnapshots(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		cfgs, err := s.Engine.ListConfigs(r.Context())
		if err != nil {
			writeEngineFailure(w, engine.NewFailure(model.CodeInternal, "cannot list configs", map[string]any{"error": err.Error()}))
			return
		}
		writeData(w, 200, map[string]any{"configs": cfgs})
	case http.MethodPost:
		if r.URL.Query().Get("fault") == "version_gap" {
			current, err := s.Engine.ActiveConfig(r.Context())
			if err != nil {
				writeEngineFailure(w, engine.NewFailure(model.CodeInternal, "active config unavailable", map[string]any{"error": err.Error()}))
				return
			}
			gapped := sample.GappedConfig(current)
			_, ferr := s.Engine.SubmitConfig(r.Context(), gapped)
			if ferr == nil {
				ferr = engine.NewFailure(model.CodeInternal, "version gap was unexpectedly accepted", nil)
			}
			writeFailure(w, ferr)
			return
		}
		var cfg model.Snapshot
		if r.Body != nil {
			dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
			if err := dec.Decode(&cfg); err != nil {
				if r.URL.Query().Get("template") == "v2" {
					cfg = sample.ConfigVersionTwo(s.Engine.Clock())
				} else {
					writeFailure(w, engine.NewFailure(model.CodeRequestInvalid, "invalid config JSON", map[string]any{"error": err.Error()}))
					return
				}
			}
		}
		if r.URL.Query().Get("template") == "v2" && cfg.Version == 0 {
			cfg = sample.ConfigVersionTwo(s.Engine.Clock())
		}
		saved, ferr := s.Engine.SubmitConfig(r.Context(), cfg)
		if ferr != nil {
			writeFailure(w, ferr)
			return
		}
		writeData(w, 200, map[string]any{"submitted": saved, "active": saved.Version, "replaced_at_ms": s.Engine.Clock()})
	default:
		methodNotAllowed(w)
	}
}

func (s *Server) batches(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	raw, err := readBody(r, 8<<20)
	if err != nil {
		writeFailure(w, engine.NewFailure(model.CodeRequestInvalid, "cannot read batch body", map[string]any{"error": err.Error()}))
		return
	}
	batch, _, perr := model.ParseBatch(raw)
	if perr != nil {
		writeFailure(w, engine.NewFailure(perr.Code, "batch parse failed; no record was persisted", map[string]any{"issues": perr.Issues, "parsed_records": len(perr.Issues)}))
		return
	}
	q := r.URL.Query()
	mode := q.Get("mode")
	if mode == "" {
		mode = batch.Mode
	}
	if mode == "" {
		mode = model.ModeFull
	}
	ref := int64Query(r, "reference_ms", batch.ReferenceMs)
	configVersion := int64Query(r, "config_version", batch.ConfigVersion)

	if fault := q.Get("fault"); strings.HasPrefix(fault, "peer_") {
		s.handlePeerFault(w, r, fault)
		return
	}

	switch mode {
	case model.ModeFast:
		limit := intQuery(r, "precheck_limit", 4)
		res, ferr := s.Engine.FastCheck(r.Context(), *batch, engine.FastOptions{ConfigVersion: configVersion, ReferenceMs: ref, PrecheckLimit: limit})
		if ferr != nil {
			writeFailure(w, ferr)
			return
		}
		writeData(w, 200, res)
	case "compare":
		limit := intQuery(r, "precheck_limit", 6)
		fast, ferr := s.Engine.FastCheck(r.Context(), *batch, engine.FastOptions{ConfigVersion: configVersion, ReferenceMs: ref, PrecheckLimit: limit})
		if ferr != nil {
			writeFailure(w, ferr)
			return
		}
		full, ferr := s.Engine.AdjudicateBatch(r.Context(), *batch, engine.AdjudicateOptions{
			ConfigVersion: configVersion, ReferenceMs: ref,
			RecordDelayMs: int64Query(r, "record_delay_ms", 0),
		})
		if ferr != nil {
			writeFailure(w, ferr)
			return
		}
		full.Comparison = engine.CompareResults(fast, full)
		writeData(w, 200, map[string]any{"fast": fast, "full": full, "comparison": full.Comparison})
	default:
		fault := q.Get("fault")
		opts := engine.AdjudicateOptions{ConfigVersion: configVersion, ReferenceMs: ref, RecordDelayMs: int64Query(r, "record_delay_ms", 0), Fault: fault}
		res, ferr := s.Engine.AdjudicateBatch(r.Context(), *batch, opts)
		if ferr != nil {
			writeFailure(w, ferr)
			return
		}
		if q.Get("compare") == "1" || q.Get("compare") == "true" {
			limit := intQuery(r, "precheck_limit", 6)
			fast, fastErr := s.Engine.FastCheck(r.Context(), *batch, engine.FastOptions{ConfigVersion: configVersion, ReferenceMs: ref, PrecheckLimit: limit})
			if fastErr == nil {
				res.Comparison = engine.CompareResults(fast, res)
				writeData(w, 200, map[string]any{"full": res, "fast": fast, "comparison": res.Comparison})
				return
			}
		}
		writeData(w, 200, res)
	}
}

func (s *Server) handlePeerFault(w http.ResponseWriter, r *http.Request, fault string) {
	q := r.URL.Query()
	check := q.Get("peer_check")
	if check == "nonce" {
		check = "nonce-query"
	} else {
		check = "signature-check"
	}
	addr := q.Get("peer_addr")
	if addr == "" {
		addr = s.DefaultPeerAddr
	}
	defaultTimeoutMs := 250
	if fault == "peer_late_ack" && r.URL.Query().Get("peer_timeout_ms") == "" {
		defaultTimeoutMs = 3000
	}
	timeoutMs := intQuery(r, "peer_timeout_ms", defaultTimeoutMs)
	client := peer.NewClient(addr, time.Duration(timeoutMs)*time.Millisecond)
	if ackDeadline := intQuery(r, "peer_ack_deadline_ms", 0); ackDeadline > 0 {
		client.AckDeadline = time.Duration(ackDeadline) * time.Millisecond
	} else if fault == "peer_late_ack" {
		client.AckDeadline = 300 * time.Millisecond
	} else {
		client.AckDeadline = 30 * time.Second
	}
	delay := int64Query(r, "peer_delay_ms", 0)
	if delay == 0 {
		switch fault {
		case "peer_timeout":
			delay = 1_500
		case "peer_late_ack":
			delay = 800
		}
	}
	if fault == "peer_refused" && !strings.Contains(addr, ":") {
		addr = "127.0.0.1:" + addr
		client = peer.NewClient(addr, time.Duration(timeoutMs)*time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(maxInt(timeoutMs+1000, 3000))*time.Millisecond)
	defer cancel()
	ack, pf := client.Confirm(ctx, peer.ConfirmRequest{CheckType: check, Behavior: fault, DelayMs: delay, RequestID: q.Get("request_id"), ClientID: q.Get("client_id"), Nonce: q.Get("nonce")})
	if pf == nil {
		if fault == "peer_timeout" {
			writeFailure(w, engine.NewFailure(model.CodeInternal, "peer timeout injection did not time out", map[string]any{"peer_addr": addr, "ack": ack}))
			return
		}
		writeData(w, 200, map[string]any{"ack": ack})
		return
	}
	if pf.Context == nil {
		pf.Context = map[string]any{}
	}
	pf.Context["injected_fault"] = fault
	pf.Context["runtime_parameter"] = "fault=" + fault
	if ack != nil {
		pf.Context["late_ack"] = ack
	}
	writeFailure(w, &engine.Failure{Code: pf.Code, Message: pf.Message, Context: pf.Context})
}

func (s *Server) batchSubroutes(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/batches/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	batchID := parts[0]
	if len(parts) == 2 && parts[1] == "resume" && r.Method == http.MethodPost {
		view, err := s.Engine.GetBatch(r.Context(), batchID)
		if err != nil {
			writeFailure(w, engine.NewFailure(model.CodeNotFound, "batch not found or not durably stored", map[string]any{"batch_id": batchID}))
			return
		}
		_ = view
		var batch model.Batch
		if err := json.Unmarshal([]byte(storedBatchJSON(s, r, batchID)), &batch); err != nil {
			writeFailure(w, engine.NewFailure(model.CodeInternal, "cannot reconstruct stored batch", map[string]any{"error": err.Error()}))
			return
		}
		ref := int64Query(r, "reference_ms", batch.ReferenceMs)
		res, ferr := s.Engine.AdjudicateBatch(r.Context(), batch, engine.AdjudicateOptions{ReferenceMs: ref, RecordDelayMs: int64Query(r, "record_delay_ms", 0)})
		if ferr != nil {
			writeFailure(w, ferr)
			return
		}
		writeData(w, 200, res)
		return
	}
	if len(parts) != 1 || r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	view, err := s.Engine.GetBatch(r.Context(), batchID)
	if err != nil {
		writeFailure(w, engine.NewFailure(model.CodeNotFound, "batch not found", map[string]any{"batch_id": batchID}))
		return
	}
	writeData(w, 200, view)
}

func storedBatchJSON(s *Server, r *http.Request, batchID string) string {
	var raw string
	err := s.Engine.Store.DB.QueryRowContext(r.Context(), `SELECT request_json FROM batch_jobs WHERE batch_id=?`, batchID).Scan(&raw)
	if err != nil {
		return ""
	}
	return raw
}

func (s *Server) decisions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	cfgVersion := int64Query(r, "config_version", 0)
	items, err := s.Engine.ListDecisions(r.Context(), cfgVersion, r.URL.Query().Get("batch_id"), r.URL.Query().Get("request_id"), intQuery(r, "limit", 200))
	if err != nil {
		writeFailure(w, engine.NewFailure(model.CodeInternal, "cannot query decisions", map[string]any{"error": err.Error()}))
		return
	}
	writeData(w, 200, map[string]any{"config_version": cfgVersion, "count": len(items), "decisions": items})
}

func (s *Server) nonces(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	cfgVersion := int64Query(r, "config_version", 0)
	items, err := s.Engine.ListOccupancy(r.Context(), cfgVersion, r.URL.Query().Get("client_id"), intQuery(r, "limit", 200))
	if err != nil {
		writeFailure(w, engine.NewFailure(model.CodeInternal, "cannot query occupancy", map[string]any{"error": err.Error()}))
		return
	}
	writeData(w, 200, map[string]any{"config_version": cfgVersion, "count": len(items), "occupancy": items, "boundary_policy": model.BoundaryDescription()})
}

func (s *Server) reclaimNonces(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	cfgVersion := int64Query(r, "config_version", 0)
	asOf := int64Query(r, "as_of_ms", 0)
	report, ferr := s.Engine.ReclaimExpiredNonces(r.Context(), cfgVersion, asOf)
	if ferr != nil {
		writeFailure(w, ferr)
		return
	}
	writeData(w, 200, report)
}

func (s *Server) recovery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	rep, err := s.Engine.Store.LatestRecovery(r.Context())
	if err != nil {
		writeFailure(w, engine.NewFailure(model.CodeNotFound, "no recovery report yet", map[string]any{"hint": "POST /v1/recovery/run or restart the process"}))
		return
	}
	history, _ := s.Engine.Store.RecoveryHistory(r.Context())
	writeData(w, 200, map[string]any{"latest": rep, "history": history})
}

func (s *Server) recoveryRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	rep, err := s.Engine.RunRecovery(r.Context())
	if err != nil {
		writeFailure(w, engine.NewFailure(model.CodeInternal, "recovery scan failed", map[string]any{"error": err.Error()}))
		return
	}
	writeData(w, 200, rep)
}

func writeData(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "data": data})
}

func writeFailure(w http.ResponseWriter, f *engine.Failure) {
	if f == nil {
		f = engine.NewFailure(model.CodeInternal, "unknown failure", nil)
	}
	status := model.HTTPStatus(f.Code)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorEnvelope{OK: false, Code: f.Code, Message: f.Message, Context: f.Context})
}

func writeEngineFailure(w http.ResponseWriter, f *engine.Failure) { writeFailure(w, f) }

func methodNotAllowed(w http.ResponseWriter) {
	writeFailure(w, engine.NewFailure(model.CodeMethodNotAllowed, "method not allowed", nil))
}

func readBody(r *http.Request, max int64) ([]byte, error) {
	defer r.Body.Close()
	return io.ReadAll(io.LimitReader(r.Body, max))
}

func intQuery(r *http.Request, key string, def int) int {
	v := r.URL.Query().Get(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func int64Query(r *http.Request, key string, def int64) int64 {
	v := r.URL.Query().Get(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return n
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
