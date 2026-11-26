package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"example.com/gsb/dnscache/internal/dnsmsg"
	"example.com/gsb/dnscache/internal/errs"
	"example.com/gsb/dnscache/internal/resolver"
	"example.com/gsb/dnscache/internal/scheduler"
	"example.com/gsb/dnscache/internal/store"
)

type Server struct {
	store   *store.Store
	sched   *scheduler.Scheduler
	metrics *resolver.Metrics
	logger  *log.Logger
	http    *http.Server
	ln      net.Listener
}

func New(st *store.Store, sched *scheduler.Scheduler, metrics *resolver.Metrics, logger *log.Logger) *Server {
	if logger == nil {
		logger = log.Default()
	}
	s := &Server{store: st, sched: sched, metrics: metrics, logger: logger}
	s.http = &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	return s
}

func (s *Server) Start(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.ln = ln
	s.logger.Printf("admin API listening on http://%s", ln.Addr())
	go func() {
		if err := s.http.Serve(ln); err != nil && err != http.ErrServerClosed {
			s.logger.Printf("admin API stopped: %v", err)
		}
	}()
	return nil
}

func (s *Server) Addr() string {
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

func (s *Server) Shutdown(ctx context.Context) error { return s.http.Shutdown(ctx) }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "status": "healthy"})
	})
	mux.HandleFunc("GET /api/zones", s.listZones)
	mux.HandleFunc("GET /api/zones/{zone}", s.getZone)
	mux.HandleFunc("GET /api/zones/{zone}/derived", s.getDerived)
	mux.HandleFunc("POST /api/zones/snapshot", s.submitSnapshot)
	mux.HandleFunc("POST /api/zones/delta", s.submitDelta)
	mux.HandleFunc("GET /api/cache", s.listCache)
	mux.HandleFunc("DELETE /api/cache", s.deleteCache)
	mux.HandleFunc("POST /api/cache/cleanup", s.cleanupCache)
	mux.HandleFunc("GET /api/metrics", s.getMetrics)
	mux.HandleFunc("GET /api/recovery", s.getRecovery)
	return mux
}

func (s *Server) listZones(w http.ResponseWriter, r *http.Request) {
	zones, err := s.store.ListZones(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "zones": zones})
}

func (s *Server) getZone(w http.ResponseWriter, r *http.Request) {
	zone := r.PathValue("zone")
	state, err := s.store.ZoneState(r.Context(), zone)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "zone": state})
}

func (s *Server) getDerived(w http.ResponseWriter, r *http.Request) {
	rows, err := s.store.DerivedRows(r.Context(), r.PathValue("zone"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "derived": rows})
}

func (s *Server) submitSnapshot(w http.ResponseWriter, r *http.Request) {
	var snap store.ZoneSnapshot
	if err := decodeJSON(r, &snap); err != nil {
		writeError(w, err)
		return
	}
	batchID := requestID(r)
	result, err := s.store.SubmitSnapshot(r.Context(), snap, batchID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "result": result})
}

func (s *Server) submitDelta(w http.ResponseWriter, r *http.Request) {
	var delta store.ZoneDelta
	if err := decodeJSON(r, &delta); err != nil {
		writeError(w, err)
		return
	}
	batchID := requestID(r)
	if batchID == "" {
		batchID = delta.DeltaID
	}
	result, err := s.store.SubmitDelta(r.Context(), delta, batchID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "result": result})
}

func (s *Server) listCache(w http.ResponseWriter, r *http.Request) {
	qname := r.URL.Query().Get("name")
	if qname != "" {
		typ, ok := dnsmsg.ParseType(r.URL.Query().Get("type"))
		if !ok {
			writeError(w, errs.New(errs.EInvalidRequest, "invalid DNS type"))
			return
		}
		hit, err := s.store.Lookup(r.Context(), qname, typ)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "lookup": hit})
		return
	}
	includeExpired, _ := strconv.ParseBool(r.URL.Query().Get("include_expired"))
	entries, err := s.store.ListCache(r.Context(), includeExpired)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "entries": entries})
}

func (s *Server) deleteCache(w http.ResponseWriter, r *http.Request) {
	qname := r.URL.Query().Get("name")
	typ, ok := dnsmsg.ParseType(r.URL.Query().Get("type"))
	if qname == "" || !ok {
		writeError(w, errs.New(errs.EInvalidRequest, "name and valid type query parameters are required"))
		return
	}
	deleted, err := s.store.DeleteCache(r.Context(), qname, typ)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "deleted": deleted})
}

func (s *Server) cleanupCache(w http.ResponseWriter, r *http.Request) {
	n, err := s.store.CleanupExpired(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "removed": n})
}

func (s *Server) getMetrics(w http.ResponseWriter, r *http.Request) {
	storeStats, err := s.store.Stats(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	out := map[string]any{
		"ok":        true,
		"resolver":  s.metrics.Snapshot(),
		"scheduler": s.sched.Stats(),
		"cache":     storeStats,
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) getRecovery(w http.ResponseWriter, r *http.Request) {
	report, err := s.store.LatestRecoveryReport(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "report": report})
}

func requestID(r *http.Request) string {
	if id := strings.TrimSpace(r.Header.Get("X-Request-ID")); id != "" {
		return id
	}
	return strings.TrimSpace(r.Header.Get("X-Batch-ID"))
}

func decodeJSON(r *http.Request, out any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(io.LimitReader(r.Body, 8<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return errs.Wrap(errs.EInvalidRequest, err, "invalid JSON request")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	code := errs.CodeOf(err)
	status := http.StatusInternalServerError
	switch code {
	case errs.EInvalidRequest, errs.EChecksumRequired, errs.EMessageMalformed:
		status = http.StatusBadRequest
	case errs.EChecksumMismatch, errs.ENegativeTTLUndefined:
		status = http.StatusUnprocessableEntity
	case errs.EStaleBase, errs.EConflictingRequest:
		status = http.StatusConflict
	case errs.EZoneNotFound, errs.ENotFound:
		status = http.StatusNotFound
	case errs.EUnsupportedType:
		status = http.StatusUnprocessableEntity
	case errs.EUpstreamQueueFull:
		status = http.StatusTooManyRequests
	}
	writeJSON(w, status, map[string]any{
		"ok":      false,
		"code":    code,
		"message": errs.MessageOf(err),
		"error":   fmt.Sprintf("%v", err),
	})
}
