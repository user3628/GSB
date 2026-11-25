package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync"

	"tm-sync/internal/service"
	"tm-sync/internal/tm"
)

type Server struct {
	Svc    *service.Service
	mux    *http.ServeMux
	stopCh chan struct{}
	once   sync.Once
}

func NewServer(svc *service.Service) *Server {
	s := &Server{Svc: svc, mux: http.NewServeMux(), stopCh: make(chan struct{})}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler        { return s.mux }
func (s *Server) StopChannel() <-chan struct{} { return s.stopCh }

func (s *Server) RequestStop() {
	s.once.Do(func() { close(s.stopCh) })
}

func (s *Server) routes() {
	s.mux.HandleFunc("/healthz", s.health)
	s.mux.HandleFunc("/api/v1/sources/import", s.importSources)
	s.mux.HandleFunc("/api/v1/increments/import", s.importIncrement)
	s.mux.HandleFunc("/api/v1/sync", s.sync)
	s.mux.HandleFunc("/api/v1/sync/stage", s.syncStage)
	s.mux.HandleFunc("/api/v1/recover", s.recover)
	s.mux.HandleFunc("/api/v1/stages", s.status)
	s.mux.HandleFunc("/api/v1/status", s.status)
	s.mux.HandleFunc("/api/v1/checkpoint", s.checkpointDetail)
	s.mux.HandleFunc("/api/v1/versions/fingerprint", s.status)
	s.mux.HandleFunc("/api/v1/versions/records", s.versionRecord)
	s.mux.HandleFunc("/api/v1/suggestions", s.suggestions)
	s.mux.HandleFunc("/api/v1/suggestions/export", s.export)
	s.mux.HandleFunc("/api/v1/units", s.units)
	s.mux.HandleFunc("/api/v1/units/export", s.units)
	s.mux.HandleFunc("/api/v1/export", s.export)
	s.mux.HandleFunc("/api/v1/publish", s.publish)
	s.mux.HandleFunc("/api/v1/admin/stop", s.stop)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	var biz *tm.BizError
	if errors.As(err, &biz) {
		status := biz.HTTPStatus
		if status == 0 {
			status = http.StatusUnprocessableEntity
		}
		writeJSON(w, status, map[string]any{"error": biz})
		return
	}
	writeJSON(w, http.StatusInternalServerError, map[string]any{"error": map[string]any{
		"code": "E_INTERNAL", "message": err.Error(),
	}})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "service": "tm-sync"})
}

func readBody(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	return io.ReadAll(io.LimitReader(r.Body, 64<<20))
}

func (s *Server) importSources(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, methodError(r))
		return
	}
	body, err := readBody(r)
	if err != nil {
		writeError(w, err)
		return
	}
	version := int64(1)
	if v := r.URL.Query().Get("version"); v != "" {
		version, err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			writeError(w, &tm.BizError{Code: "E_INVALID_VERSION", Message: err.Error(), Stage: "import", HTTPStatus: 400})
			return
		}
	}
	out, err := s.Svc.ImportSources(r.Context(), body, version)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) importIncrement(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, methodError(r))
		return
	}
	body, err := readBody(r)
	if err != nil {
		writeError(w, err)
		return
	}
	out, err := s.Svc.ApplyIncrement(r.Context(), body)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func decodeRequest(r *http.Request, dst any) error {
	body, err := readBody(r)
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return nil
	}
	if err := json.Unmarshal(body, dst); err != nil {
		return &tm.BizError{Code: "E_INVALID_JSON", Message: err.Error(), Stage: "http", HTTPStatus: 400}
	}
	return nil
}

func (s *Server) sync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, methodError(r))
		return
	}
	var req service.SyncRequest
	if err := decodeRequest(r, &req); err != nil {
		writeError(w, err)
		return
	}
	out, err := s.Svc.Sync(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	if req.SegmentID != "" && len(out.FailedSegments) > 0 {
		writeError(w, out.FailedSegments[0].Error)
		return
	}
	status := http.StatusOK
	if len(out.FailedSegments) > 0 {
		status = http.StatusMultiStatus
	}
	writeJSON(w, status, out)
}

func (s *Server) recover(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, methodError(r))
		return
	}
	out, err := s.Svc.Recover(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) syncStage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, methodError(r))
		return
	}
	var req service.StageRequest
	if err := decodeRequest(r, &req); err != nil {
		writeError(w, err)
		return
	}
	out, err := s.Svc.RunStage(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "result": out})
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	out, err := s.Svc.Status(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) checkpointDetail(w http.ResponseWriter, r *http.Request) {
	out, err := s.Svc.CheckpointDetail(r.Context(),
		r.URL.Query().Get("stage"),
		r.URL.Query().Get("segment_id"),
		r.URL.Query().Get("unit_id"),
	)
	if err != nil {
		writeError(w, err)
		return
	}
	if out == nil {
		writeError(w, &tm.BizError{Code: "E_CHECKPOINT_NOT_FOUND", Message: "no active checkpoint matches query", Stage: r.URL.Query().Get("stage"), HTTPStatus: 404})
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) versionRecord(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, methodError(r))
		return
	}
	var req service.VersionRecordRequest
	if err := decodeRequest(r, &req); err != nil {
		writeError(w, err)
		return
	}
	out, err := s.Svc.RecordVersion(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) suggestions(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	includeInvalid := true
	if raw := r.URL.Query().Get("invalid"); raw != "" {
		if parsed, err := strconv.ParseBool(raw); err == nil {
			includeInvalid = parsed
		} else if raw == "0" || raw == "false" {
			includeInvalid = false
		}
	}
	if raw := r.URL.Query().Get("include_invalid"); raw != "" {
		if parsed, err := strconv.ParseBool(raw); err == nil {
			includeInvalid = parsed
		}
	}
	out, err := s.Svc.SearchSuggestions(r.Context(), q, includeInvalid)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) units(w http.ResponseWriter, r *http.Request) {
	_, body, err := s.Svc.ExportBundle(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	if r.URL.Query().Get("format") == "jsonl" {
		w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		bundle, _, err := s.Svc.ExportBundle(r.Context())
		if err != nil {
			return
		}
		enc := json.NewEncoder(w)
		for _, u := range bundle.Units {
			_ = enc.Encode(u)
		}
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (s *Server) export(w http.ResponseWriter, r *http.Request) {
	_, body, err := s.Svc.ExportBundle(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (s *Server) publish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, methodError(r))
		return
	}
	var req service.PublishRequest
	if err := decodeRequest(r, &req); err != nil {
		writeError(w, err)
		return
	}
	out, err := s.Svc.Publish(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) stop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, methodError(r))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"stopping": true})
	go s.RequestStop()
}

func methodError(r *http.Request) error {
	return &tm.BizError{Code: "E_METHOD_NOT_ALLOWED", Message: r.Method + " not allowed", Stage: "http", HTTPStatus: http.StatusMethodNotAllowed}
}
