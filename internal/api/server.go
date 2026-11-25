package api

import (
	"encoding/json"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	webfs "regworkbench/internal/api/web"
	"regworkbench/internal/config"
	"regworkbench/internal/dataset"
	"regworkbench/internal/experiment"
	"regworkbench/internal/workerr"
)

type Server struct {
	Root     string
	Samples  string
	mu       sync.Mutex
	sessions map[string]*experiment.Session
}

func NewServer(root string) (*Server, error) {
	if err := os.MkdirAll(filepath.Join(root, "sessions"), 0o755); err != nil {
		return nil, err
	}
	samples := filepath.Join(root, "samples")
	return &Server{Root: root, Samples: samples, sessions: map[string]*experiment.Session{}}, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", s.health)
	mux.HandleFunc("POST /api/datasets/generate", s.generate)
	mux.HandleFunc("GET /api/datasets", s.listDatasets)
	mux.HandleFunc("POST /api/sessions", s.createSession)
	mux.HandleFunc("GET /api/sessions", s.listSessions)
	mux.HandleFunc("GET /api/sessions/{id}", s.getSession)
	mux.HandleFunc("POST /api/sessions/{id}/run", s.runSession)
	mux.HandleFunc("POST /api/sessions/{id}/start", s.startSession)
	mux.HandleFunc("POST /api/sessions/{id}/stop", s.stopSession)
	mux.HandleFunc("POST /api/sessions/{id}/resume", s.resumeSession)
	mux.HandleFunc("POST /api/sessions/{id}/incremental", s.incremental)
	mux.HandleFunc("GET /api/sessions/{id}/query", s.query)
	mux.HandleFunc("GET /api/sessions/{id}/export", s.exportFile)
	mux.Handle("GET /", http.FileServer(http.FS(mustWebFS())))
	return jsonMiddleware(mux)
}

func jsonMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(r.URL.Path) >= 4 && r.URL.Path[:4] == "/api" {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "service": "regworkbench", "root": s.Root})
}

func (s *Server) generate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Seed int64 `json:"seed"`
	}
	_ = decodeBody(r, &req)
	manifest, err := dataset.GenerateDirectory(s.Samples, req.Seed)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, manifest)
}

func (s *Server) listDatasets(w http.ResponseWriter, r *http.Request) {
	m, err := dataset.LoadManifest(filepath.Join(s.Samples, dataset.ManifestName))
	if err != nil {
		writeError(w, workerr.Wrap(workerr.ENotFound, err))
		return
	}
	writeJSON(w, http.StatusOK, m)
}

type createRequest struct {
	ID         string        `json:"id"`
	DatasetKey string        `json:"dataset_key"`
	Config     config.Config `json:"config"`
}

func (s *Server) createSession(w http.ResponseWriter, r *http.Request) {
	var req createRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, workerr.New(workerr.EConfig, err.Error()))
		return
	}
	if req.DatasetKey == "" {
		req.DatasetKey = "small"
	}
	id := req.ID
	if id == "" {
		id = req.DatasetKey + "-session"
	}
	dir := filepath.Join(s.Root, "sessions", id)
	ds, err := dataset.LoadDataset(filepath.Join(s.Samples, dataset.ManifestName), req.DatasetKey)
	if err != nil {
		writeError(w, workerr.Wrap(workerr.ENotFound, err))
		return
	}
	cfg := req.Config
	cfg.DatasetKey = req.DatasetKey
	normalized, err := cfg.Normalize()
	if err != nil {
		writeError(w, err)
		return
	}
	sess, err := experiment.CreateSession(dir, normalized, ds, experiment.Hooks{})
	if err != nil {
		writeError(w, err)
		return
	}
	s.mu.Lock()
	s.sessions[id] = sess
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, sess.Snapshot())
}

func (s *Server) getSession(w http.ResponseWriter, r *http.Request) {
	sess, err := s.session(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sess.Snapshot())
}

func (s *Server) listSessions(w http.ResponseWriter, r *http.Request) {
	root := filepath.Join(s.Root, "sessions")
	entries, _ := os.ReadDir(root)
	out := []map[string]any{}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if sess, err := s.session(e.Name()); err == nil {
			out = append(out, map[string]any{"id": e.Name(), "state": sess.Snapshot().State, "config_version": sess.ConfigVer})
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"sessions": out})
}

func (s *Server) runSession(w http.ResponseWriter, r *http.Request) {
	sess, err := s.session(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	rep, err := sess.Run(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

func (s *Server) startSession(w http.ResponseWriter, r *http.Request) {
	sess, err := s.session(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	go func() { _, _ = sess.Run(nilContext()) }()
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true, "id": r.PathValue("id")})
}

func (s *Server) stopSession(w http.ResponseWriter, r *http.Request) {
	sess, err := s.session(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	if err := sess.Store.RequestStop(); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"stop_requested": true, "id": r.PathValue("id")})
}

func (s *Server) resumeSession(w http.ResponseWriter, r *http.Request) {
	sess, err := s.session(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	if err := sess.Store.ClearStop(); err != nil {
		writeError(w, err)
		return
	}
	go func() { _, _ = sess.Run(nilContext()) }()
	writeJSON(w, http.StatusAccepted, map[string]any{"resume_started": true, "id": r.PathValue("id")})
}

type incrementalRequest struct {
	NewID  string        `json:"new_id"`
	Config config.Config `json:"config"`
}

func (s *Server) incremental(w http.ResponseWriter, r *http.Request) {
	base, err := s.session(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	var req incrementalRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, workerr.New(workerr.EConfig, err.Error()))
		return
	}
	newID := req.NewID
	if newID == "" {
		newID = r.PathValue("id") + "-incremental"
	}
	newRoot := filepath.Join(s.Root, "sessions", newID)
	next, rep, plan, err := experiment.RunIncremental(r.Context(), base, req.Config, newRoot, experiment.Hooks{})
	if err != nil {
		writeError(w, err)
		return
	}
	s.mu.Lock()
	s.sessions[newID] = next
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"plan": plan, "report": rep, "snapshot": next.Snapshot()})
}

func (s *Server) query(w http.ResponseWriter, r *http.Request) {
	sess, err := s.session(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	q := r.URL.Query()
	req := experiment.QueryRequest{Kind: q.Get("kind"), QueryID: q.Get("query_id")}
	if req.Kind == "" {
		req.Kind = "state"
	}
	if fold := queryInt(q, "fold"); fold != nil {
		req.Fold = fold
	}
	if v := queryFloat(q, "lambda"); v != nil {
		req.Lambda = v
	}
	if v := queryFloat(q, "l1_ratio"); v != nil {
		req.L1Ratio = v
	}
	if b := queryInt64(q, "budget_ms"); b != nil {
		req.BudgetMS = b
	}
	resp, err := sess.Query(req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) exportFile(w http.ResponseWriter, r *http.Request) {
	sess, err := s.session(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	kind := r.URL.Query().Get("kind")
	if kind == "" {
		kind = "coefficients"
	}
	name := kind + ".json"
	if kind == "summary" {
		name = "validation_summary.json"
	}
	if kind == "trajectory" {
		name = "trajectory.json"
	}
	path := filepath.Join(sess.Root, "exports", name)
	if kind == "coefficients" {
		path = filepath.Join(sess.Root, "exports", "coefficients.json")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		writeError(w, workerr.Wrap(workerr.ENotFound, err))
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename="+name)
	_, _ = w.Write(b)
}

func (s *Server) session(id string) (*experiment.Session, error) {
	s.mu.Lock()
	sess := s.sessions[id]
	s.mu.Unlock()
	if sess != nil {
		return sess, nil
	}
	dir := filepath.Join(s.Root, "sessions", id)
	opened, err := experiment.OpenSession(dir, experiment.Hooks{})
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.sessions[id] = opened
	s.mu.Unlock()
	return opened, nil
}

func decodeBody(r *http.Request, v any) error {
	defer r.Body.Close()
	if r.Body == nil {
		return nil
	}
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 32<<20))
	return dec.Decode(v)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		b = []byte(`{"code":"E_INTERNAL","message":"marshal failed"}`)
	}
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

func writeError(w http.ResponseWriter, err error) {
	if we, ok := workerr.As(err); ok {
		status := http.StatusBadRequest
		switch we.Code {
		case workerr.ELatencyBudget:
			status = http.StatusRequestTimeout
		case workerr.EConflict, workerr.EStaleCheckpoint:
			status = http.StatusConflict
		case workerr.ENotFound:
			status = http.StatusNotFound
		case workerr.EInvalidStage:
			status = http.StatusUnprocessableEntity
		case workerr.ENumeric, workerr.EInvalidFold, workerr.EIncrementalMismatch:
			status = http.StatusUnprocessableEntity
		}
		writeJSON(w, status, we)
		return
	}
	writeJSON(w, http.StatusInternalServerError, workerr.New("E_INTERNAL", err.Error()))
}

func mustWebFS() fs.FS {
	sub, err := fs.Sub(webfs.Files, ".")
	if err != nil {
		panic(err)
	}
	return sub
}
