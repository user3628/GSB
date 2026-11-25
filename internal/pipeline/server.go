package pipeline

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
)

// Server exposes the pipeline over a local HTTP interface.
type Server struct {
	workDir string
	store   *Store
	cache   *QueryCache
	lock    *FileLock
	mu      sync.Mutex
	engine  *Engine
	// queryHook, when set, is invoked inside the cached query compute path.
	// Tests use it to force work past the latency budget.
	queryHook func(kind string)
}

func NewServer(workDir string) (*Server, error) {
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return nil, err
	}
	lock, err := AcquireFileLock(filepath.Join(workDir, "workspace.lock"))
	if err != nil {
		return nil, err
	}
	store, err := OpenStore(filepath.Join(workDir, "recon.db"))
	if err != nil {
		lock.Release()
		return nil, err
	}
	return &Server{
		workDir: workDir,
		store:   store,
		cache:   NewQueryCache(),
		lock:    lock,
	}, nil
}

func (s *Server) Close() {
	_ = s.store.Close()
	_ = s.lock.Release()
}

// Handler builds the HTTP mux.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/api/dataset/generate", s.handleGenerate)
	mux.HandleFunc("/api/dataset/manifest", s.handleGetManifest)
	mux.HandleFunc("/api/shards/", s.handleShard)
	mux.HandleFunc("/api/config", s.handleConfig)
	mux.HandleFunc("/api/reconstruct", s.handleReconstruct)
	mux.HandleFunc("/api/stop", s.handleStop)
	mux.HandleFunc("/api/jobs/", s.handleJob)
	mux.HandleFunc("/api/query/", s.handleQuery)
	mux.HandleFunc("/api/checkpoints", s.handleCheckpoints)
	return loggingMux(mux)
}

func newQueryID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return "q-" + hex.EncodeToString(b[:])
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	_ = enc.Encode(v)
}

// writeBiz emits a business error with the same structured fields in the HTTP
// body and on stderr (requirement 8).
func writeBiz(w http.ResponseWriter, err error) {
	be := asBiz(err)
	be.Emit()
	status := http.StatusBadRequest
	switch be.Code {
	case CodeNotFound:
		status = http.StatusNotFound
	case CodeLocked:
		status = http.StatusConflict
	case CodeLatencyBudget:
		status = http.StatusServiceUnavailable
	case CodeStaleCheckpoint, CodeCheckpointRollback, CodeCheckpointTampered:
		status = http.StatusConflict
	}
	writeJSON(w, status, be)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type generateRequest struct {
	Seed     int64 `json:"seed"`
	Shards   int   `json:"shards"`
	ViewsPer int   `json:"views_per"`
	Width    int   `json:"width"`
	Height   int   `json:"height"`
}

func (s *Server) handleGenerate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeBiz(w, bizError(CodeInvalidRequest, "use POST"))
		return
	}
	var req generateRequest
	if err := decodeBody(r, &req); err != nil {
		writeBiz(w, err)
		return
	}
	if req.Seed == 0 {
		req.Seed = 20240117
	}
	manifest, err := GenerateDataset(GenerateParams{
		Seed:     req.Seed,
		Shards:   req.Shards,
		ViewsPer: req.ViewsPer,
		ImageW:   req.Width,
		ImageH:   req.Height,
		OutDir:   s.workDir,
		Config:   DefaultConfig(),
	})
	if err != nil {
		writeBiz(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"seed":           manifest.Seed,
		"shards":         len(manifest.Shards),
		"views":          totalViews(manifest),
		"config_version": ConfigVersion(manifest.Config),
		"truth_points":   len(manifest.Truth),
	})
}

func (s *Server) handleGetManifest(w http.ResponseWriter, r *http.Request) {
	manifest, err := LoadManifest(filepath.Join(s.workDir, "manifest.json"))
	if err != nil {
		writeBiz(w, err)
		return
	}
	writeJSON(w, http.StatusOK, manifest)
}

// handleShard supports PUT /api/shards/{shardID}/{viewID} to import or replace
// a grayscale tile (raw PNG body), and GET to fetch metadata.
func (s *Server) handleShard(w http.ResponseWriter, r *http.Request) {
	rel := r.URL.Path[len("/api/shards/"):]
	parts := splitPath(rel)
	if len(parts) < 2 {
		writeBiz(w, bizError(CodeInvalidRequest, "expected /api/shards/{shard}/{view}"))
		return
	}
	shardID, viewID := parts[0], parts[1]
	switch r.Method {
	case http.MethodPut:
		data, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
		if err != nil {
			writeBiz(w, bizError(CodeInvalidRequest, "read body: %v", err))
			return
		}
		dir := filepath.Join(s.workDir, shardID)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			writeBiz(w, err)
			return
		}
		if err := AtomicWriteFile(filepath.Join(dir, viewID+".png"), data); err != nil {
			writeBiz(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"imported": viewID, "shard": shardID})
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]string{"shard": shardID, "view": viewID})
	default:
		writeBiz(w, bizError(CodeInvalidRequest, "method not allowed"))
	}
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	manifestPath := filepath.Join(s.workDir, "manifest.json")
	manifest, err := LoadManifest(manifestPath)
	if err != nil {
		writeBiz(w, err)
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, manifest.Config)
	case http.MethodPut:
		var patch map[string]any
		if err := decodeBody(r, &patch); err != nil {
			writeBiz(w, err)
			return
		}
		updated, err := applyConfigPatch(manifest.Config, patch)
		if err != nil {
			writeBiz(w, err)
			return
		}
		manifest.Config = updated
		if err := writeJSONFile(manifestPath, manifest); err != nil {
			writeBiz(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"config":         updated,
			"config_version": ConfigVersion(updated),
		})
	default:
		writeBiz(w, bizError(CodeInvalidRequest, "method not allowed"))
	}
}

type reconstructRequest struct {
	ForceFull      bool `json:"force_full"`
	VerifyEquality bool `json:"verify_equality"`
}

func (s *Server) handleReconstruct(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeBiz(w, bizError(CodeInvalidRequest, "use POST"))
		return
	}
	var req reconstructRequest
	if r.Body != nil {
		_ = decodeBody(r, &req)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	jobID := "job-" + newQueryID()[2:]
	eng := NewEngine(s.workDir, s.store, s.cache)
	s.engine = eng
	result, err := eng.Reconstruct(jobID, req.ForceFull)
	if err != nil {
		if be, ok := err.(*BizError); ok && be.Code == CodeNumericMismatch {
			// Numeric mismatch is a completed run with failing validation: return
			// the result alongside the structured error details.
			writeJSON(w, http.StatusUnprocessableEntity, map[string]any{
				"result": result, "error": be,
			})
			return
		}
		writeBiz(w, err)
		return
	}

	resp := map[string]any{"result": result}
	if req.VerifyEquality {
		manifest, images, lerr := LoadDataset(s.workDir)
		if lerr != nil {
			writeBiz(w, lerr)
			return
		}
		ref, ferr := ComputeFieldFromScratch(manifest, images)
		if ferr != nil {
			writeBiz(w, ferr)
			return
		}
		inc, gerr := loadGlobalField(result.ArtifactDir)
		if gerr != nil {
			writeBiz(w, gerr)
			return
		}
		if verr := VerifyFieldEquivalence(inc, ref); verr != nil {
			if be, ok := verr.(*BizError); ok {
				be.Emit()
				writeJSON(w, http.StatusConflict, be)
			} else {
				writeBiz(w, verr)
			}
			return
		}
		resp["equivalence"] = "byte_identical"
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleStop(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	eng := s.engine
	s.mu.Unlock()
	if eng == nil {
		writeBiz(w, bizError(CodeNotFound, "no active reconstruction"))
		return
	}
	eng.SignalStop()
	writeJSON(w, http.StatusOK, map[string]string{"status": "stop_requested"})
}

func (s *Server) handleJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.URL.Path[len("/api/jobs/"):]
	job, err := s.store.GetJob(jobID)
	if err != nil {
		writeBiz(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

// handleQuery serves summary / prune / errors under the latency budget.
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	kind := r.URL.Path[len("/api/query/"):]
	manifest, images, err := LoadDataset(s.workDir)
	if err != nil {
		writeBiz(w, err)
		return
	}
	fp, err := ComputeFingerprints(manifest, images)
	if err != nil {
		writeBiz(w, err)
		return
	}
	cfgVer := ConfigVersion(manifest.Config)
	queryID := newQueryID()
	budget := manifest.Config.QueryBudgetMs

	compute := func() (any, error) {
		if s.queryHook != nil {
			s.queryHook(kind)
		}
		field, err := loadGlobalField(s.artifactDirFor(cfgVer))
		if err != nil {
			return nil, err
		}
		switch kind {
		case "summary":
			pruned, _ := loadPruned(s.artifactDirFor(cfgVer))
			return SummarizePointCloud(field, pruned), nil
		case "prune":
			pruned, _ := loadPruned(s.artifactDirFor(cfgVer))
			return PruneReasons(pruned), nil
		case "errors":
			stats, berr := ValidateReconstruction(manifest.Config, manifest, field)
			if berr != nil {
				return nil, berr
			}
			return stats, nil
		default:
			return nil, bizError(CodeNotFound, "unknown query kind: %s", kind)
		}
	}
	res, err := s.cache.RunQuery(kind, queryID, fp, cfgVer, budget, compute)
	if err != nil {
		writeBiz(w, err)
		return
	}
	// Re-marshal payload through json.RawMessage so the response stays compact.
	writeJSON(w, http.StatusOK, map[string]any{
		"query_id":   res.QueryID,
		"kind":       res.Kind,
		"cache_hit":  res.CacheHit,
		"elapsed_ms": res.ElapsedMs,
		"data":       json.RawMessage(res.Payload),
	})
}

func (s *Server) handleCheckpoints(w http.ResponseWriter, r *http.Request) {
	cps, err := s.store.ListCheckpoints()
	if err != nil {
		writeBiz(w, err)
		return
	}
	writeJSON(w, http.StatusOK, cps)
}

func (s *Server) artifactDirFor(cfgVer string) string {
	return filepath.Join(s.workDir, "artifacts", cfgVer[:12])
}

func decodeBody(r *http.Request, v any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, 4<<20))
	if err := dec.Decode(v); err != nil {
		return bizError(CodeInvalidRequest, "invalid json body: %v", err)
	}
	return nil
}

func splitPath(p string) []string {
	var out []string
	cur := ""
	for _, c := range p {
		if c == '/' {
			if cur != "" {
				out = append(out, cur)
				cur = ""
			}
			continue
		}
		cur += string(c)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

func writeJSONFile(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return AtomicWriteFile(path, b)
}

func loadGlobalField(dir string) (GlobalField, error) {
	var field GlobalField
	if err := readJSONFile(filepath.Join(dir, "global-field.json"), &field); err != nil {
		return field, bizError(CodeNotFound, "reconstructed field not available: %v", err)
	}
	return field, nil
}

func loadPruned(dir string) ([]GlobalVoxel, error) {
	var pruned []GlobalVoxel
	if err := readJSONFile(filepath.Join(dir, "pruned.json"), &pruned); err != nil {
		return nil, err
	}
	return pruned, nil
}
