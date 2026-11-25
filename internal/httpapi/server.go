package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"ropechunk/internal/engine"
	"ropechunk/internal/model"
	"ropechunk/internal/samples"
	"ropechunk/internal/store"
)

type Server struct {
	eng       *engine.Engine
	store     *store.Store
	computeMu sync.Mutex
}

type BlockView struct {
	model.Checkpoint
	Result json.RawMessage `json:"result,omitempty"`
}

func New(eng *engine.Engine) *Server {
	return &Server{eng: eng, store: eng.Store()}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/api/health", s.handleHealth)
	mux.HandleFunc("/api/import", s.handleImport)
	mux.HandleFunc("/api/demo", s.handleDemo)
	mux.HandleFunc("/api/compute", s.handleCompute)
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/fingerprints", s.handleFingerprints)
	mux.HandleFunc("/api/verify", s.handleVerify)
	mux.HandleFunc("/api/blocks", s.handleBlocks)
	mux.HandleFunc("/api/budget", s.handleBudget)
	mux.HandleFunc("/api/memory-plans", s.handleMemoryPlans)
	mux.HandleFunc("/api/invalidations", s.handleInvalidations)
	mux.HandleFunc("/api/version-events", s.handleVersionEvents)
	mux.HandleFunc("/api/configs", s.handleConfigs)
	mux.HandleFunc("/api/declarations", s.handleDeclarations)
	mux.HandleFunc("/api/runs", s.handleRuns)
	mux.HandleFunc("/api/stop", s.handleStop)
	mux.HandleFunc("/api/resume", s.handleResume)
	return mux
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		writeError(w, model.NewAppError(model.CodeNotFound, "route not found"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"name": "ropechunk",
		"endpoints": []string{
			"POST /api/import", "POST /api/demo", "POST /api/compute", "GET /api/status",
			"GET /api/fingerprints", "POST /api/verify", "GET /api/blocks", "GET /api/budget",
			"GET /api/memory-plans", "GET /api/invalidations", "GET /api/version-events",
			"POST /api/stop", "POST /api/resume",
		},
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "stopped": s.eng.IsStopped()})
}

func (s *Server) handleImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	var bundle model.Bundle
	if err := decodeJSON(w, r, &bundle, 128<<20); err != nil {
		writeError(w, err)
		return
	}
	report, err := s.eng.Import(r.Context(), bundle)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) handleDemo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	bundle, err := samples.Generate()
	if err != nil {
		writeError(w, err)
		return
	}
	report, err := s.eng.Import(r.Context(), bundle)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"shards": len(bundle.Shards), "import": report})
}

type computeRequest struct {
	Stage    string   `json:"stage"`
	ShardIDs []string `json:"shard_ids"`
	RunID    string   `json:"run_id"`
	Note     string   `json:"note"`
}

func (s *Server) handleCompute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	if !s.computeMu.TryLock() {
		writeStatusCode(w, http.StatusConflict, model.NewAppError(model.CodeState, "another compute request is running"))
		return
	}
	defer s.computeMu.Unlock()
	var req computeRequest
	if err := decodeJSON(w, r, &req, 1<<20); err != nil {
		writeError(w, err)
		return
	}
	report, err := s.eng.Compute(r.Context(), engine.ComputeOptions{Stage: req.Stage, ShardIDs: req.ShardIDs, RunID: req.RunID, Note: req.Note})
	if err != nil {
		writeJSON(w, statusForError(err), map[string]any{"error": asError(err), "report": report})
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) handleStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	s.eng.Stop()
	writeJSON(w, http.StatusAccepted, map[string]any{"stopped": true, "message": "finish current indivisible block, then exit"})
}

func (s *Server) handleResume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	if !s.computeMu.TryLock() {
		writeStatusCode(w, http.StatusConflict, model.NewAppError(model.CodeState, "another compute request is running"))
		return
	}
	defer s.computeMu.Unlock()
	s.eng.ResetStop()
	if r.ContentLength == 0 {
		writeJSON(w, http.StatusOK, map[string]any{"stopped": false, "message": "stop flag cleared"})
		return
	}
	var req computeRequest
	if err := decodeJSON(w, r, &req, 1<<20); err != nil {
		writeError(w, err)
		return
	}
	report, err := s.eng.Compute(r.Context(), engine.ComputeOptions{Stage: req.Stage, ShardIDs: req.ShardIDs, RunID: req.RunID, Note: req.Note})
	if err != nil {
		writeJSON(w, statusForError(err), map[string]any{"error": asError(err), "report": report})
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	report, err := s.eng.Status(r.Context(), shardIDs(r))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) handleFingerprints(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	status, err := s.eng.Status(r.Context(), shardIDs(r))
	if err != nil {
		writeError(w, err)
		return
	}
	out := make([]map[string]any, 0, len(status.Shards))
	for _, shard := range status.Shards {
		out = append(out, map[string]any{"shard_id": shard.ShardID, "shape": shard.Shape, "fingerprints": shard.Fingerprints})
	}
	writeJSON(w, http.StatusOK, map[string]any{"config_version": status.Config.SnapshotVersion, "config_digest": status.ConfigDigest, "shards": out})
}

func (s *Server) handleVerify(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		methodNotAllowed(w, "GET, POST")
		return
	}
	report, err := s.eng.VerifyReferences(r.Context(), shardIDs(r))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) handleBlocks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	shardID := r.URL.Query().Get("shard_id")
	if shardID == "" {
		writeError(w, model.NewAppError(model.CodeInvalidConfig, "shard_id is required"))
		return
	}
	stage := r.URL.Query().Get("stage")
	var records []store.CheckpointRecord
	var err error
	if stage == "" {
		for _, candidate := range []string{model.StageRotation, model.StageScore} {
			part, partErr := s.store.ListCheckpoints(r.Context(), shardID, candidate)
			if partErr != nil {
				err = partErr
				break
			}
			records = append(records, part...)
		}
	} else {
		records, err = s.store.ListCheckpoints(r.Context(), shardID, stage)
	}
	if err != nil {
		writeError(w, err)
		return
	}
	batch, hasBatch, err := optionalInt(r, "batch")
	if err != nil {
		writeError(w, model.NewAppError(model.CodeInvalidConfig, err.Error()))
		return
	}
	head, hasHead, err := optionalInt(r, "head")
	if err != nil {
		writeError(w, model.NewAppError(model.CodeInvalidConfig, err.Error()))
		return
	}
	rowBlock, hasRow, err := optionalInt(r, "row_block")
	if err != nil {
		writeError(w, model.NewAppError(model.CodeInvalidConfig, err.Error()))
		return
	}
	views := make([]BlockView, 0, len(records))
	for _, rec := range records {
		if hasBatch && rec.Batch != batch || hasHead && rec.Head != head || hasRow && rec.RowBlock != rowBlock {
			continue
		}
		view := BlockView{Checkpoint: rec.Checkpoint}
		if len(rec.ResultPayload) > 0 {
			view.Result = json.RawMessage(rec.ResultPayload)
		}
		views = append(views, view)
	}
	writeJSON(w, http.StatusOK, map[string]any{"shard_id": shardID, "blocks": views})
}

func (s *Server) handleBudget(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	events, err := s.store.ListBudgetEvents(r.Context(), r.URL.Query().Get("run_id"))
	if err != nil {
		writeError(w, err)
		return
	}
	shardID := r.URL.Query().Get("shard_id")
	if shardID != "" {
		filtered := events[:0]
		for _, event := range events {
			if event.ShardID == shardID {
				filtered = append(filtered, event)
			}
		}
		events = filtered
	}
	writeJSON(w, http.StatusOK, events)
}

func (s *Server) handleMemoryPlans(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	plans, err := s.store.ListMemoryPlans(r.Context(), r.URL.Query().Get("run_id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, plans)
}

func (s *Server) handleInvalidations(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	events, err := s.store.ListInvalidations(r.Context(), r.URL.Query().Get("shard_id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, events)
}

func (s *Server) handleVersionEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	events, err := s.store.ListVersionEvents(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, events)
}

func (s *Server) handleConfigs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	configs, err := s.store.ListConfigs(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, configs)
}

func (s *Server) handleDeclarations(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	decls, err := s.store.ListDeclarations(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, decls)
}

func (s *Server) handleRuns(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	runs, err := s.store.ListRuns(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, runs)
}

func shardIDs(r *http.Request) []string {
	raw := r.URL.Query().Get("shard_ids")
	if raw == "" {
		raw = r.URL.Query().Get("shard_id")
	}
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func optionalInt(r *http.Request, name string) (int, bool, error) {
	raw, present := r.URL.Query()[name]
	if !present {
		return 0, false, nil
	}
	if len(raw) == 0 || raw[0] == "" {
		return 0, false, fmt.Errorf("%s is empty", name)
	}
	value, err := strconv.Atoi(raw[0])
	if err != nil || value < 0 {
		return 0, false, fmt.Errorf("%s must be a non-negative integer", name)
	}
	return value, true, nil
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any, limit int64) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, limit))
	if err := decoder.Decode(dst); err != nil {
		if errors.Is(err, io.EOF) {
			return model.NewAppError(model.CodeInvalidConfig, "request body is empty")
		}
		return model.NewAppError(model.CodeInvalidConfig, "invalid JSON: "+err.Error())
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(value)
}

func writeError(w http.ResponseWriter, err error) {
	writeStatusCode(w, statusForError(err), asError(err))
}

func writeStatusCode(w http.ResponseWriter, status int, app *model.AppError) {
	writeJSON(w, status, map[string]any{"error": app})
}

func asError(err error) *model.AppError {
	if app, ok := model.AsAppError(err); ok {
		return app
	}
	return &model.AppError{Code: model.CodeIO, Message: err.Error()}
}

func statusForError(err error) int {
	app, ok := model.AsAppError(err)
	if !ok {
		return http.StatusInternalServerError
	}
	switch app.Code {
	case model.CodeNotFound:
		return http.StatusNotFound
	case model.CodeStaleCheckpoint, model.CodeState:
		return http.StatusConflict
	case model.CodeMemoryBudget:
		return http.StatusInsufficientStorage
	case model.CodeNumeric, model.CodeInvalidConfig:
		return http.StatusUnprocessableEntity
	default:
		return http.StatusInternalServerError
	}
}

func methodNotAllowed(w http.ResponseWriter, allowed string) {
	w.Header().Set("Allow", allowed)
	writeStatusCode(w, http.StatusMethodNotAllowed, model.NewAppError(model.CodeInvalidConfig, "method not allowed"))
}
