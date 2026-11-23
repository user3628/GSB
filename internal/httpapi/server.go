package httpapi

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"ciartifacts/internal/cache"
)

type Server struct {
	store *cache.Store
	mux   *http.ServeMux
}

func New(store *cache.Store) http.Handler {
	server := &Server{store: store, mux: http.NewServeMux()}
	server.mux.HandleFunc("/healthz", methodOnly(http.MethodGet, server.health))
	server.mux.HandleFunc("/v1/entries", methodOnly(http.MethodPost, server.writeEntry))
	server.mux.HandleFunc("/v1/hit", methodOnly(http.MethodGet, server.hit))
	server.mux.HandleFunc("/v1/entries/transition", methodOnly(http.MethodPost, server.transition))
	server.mux.HandleFunc("/v1/manifest", methodOnly(http.MethodGet, server.exportManifest))
	server.mux.HandleFunc("/v1/snapshots/switch", methodOnly(http.MethodPost, server.switchSnapshot))
	server.mux.HandleFunc("/v1/evict", methodOnly(http.MethodPost, server.evict))
	server.mux.HandleFunc("/v1/recovery/report", methodOnly(http.MethodGet, server.recoveryReport))
	server.mux.HandleFunc("/", server.notFound)
	return server
}

func methodOnly(method string, handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{
				"code":    cache.CodeMethodNotAllowed,
				"message": "HTTP method is not allowed for this route",
				"context": map[string]any{"method": r.Method, "allowed_method": method, "path": r.URL.Path},
			})
			return
		}
		handler(w, r)
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

type entryRequest struct {
	StageID          string `json:"stage_id"`
	InputFingerprint string `json:"input_fingerprint"`
	ToolchainVersion string `json:"toolchain_version"`
	ArtifactContent  string `json:"artifact_content"`
	ArtifactDigest   string `json:"artifact_digest"`
	ArtifactSize     int64  `json:"artifact_size"`
	RetainUntil      string `json:"retain_until"`
}

type entryDTO struct {
	EntryID          string `json:"entry_id"`
	CacheKey         string `json:"cache_key"`
	StageID          string `json:"stage_id"`
	InputFingerprint string `json:"input_fingerprint"`
	ToolchainVersion string `json:"toolchain_version"`
	ArtifactDigest   string `json:"artifact_digest"`
	ArtifactSize     int64  `json:"artifact_size"`
	Hits             int64  `json:"hits"`
	RetainUntil      string `json:"retain_until"`
	CreatedAt        string `json:"created_at"`
	LastHitAt        string `json:"last_hit_at"`
	State            string `json:"state"`
	Version          int64  `json:"version"`
}

type transitionRequest struct {
	StageID          string      `json:"stage_id"`
	InputFingerprint string      `json:"input_fingerprint"`
	ToolchainVersion string      `json:"toolchain_version"`
	ToState          cache.State `json:"to_state"`
}

type snapshotSwitchRequest struct {
	Path     string          `json:"path"`
	Snapshot *cache.Snapshot `json:"snapshot"`
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "status": "ready"})
}

func (s *Server) notFound(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusNotFound, map[string]any{
		"code":    cache.CodeNotFound,
		"message": "HTTP route not found",
		"context": map[string]any{"method": r.Method, "path": r.URL.Path},
	})
}

func (s *Server) writeEntry(w http.ResponseWriter, r *http.Request) {
	var request entryRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, r, err)
		return
	}
	retainUntil, err := time.Parse(time.RFC3339, request.RetainUntil)
	if err != nil {
		writeError(w, r, cache.NewEntryInvalid("retain_until must be RFC3339", map[string]any{
			"retain_until": request.RetainUntil,
		}, err))
		return
	}
	result, err := s.store.Put(r.Context(), cache.EntryInput{
		StageID:          request.StageID,
		InputFingerprint: request.InputFingerprint,
		ToolchainVersion: request.ToolchainVersion,
		ArtifactContent:  []byte(request.ArtifactContent),
		ArtifactDigest:   request.ArtifactDigest,
		ArtifactSize:     request.ArtifactSize,
		RetainUntil:      retainUntil,
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	status := http.StatusCreated
	if result.Idempotent {
		status = http.StatusOK
	}
	writeJSON(w, status, map[string]any{
		"ok":         true,
		"idempotent": result.Idempotent,
		"entry":      toEntryDTO(result.Entry),
	})
}

func (s *Server) hit(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	result, err := s.store.Hit(r.Context(), cache.Key{
		StageID:          query.Get("stage_id"),
		InputFingerprint: query.Get("input_fingerprint"),
		ToolchainVersion: query.Get("toolchain_version"),
	}, query.Get("expected_digest"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                      true,
		"hit":                     true,
		"hits":                    result.Entry.Hits,
		"entry":                   toEntryDTO(result.Entry),
		"artifact_content_base64": base64.StdEncoding.EncodeToString(result.Content),
	})
}

func (s *Server) transition(w http.ResponseWriter, r *http.Request) {
	var request transitionRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, r, err)
		return
	}
	entry, err := s.store.Transition(r.Context(), cache.Key{
		StageID:          request.StageID,
		InputFingerprint: request.InputFingerprint,
		ToolchainVersion: request.ToolchainVersion,
	}, request.ToState)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "entry": toEntryDTO(entry)})
}

func (s *Server) exportManifest(w http.ResponseWriter, r *http.Request) {
	snapshot, err := s.store.ExportSnapshot(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	path := strings.TrimSpace(r.URL.Query().Get("path"))
	if path != "" {
		if err := cache.WriteSnapshotFile(path, snapshot); err != nil {
			writeError(w, r, err)
			return
		}
	}
	response := map[string]any{
		"ok":       true,
		"sequence": snapshot.Sequence,
		"checksum": snapshot.Checksum,
		"entries":  len(snapshot.Entries),
	}
	if path != "" {
		response["path"] = path
	} else {
		response["snapshot"] = snapshot
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) switchSnapshot(w http.ResponseWriter, r *http.Request) {
	var request snapshotSwitchRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, r, err)
		return
	}
	if (request.Path == "") == (request.Snapshot == nil) {
		writeError(w, r, cache.NewSnapshotInvalid("exactly one of path or snapshot is required", map[string]any{
			"has_path":     request.Path != "",
			"has_snapshot": request.Snapshot != nil,
		}))
		return
	}
	var snapshot cache.Snapshot
	var err error
	if request.Path != "" {
		snapshot, err = cache.LoadSnapshotFile(request.Path)
	} else {
		snapshot = *request.Snapshot
	}
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := s.store.SwitchSnapshot(r.Context(), snapshot)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "switch": result})
}

func (s *Server) evict(w http.ResponseWriter, r *http.Request) {
	result, err := s.store.Evict(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "eviction": result})
}

func (s *Server) recoveryReport(w http.ResponseWriter, r *http.Request) {
	report, err := s.store.LatestRecoveryReport(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "report": report})
}

func toEntryDTO(entry cache.Entry) entryDTO {
	return entryDTO{
		EntryID:          entry.EntryID,
		CacheKey:         entry.CacheKey,
		StageID:          entry.StageID,
		InputFingerprint: entry.InputFingerprint,
		ToolchainVersion: entry.ToolchainVersion,
		ArtifactDigest:   entry.ArtifactDigest,
		ArtifactSize:     entry.ArtifactSize,
		Hits:             entry.Hits,
		RetainUntil:      entry.RetainUntil.UTC().Format(time.RFC3339Nano),
		CreatedAt:        entry.CreatedAt.UTC().Format(time.RFC3339Nano),
		LastHitAt:        entry.LastHitAt.UTC().Format(time.RFC3339Nano),
		State:            string(entry.State),
		Version:          entry.Version,
	}
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return cache.NewEntryInvalid("request body is not valid JSON", nil, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err != nil {
			return cache.NewEntryInvalid("request body contains invalid trailing data", nil, err)
		}
		return cache.NewEntryInvalid("request body contains trailing JSON values", nil, nil)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, r *http.Request, err error) {
	appErr := &cache.AppError{}
	if !errors.As(err, &appErr) {
		internal := cache.NewInternal("unexpected server error", nil, err)
		_ = errors.As(internal, &appErr)
	}
	context := map[string]any{
		"method": r.Method,
		"path":   r.URL.Path,
	}
	for key, value := range appErr.Context {
		context[key] = value
	}
	writeJSON(w, statusForCode(appErr.Code), map[string]any{
		"code":    appErr.Code,
		"message": appErr.Message,
		"context": context,
	})
}

func statusForCode(code string) int {
	switch code {
	case cache.CodeEntryInvalid:
		return http.StatusBadRequest
	case cache.CodeInvalidTransition, cache.CodeConflictingWrite:
		return http.StatusConflict
	case cache.CodeDigestMismatch:
		return http.StatusUnprocessableEntity
	case cache.CodeEntryExpired:
		return http.StatusGone
	case cache.CodeCacheMiss:
		return http.StatusNotFound
	case cache.CodeSnapshotInvalid:
		return http.StatusBadRequest
	case cache.CodeSnapshotSequence:
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}
