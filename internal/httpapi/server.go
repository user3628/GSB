package httpapi

import (
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"maskrevision/internal/imageio"
	"maskrevision/internal/model"
	"maskrevision/internal/store"
	"maskrevision/internal/workspace"
)

//go:embed dashboard.html
var assets embed.FS

type Server struct {
	Workspace *workspace.Workspace
	StartedAt time.Time
}

func New(ws *workspace.Workspace) *Server {
	return &Server{Workspace: ws, StartedAt: time.Now().UTC()}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", s.dashboard)
	mux.HandleFunc("GET /api/health", s.health)
	mux.HandleFunc("GET /api/shards", s.listShards)
	mux.HandleFunc("GET /api/shards/{shard}", s.getShard)
	mux.HandleFunc("GET /api/shards/{shard}/revisions", s.revisions)
	mux.HandleFunc("GET /api/shards/{shard}/snapshots/{revision}", s.snapshot)
	mux.HandleFunc("GET /api/shards/{shard}/masks/{revision}", s.maskPNG)
	mux.HandleFunc("GET /api/shards/{shard}/diff", s.diff)
	mux.HandleFunc("GET /api/shards/{shard}/checkpoints", s.checkpoints)
	mux.HandleFunc("POST /api/shards/{shard}/publish", s.publish)
	mux.HandleFunc("POST /api/shards/{shard}/apply-log", s.applyLog)
	mux.HandleFunc("POST /api/shards/{shard}/upgrade/export", s.exportUpgrade)
	mux.HandleFunc("GET /api/fixtures", s.fixtures)
	mux.HandleFunc("GET /api/demo", s.demoReport)
	return requestLogger(mux)
}

func (s *Server) dashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	data, err := assets.ReadFile("dashboard.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok", "started_at": s.StartedAt.Format(time.RFC3339Nano),
		"workspace_digest": s.Workspace.Digest(), "database": s.Workspace.Store.Path(),
	})
}

func (s *Server) listShards(w http.ResponseWriter, r *http.Request) {
	shards, err := s.Workspace.ShardsSummary()
	if err != nil {
		writeError(w, http.StatusInternalServerError, model.NewBusinessError("E_INTERNAL", err.Error(), nil))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"shards": shards, "workspace_digest": s.Workspace.Digest()})
}

func (s *Server) getShard(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("shard")
	shards, err := s.Workspace.ShardsSummary()
	if err != nil {
		writeError(w, http.StatusInternalServerError, model.NewBusinessError("E_INTERNAL", err.Error(), nil))
		return
	}
	var summary *workspace.ShardSummary
	for i := range shards {
		if shards[i].ShardID == id {
			summary = &shards[i]
			break
		}
	}
	if summary == nil {
		writeError(w, http.StatusNotFound, model.WithContext(model.NewBusinessError("E_SHARD_NOT_FOUND", "unknown shard", nil), id, 0))
		return
	}
	revisions, be := s.Workspace.Revisions(id)
	if be != nil {
		writeError(w, statusFor(be), be)
		return
	}
	checkpoints, be := s.Workspace.Checkpoints(id)
	if be != nil {
		writeError(w, statusFor(be), be)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"summary": summary, "revisions": revisions, "checkpoints": checkpoints})
}

func (s *Server) revisions(w http.ResponseWriter, r *http.Request) {
	revisions, be := s.Workspace.Revisions(r.PathValue("shard"))
	if be != nil {
		writeError(w, statusFor(be), be)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"revisions": revisions})
}

func (s *Server) snapshot(w http.ResponseWriter, r *http.Request) {
	rev, err := strconv.Atoi(r.PathValue("revision"))
	if err != nil {
		writeError(w, http.StatusBadRequest, model.NewBusinessError("E_REVISION_INVALID", "revision must be an integer", nil))
		return
	}
	snap, be := s.Workspace.Snapshot(r.PathValue("shard"), rev)
	if be != nil {
		writeError(w, statusFor(be), be)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) maskPNG(w http.ResponseWriter, r *http.Request) {
	rev, err := strconv.Atoi(strings.TrimSuffix(r.PathValue("revision"), ".png"))
	if err != nil {
		writeError(w, http.StatusBadRequest, model.NewBusinessError("E_REVISION_INVALID", "revision must be an integer", nil))
		return
	}
	snap, be := s.Workspace.Snapshot(r.PathValue("shard"), rev)
	if be != nil {
		writeError(w, statusFor(be), be)
		return
	}
	segments := make(map[int]*model.Segment, len(snap.Segments))
	for i := range snap.Segments {
		seg := snap.Segments[i]
		segments[seg.ID] = &seg
	}
	data, err := imageio.EncodeIndexed(snap.Width, snap.Height, snap.Pixels, segments)
	if err != nil {
		writeError(w, http.StatusInternalServerError, model.NewBusinessError("E_MASK_ENCODE", err.Error(), nil))
		return
	}
	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(data)
}

func (s *Server) diff(w http.ResponseWriter, r *http.Request) {
	from, err1 := strconv.Atoi(r.URL.Query().Get("from"))
	to, err2 := strconv.Atoi(r.URL.Query().Get("to"))
	if err1 != nil || err2 != nil {
		writeError(w, http.StatusBadRequest, model.NewBusinessError("E_REVISION_INVALID", "from and to must be integers", nil))
		return
	}
	result, be := s.Workspace.Diff(r.PathValue("shard"), from, to)
	if be != nil {
		writeError(w, statusFor(be), be)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) checkpoints(w http.ResponseWriter, r *http.Request) {
	checkpoints, be := s.Workspace.Checkpoints(r.PathValue("shard"))
	if be != nil {
		writeError(w, statusFor(be), be)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"checkpoints": checkpoints})
}

type publishBody struct {
	ExpectedRevision int    `json:"expected_revision"`
	Revision         int    `json:"revision"`
	Publisher        string `json:"publisher"`
	SnapshotDigest   string `json:"snapshot_digest"`
}

func (s *Server) publish(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("shard")
	var body publishBody
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, model.NewBusinessError("E_REQUEST_JSON", err.Error(), nil))
		return
	}
	if body.Publisher == "" {
		body.Publisher = "http"
	}
	res, be := s.Workspace.Store.Publish(store.PublishLockPath(s.Workspace.Layout.Root, id), store.PublishRequest{
		ShardID: id, Revision: body.Revision, ExpectedRevision: body.ExpectedRevision,
		Publisher: body.Publisher, SnapshotDigest: body.SnapshotDigest,
	})
	if be != nil {
		writeError(w, statusFor(be), be)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) applyLog(w http.ResponseWriter, r *http.Request) {
	data, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, model.NewBusinessError("E_REQUEST_BODY", err.Error(), nil))
		return
	}
	report, be := s.Workspace.ApplyIncomingLog(r.PathValue("shard"), data)
	if be != nil {
		writeError(w, statusFor(be), be)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sync": report, "workspace_digest": s.Workspace.Digest()})
}

func (s *Server) exportUpgrade(w http.ResponseWriter, r *http.Request) {
	verification, be := s.Workspace.ExportAndVerifyV1(r.PathValue("shard"))
	if be != nil {
		writeError(w, statusFor(be), be)
		return
	}
	writeJSON(w, http.StatusOK, verification)
}

func (s *Server) fixtures(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	paths := map[string]string{
		"fork": "fixtures/shard-00.fork.json", "edited": "fixtures/shard-00.edited.json",
		"rollback": "fixtures/shard-00.rollback.json",
	}
	path, ok := paths[name]
	if !ok {
		writeError(w, http.StatusBadRequest, model.NewBusinessError("E_FIXTURE_UNKNOWN", "name must be fork, edited, or rollback", nil))
		return
	}
	result, be := s.Workspace.ClassifyFixture("shard-00", path)
	if be != nil {
		writeError(w, statusFor(be), be)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) demoReport(w http.ResponseWriter, r *http.Request) {
	path := filepath.Join(s.Workspace.Layout.Root, "report.json")
	data, err := os.ReadFile(path)
	if err != nil {
		writeError(w, http.StatusNotFound, model.NewBusinessError("E_REPORT_NOT_FOUND", "run the demo command first", nil))
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, _ = w.Write(data)
}

func statusFor(be *model.BusinessError) int {
	if be == nil {
		return http.StatusOK
	}
	switch be.Code {
	case "E_SHARD_NOT_FOUND", "E_REPORT_NOT_FOUND":
		return http.StatusNotFound
	case "E_STALE_CHECKPOINT":
		return http.StatusConflict
	case "E_REVISION_OUT_OF_RANGE", "E_REVISION_UNDONE", "E_REVISION_ORDER", "E_UNDO_INITIAL", "E_FORK_CONFLICT":
		return http.StatusUnprocessableEntity
	case "E_REQUEST_JSON", "E_REVISION_INVALID", "E_FIXTURE_UNKNOWN", "E_LOG_INVALID":
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, status int, be *model.BusinessError) {
	writeJSON(w, status, map[string]any{"error": be})
}

func requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(rec, r)
		fmt.Printf("%s %s -> %d (%s)\n", r.Method, r.URL.Path, rec.status, time.Since(start).Round(time.Millisecond))
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}
