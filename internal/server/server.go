// Package server exposes the workbench over a local HTTP diagnostic API.
package server

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"

	"matbench/internal/demo"
	"matbench/internal/pipeline"
	"matbench/internal/recipe"
	"matbench/internal/store"
)

// Server wires the workspace, store and pipeline.
type Server struct {
	dir string
	st  *store.Store
	p   *pipeline.Pipeline
}

// New opens the workbench server for a workspace.
func New(dir string) (*Server, error) {
	st, err := store.Open(dir)
	if err != nil {
		return nil, err
	}
	return &Server{dir: dir, st: st, p: &pipeline.Pipeline{Dir: dir, Store: st}}, nil
}

// Close releases the database.
func (s *Server) Close() error { return s.st.Close() }

// Store exposes the checkpoint store for CLI diagnostics.
func (s *Server) Store() *store.Store { return s.st }

// Handler returns the HTTP mux.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/health", s.handleHealth)
	mux.HandleFunc("/api/v1/import", s.handleImport)
	mux.HandleFunc("/api/v1/run", s.handleRun)
	mux.HandleFunc("/api/v1/incremental", s.handleIncremental)
	mux.HandleFunc("/api/v1/status", s.handleStatus)
	mux.HandleFunc("/api/v1/materials/", s.handleMaterial)
	mux.HandleFunc("/api/v1/stop", s.handleStop)
	mux.HandleFunc("/api/v1/resume", s.handleResume)
	mux.HandleFunc("/api/v1/export", s.handleExport)
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]any{"error": err.Error()})
}

func (s *Server) handleHealth(w http.ResponseWriter, req *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleImport(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, os.ErrPermission)
		return
	}
	book, err := demo.Setup(s.dir)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"materials": len(book.Recipes), "cards": len(book.Cards),
		"book_path": filepath.Join(s.dir, demo.BookPath),
	})
}

type runRequest struct {
	Workers int      `json:"workers"`
	OnlyIDs []string `json:"only_ids"`
	Force   bool     `json:"force_all"`
}

func (s *Server) handleRun(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, os.ErrPermission)
		return
	}
	var in runRequest
	if req.Body != nil {
		_ = json.NewDecoder(req.Body).Decode(&in)
	}
	book, err := demo.Load(s.dir)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	stop := demo.FileStop{Path: filepath.Join(s.dir, demo.StopFile)}
	sum, err := s.p.RunFull(book, pipeline.RunOptions{
		Workers: in.Workers, OnlyIDs: in.OnlyIDs, ForceAll: in.Force, Stop: stop,
	})
	if err != nil {
		writeErr(w, http.StatusUnprocessableEntity, err)
		return
	}
	writeJSON(w, http.StatusOK, sum)
}

func (s *Server) handleIncremental(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, os.ErrPermission)
		return
	}
	var in struct {
		Workers int            `json:"workers"`
		Patches []recipe.Patch `json:"patches"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	book, err := demo.Load(s.dir)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	sum, err := s.p.RunIncremental(book, in.Patches, pipeline.RunOptions{Workers: in.Workers})
	if err != nil {
		writeErr(w, http.StatusUnprocessableEntity, err)
		return
	}
	writeJSON(w, http.StatusOK, sum)
}

func (s *Server) handleStatus(w http.ResponseWriter, req *http.Request) {
	rows, err := s.st.AllActive()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"checkpoints": rows})
}

func (s *Server) handleMaterial(w http.ResponseWriter, req *http.Request) {
	id := req.URL.Path[len("/api/v1/materials/"):]
	if id == "" {
		writeErr(w, http.StatusBadRequest, os.ErrInvalid)
		return
	}
	kind := req.URL.Query().Get("kind")
	if kind == "invalidations" {
		rows, err := s.st.Invalidations(id)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"material_id": id, "invalidations": rows})
		return
	}
	stage := req.URL.Query().Get("stage")
	if stage == "" {
		stage = pipeline.StageReport
	}
	cp, err := s.st.Active(id, stage)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if cp == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "checkpoint not found"})
		return
	}
	var payload any
	if err := json.Unmarshal(cp.Payload, &payload); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"material_id": id, "stage": stage, "seq": cp.Seq,
		"recipe_fp": cp.RecipeFP, "artifact_fp": cp.ArtifactFP, "payload": payload,
	})
}

func (s *Server) handleStop(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, os.ErrPermission)
		return
	}
	stop := demo.FileStop{Path: filepath.Join(s.dir, demo.StopFile)}
	if err := stop.RequestStop(); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "stop requested"})
}

func (s *Server) handleResume(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, os.ErrPermission)
		return
	}
	stop := demo.FileStop{Path: filepath.Join(s.dir, demo.StopFile)}
	if err := stop.ClearStop(); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	book, err := demo.Load(s.dir)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	sum, err := s.p.RunFull(book, pipeline.RunOptions{})
	if err != nil {
		writeErr(w, http.StatusUnprocessableEntity, err)
		return
	}
	writeJSON(w, http.StatusOK, sum)
}

func (s *Server) handleExport(w http.ResponseWriter, req *http.Request) {
	book, err := demo.Load(s.dir)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	report := map[string]any{"materials": []any{}}
	for _, r := range book.Recipes {
		cp, err := s.st.Active(r.MaterialID, pipeline.StageReport)
		if err != nil || cp == nil {
			continue
		}
		var v any
		if err := json.Unmarshal(cp.Payload, &v); err == nil {
			report["materials"] = append(report["materials"].([]any), v)
		}
	}
	path := filepath.Join(s.dir, demo.ReportDir, "diagnostic_report.json")
	data, _ := json.MarshalIndent(report, "", "  ")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"report_path": path})
}
