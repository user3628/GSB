package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"dialogalign/internal/domain"
	"dialogalign/internal/engine"
	"dialogalign/internal/generator"
)

type Server struct {
	root string
}

func New(root string) *Server { return &Server{root: root} }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /v1/generate", func(w http.ResponseWriter, r *http.Request) {
		if err := generator.Generate(s.root); err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"generated": true, "data_dir": engine.DataDir(s.root)})
	})
	mux.HandleFunc("POST /v1/import", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 32<<20))
		if err != nil {
			writeError(w, domain.BizError(domain.CodeInvalidRequest, err.Error()))
			return
		}
		if err := engine.ImportSessions(s.root, body); err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"imported": true})
	})
	mux.HandleFunc("POST /v1/run", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ClearStop bool `json:"clear_stop"`
			Reset     bool `json:"reset"`
		}
		if r.Body != nil {
			_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req)
		}
		run(w, r, s.root, engine.RunOptions{ClearStop: req.ClearStop, Reset: req.Reset, RunMode: "http"})
	})
	mux.HandleFunc("POST /v1/resume", func(w http.ResponseWriter, r *http.Request) {
		run(w, r, s.root, engine.RunOptions{ClearStop: true, RunMode: "resume"})
	})
	mux.HandleFunc("POST /v1/stop", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Reason string `json:"reason"`
		}
		_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req)
		if req.Reason == "" {
			req.Reason = "http stop request"
		}
		if err := engine.New(s.root).Stop(r.Context(), req.Reason); err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"stop_requested": true, "reason": req.Reason})
	})
	mux.HandleFunc("GET /v1/sessions", func(w http.ResponseWriter, r *http.Request) {
		summary, err := engine.New(s.root).Summary(r.Context())
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, summary)
	})
	mux.HandleFunc("GET /v1/sessions/{id}/turns", func(w http.ResponseWriter, r *http.Request) {
		state, err := engine.New(s.root).SessionState(r.Context(), r.PathValue("id"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, state)
	})
	mux.HandleFunc("GET /v1/sessions/{id}/fingerprints", func(w http.ResponseWriter, r *http.Request) {
		fp, err := engine.New(s.root).Fingerprints(r.Context(), r.PathValue("id"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, fp)
	})
	mux.HandleFunc("GET /v1/alignments", func(w http.ResponseWriter, r *http.Request) {
		reports, err := engine.New(s.root).Alignments(r.Context(), r.URL.Query().Get("session"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, reports)
	})
	mux.HandleFunc("GET /v1/invalidations", func(w http.ResponseWriter, r *http.Request) {
		records, err := engine.New(s.root).Invalidations(r.Context(), r.URL.Query().Get("session"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, records)
	})
	mux.HandleFunc("GET /v1/export", func(w http.ResponseWriter, r *http.Request) {
		bundle, err := engine.New(s.root).Export(r.Context())
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, bundle)
	})
	mux.HandleFunc("POST /v1/verify", func(w http.ResponseWriter, r *http.Request) {
		if err := engine.New(s.root).Verify(r.Context()); err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"integrity": "ok"})
	})
	mux.HandleFunc("GET /v1/active-map", func(w http.ResponseWriter, r *http.Request) {
		dataset, err := engine.LoadDataset(s.root)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, domain.ActiveMap{Version: dataset.ActiveMap.Version})
	})
	mux.HandleFunc("POST /v1/active-map", func(w http.ResponseWriter, r *http.Request) {
		var req domain.ActiveMap
		if err := decodeJSON(r, &req); err != nil {
			writeError(w, err)
			return
		}
		if err := engine.ActivateMap(s.root, req.Version); err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, req)
	})
	mux.HandleFunc("PUT /v1/role-maps/{version}", func(w http.ResponseWriter, r *http.Request) {
		var roleMap domain.RoleMap
		if err := decodeJSON(r, &roleMap); err != nil {
			writeError(w, err)
			return
		}
		if r.PathValue("version") != roleMap.Version {
			writeError(w, domain.BizError(domain.CodeMapVersionConflict, "path version and role-map version differ"))
			return
		}
		if err := engine.ValidateRoleMap(roleMap); err != nil {
			writeError(w, err)
			return
		}
		if err := engine.SaveRoleMap(s.root, roleMap); err != nil {
			writeError(w, err)
			return
		}
		activated := false
		if strings.EqualFold(r.URL.Query().Get("activate"), "true") {
			if err := engine.ActivateMap(s.root, roleMap.Version); err != nil {
				writeError(w, err)
				return
			}
			activated = true
		}
		writeJSON(w, http.StatusOK, map[string]any{"version": roleMap.Version, "saved": true, "activated": activated})
	})
	return recoverMiddleware(mux)
}

func run(w http.ResponseWriter, r *http.Request, root string, opts engine.RunOptions) {
	report, err := engine.New(root).Run(r.Context(), opts)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func decodeJSON(r *http.Request, v any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, 16<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(v); err != nil {
		return domain.BizError(domain.CodeInvalidRequest, err.Error())
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	blob, err := domain.CanonicalJSON(v)
	if err != nil {
		http.Error(w, `{"code":"E_INTERNAL","message":"json encode failed"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(blob)
	_, _ = w.Write([]byte("\n"))
}

func writeError(w http.ResponseWriter, err error) {
	be := domain.AsBusinessError(err)
	status := http.StatusBadRequest
	switch be.Code {
	case domain.CodeStaleCheckpoint, domain.CodeCheckpointRollback, domain.CodeCheckpointTampered, domain.CodeMapVersionConflict:
		status = http.StatusConflict
	case domain.CodeWorkspaceCorrupt:
		status = http.StatusInternalServerError
	}
	writeJSON(w, status, be)
}

func recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				writeError(w, fmt.Errorf("panic: %v", recovered))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func Serve(ctx context.Context, root, addr string) error {
	server := &http.Server{
		Addr:              addr,
		Handler:           New(root).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServe() }()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return ctx.Err()
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
