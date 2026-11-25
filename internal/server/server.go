package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"queryrewrite/internal/engine"
	"queryrewrite/internal/gen"
	"queryrewrite/internal/policy"
	"queryrewrite/internal/spec"
	"queryrewrite/internal/store"
)

// Run starts the deterministic demo HTTP server with graceful shutdown.
func Run(addr, dataDir, dbPath string) error {
	h, err := newHandler(dataDir, dbPath)
	if err != nil {
		return err
	}
	defer h.close()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", h.health)
	mux.HandleFunc("/import", h.importShards)
	mux.HandleFunc("/policy", h.getPolicy)
	mux.HandleFunc("/policy/patch", h.applyPatch)
	mux.HandleFunc("/policy/replace", h.replacePolicy)
	mux.HandleFunc("/run", h.runTurn)
	mux.HandleFunc("/recover", h.recover)
	mux.HandleFunc("/status", h.status)
	mux.HandleFunc("/report", h.report)
	mux.HandleFunc("/stop", h.stop)

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	h.srv = srv
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	select {
	case <-ctx.Done():
		shutdownCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		return srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

type handler struct {
	mu      sync.Mutex
	dataDir string
	dbPath  string
	store   *store.Store
	engine  *engine.Engine
	srv     *http.Server
}

func newHandler(dataDir, dbPath string) (*handler, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	h := &handler{dataDir: dataDir, dbPath: dbPath}
	if _, err := os.Stat(dataDir + "/policy.yaml"); errors.Is(err, os.ErrNotExist) {
		if _, err := gen.WritePolicy(dataDir, gen.DemoPolicy()); err != nil {
			return nil, err
		}
	}
	p, err := policy.LoadYAML(dataDir + "/policy.yaml")
	if err != nil {
		return nil, err
	}
	st, err := store.Open(dbPath, dbPath+".lock")
	if err != nil {
		return nil, err
	}
	if err := st.SavePolicy(p, "baseline"); err != nil {
		st.Close()
		return nil, err
	}
	e, err := engine.New(st, p)
	if err != nil {
		st.Close()
		return nil, err
	}
	h.store, h.engine = st, e
	return h, nil
}

func (h *handler) close() {
	if h.store != nil {
		_ = h.store.Close()
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeBiz(w http.ResponseWriter, err error) {
	berr, ok := err.(*spec.BizError)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	code := http.StatusBadRequest
	switch berr.Code {
	case spec.CodeStaleVersion, spec.CodeStaleCheckpoint, spec.CodeStaleRead:
		code = http.StatusConflict
	case spec.CodeNumeric:
		code = http.StatusUnprocessableEntity
	}
	writeJSON(w, code, berr)
}

func (h *handler) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *handler) importShards(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		writeBiz(w, err)
		return
	}
	var turns []spec.QueryTurn
	if len(bytes.TrimSpace(body)) > 0 {
		if err := json.Unmarshal(body, &turns); err != nil {
			writeBiz(w, err)
			return
		}
	}
	if len(turns) == 0 {
		turns = gen.DemoSessions()
	}
	paths, err := gen.WriteShards(h.dataDir, turns)
	if err != nil {
		writeBiz(w, err)
		return
	}
	policyPath, err := gen.WritePolicy(h.dataDir, gen.DemoPolicy())
	if err != nil {
		writeBiz(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"shards": paths, "policy": policyPath})
}

func (h *handler) getPolicy(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	writeJSON(w, http.StatusOK, h.engine.Policy())
}

func (h *handler) applyPatch(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	var patch spec.PolicyPatch
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		writeBiz(w, err)
		return
	}
	cur := h.engine.Policy()
	patch.Version = cur.Version + 1
	next, earliest, err := policy.ApplyPatch(cur, patch)
	if err != nil {
		writeBiz(w, err)
		return
	}
	if err := h.engine.SetPolicy(next, "incremental"); err != nil {
		writeBiz(w, err)
		return
	}
	if _, err := gen.WritePolicy(h.dataDir, next); err != nil {
		writeBiz(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"policy": next, "earliest_affected_stage": earliest})
}

func (h *handler) replacePolicy(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	var incoming spec.Policy
	if err := json.NewDecoder(r.Body).Decode(&incoming); err != nil {
		writeBiz(w, err)
		return
	}
	incoming.ContentDigest = policy.Digest(incoming)
	kind, berr := policy.Classify(h.engine.Policy(), incoming)
	if berr != nil {
		writeBiz(w, berr)
		return
	}
	if kind != "replacement" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "classified as " + kind})
		return
	}
	if err := h.engine.SetPolicy(incoming, "replacement"); err != nil {
		writeBiz(w, err)
		return
	}
	_, _ = gen.WritePolicy(h.dataDir, incoming)
	writeJSON(w, http.StatusOK, map[string]string{"status": "replaced", "digest": incoming.ContentDigest})
}

func (h *handler) runTurn(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	var q spec.QueryTurn
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		writeBiz(w, err)
		return
	}
	out, err := h.engine.RunTurn(q)
	if err != nil {
		writeBiz(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *handler) recover(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	rep, err := h.engine.BuildReport()
	if err != nil {
		writeBiz(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"summary": rep.Summary})
}

func (h *handler) status(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	sessionID := r.URL.Query().Get("session")
	turn, _ := strconv.Atoi(r.URL.Query().Get("turn"))
	_, hasObserved := r.URL.Query()["observed_version"]
	observed, _ := strconv.Atoi(r.URL.Query().Get("observed_version"))
	if hasObserved {
		adj, berr := h.engine.ReadAdjudication(sessionID, turn, observed)
		if berr != nil {
			writeBiz(w, berr)
			return
		}
		writeJSON(w, http.StatusOK, adj)
		return
	}
	status, err := h.engine.StageStatus(sessionID, turn)
	if err != nil {
		writeBiz(w, err)
		return
	}
	ver, dig := h.engine.VersionFingerprint()
	writeJSON(w, http.StatusOK, map[string]any{"status": status, "policy_version": ver, "digest": dig})
}

func (h *handler) report(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	rep, err := h.engine.BuildReport()
	if err != nil {
		writeBiz(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

func (h *handler) stop(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "shutting down"})
	go func() {
		time.Sleep(100 * time.Millisecond)
		ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = h.srv.Shutdown(ctx)
	}()
}
