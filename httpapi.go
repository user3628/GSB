package speakerdiar

import (
	"encoding/json"
	"errors"
	"net/http"
	"sync"
)

// RunHTTPRequest is the optional body for POST /v1/run.
type RunHTTPRequest struct {
	ProducerID string `json:"producer_id"`
	Injection  string `json:"injection"`
	Verify     bool   `json:"verify_incremental"`
}

// HTTPAPIServer exposes local deterministic run, stop, artifact, and injection endpoints.
type HTTPAPIServer struct {
	WorkDir    string
	ProducerID string

	mu     sync.Mutex
	active *Service
}

// NewHTTPHandler builds the standard-library HTTP integration handler.
func NewHTTPHandler(workDir, producerID string) http.Handler {
	api := &HTTPAPIServer{WorkDir: workDir, ProducerID: producerID}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", api.health)
	mux.HandleFunc("GET /v1/manifest", api.manifest)
	mux.HandleFunc("POST /v1/run", api.run)
	mux.HandleFunc("POST /v1/stop", api.stop)
	mux.HandleFunc("POST /v1/inject/{code}", api.inject)
	mux.HandleFunc("GET /v1/checkpoints", api.checkpoints)
	mux.HandleFunc("GET /v1/artifacts/{name}", api.artifact)
	return mux
}

func (a *HTTPAPIServer) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "service": "speakerdiar"})
}

func (a *HTTPAPIServer) manifest(w http.ResponseWriter, r *http.Request) {
	m, err := LoadManifest(a.WorkDir)
	if err != nil {
		a.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, m)
}

func (a *HTTPAPIServer) run(w http.ResponseWriter, r *http.Request) {
	var req RunHTTPRequest
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			a.writeError(w, newError(CodeInvalidParam, "invalid JSON request body"))
			return
		}
	}
	producer := req.ProducerID
	if producer == "" {
		producer = a.ProducerID
	}
	svc, err := NewService(a.WorkDir, producer, Injection(req.Injection))
	if err != nil {
		a.writeError(w, err)
		return
	}
	a.mu.Lock()
	a.active = svc
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		a.active = nil
		a.mu.Unlock()
		_ = svc.Close()
	}()

	report, err := svc.Run()
	if err != nil {
		a.writeError(w, err)
		return
	}
	if req.Verify && !report.GracefulStop {
		if err := VerifyIncrementalEqualsFull(a.WorkDir, svc.Manifest, report); err != nil {
			a.writeError(w, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, report)
}

func (a *HTTPAPIServer) inject(w http.ResponseWriter, r *http.Request) {
	req := RunHTTPRequest{Injection: r.PathValue("code")}
	if r.ContentLength > 0 {
		_ = json.NewDecoder(r.Body).Decode(&req)
		req.Injection = r.PathValue("code")
	}
	producer := req.ProducerID
	if producer == "" {
		producer = a.ProducerID + "-inject"
	}
	svc, err := NewService(a.WorkDir, producer, Injection(req.Injection))
	if err != nil {
		a.writeError(w, err)
		return
	}
	defer svc.Close()
	report, err := svc.Run()
	if err != nil {
		a.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (a *HTTPAPIServer) stop(w http.ResponseWriter, _ *http.Request) {
	a.mu.Lock()
	active := a.active
	a.mu.Unlock()
	if active == nil {
		writeJSON(w, http.StatusOK, map[string]any{"stop_requested": false, "reason": "no active run"})
		return
	}
	active.RequestStop()
	writeJSON(w, http.StatusAccepted, map[string]any{"stop_requested": true})
}

func (a *HTTPAPIServer) checkpoints(w http.ResponseWriter, _ *http.Request) {
	store, err := OpenStore(a.WorkDir)
	if err != nil {
		a.writeError(w, err)
		return
	}
	defer store.Close()
	m, err := LoadManifest(a.WorkDir)
	if err != nil {
		a.writeError(w, err)
		return
	}
	cps, err := store.ListCheckpoints(m.SessionID)
	if err != nil {
		a.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"checkpoints": cps})
}

func (a *HTTPAPIServer) artifact(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	allowed := map[string]string{
		"segments":    "segments.json",
		"overlaps":    "overlaps.json",
		"voiceprints": "voiceprints.json",
		"final":       "final.json",
	}
	fileName, ok := allowed[name]
	if !ok {
		a.writeError(w, newError(CodeInvalidParam, "unknown artifact "+name))
		return
	}
	b, err := ReadOutputFile(a.WorkDir, fileName)
	if err != nil {
		a.writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (a *HTTPAPIServer) writeError(w http.ResponseWriter, err error) {
	code := CodeOf(err)
	status := http.StatusInternalServerError
	switch code {
	case CodeStaleCheckpoint:
		status = http.StatusConflict
	case CodeInvalidParam, CodeInputLength, CodeRateMismatch, CodeEmbedding, CodeTimeline, CodeSeparation, CodeNumeric:
		status = http.StatusUnprocessableEntity
	case CodeSnapshot, CodeCheckpointUnavailable:
		status = http.StatusInternalServerError
	}
	var de *Error
	_ = errors.As(err, &de)
	writeJSON(w, status, map[string]any{"ok": false, "code": code, "error": de, "message": err.Error()})
}
