package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"local/configmiddleware/internal/core"
	"local/configmiddleware/internal/engine"
)

type API struct {
	engine *engine.Engine
}

func New(e *engine.Engine) *API { return &API{engine: e} }

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/health", a.health)
	mux.HandleFunc("POST /api/v1/snapshots", a.submitSnapshot)
	mux.HandleFunc("GET /api/v1/snapshots/latest", a.latestSnapshot)
	mux.HandleFunc("GET /api/v1/snapshots/{version}", a.getSnapshot)
	mux.HandleFunc("POST /api/v1/clients", a.registerClient)
	mux.HandleFunc("PUT /api/v1/clients/{client_id}", a.updateClient)
	mux.HandleFunc("GET /api/v1/clients", a.listClients)
	mux.HandleFunc("POST /api/v1/pushes", a.triggerPush)
	mux.HandleFunc("POST /api/v1/acks", a.acknowledge)
	mux.HandleFunc("POST /api/v1/results", a.submitResult)
	mux.HandleFunc("GET /api/v1/inconsistencies", a.inconsistencies)
	mux.HandleFunc("GET /api/v1/recovery", a.recovery)
	mux.HandleFunc("GET /api/v1/state", a.state)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capture := &responseCapture{header: make(http.Header)}
		mux.ServeHTTP(capture, r)
		if isMuxError(capture) {
			code := "E_ROUTE_NOT_FOUND"
			message := "HTTP route not found"
			if capture.status == http.StatusMethodNotAllowed {
				code = "E_METHOD_NOT_ALLOWED"
				message = "HTTP method not allowed for route"
			}
			writeError(w, core.NewBizError(code, message, map[string]any{"method": r.Method, "path": r.URL.Path}))
			return
		}
		for key, values := range capture.header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		if capture.status == 0 {
			capture.status = http.StatusOK
		}
		w.WriteHeader(capture.status)
		_, _ = w.Write(capture.body.Bytes())
	})
}

func isMuxError(capture *responseCapture) bool {
	if capture.status != http.StatusNotFound && capture.status != http.StatusMethodNotAllowed {
		return false
	}
	contentType := strings.ToLower(capture.header.Get("Content-Type"))
	return !strings.HasPrefix(contentType, "application/json")
}

type responseCapture struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (w *responseCapture) Header() http.Header { return w.header }

func (w *responseCapture) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *responseCapture) Write(value []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(value)
}

func (a *API) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "time": core.Now()})
}

func (a *API) submitSnapshot(w http.ResponseWriter, r *http.Request) {
	var snap core.Snapshot
	if !decode(w, r, &snap) {
		return
	}
	stored, err := a.engine.SubmitSnapshot(r.Context(), snap)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, stored)
}

func (a *API) latestSnapshot(w http.ResponseWriter, r *http.Request) {
	snap, err := a.engine.LatestSnapshot(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (a *API) getSnapshot(w http.ResponseWriter, r *http.Request) {
	version, err := strconv.ParseInt(r.PathValue("version"), 10, 64)
	if err != nil {
		writeError(w, core.NewBizError("E_BAD_REQUEST", "invalid snapshot version", nil))
		return
	}
	snap, err := a.engine.GetSnapshot(r.Context(), version)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (a *API) registerClient(w http.ResponseWriter, r *http.Request) {
	var client core.Client
	if !decode(w, r, &client) {
		return
	}
	stored, err := a.engine.RegisterClient(r.Context(), client)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, stored)
}

func (a *API) updateClient(w http.ResponseWriter, r *http.Request) {
	var client core.Client
	if !decode(w, r, &client) {
		return
	}
	client.ClientID = r.PathValue("client_id")
	stored, err := a.engine.UpdateClient(r.Context(), client)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, stored)
}

func (a *API) listClients(w http.ResponseWriter, r *http.Request) {
	clients, err := a.engine.ListClients(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"clients": clients})
}

func (a *API) triggerPush(w http.ResponseWriter, r *http.Request) {
	var req engine.TriggerRequest
	if !decode(w, r, &req) {
		return
	}
	result, err := a.engine.TriggerPush(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (a *API) acknowledge(w http.ResponseWriter, r *http.Request) {
	var ack core.Ack
	if !decode(w, r, &ack) {
		return
	}
	stored, err := a.engine.Acknowledge(r.Context(), ack)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, stored)
}

func (a *API) submitResult(w http.ResponseWriter, r *http.Request) {
	var report core.ResultReport
	if !decode(w, r, &report) {
		return
	}
	result, err := a.engine.SubmitResult(r.Context(), report)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (a *API) inconsistencies(w http.ResponseWriter, r *http.Request) {
	var version int64
	if raw := r.URL.Query().Get("version"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			writeError(w, core.NewBizError("E_BAD_REQUEST", "invalid version query", map[string]any{"version": raw}))
			return
		}
		version = parsed
	}
	report, err := a.engine.Inconsistencies(r.Context(), version)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (a *API) recovery(w http.ResponseWriter, r *http.Request) {
	report, err := a.engine.LatestRecovery(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (a *API) state(w http.ResponseWriter, r *http.Request) {
	state, err := a.engine.State(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, state)
}

func decode(w http.ResponseWriter, r *http.Request, target any) bool {
	defer r.Body.Close()
	dec := json.NewDecoder(io.LimitReader(r.Body, 4<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(target); err != nil {
		if errors.Is(err, io.EOF) {
			writeError(w, core.NewBizError("E_BAD_REQUEST", "request body is required", nil))
		} else {
			writeError(w, core.NewBizError("E_BAD_REQUEST", "invalid JSON request", map[string]any{"error": err.Error()}))
		}
		return false
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, core.NewBizError("E_BAD_REQUEST", "request body must contain a single JSON value", map[string]any{"error": err.Error()}))
		return false
	}
	return true
}

func writeError(w http.ResponseWriter, err error) {
	status, body := core.ErrorResponse(err)
	writeJSON(w, status, body)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
