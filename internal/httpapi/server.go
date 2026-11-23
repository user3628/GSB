package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"upstreampool/internal/model"
	"upstreampool/internal/service"
	"upstreampool/internal/sim"
	"upstreampool/internal/statemachine"
)

type Server struct {
	svc *service.Service
	srv *http.Server
}

func New(addr string, svc *service.Service) *Server {
	api := &Server{svc: svc}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", api.health)
	mux.HandleFunc("GET /v1/endpoints", api.endpoints)
	mux.HandleFunc("POST /v1/tracks", api.submit)
	mux.HandleFunc("GET /v1/tracks/{track_id}/snapshot", api.snapshot)
	mux.HandleFunc("GET /v1/tracks/{track_id}/connections/{connection_id}/trace", api.trace)
	mux.HandleFunc("GET /v1/tracks/{track_id}/connections/{connection_id}", api.trace)
	mux.HandleFunc("GET /v1/recovery", api.recovery)
	mux.HandleFunc("POST /v1/statemachine/validate", api.validateTransition)
	api.srv = &http.Server{
		Addr:              addr,
		Handler:           requestLogger(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      5 * time.Minute,
		IdleTimeout:       60 * time.Second,
	}
	return api
}

func (s *Server) ListenAndServe() error { return s.srv.ListenAndServe() }

func (s *Server) Shutdown() error { return s.srv.Close() }

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{"ok": true, "service": "upstream-pool-diagnostics"})
}

func (s *Server) endpoints(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{"endpoints": s.svc.Endpoints()})
}

func (s *Server) submit(w http.ResponseWriter, r *http.Request) {
	var in model.SubmitRequest
	if err := decodeJSON(w, r, &in); err != nil {
		writeBusiness(w, model.NewError(400, model.ErrSpecInvalid, "invalid submission JSON", map[string]any{"error": err.Error()}))
		return
	}
	out, err := s.svc.Submit(r.Context(), in)
	if err != nil {
		writeAnyError(w, err)
		return
	}
	writeJSON(w, 200, out)
}

type snapshotResponse struct {
	*sim.Snapshot
	RequestedAtMS *int64 `json:"requested_at_ms,omitempty"`
	HorizonMS     int64  `json:"horizon_ms"`
	FinalSeq      int64  `json:"final_seq"`
	Final         bool   `json:"final"`
}

func (s *Server) snapshot(w http.ResponseWriter, r *http.Request) {
	trackID := strings.TrimSpace(r.PathValue("track_id"))
	if trackID == "" {
		writeBusiness(w, model.NewError(400, model.ErrSpecInvalid, "track_id is required", nil))
		return
	}
	var at *int64
	if raw := r.URL.Query().Get("at_ms"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value < 0 {
			writeBusiness(w, model.NewError(400, model.ErrSpecInvalid, "at_ms must be a non-negative integer", map[string]any{"at_ms": raw}))
			return
		}
		at = &value
	}
	snap, horizon, finalSeq, err := s.svc.Snapshot(r.Context(), trackID, at)
	if err != nil {
		writeAnyError(w, err)
		return
	}
	isFinal := at == nil || snap.Seq == finalSeq
	writeJSON(w, 200, snapshotResponse{Snapshot: snap, RequestedAtMS: at, HorizonMS: horizon, FinalSeq: finalSeq, Final: isFinal})
}

func (s *Server) trace(w http.ResponseWriter, r *http.Request) {
	trackID := strings.TrimSpace(r.PathValue("track_id"))
	connID := strings.TrimSpace(r.PathValue("connection_id"))
	if trackID == "" || connID == "" {
		writeBusiness(w, model.NewError(400, model.ErrSpecInvalid, "track_id and connection_id are required", nil))
		return
	}
	tr, err := s.svc.Trace(r.Context(), trackID, connID)
	if err != nil {
		writeAnyError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"track_id": trackID, "connection_id": connID, "transitions": tr})
}

func (s *Server) recovery(w http.ResponseWriter, r *http.Request) {
	report, err := s.svc.LatestRecovery(r.Context())
	if err != nil {
		writeAnyError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"recovery": report})
}

type transitionRequest struct {
	From string `json:"from"`
	To   string `json:"to"`
}

func (s *Server) validateTransition(w http.ResponseWriter, r *http.Request) {
	var in transitionRequest
	if err := decodeJSON(w, r, &in); err != nil {
		writeBusiness(w, model.NewError(400, model.ErrSpecInvalid, "invalid transition JSON", map[string]any{"error": err.Error()}))
		return
	}
	from := statemachine.Stage(strings.ToUpper(strings.TrimSpace(in.From)))
	to := statemachine.Stage(strings.ToUpper(strings.TrimSpace(in.To)))
	if err := statemachine.Validate(from, to); err != nil {
		writeAnyError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"valid": true, "from": from, "to": to})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 16<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return errors.New("request body must contain exactly one JSON object")
	}
	return nil
}

func writeAnyError(w http.ResponseWriter, err error) {
	var be *model.BusinessError
	if errors.As(err, &be) {
		writeBusiness(w, be)
		return
	}
	log.Printf("internal error: %v", err)
	writeBusiness(w, model.NewError(500, model.ErrInternal, "internal server error", nil))
}

func writeBusiness(w http.ResponseWriter, be *model.BusinessError) {
	status := be.Status
	if status == 0 {
		status = 500
	}
	writeJSON(w, status, be.Body)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("write response: %v", err)
	}
}

func requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.RequestURI(), time.Since(start).Round(time.Millisecond))
	})
}
