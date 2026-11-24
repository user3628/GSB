package simulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"driftconverge/internal/apperr"
	"driftconverge/internal/model"
)

type Config struct {
	ListenAddr   string `json:"listenAddr"`
	Mode         string `json:"mode"`
	WriteDelayMs int    `json:"writeDelayMs"`
	ReadDelayMs  int    `json:"readDelayMs"`
}

func LoadConfig(path string) (Config, error) {
	if path == "" {
		return Config{}, fmt.Errorf("simulator config path is empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if c.Mode != "normal" && c.Mode != "unresolved" && c.Mode != "timeout" && c.Mode != "fail" {
		return fmt.Errorf("simulator mode must be normal, unresolved, timeout or fail")
	}
	if c.WriteDelayMs < 0 || c.ReadDelayMs < 0 || c.WriteDelayMs > 600_000 || c.ReadDelayMs > 600_000 {
		return fmt.Errorf("simulator delays must be in [0,600000]ms")
	}
	if c.ListenAddr != "" && !IsLoopbackListenAddr(c.ListenAddr) {
		return fmt.Errorf("simulator must listen on a loopback address")
	}
	return nil
}

type WriteRequest struct {
	RoundID         string                       `json:"roundId"`
	BatchID         string                       `json:"batchId"`
	ObjectID        string                       `json:"objectId"`
	DesiredFields   map[string]model.FieldValue  `json:"desiredFields"`
	ExpectedSources map[string]model.FieldSource `json:"expectedSources,omitempty"`
	RemoveFields    []string                     `json:"removeFields,omitempty"`
}

type ObjectState struct {
	ID        string                       `json:"id"`
	Kind      string                       `json:"kind"`
	Namespace string                       `json:"namespace"`
	Actual    map[string]model.FieldValue  `json:"actual"`
	Sources   map[string]model.FieldSource `json:"sources"`
	Revision  int64                        `json:"revision"`
	LastRound string                       `json:"lastRound,omitempty"`
	LastBatch string                       `json:"lastBatch,omitempty"`
}

type WriteResponse struct {
	OK       bool   `json:"ok"`
	Mode     string `json:"mode"`
	ObjectID string `json:"objectId"`
	Revision int64  `json:"revision"`
	Applied  bool   `json:"applied"`
	Note     string `json:"note,omitempty"`
}

type Server struct {
	mu       sync.RWMutex
	config   Config
	objects  map[string]*ObjectState
	httpSrv  *http.Server
	listener net.Listener
}

func New(cluster *model.ClusterSpec, cfg Config) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	s := &Server{config: cfg, objects: make(map[string]*ObjectState, len(cluster.Objects))}
	for _, obj := range cluster.Objects {
		s.objects[obj.ID] = &ObjectState{
			ID:        obj.ID,
			Kind:      obj.Kind,
			Namespace: obj.Namespace,
			Actual:    cloneFields(obj.Actual),
			Sources:   cloneSources(obj.Sources),
		}
	}
	return s, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/write", s.handleWrite)
	mux.HandleFunc("GET /v1/objects/{id}", s.handleRead)
	mux.HandleFunc("GET /v1/read", s.handleReadQuery)
	mux.HandleFunc("GET /v1/health", s.handleHealth)
	return s.loopbackOnly(mux)
}

func (s *Server) Start(addr string) (string, error) {
	if addr == "" {
		addr = s.config.ListenAddr
	}
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	if !IsLoopbackListenAddr(addr) {
		return "", fmt.Errorf("simulator listen address must be loopback")
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", err
	}
	s.listener = ln
	s.httpSrv = &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = s.httpSrv.Serve(ln) }()
	return "http://" + ln.Addr().String(), nil
}

func (s *Server) Close(ctx context.Context) error {
	if s == nil || s.httpSrv == nil {
		return nil
	}
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err := s.httpSrv.Shutdown(shutdownCtx)
	if s.listener != nil {
		_ = s.listener.Close()
	}
	return err
}

func (s *Server) State(objectID string) (*ObjectState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state, ok := s.objects[objectID]
	if !ok {
		return nil, false
	}
	copy := *state
	copy.Actual = cloneFields(state.Actual)
	copy.Sources = cloneSources(state.Sources)
	return &copy, true
}

func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request) {
	var req WriteRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, string(apperr.SpecInvalid), "decode write request: "+err.Error())
		return
	}
	if req.ObjectID == "" {
		writeError(w, http.StatusBadRequest, string(apperr.SpecInvalid), "objectId is required")
		return
	}
	for field, value := range req.DesiredFields {
		if strings.TrimSpace(field) == "" || (value.Type != model.TypeString && value.Type != model.TypeInteger && value.Type != model.TypeFloat) {
			writeError(w, http.StatusBadRequest, string(apperr.SpecInvalid), "invalid desired field "+field)
			return
		}
	}
	if s.config.WriteDelayMs > 0 || s.config.Mode == "timeout" {
		delay := time.Duration(s.config.WriteDelayMs) * time.Millisecond
		if delay <= 0 {
			delay = 2 * time.Second
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(time.Duration(s.config.WriteDelayMs) * time.Millisecond):
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.objects[req.ObjectID]
	if !ok {
		writeError(w, http.StatusNotFound, string(apperr.SpecInvalid), "object not found")
		return
	}
	if s.config.Mode == "fail" {
		writeError(w, http.StatusInternalServerError, string(apperr.WriteFailed), "scripted simulator failure")
		return
	}
	applied := false
	if s.config.Mode == "normal" {
		next := cloneFields(req.DesiredFields)
		nextSources := cloneSources(state.Sources)
		for _, field := range req.RemoveFields {
			delete(nextSources, field)
		}
		state.Actual = next
		for field, source := range req.ExpectedSources {
			current := nextSources[field]
			if source.Expected != "" {
				current.Actual = source.Expected
			}
			nextSources[field] = current
		}
		state.Sources = nextSources
		state.Revision++
		state.LastRound = req.RoundID
		state.LastBatch = req.BatchID
		applied = true
	}
	response := WriteResponse{
		OK: true, Mode: s.config.Mode, ObjectID: req.ObjectID, Revision: state.Revision, Applied: applied,
	}
	if !applied && s.config.Mode == "unresolved" {
		response.Note = "scripted success without mutating actual fields"
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleRead(w http.ResponseWriter, r *http.Request) {
	if s.config.ReadDelayMs > 0 {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(time.Duration(s.config.ReadDelayMs) * time.Millisecond):
		}
	}
	id := r.PathValue("id")
	if unescaped, err := url.PathUnescape(id); err == nil {
		id = unescaped
	}
	s.mu.RLock()
	state, ok := s.objects[id]
	if !ok {
		s.mu.RUnlock()
		writeError(w, http.StatusNotFound, "E_OBJECT_NOT_FOUND", "object not found")
		return
	}
	copy := *state
	copy.Actual = cloneFields(state.Actual)
	copy.Sources = cloneSources(state.Sources)
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, copy)
}

func (s *Server) handleReadQuery(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("objectId")
	if id == "" {
		writeError(w, http.StatusBadRequest, string(apperr.SpecInvalid), "objectId is required")
		return
	}
	if s.config.ReadDelayMs > 0 {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(time.Duration(s.config.ReadDelayMs) * time.Millisecond):
		}
	}
	s.mu.RLock()
	state, ok := s.objects[id]
	if !ok {
		s.mu.RUnlock()
		writeError(w, http.StatusNotFound, "E_OBJECT_NOT_FOUND", "object not found")
		return
	}
	copy := *state
	copy.Actual = cloneFields(state.Actual)
	copy.Sources = cloneSources(state.Sources)
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, copy)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "mode": s.config.Mode})
}

func (s *Server) loopbackOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
			writeError(w, http.StatusForbidden, string(apperr.EndpointInvalid), "simulator accepts loopback clients only")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func IsLoopbackListenAddr(addr string) bool {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || port == "" {
		return false
	}
	if host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, apperr.Error{Code: apperr.Code(code), Message: message})
}

func cloneFields(input map[string]model.FieldValue) map[string]model.FieldValue {
	result := make(map[string]model.FieldValue, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func cloneSources(input map[string]model.FieldSource) map[string]model.FieldSource {
	result := make(map[string]model.FieldSource, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}
