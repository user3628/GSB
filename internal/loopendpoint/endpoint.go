package loopendpoint

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"driftengine/internal/spec"
)

type ObjectState struct {
	ID            string                       `json:"id"`
	Namespace     string                       `json:"namespace,omitempty"`
	Fields        spec.Fields                  `json:"fields"`
	SourceMarkers map[string]spec.SourceMarker `json:"source_markers,omitempty"`
	UpdatedAt     time.Time                    `json:"updated_at,omitempty"`
}

type Server struct {
	mu       sync.RWMutex
	states   map[string]ObjectState
	listener net.Listener
	server   *http.Server
}

func LoadStateFile(path string) (map[string]ObjectState, error) {
	if path == "" {
		return map[string]ObjectState{}, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var states map[string]ObjectState
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.UseNumber()
	if err := dec.Decode(&states); err != nil {
		return nil, err
	}
	if states == nil {
		states = map[string]ObjectState{}
	}
	return states, nil
}

func New(addr string, initial map[string]ObjectState) (*Server, error) {
	if initial == nil {
		initial = map[string]ObjectState{}
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	host, _, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		_ = listener.Close()
		return nil, &net.AddrError{Err: "test endpoint must listen on loopback", Addr: addr}
	}
	s := &Server{states: initial, listener: listener}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/loopback/v1/state", s.handleAllState)
	mux.HandleFunc("/loopback/v1/objects/", s.handleObject)
	s.server = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !remoteIsLoopback(r) {
			http.Error(w, "only loopback clients are allowed", http.StatusForbidden)
			return
		}
		mux.ServeHTTP(w, r)
	})}
	return s, nil
}

func (s *Server) ListenAndServe() error {
	return s.server.Serve(s.listener)
}

func (s *Server) Start() {
	go func() { _ = s.ListenAndServe() }()
}

func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return s.server.Shutdown(ctx)
}

func (s *Server) URL() string {
	return "http://" + s.listener.Addr().String()
}

func (s *Server) Addr() string { return s.listener.Addr().String() }

func (s *Server) Get(id string) (ObjectState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state, ok := s.states[id]
	return state, ok
}

func (s *Server) Put(state ObjectState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if state.Fields == nil {
		state.Fields = spec.Fields{}
	}
	state.UpdatedAt = time.Now().UTC()
	s.states[state.ID] = state
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleAllState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	writeJSON(w, http.StatusOK, s.states)
}

func (s *Server) handleObject(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/loopback/v1/objects/")
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	id := parts[0]
	if len(parts) == 1 && r.Method == http.MethodGet {
		state, ok := s.Get(id)
		if !ok {
			state = ObjectState{ID: id, Fields: spec.Fields{}}
		}
		writeJSON(w, http.StatusOK, state)
		return
	}
	if len(parts) == 2 && parts[1] == "fields" && (r.Method == http.MethodPut || r.Method == http.MethodPost) {
		var req struct {
			Fields spec.Fields `json:"fields"`
		}
		dec := json.NewDecoder(r.Body)
		dec.UseNumber()
		if err := dec.Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		state, _ := s.Get(id)
		state.ID = id
		state.Fields = req.Fields
		s.Put(state)
		writeJSON(w, http.StatusOK, map[string]any{"acknowledged": true, "id": id, "fields": req.Fields})
		return
	}
	http.NotFound(w, r)
}

func remoteIsLoopback(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return false
	}
	if host == "::1" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(value)
}
