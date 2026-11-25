package probe

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	"auditdrift/internal/model"
)

type Endpoint struct {
	Server     *httptest.Server
	httpServer *http.Server
	listener   net.Listener
	url        string

	mu     sync.Mutex
	values map[string]Response
	delays map[string]time.Duration
}

func NewTestEndpoint() *Endpoint {
	e := &Endpoint{values: make(map[string]Response), delays: make(map[string]time.Duration)}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/probe", e.handleProbe)
	e.Server = httptest.NewServer(mux)
	return e
}

func NewListeningEndpoint(addr string) (*Endpoint, error) {
	e := &Endpoint{values: make(map[string]Response), delays: make(map[string]time.Duration)}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/probe", e.handleProbe)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	e.listener = listener
	e.httpServer = &http.Server{Handler: mux}
	e.url = "http://" + listener.Addr().String()
	go func() { _ = e.httpServer.Serve(listener) }()
	return e, nil
}

func (e *Endpoint) URL() string {
	if e.url != "" {
		return e.url
	}
	return e.Server.URL
}

func (e *Endpoint) Close() {
	if e.Server != nil {
		e.Server.Close()
	}
	if e.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = e.httpServer.Shutdown(ctx)
	}
	if e.listener != nil {
		_ = e.listener.Close()
	}
}

func (e *Endpoint) SetObservation(obs model.Observation) {
	e.mu.Lock()
	defer e.mu.Unlock()
	key := endpointKey(obs.Host, obs.BaselineID)
	raw, _ := json.Marshal(obs.Actual)
	e.values[key] = Response{Host: obs.Host, BaselineID: obs.BaselineID, Actual: raw, SampledAtMS: obs.SampledAtMS}
}

func (e *Endpoint) SetValue(host, baselineID string, value model.Value, sampledAtMS int64) {
	e.SetObservation(model.Observation{Host: host, BaselineID: baselineID, Actual: value, SampledAtMS: sampledAtMS})
}

func (e *Endpoint) SetDelay(baselineID string, delay time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.delays[baselineID] = delay
}

func (e *Endpoint) handleProbe(w http.ResponseWriter, r *http.Request) {
	host := r.URL.Query().Get("host")
	baselineID := r.URL.Query().Get("baseline_id")
	e.mu.Lock()
	delay := e.delays[baselineID]
	resp, ok := e.values[endpointKey(host, baselineID)]
	e.mu.Unlock()

	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.Context().Done():
			return
		}
	}
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "fixture value not found"})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func endpointKey(host, baselineID string) string {
	return host + "\x00" + baselineID
}

func ClosedLoopbackURL() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		return "", err
	}
	return "http://" + addr, nil
}

var _ = context.Background
var _ = fmt.Sprintf
