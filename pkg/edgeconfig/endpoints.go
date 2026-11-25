package edgeconfig

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type MockNode struct {
	mu sync.Mutex

	NodeID        string
	AppliedDigest string
	Version       int64
	EffectiveAtMS int64
	RejectStatus  int
	RejectCode    string
	Hang          time.Duration

	RequestCount int
	Active       int
	MaxActive    int
}

func NewMockNode(nodeID, digest string, version int64) *MockNode {
	return &MockNode{NodeID: nodeID, AppliedDigest: digest, Version: version, EffectiveAtMS: nowMS()}
}

func (m *MockNode) SetState(digest string, version int64, effectiveAtMS int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.AppliedDigest = digest
	m.Version = version
	m.EffectiveAtMS = effectiveAtMS
}

func (m *MockNode) SetRejection(status int, code string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.RejectStatus = status
	m.RejectCode = code
}

func (m *MockNode) SetHang(hang time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Hang = hang
}

func (m *MockNode) Stats() (requests, active, maxActive int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.RequestCount, m.Active, m.MaxActive
}

func (m *MockNode) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	m.RequestCount++
	m.Active++
	if m.Active > m.MaxActive {
		m.MaxActive = m.Active
	}
	hang := m.Hang
	rejectStatus := m.RejectStatus
	rejectCode := m.RejectCode
	state := EndpointResponse{
		NodeID:          m.NodeID,
		AppliedDigest:   m.AppliedDigest,
		ObservedVersion: m.Version,
		EffectiveAtMS:   m.EffectiveAtMS,
	}
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.Active--
		m.mu.Unlock()
	}()

	if hang > 0 {
		timer := time.NewTimer(hang)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.Context().Done():
			return
		}
	}
	if rejectStatus >= 400 {
		if rejectCode == "" {
			rejectCode = "E_BUSINESS_REJECTED"
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(rejectStatus)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{
			"code":    rejectCode,
			"message": "mock node rejected probe",
			"node_id": m.NodeID,
		}})
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(state)
}

type MockServer struct {
	Server   *http.Server
	Listener net.Listener
	URL      string
}

func StartFreeMockServer(handler http.Handler) (*MockServer, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	server := &http.Server{Handler: handler}
	mock := &MockServer{Server: server, Listener: listener, URL: "http://" + listener.Addr().String() + "/applied"}
	go func() { _ = server.Serve(listener) }()
	return mock, nil
}

func StartMockAtEndpoint(endpoint string, handler http.Handler) (*MockServer, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	address := parsed.Host
	if parsed.Hostname() == "" {
		return nil, errors.New("endpoint host is required")
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	path := parsed.Path
	if path == "" {
		path = "/applied"
	}
	mux.Handle(path, handler)
	server := &http.Server{Handler: mux}
	mock := &MockServer{Server: server, Listener: listener, URL: endpoint}
	go func() { _ = server.Serve(listener) }()
	return mock, nil
}

func (m *MockServer) Close(ctx context.Context) error {
	if m == nil || m.Server == nil {
		return nil
	}
	return m.Server.Shutdown(ctx)
}
