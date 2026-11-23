package probe

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"gatewayworkbench/internal/model"
)

func httpInstance(t *testing.T, path string, status int, body string, timeoutMS int) (model.Instance, func()) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			time.Sleep(500 * time.Millisecond)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	port := portFromURL(t, server.URL)
	ins := model.Instance{
		ID: "test-http", Port: port, Zone: "z", Weight: 100,
		Probe: model.ProbeSpec{
			Mode: "http", Path: path, ExpectedStatus: http.StatusOK,
			BodyMarker: "UPSTREAM-OK", TimeoutMillis: timeoutMS,
			SuccessThreshold: 1, FailureThreshold: 1,
		},
	}
	return ins, server.Close
}

func portFromURL(t *testing.T, raw string) int {
	t.Helper()
	_, portText, err := net.SplitHostPort(raw[len("http://"):])
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	return port
}

func TestProbeHTTPSuccessAndMarkerMismatch(t *testing.T) {
	ins, cleanup := httpInstance(t, "/healthz", http.StatusOK, "UPSTREAM-OK body", 1000)
	defer cleanup()
	outcome := New().Probe(context.Background(), ins)
	if !outcome.Success || outcome.ErrCode != "" || outcome.StatusCode != http.StatusOK {
		t.Fatalf("expected successful probe, got %+v", outcome)
	}

	ins.Probe.BodyMarker = "MISSING-MARKER"
	outcome = New().Probe(context.Background(), ins)
	if outcome.Success || outcome.ErrCode != model.CodeProbeFailed {
		t.Fatalf("expected marker mismatch failure, got %+v", outcome)
	}
}

func TestProbeRealTimeoutReleasesSlot(t *testing.T) {
	ins, cleanup := httpInstance(t, "/slow", http.StatusOK, "slow-done", 80)
	defer cleanup()
	start := time.Now()
	outcome := New().Probe(context.Background(), ins)
	elapsed := time.Since(start)
	if outcome.Success || outcome.ErrCode != model.CodeProbeTimeout {
		t.Fatalf("expected E_PROBE_TIMEOUT, got %+v", outcome)
	}
	if elapsed > 400*time.Millisecond {
		t.Fatalf("timeout must fire near the deadline instead of waiting, elapsed=%s", elapsed)
	}
	// 超时后立刻再探同一端点，验证底层连接已关闭、没有名额/连接泄漏导致阻塞。
	done := make(chan model.ProbeOutcome, 1)
	go func() { done <- New().Probe(context.Background(), ins) }()
	select {
	case next := <-done:
		if next.ErrCode != model.CodeProbeTimeout {
			t.Fatalf("second probe should timeout independently, got %+v", next)
		}
	case <-time.After(700 * time.Millisecond):
		t.Fatal("probe slot/connection leaked after timeout")
	}
}

func TestProbeTCPRefused(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close() // 释放端口，确保后续连接被拒绝

	ins := model.Instance{
		ID: "refused", Port: port, Zone: "z", Weight: 100,
		Probe: model.ProbeSpec{Mode: "tcp", TimeoutMillis: 500},
	}
	outcome := New().Probe(context.Background(), ins)
	if outcome.Success || outcome.ErrCode != model.CodeProbeRefused {
		t.Fatalf("expected E_PROBE_REFUSED, got %+v", outcome)
	}
}

func TestProbeTCPSuccess(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			_ = conn.Close()
		}
	}()
	ins := model.Instance{
		ID: "tcp-ok", Port: listener.Addr().(*net.TCPAddr).Port, Zone: "z", Weight: 100,
		Probe: model.ProbeSpec{Mode: "tcp", TimeoutMillis: 500},
	}
	outcome := New().Probe(context.Background(), ins)
	if !outcome.Success {
		t.Fatalf("expected tcp success, got %+v", outcome)
	}
}
