package probe_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"recoveryengine/internal/model"
	"recoveryengine/internal/probe"
)

func TestProbeTimeoutClosesRequestContext(t *testing.T) {
	cancelled := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		select {
		case <-time.After(2 * time.Second):
			http.Error(writer, "too late", http.StatusGatewayTimeout)
		case <-request.Context().Done():
			select {
			case cancelled <- struct{}{}:
			default:
			}
		}
	}))
	defer server.Close()

	client := &probe.Client{}
	started := time.Now()
	result := client.Execute(context.Background(), probe.Operation{
		Kind: probe.KindProbe, ComponentID: "slow", URL: server.URL + "/health?mode=timeout",
		Method: http.MethodGet, Timeout: 90 * time.Millisecond,
	})
	elapsed := time.Since(started)
	if result.Code != model.CodeProbeTimeout {
		t.Fatalf("code = %s, want %s (%s)", result.Code, model.CodeProbeTimeout, result.Message)
	}
	if elapsed > 700*time.Millisecond {
		t.Fatalf("timeout waited too long: %s", elapsed)
	}
	if !result.ConnectionClosed {
		t.Fatal("timeout result must mark the transport connection closed")
	}
	select {
	case <-cancelled:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("server did not observe context cancellation; request connection may still be open")
	}
}

func TestConnectionRefusedIsDistinct(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	client := &probe.Client{}
	result := client.Execute(context.Background(), probe.Operation{
		Kind: probe.KindProbe, ComponentID: "closed", URL: "http://" + address + "/health",
		Method: http.MethodGet, Timeout: 200 * time.Millisecond,
	})
	if result.Code != model.CodeProbeConnectionRefused {
		t.Fatalf("code = %s, want %s (%s)", result.Code, model.CodeProbeConnectionRefused, result.Message)
	}
}

func TestBusinessRejectionIsDistinct(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Error(writer, "denied", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client := &probe.Client{}
	result := client.Execute(context.Background(), probe.Operation{
		Kind: probe.KindRecover, ComponentID: "reject", URL: server.URL + "/recover",
		Method: http.MethodPost, Timeout: 200 * time.Millisecond,
	})
	if result.Code != model.CodeRecoverRejected {
		t.Fatalf("code = %s, want %s (%s)", result.Code, model.CodeRecoverRejected, result.Message)
	}
	if result.HTTPStatus != http.StatusServiceUnavailable {
		t.Fatalf("http status = %d", result.HTTPStatus)
	}
}

func TestNonLoopbackTargetIsRejected(t *testing.T) {
	client := &probe.Client{}
	result := client.Execute(context.Background(), probe.Operation{
		Kind: probe.KindProbe, URL: "http://example.com/health", Method: http.MethodGet,
		Timeout: 100 * time.Millisecond,
	})
	if result.Code != model.CodeSpecInvalid {
		t.Fatalf("code = %s, want %s", result.Code, model.CodeSpecInvalid)
	}
}
