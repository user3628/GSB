package probetarget

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Handler implements the deterministic loopback ownership endpoint.
func Handler() http.Handler {
	mux := http.NewServeMux()
	handler := func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		routeKey := query.Get("route_key")
		if routeKey == "" {
			routeKey = "unknown-route"
		}
		mode := query.Get("mode")
		if mode == "" {
			mode = "ok"
		}
		owner := query.Get("owner_version")
		if owner == "" {
			owner = "owner-" + routeKey
		}

		switch mode {
		case "timeout":
			select {
			case <-r.Context().Done():
				return
			case <-time.After(30 * time.Second):
				writeOwner(w, routeKey, owner, "too-late")
			}
			return
		case "late_response", "late":
			delay := 120 * time.Millisecond
			if raw := query.Get("delay_ms"); raw != "" {
				if parsed, err := time.ParseDuration(raw + "ms"); err == nil {
					delay = parsed
				}
			}
			time.Sleep(delay)
			writeOwner(w, routeKey, owner, "late")
			return
		case "mismatch":
			wrong := query.Get("wrong_owner")
			if wrong == "" {
				wrong = "wrong-owner-" + routeKey
			}
			writeOwner(w, routeKey, wrong, "mismatch-injected")
			return
		default:
			writeOwner(w, routeKey, owner, "owner")
		}
	}
	mux.HandleFunc("/probe", handler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	return mux
}

func writeOwner(w http.ResponseWriter, routeKey, owner, status string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, _ = w.Write([]byte(`{"route_key":"` + routeKey + `","owner_version":"` + owner + `","status":"` + status + `"}`))
}

func NewTestServer(t testing.TB) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(Handler())
	t.Cleanup(server.Close)
	return server
}

// RefusedEndpoint reserves and releases a loopback port, yielding an endpoint
// that is very likely to return connection refused.
func RefusedEndpoint(t testing.TB) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve refused endpoint: %v", err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	return "http://" + addr + "/probe"
}

func RefusedEndpointString() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	return "http://" + addr + "/probe", nil
}
