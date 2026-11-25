package api

import (
	"net/http"
	"strconv"
	"time"
)

// RegisterTestProbeRoutes provides deterministic loopback targets. The slow endpoint intentionally
// waits on the request context so client-side timeout/cancellation closes the active connection.
func RegisterTestProbeRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/probe/ok", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true,"probe":"ok"}`))
	})
	mux.HandleFunc("/probe/fail", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"ok":false,"probe":"fail"}`))
	})
	mux.HandleFunc("/probe/slow", func(w http.ResponseWriter, r *http.Request) {
		delay := 2 * time.Second
		if raw := r.URL.Query().Get("delay"); raw != "" {
			if parsed, err := time.ParseDuration(raw); err == nil {
				delay = parsed
			} else if ms, err := strconv.Atoi(raw); err == nil {
				delay = time.Duration(ms) * time.Millisecond
			}
		}
		if delay > 60*time.Second {
			delay = 60 * time.Second
		}
		select {
		case <-time.After(delay):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true,"probe":"slow"}`))
		case <-r.Context().Done():
			// Connection is being closed by the timed-out/canceled client.
			return
		}
	})
}
