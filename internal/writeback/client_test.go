package writeback

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"driftengine/internal/errs"
	"driftengine/internal/spec"
)

func TestWriteTimeoutClosesServerContext(t *testing.T) {
	var cancels atomic.Int64
	entered := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
		cancels.Add(1)
	}))
	defer server.Close()

	client := NewClient(server.URL, 50*time.Millisecond)
	defer client.Close()
	err := client.WriteFields(context.Background(), "slow", spec.Fields{"v": 1})
	if errs.Code(err) != errs.WriteTimeout {
		t.Fatalf("code=%s err=%v", errs.Code(err), err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && cancels.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if cancels.Load() != 1 {
		t.Fatalf("server cancels=%d", cancels.Load())
	}
}
