package client_test

import (
	"testing"
	"time"

	"upstreampool/internal/client"
	"upstreampool/internal/model"
	"upstreampool/internal/upstream"
)

func TestSlowReceiverCausesRealWriteTimeoutAndClose(t *testing.T) {
	up, err := upstream.Start("127.0.0.1:0", 256<<10, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer up.Close()
	conn, dialResult := client.Dial(up.Addr(), time.Second)
	if dialResult.ErrorCode != "" {
		t.Fatal(dialResult)
	}
	result := client.RoundTrip(conn, "slow-write", 48<<20, 50<<20, 16, client.Options{
		DialTimeout: time.Second, WriteTimeout: 30 * time.Millisecond, ReadTimeout: time.Second, RateChunkBytes: 4096,
	})
	if result.ErrorCode != model.ErrSendTimeout {
		t.Fatalf("expected E_SEND_TIMEOUT, got %+v", result)
	}
	if _, err := conn.Write([]byte("x")); err == nil {
		t.Fatal("timeout path must leave the socket actually closed")
	}
}
