package target_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"protocolfuzzlab/internal/codes"
	"protocolfuzzlab/internal/protocol"
	"protocolfuzzlab/internal/target"
)

func TestLoopbackDeterministicResponsesAndErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server, err := target.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if host, _, err := net.SplitHostPort(server.Addr()); err != nil || host != "127.0.0.1" {
		t.Fatalf("server not bound to loopback: %s %v", server.Addr(), err)
	}
	if code := exchange(t, server.Addr(), []byte("HELLO"), false); code != codes.OK {
		t.Fatalf("handshake code=%s", code)
	}
	if code := exchange(t, server.Addr(), []byte("HELLO"), true); code != codes.BadChecksum {
		t.Fatalf("checksum code=%s", code)
	}
	large := make([]byte, protocol.MaxPayload+1)
	if code := exchange(t, server.Addr(), large, false); code != codes.PayloadOverflow {
		t.Fatalf("overflow code=%s", code)
	}
}

func exchange(t *testing.T, addr string, payload []byte, tamper bool) string {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	frame := protocol.Frame{Type: protocol.MsgHandshake, Payload: payload}
	var wire []byte
	if tamper {
		wire, err = protocol.EncodeTampered(frame)
	} else {
		wire, err = protocol.Encode(frame)
	}
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(wire); err != nil {
		t.Fatal(err)
	}
	response, err := protocol.Decode(conn)
	if err != nil {
		if errors.Is(err, protocol.ErrPayloadOverflow) {
			return codes.PayloadOverflow
		}
		t.Fatal(err)
	}
	code, _, _, err := protocol.DecodeResponse(response)
	if err != nil {
		t.Fatal(err)
	}
	return code
}
