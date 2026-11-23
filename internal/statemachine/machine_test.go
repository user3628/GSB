package statemachine

import "testing"

func TestLegalTransitions(t *testing.T) {
	paths := [][2]Stage{
		{"", CONNECTING},
		{CONNECTING, HANDSHAKING},
		{HANDSHAKING, SEND_BODY},
		{SEND_BODY, RECV_RESPONSE},
		{RECV_RESPONSE, IDLE},
		{IDLE, HANDSHAKING},
		{HANDSHAKING, SEND_BODY},
		{RECV_RESPONSE, DRAINING},
		{DRAINING, CLOSED},
	}
	for _, p := range paths {
		if !Valid(p[0], p[1]) {
			t.Fatalf("expected %q -> %q to be legal", p[0], p[1])
		}
	}
	if Valid(IDLE, SEND_BODY) {
		t.Fatal("IDLE must pass through HANDSHAKING before SEND_BODY")
	}
	if Valid(CLOSED, IDLE) {
		t.Fatal("CLOSED must be terminal")
	}
	if err := Validate(RECV_RESPONSE, SEND_BODY); err == nil || err.Body.Code != "E_INVALID_TRANSITION" {
		t.Fatalf("expected E_INVALID_TRANSITION, got %v", err)
	}
}
