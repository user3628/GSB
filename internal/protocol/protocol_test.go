package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func TestFrameRoundTripAndChecksum(t *testing.T) {
	frame := Frame{Type: MsgData, Payload: []byte("deterministic")}
	wire, err := Encode(frame)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(bytes.NewReader(wire))
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != frame.Type || !bytes.Equal(got.Payload, frame.Payload) {
		t.Fatalf("round trip mismatch: %#v", got)
	}
	wire[len(wire)-1] ^= 0xff
	if _, err := Decode(bytes.NewReader(wire)); !errors.Is(err, ErrBadChecksum) {
		t.Fatalf("want bad checksum, got %v", err)
	}
}

func TestDecodePayloadOverflowDrainsWireFrame(t *testing.T) {
	payload := bytes.Repeat([]byte{0x41}, MaxPayload+1)
	var wire bytes.Buffer
	wire.WriteByte(byte(MsgData))
	_ = binary.Write(&wire, binary.BigEndian, uint16(len(payload)))
	wire.Write(payload)
	_ = binary.Write(&wire, binary.BigEndian, Checksum(MsgData, payload))
	if _, err := Decode(&wire); !errors.Is(err, ErrPayloadOverflow) {
		t.Fatalf("want overflow, got %v", err)
	}
	if wire.Len() != 0 {
		t.Fatalf("overflow decoder left %d unread bytes", wire.Len())
	}
}

func TestStateMachineTraceAndIllegalStay(t *testing.T) {
	m := NewMachine()
	sequence := []MessageType{MsgHandshake, MsgAuth, MsgData, MsgData, MsgClose, MsgPing}
	wantStates := []State{StateAuth, StateReady, StateTransfer, StateTransfer, StateClosed, StateClosed}
	wantCodes := []string{"E_OK", "E_OK", "E_OK", "E_OK", "E_OK", "E_ILLEGAL_TRANSITION"}
	for i, typ := range sequence {
		tr := m.Apply(typ)
		if tr.To != wantStates[i] || tr.Code != wantCodes[i] {
			t.Fatalf("step %d: got %s, want %s/%s", i, tr, wantStates[i], wantCodes[i])
		}
	}
	if m.State() != StateClosed {
		t.Fatalf("final state %s", m.State())
	}
	if len(m.Trace()) != len(sequence) {
		t.Fatalf("trace length %d", len(m.Trace()))
	}
	if m.Trace()[5].From != StateClosed || m.Trace()[5].To != StateClosed {
		t.Fatalf("illegal transition did not preserve state: %#v", m.Trace()[5])
	}
}

func TestResponseRoundTrip(t *testing.T) {
	frame, err := EncodeResponse("E_ILLEGAL_TRANSITION", StateClosed, StateClosed)
	if err != nil {
		t.Fatal(err)
	}
	code, from, to, err := DecodeResponse(frame)
	if err != nil {
		t.Fatal(err)
	}
	if code != "E_ILLEGAL_TRANSITION" || from != StateClosed || to != StateClosed {
		t.Fatalf("unexpected response %s %s %s", code, from, to)
	}
}
