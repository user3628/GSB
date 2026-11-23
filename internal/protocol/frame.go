package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	// MaxPayload is the laboratory policy limit. Frames above this are rejected
	// with E_PAYLOAD_OVERFLOW even though the uint16 wire field can represent more.
	MaxPayload = 4096
	// MaxWirePayload is the representable uint16 payload size.
	MaxWirePayload = 65535
	ResponseType   = MessageType(0xf1)
)

var (
	ErrBadChecksum     = errors.New("bad checksum")
	ErrPayloadOverflow = errors.New("payload length exceeds policy limit")
	ErrTruncatedFrame  = errors.New("truncated frame")
	ErrBadWireLength   = errors.New("wire payload length is not representable")
)

// Frame is type(1) || payload_len(2, big endian) || payload || checksum(2).
type Frame struct {
	Type    MessageType
	Payload []byte
}

func Checksum(typ MessageType, payload []byte) uint16 {
	crc := uint16(0xffff)
	update := func(b byte) {
		crc ^= uint16(b) << 8
		for i := 0; i < 8; i++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	update(byte(typ))
	update(byte(len(payload) >> 8))
	update(byte(len(payload)))
	for _, b := range payload {
		update(b)
	}
	return crc
}

func Encode(f Frame) ([]byte, error) {
	if len(f.Payload) > MaxWirePayload {
		return nil, ErrBadWireLength
	}
	out := make([]byte, 5+len(f.Payload))
	out[0] = byte(f.Type)
	binary.BigEndian.PutUint16(out[1:3], uint16(len(f.Payload)))
	copy(out[3:], f.Payload)
	binary.BigEndian.PutUint16(out[3+len(f.Payload):], Checksum(f.Type, f.Payload))
	return out, nil
}

func EncodeTampered(f Frame) ([]byte, error) {
	out, err := Encode(f)
	if err != nil {
		return nil, err
	}
	out[len(out)-1] ^= 0xff
	return out, nil
}

// Decode reads one complete frame. A policy overflow is reported as soon as the
// length is known; the caller should close or drain the stream.
func Decode(r io.Reader) (Frame, error) {
	var header [3]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Frame{}, ErrTruncatedFrame
		}
		return Frame{}, err
	}
	typ := MessageType(header[0])
	n := int(binary.BigEndian.Uint16(header[1:3]))
	if n > MaxPayload {
		// Drain a bounded wire frame so the server can return the policy error
		// without closing a socket that still has unread data (which can trigger
		// a TCP reset and discard the response).
		_, _ = io.CopyN(io.Discard, r, int64(n)+2)
		return Frame{}, ErrPayloadOverflow
	}
	body := make([]byte, n+2)
	if _, err := io.ReadFull(r, body); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Frame{}, ErrTruncatedFrame
		}
		return Frame{}, err
	}
	payload := append([]byte(nil), body[:n]...)
	want := binary.BigEndian.Uint16(body[n:])
	if got := Checksum(typ, payload); got != want {
		return Frame{}, ErrBadChecksum
	}
	return Frame{Type: typ, Payload: payload}, nil
}

func (f Frame) String() string {
	return fmt.Sprintf("%s(%d bytes)", f.Type, len(f.Payload))
}
