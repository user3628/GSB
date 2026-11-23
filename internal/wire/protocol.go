package wire

import (
	"encoding/binary"
	"fmt"
	"io"
)

const Magic = "DGP1"

const (
	HeaderSize      = 24
	DefaultMaxFrame = 64 << 20
)

type RequestHeader struct {
	IDLength       uint32
	BodyLength     uint64
	ResponseLength uint64
}

func WriteHeader(w io.Writer, idLength int, bodyLength, responseLength uint64) error {
	if idLength < 0 || uint64(idLength) > 1<<32-1 {
		return fmt.Errorf("request id is too long")
	}
	var hdr [HeaderSize]byte
	copy(hdr[:4], Magic)
	binary.BigEndian.PutUint32(hdr[4:8], uint32(idLength))
	binary.BigEndian.PutUint64(hdr[8:16], bodyLength)
	binary.BigEndian.PutUint64(hdr[16:24], responseLength)
	_, err := w.Write(hdr[:])
	return err
}

func ReadHeader(r io.Reader) (RequestHeader, error) {
	var hdr [HeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return RequestHeader{}, err
	}
	if string(hdr[:4]) != Magic {
		return RequestHeader{}, fmt.Errorf("invalid upstream frame magic")
	}
	return RequestHeader{
		IDLength:       binary.BigEndian.Uint32(hdr[4:8]),
		BodyLength:     binary.BigEndian.Uint64(hdr[8:16]),
		ResponseLength: binary.BigEndian.Uint64(hdr[16:24]),
	}, nil
}
