package parse

import (
	"encoding/binary"
	"fmt"

	"flowtrace/internal/codes"
	"flowtrace/internal/model"
)

// Binary layout:
//
//	header: magic "SFEV" (4 bytes) + version (1 byte: 1 or 2)
//	v1 record: seq u64le, ts u64le, op u8,
//	           subjectLen u8, srcLen u8, dstLen u8, subject, src, dst
//	v2 record: op u8, flags u8, seq u64le, ts u64le,
//	           subjectLen u16le, srcLen u16le, dstLen u16le, subject, src, dst
var magic = []byte{'S', 'F', 'E', 'V'}

const (
	Version1 byte = 1
	Version2 byte = 2
)

// EncodeBinary serializes events with the given layout version. It is used by
// the deterministic sample generator and by round-trip tests.
func EncodeBinary(version byte, events []model.Event) ([]byte, error) {
	if version != Version1 && version != Version2 {
		return nil, fmt.Errorf("unsupported version %d", version)
	}
	out := append([]byte{}, magic...)
	out = append(out, version)
	for _, ev := range events {
		switch version {
		case Version1:
			var hdr [19]byte
			binary.LittleEndian.PutUint64(hdr[0:8], ev.Seq)
			binary.LittleEndian.PutUint64(hdr[8:16], ev.LogicalTime)
			hdr[16] = uint8(ev.Op)
			hdr[17] = uint8(len(ev.Subject))
			hdr[18] = uint8(len(ev.SrcField))
			out = append(out, hdr[:]...)
			out = append(out, byte(len(ev.DstField)))
			out = append(out, ev.Subject...)
			out = append(out, ev.SrcField...)
			out = append(out, ev.DstField...)
		case Version2:
			var hdr [22]byte
			hdr[0] = uint8(ev.Op)
			hdr[1] = 0 // flags
			binary.LittleEndian.PutUint64(hdr[2:10], ev.Seq)
			binary.LittleEndian.PutUint64(hdr[10:18], ev.LogicalTime)
			binary.LittleEndian.PutUint16(hdr[18:20], uint16(len(ev.Subject)))
			binary.LittleEndian.PutUint16(hdr[20:22], uint16(len(ev.SrcField)))
			out = append(out, hdr[:]...)
			var dl [2]byte
			binary.LittleEndian.PutUint16(dl[:], uint16(len(ev.DstField)))
			out = append(out, dl[:]...)
			out = append(out, ev.Subject...)
			out = append(out, ev.SrcField...)
			out = append(out, ev.DstField...)
		}
	}
	return out, nil
}

// ParseBinary decodes a binary event log, classifying failures into stable
// business codes. Bad records are skipped and never pollute the result.
func ParseBinary(data []byte) Result {
	res := Result{}
	if len(data) < len(magic)+1 || string(data[:len(magic)]) != string(magic) {
		res.Errors = append(res.Errors, Error{Code: codes.EUnknownVersion, Offset: 0, Detail: "bad magic or missing header"})
		return res
	}
	version := data[len(magic)]
	if version != Version1 && version != Version2 {
		res.Errors = append(res.Errors, Error{Code: codes.EUnknownVersion, Offset: len(magic), Detail: fmt.Sprintf("unknown version %d", version)})
		return res
	}
	pos := len(magic) + 1
	for pos < len(data) {
		ev, n, perr := decodeRecord(version, data[pos:])
		if perr != nil {
			perr.Offset = pos
			res.Errors = append(res.Errors, *perr)
			if perr.Code == codes.ETruncated {
				break // nothing reliable beyond a truncated record
			}
			pos += n
			continue
		}
		pos += n
		if ev.Subject == "" || ev.SrcField == "" || ev.DstField == "" {
			res.Errors = append(res.Errors, Error{Code: codes.EEventFieldMissing, Offset: pos, Detail: "empty required field"})
			continue
		}
		res.Events = append(res.Events, ev)
	}
	return res
}

func decodeRecord(version byte, buf []byte) (model.Event, int, *Error) {
	var ev model.Event
	switch version {
	case Version1:
		if len(buf) < 20 {
			return ev, 0, &Error{Code: codes.ETruncated, Detail: "v1 record header truncated"}
		}
		ev.Seq = binary.LittleEndian.Uint64(buf[0:8])
		ev.LogicalTime = binary.LittleEndian.Uint64(buf[8:16])
		ev.Op = model.Op(buf[16])
		sl, rl, dl := int(buf[17]), int(buf[18]), int(buf[19])
		if len(buf) < 20+sl+rl+dl {
			return ev, 0, &Error{Code: codes.ETruncated, Detail: "v1 record body truncated"}
		}
		p := 20
		ev.Subject = string(buf[p : p+sl])
		p += sl
		ev.SrcField = string(buf[p : p+rl])
		p += rl
		ev.DstField = string(buf[p : p+dl])
		p += dl
		return ev, p, nil
	case Version2:
		if len(buf) < 24 {
			return ev, 0, &Error{Code: codes.ETruncated, Detail: "v2 record header truncated"}
		}
		ev.Op = model.Op(buf[0])
		ev.Seq = binary.LittleEndian.Uint64(buf[2:10])
		ev.LogicalTime = binary.LittleEndian.Uint64(buf[10:18])
		sl := int(binary.LittleEndian.Uint16(buf[18:20]))
		rl := int(binary.LittleEndian.Uint16(buf[20:22]))
		dl := int(binary.LittleEndian.Uint16(buf[22:24]))
		if len(buf) < 24+sl+rl+dl {
			return ev, 0, &Error{Code: codes.ETruncated, Detail: "v2 record body truncated"}
		}
		p := 24
		ev.Subject = string(buf[p : p+sl])
		p += sl
		ev.SrcField = string(buf[p : p+rl])
		p += rl
		ev.DstField = string(buf[p : p+dl])
		p += dl
		return ev, p, nil
	}
	return ev, 0, &Error{Code: codes.EUnknownVersion, Detail: "unreachable"}
}
