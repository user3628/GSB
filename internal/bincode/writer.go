package bincode

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

// putUvar writes a length-prefixed UTF-8 string (uint16 length + bytes).
func putStr(buf *bytes.Buffer, s string) {
	var l [2]byte
	binary.BigEndian.PutUint16(l[:], uint16(len(s)))
	buf.Write(l[:])
	buf.WriteString(s)
}

func putI64(buf *bytes.Buffer, v int64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	buf.Write(b[:])
}

// encodeRecord serializes one record including its outer 4-byte length prefix.
// v1 layout omits PPID; v2 inserts it after PID.
func encodeRecord(version uint8, r Record) []byte {
	var body bytes.Buffer
	body.WriteByte(r.TypeCode)
	body.WriteByte(r.TimeEnc)
	putStr(&body, r.RawTime)
	putStr(&body, r.SessionID)
	putI64(&body, r.PID)
	if version == V2 {
		putI64(&body, r.PPID)
	}
	putStr(&body, r.Subject)
	putStr(&body, r.Object)
	if len(r.RawPayload) > 0 {
		body.Write(r.RawPayload)
	}

	out := make([]byte, 4+body.Len())
	binary.BigEndian.PutUint32(out[:4], uint32(body.Len()))
	copy(out[4:], body.Bytes())
	return out
}

// Marshal builds a complete binary audit log for the given version and records.
// The header carries a deterministic created-at nanosecond value so that equal
// inputs always produce byte-identical files.
func Marshal(version uint8, createdAtUnixNano int64, records []Record) ([]byte, error) {
	var out bytes.Buffer
	out.WriteString(HeaderMagic)
	out.WriteByte(version)
	var hdr [8]byte
	binary.BigEndian.PutUint64(hdr[:], uint64(createdAtUnixNano))
	out.Write(hdr[:])
	var cnt [4]byte
	binary.BigEndian.PutUint32(cnt[:], uint32(len(records)))
	out.Write(cnt[:])
	out.WriteByte(0) // reserved/flags byte; fixed header is HeaderLen (18)
	if out.Len() != HeaderLen {
		panic("header length mismatch")
	}

	bodyStart := out.Len()
	for _, r := range records {
		out.Write(encodeRecord(version, r))
	}
	body := out.Bytes()[bodyStart:]

	out.WriteString(TailMagic)
	var bl [4]byte
	binary.BigEndian.PutUint32(bl[:], uint32(len(body)))
	out.Write(bl[:])
	var ec [4]byte
	binary.BigEndian.PutUint32(ec[:], uint32(len(records)))
	out.Write(ec[:])
	var crc [4]byte
	binary.BigEndian.PutUint32(crc[:], crc32.ChecksumIEEE(body))
	out.Write(crc[:])
	return out.Bytes(), nil
}
