package bincode

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"forensicworkbench/internal/codes"
)

// ParseError is a business-tagged decode error with a skippable byte range.
type ParseError struct {
	Code      string
	Message   string
	Offset    int64
	EndOffset int64
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("%s: %s [offset=%d end=%d]", e.Code, e.Message, e.Offset, e.EndOffset)
}

// DecodedRecord pairs a decoded record with its exact source byte range.
type DecodedRecord struct {
	Record    Record
	Offset    int64
	EndOffset int64
	Seq       int
}

// Header describes the parsed version header.
type Header struct {
	Version         uint8
	CreatedAtNano   int64
	RecordCountHint uint32
}

// ParseResult is the full decode outcome. TailValid reports checksum status;
// records and errors are both retained so partial progress survives damage.
type ParseResult struct {
	Header    Header
	Records   []DecodedRecord
	Errors    []*ParseError
	TailValid bool
	BodyEnd   int64
}

type cursor struct {
	data []byte
	pos  int
}

func (c *cursor) byte() (byte, error) {
	if c.pos+1 > len(c.data) {
		return 0, errors.New("truncated byte")
	}
	v := c.data[c.pos]
	c.pos++
	return v, nil
}

func (c *cursor) u16() (uint16, error) {
	if c.pos+2 > len(c.data) {
		return 0, errors.New("truncated uint16")
	}
	v := binary.BigEndian.Uint16(c.data[c.pos:])
	c.pos += 2
	return v, nil
}

func (c *cursor) i64() (int64, error) {
	if c.pos+8 > len(c.data) {
		return 0, errors.New("truncated int64")
	}
	v := int64(binary.BigEndian.Uint64(c.data[c.pos:]))
	c.pos += 8
	return v, nil
}

func (c *cursor) str() (string, error) {
	n, err := c.u16()
	if err != nil {
		return "", err
	}
	if c.pos+int(n) > len(c.data) {
		return "", errors.New("truncated string")
	}
	s := string(c.data[c.pos : c.pos+int(n)])
	c.pos += int(n)
	return s, nil
}

// Parse decodes a full binary audit log. It never panics on malformed input:
// structural errors become tagged ParseError values and parsing of subsequent
// records continues whenever a safe boundary is available.
func Parse(data []byte) (*ParseResult, error) {
	res := &ParseResult{}
	if len(data) < HeaderLen || string(data[:4]) != HeaderMagic {
		return nil, &ParseError{Code: codes.EBadHeader, Message: "bad magic or header too short", EndOffset: int64(len(data))}
	}
	version := data[4]
	if version != V1 && version != V2 {
		return nil, &ParseError{
			Code:      codes.EUnknownBinaryVersion,
			Message:   fmt.Sprintf("unsupported binary version %d", version),
			Offset:    4,
			EndOffset: 5,
		}
	}
	res.Header.Version = version
	res.Header.CreatedAtNano = int64(binary.BigEndian.Uint64(data[5:13]))
	res.Header.RecordCountHint = binary.BigEndian.Uint32(data[13:17])
	// data[17] reserved flags; body starts at the fixed header (18 bytes).

	pos := HeaderLen
	bodyEnd := len(data)
	if len(data) >= TailLen {
		tailAt := len(data) - TailLen
		if string(data[tailAt:tailAt+4]) == TailMagic {
			bodyEnd = tailAt
		}
	}
	seq := 0
	for pos < bodyEnd {
		if pos+4 > bodyEnd {
			res.Errors = append(res.Errors, &ParseError{
				Code: codes.ETruncatedRecord, Message: "truncated record length prefix",
				Offset: int64(pos), EndOffset: int64(bodyEnd),
			})
			pos = bodyEnd
			break
		}
		recLen := int(binary.BigEndian.Uint32(data[pos:]))
		recStart := pos
		recEnd := pos + 4 + recLen
		if recEnd > bodyEnd {
			res.Errors = append(res.Errors, &ParseError{
				Code:    codes.ETruncatedRecord,
				Message: fmt.Sprintf("record length %d exceeds body remaining %d", recLen, bodyEnd-pos-4),
				Offset:  int64(recStart), EndOffset: int64(bodyEnd),
			})
			// No trustworthy boundary remains; stop this source cleanly.
			pos = bodyEnd
			break
		}
		rec, err := decodeRecord(version, data[pos+4:recEnd])
		if err != nil {
			res.Errors = append(res.Errors, &ParseError{
				Code: codes.ETruncatedRecord, Message: err.Error(),
				Offset: int64(recStart), EndOffset: int64(recEnd),
			})
		} else {
			res.Records = append(res.Records, DecodedRecord{
				Record: rec, Offset: int64(recStart), EndOffset: int64(recEnd), Seq: seq,
			})
			seq++
		}
		pos = recEnd
	}
	res.BodyEnd = int64(bodyEnd)

	// Validate the trailing checksum block if present and well-formed.
	if len(data) >= TailLen {
		tailAt := len(data) - TailLen
		if string(data[tailAt:tailAt+4]) == TailMagic {
			bodyLen := binary.BigEndian.Uint32(data[tailAt+4 : tailAt+8])
			wantCRC := binary.BigEndian.Uint32(data[tailAt+12 : tailAt+16])
			body := data[HeaderLen : HeaderLen+int(bodyLen)]
			if int(bodyLen) == bodyEnd-HeaderLen && crc32.ChecksumIEEE(body) == wantCRC {
				res.TailValid = true
			} else {
				res.Errors = append(res.Errors, &ParseError{
					Code: codes.EChecksum, Message: "tail checksum block mismatch",
					Offset: int64(tailAt), EndOffset: int64(len(data)),
				})
			}
		}
	}
	return res, nil
}

func decodeRecord(version uint8, body []byte) (Record, error) {
	c := &cursor{data: body}
	var r Record
	var err error
	if r.TypeCode, err = c.byte(); err != nil {
		return r, err
	}
	if r.TimeEnc, err = c.byte(); err != nil {
		return r, err
	}
	if r.RawTime, err = c.str(); err != nil {
		return r, err
	}
	if r.SessionID, err = c.str(); err != nil {
		return r, err
	}
	if r.PID, err = c.i64(); err != nil {
		return r, err
	}
	if version == V2 {
		if r.PPID, err = c.i64(); err != nil {
			return r, err
		}
	}
	if r.Subject, err = c.str(); err != nil {
		return r, err
	}
	if r.Object, err = c.str(); err != nil {
		return r, err
	}
	if c.pos < len(body) {
		r.RawPayload = append([]byte(nil), body[c.pos:]...)
	}
	return r, nil
}
