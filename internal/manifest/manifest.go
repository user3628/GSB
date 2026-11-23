package manifest

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"depverify/internal/model"
)

// Magic 是清单文件头魔数，共 8 字节。
var Magic = []byte("DEPVRFY1")

// 摘要算法标识。
const (
	AlgoSHA256 = 1
	AlgoSHA512 = 2
)

// Error 携带业务码与解析进度，便于上层保留已解析条目数与最后偏移。
type Error struct {
	Code     string
	Offset   int
	Resolved int
	Message  string
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s at offset %d: %s", e.Code, e.Offset, e.Message)
}

// Encode 将组件清单编码为带魔数与长度前缀的紧凑二进制格式。
func Encode(components []model.Component) []byte {
	buf := make([]byte, 0, 64)
	buf = append(buf, Magic...)
	for _, c := range components {
		rec := encodeRecord(c)
		var lenBuf [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(lenBuf[:], uint64(len(rec)))
		buf = append(buf, lenBuf[:n]...)
		buf = append(buf, rec...)
	}
	return buf
}

func encodeRecord(c model.Component) []byte {
	rec := make([]byte, 0, 32)
	var vb [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(vb[:], uint64(len(c.ID)))
	rec = append(rec, vb[:n]...)
	rec = append(rec, c.ID...)

	n = binary.PutUvarint(vb[:], uint64(len(c.Version)))
	rec = append(rec, vb[:n]...)
	for _, v := range c.Version {
		n = binary.PutUvarint(vb[:], uint64(v))
		rec = append(rec, vb[:n]...)
	}

	algo := AlgoSHA256
	if c.DigestAlgo == "SHA512" {
		algo = AlgoSHA512
	}
	n = binary.PutUvarint(vb[:], uint64(algo))
	rec = append(rec, vb[:n]...)

	n = binary.PutUvarint(vb[:], uint64(len(c.Digest)))
	rec = append(rec, vb[:n]...)
	rec = append(rec, c.Digest...)
	return rec
}

// reader 在 io.Reader 上维护全局偏移，实现逐条流式读取。
type reader struct {
	r        io.Reader
	offset   int
	resolved int
}

func (rd *reader) readFull(n int) ([]byte, error) {
	buf := make([]byte, n)
	got := 0
	for got < n {
		m, err := rd.r.Read(buf[got:])
		got += m
		rd.offset += m
		if err != nil {
			if errors.Is(err, io.EOF) && got > 0 && got < n {
				return buf[:got], io.ErrUnexpectedEOF
			}
			return buf[:got], err
		}
	}
	return buf, nil
}

// Parse 流式解析清单：先校验魔数，再按长度前缀逐条读取记录，
// 严格保证记录边界不越出源缓冲。解析失败时返回已成功解析的组件。
func Parse(r io.Reader) ([]model.Component, *Error) {
	rd := &reader{r: r}

	head, err := rd.readFull(len(Magic))
	if err != nil {
		if string(head) != "" && !equalMagic(head) {
			return nil, &Error{Code: model.EBadMagic, Offset: rd.offset, Message: "magic truncated or mismatched"}
		}
		return nil, &Error{Code: model.ETruncated, Offset: rd.offset, Message: "missing magic header"}
	}
	if !equalMagic(head) {
		return nil, &Error{Code: model.EBadMagic, Offset: rd.offset, Message: "bad magic header"}
	}

	var components []model.Component
	for {
		recStart := rd.offset
		recLen, perr := rd.readUvarint()
		if perr != nil {
			if perr.Code == model.EEOF {
				return components, nil // 记录边界上的正常结束
			}
			return components, perr
		}
		// 长度字段回绕 / 越界判定：累计偏移不得溢出 int，记录不得超出可寻址缓冲。
		if recLen > uint64(math.MaxInt64) || uint64(rd.offset)+recLen > math.MaxInt64 {
			return components, &Error{Code: model.ELengthOverflow, Offset: recStart, Resolved: rd.resolved,
				Message: "record length overflows addressable buffer"}
		}
		payload, rerr := rd.readFull(int(recLen))
		if rerr != nil {
			return components, &Error{Code: model.ETruncated, Offset: rd.offset, Resolved: rd.resolved,
				Message: "record payload truncated"}
		}
		comp, perr := parseRecord(payload)
		if perr != nil {
			perr.Offset = recStart
			perr.Resolved = rd.resolved
			return components, perr
		}
		components = append(components, comp)
		rd.resolved++
	}
}

func (rd *reader) readUvarint() (uint64, *Error) {
	var x uint64
	var shift uint
	start := rd.offset
	for i := 0; i < binary.MaxVarintLen64; i++ {
		b, err := rd.readFull(1)
		if err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return 0, &Error{Code: model.ETruncated, Offset: rd.offset, Resolved: rd.resolved,
					Message: "varint truncated"}
			}
			if errors.Is(err, io.EOF) && i == 0 {
				return 0, &Error{Code: model.EEOF, Offset: rd.offset}
			}
			return 0, &Error{Code: model.ETruncated, Offset: rd.offset, Resolved: rd.resolved,
				Message: "varint read failed"}
		}
		if b[0] < 0x80 {
			if i == binary.MaxVarintLen64-1 && b[0] > 1 {
				return 0, &Error{Code: model.ELengthOverflow, Offset: start, Resolved: rd.resolved,
					Message: "varint length field overflow (wrapped)"}
			}
			return x | uint64(b[0])<<shift, nil
		}
		x |= uint64(b[0]&0x7f) << shift
		shift += 7
	}
	return 0, &Error{Code: model.ELengthOverflow, Offset: start, Resolved: rd.resolved,
		Message: "varint length field overflow (wrapped)"}
}

func parseRecord(buf []byte) (model.Component, *Error) {
	cur := cursor{buf: buf}
	id, ok := cur.bytesField()
	if !ok {
		return model.Component{}, cur.fail()
	}
	count, ok := cur.varint()
	if !ok {
		return model.Component{}, cur.fail()
	}
	versions := make([]int, 0, count)
	for i := uint64(0); i < count; i++ {
		v, ok := cur.varint()
		if !ok {
			return model.Component{}, cur.fail()
		}
		versions = append(versions, int(v))
	}
	algoID, ok := cur.varint()
	if !ok {
		return model.Component{}, cur.fail()
	}
	digest, ok := cur.bytesField()
	if !ok {
		return model.Component{}, cur.fail()
	}
	if cur.pos != len(buf) {
		return model.Component{}, &Error{Code: model.ELengthOverflow, Message: "record boundary exceeded payload"}
	}
	algo := "SHA256"
	switch algoID {
	case AlgoSHA256:
		algo = "SHA256"
	case AlgoSHA512:
		algo = "SHA512"
	default:
		return model.Component{}, &Error{Code: model.EBadMagic, Message: fmt.Sprintf("unknown digest algo id %d", algoID)}
	}
	return model.Component{
		ID:               model.NormalizeKey(string(id)),
		Version:          versions,
		DigestAlgo:       algo,
		Digest:           digest,
		RegisteredDigest: append([]byte(nil), digest...),
	}, nil
}

type cursor struct {
	buf []byte
	pos int
}

func (c *cursor) varint() (uint64, bool) {
	x, n := binary.Uvarint(c.buf[c.pos:])
	if n <= 0 {
		c.pos = len(c.buf)
		return 0, false
	}
	c.pos += n
	return x, true
}

func (c *cursor) bytesField() ([]byte, bool) {
	length, ok := c.varint()
	if !ok || length > uint64(len(c.buf)-c.pos) {
		c.pos = len(c.buf)
		return nil, false
	}
	out := make([]byte, length)
	copy(out, c.buf[c.pos:c.pos+int(length)])
	c.pos += int(length)
	return out, true
}

func (c *cursor) fail() *Error {
	return &Error{Code: model.ETruncated, Message: "record field truncated inside payload"}
}

func equalMagic(b []byte) bool {
	if len(b) != len(Magic) {
		return false
	}
	for i := range Magic {
		if b[i] != Magic[i] {
			return false
		}
	}
	return true
}
