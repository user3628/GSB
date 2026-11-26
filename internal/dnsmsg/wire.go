package dnsmsg

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"example.com/gsb/dnscache/internal/errs"
)

const (
	HeaderLen   = 12
	MaxDNSFrame = 65535
)

func malformed(format string, args ...any) error {
	return errs.New(errs.EMessageMalformed, format, args...)
}

// HeaderID extracts the transaction ID even when the rest of the packet is
// malformed. It lets a listener return FORMERR with the client's ID.
func HeaderID(data []byte) (uint16, bool) {
	if len(data) < 2 {
		return 0, false
	}
	return binary.BigEndian.Uint16(data[:2]), true
}

func Parse(data []byte) (*Message, error) {
	if len(data) < HeaderLen {
		return nil, malformed("packet length %d is shorter than DNS header", len(data))
	}
	m := &Message{Header: Header{
		ID:      binary.BigEndian.Uint16(data[0:2]),
		Flags:   binary.BigEndian.Uint16(data[2:4]),
		QDCount: binary.BigEndian.Uint16(data[4:6]),
		ANCount: binary.BigEndian.Uint16(data[6:8]),
		NSCount: binary.BigEndian.Uint16(data[8:10]),
		ARCount: binary.BigEndian.Uint16(data[10:12]),
	}}
	off := HeaderLen
	for i := 0; i < int(m.Header.QDCount); i++ {
		name, next, err := readName(data, off)
		if err != nil {
			return nil, err
		}
		if next+4 > len(data) {
			return nil, malformed("truncated question %d", i)
		}
		q := Question{
			Name:  name,
			Type:  Type(binary.BigEndian.Uint16(data[next : next+2])),
			Class: binary.BigEndian.Uint16(data[next+2 : next+4]),
		}
		m.Questions = append(m.Questions, q)
		off = next + 4
	}
	var err error
	m.Answers, off, err = parseRRs(data, off, int(m.Header.ANCount), "answer")
	if err != nil {
		return nil, err
	}
	m.Authorities, off, err = parseRRs(data, off, int(m.Header.NSCount), "authority")
	if err != nil {
		return nil, err
	}
	m.Additionals, off, err = parseRRs(data, off, int(m.Header.ARCount), "additional")
	if err != nil {
		return nil, err
	}
	if off != len(data) {
		return nil, malformed("%d trailing bytes after declared DNS sections", len(data)-off)
	}
	return m, nil
}

// readName decodes labels and RFC 1035 compression pointers. Pointers must
// target an earlier offset; this both implements the RFC's backward-reference
// rule and makes pointer cycles impossible.
func readName(data []byte, start int) (string, int, error) {
	if start < 0 || start >= len(data) {
		return "", 0, malformed("name offset %d outside packet", start)
	}
	pos := start
	next := -1
	visited := make(map[int]bool)
	labels := make([]string, 0, 4)
	wireLen := 1 // terminating root label
	for steps := 0; ; steps++ {
		if steps > 128 {
			return "", 0, malformed("too many compression pointers")
		}
		if pos < 0 || pos >= len(data) {
			return "", 0, malformed("compression pointer outside packet")
		}
		if visited[pos] {
			return "", 0, malformed("compression pointer cycle at offset %d", pos)
		}
		visited[pos] = true
		b := data[pos]
		switch b & 0xc0 {
		case 0xc0:
			if pos+1 >= len(data) {
				return "", 0, malformed("truncated compression pointer")
			}
			ptr := int(binary.BigEndian.Uint16(data[pos:pos+2]) & 0x3fff)
			if ptr >= len(data) {
				return "", 0, malformed("compression pointer %d outside packet", ptr)
			}
			if ptr >= pos {
				return "", 0, malformed("compression pointer %d does not point backward", ptr)
			}
			if next < 0 {
				next = pos + 2
			}
			pos = ptr
		case 0x00:
			if b == 0 {
				if next < 0 {
					next = pos + 1
				}
				if len(labels) == 0 {
					return ".", next, nil
				}
				return strings.Join(labels, ".") + ".", next, nil
			}
			labelLen := int(b)
			if labelLen > 63 {
				return "", 0, malformed("label length %d exceeds 63", labelLen)
			}
			if pos+1+labelLen > len(data) {
				return "", 0, malformed("truncated label at offset %d", pos)
			}
			wireLen += 1 + labelLen
			if wireLen > 255 {
				return "", 0, malformed("expanded domain name exceeds 255 wire bytes")
			}
			labels = append(labels, string(data[pos+1:pos+1+labelLen]))
			pos += 1 + labelLen
		default:
			return "", 0, malformed("reserved label type 0x%02x at offset %d", b&0xc0, pos)
		}
	}
}

func parseRRs(data []byte, off, count int, section string) ([]RR, int, error) {
	rrs := make([]RR, 0, count)
	for i := 0; i < count; i++ {
		rr, next, err := parseRR(data, off)
		if err != nil {
			return nil, 0, fmt.Errorf("%s RR %d: %w", section, i, err)
		}
		rrs = append(rrs, rr)
		off = next
	}
	return rrs, off, nil
}

func parseRR(data []byte, off int) (RR, int, error) {
	name, next, err := readName(data, off)
	if err != nil {
		return RR{}, 0, err
	}
	if next+10 > len(data) {
		return RR{}, 0, malformed("truncated resource record header")
	}
	rr := RR{
		Name:  name,
		Type:  Type(binary.BigEndian.Uint16(data[next : next+2])),
		Class: binary.BigEndian.Uint16(data[next+2 : next+4]),
		TTL:   binary.BigEndian.Uint32(data[next+4 : next+8]),
	}
	rdlen := int(binary.BigEndian.Uint16(data[next+8 : next+10]))
	rstart := next + 10
	rend := rstart + rdlen
	if rend > len(data) {
		return RR{}, 0, malformed("rdata length %d exceeds packet", rdlen)
	}
	if err := parseRData(data, rstart, rend, &rr); err != nil {
		return RR{}, 0, err
	}
	return rr, rend, nil
}

func parseRData(data []byte, start, end int, rr *RR) error {
	exactName := func(pos int) (string, error) {
		name, next, err := readName(data, pos)
		if err != nil {
			return "", err
		}
		if next != end {
			return "", malformed("name in %s rdata has %d trailing bytes", rr.Type, end-next)
		}
		return name, nil
	}
	switch rr.Type {
	case TypeA:
		if end-start != 4 {
			return malformed("A rdata length is %d, want 4", end-start)
		}
		rr.Data.Address = net.IP(append([]byte(nil), data[start:end]...)).String()
	case TypeAAAA:
		if end-start != 16 {
			return malformed("AAAA rdata length is %d, want 16", end-start)
		}
		rr.Data.Address = net.IP(append([]byte(nil), data[start:end]...)).String()
	case TypeCNAME, TypeNS:
		name, err := exactName(start)
		if err != nil {
			return err
		}
		rr.Data.Target = name
	case TypeMX:
		if end-start < 3 {
			return malformed("MX rdata is too short")
		}
		rr.Data.Preference = binary.BigEndian.Uint16(data[start : start+2])
		name, err := exactName(start + 2)
		if err != nil {
			return err
		}
		rr.Data.Target = name
	case TypeTXT:
		pos := start
		for pos < end {
			l := int(data[pos])
			pos++
			if pos+l > end {
				return malformed("TXT character-string exceeds rdata")
			}
			rr.Data.Texts = append(rr.Data.Texts, string(data[pos:pos+l]))
			pos += l
		}
	case TypeSOA:
		mname, next, err := readName(data, start)
		if err != nil {
			return err
		}
		rname, next2, err := readName(data, next)
		if err != nil {
			return err
		}
		if next2+20 != end {
			return malformed("SOA rdata has invalid length")
		}
		soa := &SOA{MName: mname, RName: rname}
		soa.Serial = binary.BigEndian.Uint32(data[next2 : next2+4])
		soa.Refresh = binary.BigEndian.Uint32(data[next2+4 : next2+8])
		soa.Retry = binary.BigEndian.Uint32(data[next2+8 : next2+12])
		soa.Expire = binary.BigEndian.Uint32(data[next2+12 : next2+16])
		soa.Minimum = binary.BigEndian.Uint32(data[next2+16 : next2+20])
		rr.Data.SOA = soa
	default:
		rr.Data.Raw = append([]byte(nil), data[start:end]...)
	}
	return nil
}

type packer struct {
	buf      []byte
	compress map[string]int
}

func (m *Message) Pack() ([]byte, error) {
	m.SetCounts()
	if len(m.Questions) > 65535 || len(m.Answers) > 65535 || len(m.Authorities) > 65535 || len(m.Additionals) > 65535 {
		return nil, errs.New(errs.EMessageMalformed, "too many DNS sections")
	}
	p := &packer{compress: make(map[string]int)}
	p.buf = make([]byte, HeaderLen)
	binary.BigEndian.PutUint16(p.buf[0:2], m.Header.ID)
	binary.BigEndian.PutUint16(p.buf[2:4], m.Header.Flags)
	binary.BigEndian.PutUint16(p.buf[4:6], m.Header.QDCount)
	binary.BigEndian.PutUint16(p.buf[6:8], m.Header.ANCount)
	binary.BigEndian.PutUint16(p.buf[8:10], m.Header.NSCount)
	binary.BigEndian.PutUint16(p.buf[10:12], m.Header.ARCount)
	for _, q := range m.Questions {
		if err := p.writeName(q.Name); err != nil {
			return nil, err
		}
		p.writeU16(uint16(q.Type))
		p.writeU16(q.Class)
	}
	for _, group := range [][]RR{m.Answers, m.Authorities, m.Additionals} {
		for _, rr := range group {
			if err := p.writeRR(rr); err != nil {
				return nil, err
			}
		}
	}
	if len(p.buf) > MaxDNSFrame {
		return nil, errs.New(errs.EMessageMalformed, "encoded DNS packet is %d bytes", len(p.buf))
	}
	return p.buf, nil
}

func (p *packer) writeU16(v uint16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	p.buf = append(p.buf, b[:]...)
}

func (p *packer) writeU32(v uint32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	p.buf = append(p.buf, b[:]...)
}

func (p *packer) writeName(name string) error {
	canonical := CanonicalName(name)
	if canonical == "." {
		p.buf = append(p.buf, 0)
		return nil
	}
	trimmed := strings.TrimSuffix(canonical, ".")
	rawLabels := strings.Split(trimmed, ".")
	if len(rawLabels) > 127 {
		return errs.New(errs.EMessageMalformed, "domain name has too many labels")
	}
	wireLen := 1
	for _, label := range rawLabels {
		if label == "" {
			return errs.New(errs.EMessageMalformed, "domain name contains empty label")
		}
		if len(label) > 63 {
			return errs.New(errs.EMessageMalformed, "label length %d exceeds 63", len(label))
		}
		wireLen += 1 + len(label)
	}
	if wireLen > 255 {
		return errs.New(errs.EMessageMalformed, "domain name exceeds 255 wire bytes")
	}
	for i, label := range rawLabels {
		suffix := strings.ToLower(strings.Join(rawLabels[i:], ".")) + "."
		if ptr, ok := p.compress[suffix]; ok && ptr < 0x4000 {
			p.writeU16(uint16(0xc000 | ptr))
			return nil
		}
		offset := len(p.buf)
		if offset < 0x4000 {
			p.compress[suffix] = offset
		}
		p.buf = append(p.buf, byte(len(label)))
		p.buf = append(p.buf, label...)
	}
	p.buf = append(p.buf, 0)
	return nil
}

func (p *packer) writeRR(rr RR) error {
	if err := p.writeName(rr.Name); err != nil {
		return err
	}
	p.writeU16(uint16(rr.Type))
	class := rr.Class
	if class == 0 {
		class = 1
	}
	p.writeU16(class)
	p.writeU32(rr.TTL)
	lenPos := len(p.buf)
	p.buf = append(p.buf, 0, 0)
	start := len(p.buf)
	var err error
	switch rr.Type {
	case TypeA:
		ip := net.ParseIP(rr.Data.Address)
		if ip == nil || ip.To4() == nil || strings.Contains(rr.Data.Address, ":") {
			err = errs.New(errs.EMessageMalformed, "invalid A address %q", rr.Data.Address)
		} else {
			p.buf = append(p.buf, ip.To4()...)
		}
	case TypeAAAA:
		ip := net.ParseIP(rr.Data.Address)
		if ip == nil || ip.To16() == nil || !strings.Contains(rr.Data.Address, ":") {
			err = errs.New(errs.EMessageMalformed, "invalid AAAA address %q", rr.Data.Address)
		} else {
			p.buf = append(p.buf, ip.To16()...)
		}
	case TypeCNAME, TypeNS:
		if rr.Data.Target == "" {
			err = errs.New(errs.EMessageMalformed, "%s target is empty", rr.Type)
		} else {
			err = p.writeName(rr.Data.Target)
		}
	case TypeMX:
		if rr.Data.Target == "" {
			err = errs.New(errs.EMessageMalformed, "MX target is empty")
		} else {
			p.writeU16(rr.Data.Preference)
			err = p.writeName(rr.Data.Target)
		}
	case TypeTXT:
		for _, text := range rr.Data.Texts {
			if len(text) > 255 {
				err = errs.New(errs.EMessageMalformed, "TXT character-string length %d exceeds 255", len(text))
				break
			}
			p.buf = append(p.buf, byte(len(text)))
			p.buf = append(p.buf, text...)
		}
	case TypeSOA:
		if rr.Data.SOA == nil {
			err = errs.New(errs.EMessageMalformed, "SOA rdata is missing")
			break
		}
		if err = p.writeName(rr.Data.SOA.MName); err == nil {
			if err = p.writeName(rr.Data.SOA.RName); err == nil {
				p.writeU32(rr.Data.SOA.Serial)
				p.writeU32(rr.Data.SOA.Refresh)
				p.writeU32(rr.Data.SOA.Retry)
				p.writeU32(rr.Data.SOA.Expire)
				p.writeU32(rr.Data.SOA.Minimum)
			}
		}
	default:
		p.buf = append(p.buf, rr.Data.Raw...)
	}
	if err != nil {
		return err
	}
	rdlen := len(p.buf) - start
	if rdlen > 65535 {
		return errs.New(errs.EMessageMalformed, "rdata exceeds 65535 bytes")
	}
	binary.BigEndian.PutUint16(p.buf[lenPos:lenPos+2], uint16(rdlen))
	return nil
}

func WriteTCP(w io.Writer, m *Message) error {
	b, err := m.Pack()
	if err != nil {
		return err
	}
	if len(b) > 65535 {
		return errs.New(errs.ETCPFrameInvalid, "DNS TCP frame length %d exceeds 65535", len(b))
	}
	var prefix [2]byte
	binary.BigEndian.PutUint16(prefix[:], uint16(len(b)))
	if _, err := w.Write(prefix[:]); err != nil {
		return errs.Wrap(errs.ETCPFrameInvalid, err, "writing TCP length prefix")
	}
	_, err = w.Write(b)
	return err
}

// ReadTCP reads exactly one length-prefixed DNS TCP message. A short or zero
// read after a non-zero declared length is a framing error, not a DNS parse
// error.
func ReadTCP(r io.Reader) ([]byte, error) {
	var prefix [2]byte
	n, err := io.ReadFull(r, prefix[:])
	if err != nil {
		if errors.Is(err, io.EOF) && n == 0 {
			return nil, io.EOF
		}
		return nil, errs.Wrap(errs.ETCPFrameInvalid, err, "reading TCP length prefix")
	}
	length := int(binary.BigEndian.Uint16(prefix[:]))
	if length == 0 {
		return nil, errs.New(errs.ETCPFrameInvalid, "zero-length DNS TCP frame")
	}
	packet := make([]byte, length)
	n, err = io.ReadFull(r, packet)
	if err != nil {
		return nil, errs.Wrap(errs.ETCPFrameInvalid, err, "declared TCP frame length %d but read %d bytes", length, n)
	}
	return packet, nil
}
