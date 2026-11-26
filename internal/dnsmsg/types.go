package dnsmsg

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

type Type uint16

const (
	TypeA     Type = 1
	TypeNS    Type = 2
	TypeCNAME Type = 5
	TypeSOA   Type = 6
	TypeMX    Type = 15
	TypeTXT   Type = 16
	TypeAAAA  Type = 28
	TypeOPT   Type = 41
	TypeANY   Type = 255
)

func (t Type) String() string {
	switch t {
	case TypeA:
		return "A"
	case TypeNS:
		return "NS"
	case TypeCNAME:
		return "CNAME"
	case TypeSOA:
		return "SOA"
	case TypeMX:
		return "MX"
	case TypeTXT:
		return "TXT"
	case TypeAAAA:
		return "AAAA"
	case TypeOPT:
		return "OPT"
	case TypeANY:
		return "ANY"
	default:
		return "TYPE" + strconv.Itoa(int(t))
	}
}

func ParseType(s string) (Type, bool) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "A":
		return TypeA, true
	case "NS":
		return TypeNS, true
	case "CNAME":
		return TypeCNAME, true
	case "SOA":
		return TypeSOA, true
	case "MX":
		return TypeMX, true
	case "TXT":
		return TypeTXT, true
	case "AAAA":
		return TypeAAAA, true
	case "ANY":
		return TypeANY, true
	default:
		n, err := strconv.Atoi(strings.TrimPrefix(strings.ToUpper(s), "TYPE"))
		if err == nil && n > 0 && n <= 65535 {
			return Type(n), true
		}
		return 0, false
	}
}

func SupportedType(t Type) bool {
	switch t {
	case TypeA, TypeAAAA, TypeCNAME, TypeMX, TypeTXT, TypeNS, TypeSOA:
		return true
	default:
		return false
	}
}

type RCode uint16

const (
	RCodeSuccess  RCode = 0
	RCodeFormErr  RCode = 1
	RCodeServFail RCode = 2
	RCodeNXDomain RCode = 3
	RCodeNotImp   RCode = 4
	RCodeRefused  RCode = 5
)

type Header struct {
	ID      uint16
	Flags   uint16
	QDCount uint16
	ANCount uint16
	NSCount uint16
	ARCount uint16
}

const (
	FlagQR uint16 = 1 << 15
	FlagAA uint16 = 1 << 10
	FlagTC uint16 = 1 << 9
	FlagRD uint16 = 1 << 8
	FlagRA uint16 = 1 << 7
)

func (h Header) RCode() RCode           { return RCode(h.Flags & 0x000f) }
func (h Header) Opcode() uint16         { return (h.Flags >> 11) & 0x000f }
func (h Header) RecursionDesired() bool { return h.Flags&FlagRD != 0 }

type Question struct {
	Name  string
	Type  Type
	Class uint16
}

type SOA struct {
	MName   string `json:"mname"`
	RName   string `json:"rname"`
	Serial  uint32 `json:"serial"`
	Refresh uint32 `json:"refresh"`
	Retry   uint32 `json:"retry"`
	Expire  uint32 `json:"expire"`
	Minimum uint32 `json:"minimum"`
}

// RData is a lossless-enough representation of the record types supported by
// the component. Raw is used for unsupported records (for example OPT).
type RData struct {
	Address    string   `json:"address,omitempty"`
	Target     string   `json:"target,omitempty"`
	Preference uint16   `json:"preference,omitempty"`
	Texts      []string `json:"texts,omitempty"`
	SOA        *SOA     `json:"soa,omitempty"`
	Raw        []byte   `json:"raw,omitempty"`
}

type RR struct {
	Name  string `json:"name"`
	Type  Type   `json:"type"`
	Class uint16 `json:"class"`
	TTL   uint32 `json:"ttl"`
	Data  RData  `json:"data"`
}

type Message struct {
	Header      Header
	Questions   []Question
	Answers     []RR
	Authorities []RR
	Additionals []RR
}

func NewQuery(id uint16, name string, typ Type) *Message {
	return &Message{
		Header:    Header{ID: id, Flags: FlagRD, QDCount: 1},
		Questions: []Question{{Name: CanonicalName(name), Type: typ, Class: 1}},
	}
}

func NewResponse(query *Message, rcode RCode) *Message {
	m := &Message{
		Header:    Header{ID: query.Header.ID, Flags: FlagQR | FlagRA | (query.Header.Flags & FlagRD) | uint16(rcode)},
		Questions: append([]Question(nil), query.Questions...),
	}
	m.Header.QDCount = uint16(len(m.Questions))
	return m
}

func (m *Message) SetCounts() {
	m.Header.QDCount = uint16(len(m.Questions))
	m.Header.ANCount = uint16(len(m.Answers))
	m.Header.NSCount = uint16(len(m.Authorities))
	m.Header.ARCount = uint16(len(m.Additionals))
}

func (m *Message) Clone() *Message {
	out := *m
	out.Questions = append([]Question(nil), m.Questions...)
	out.Answers = CloneRRs(m.Answers)
	out.Authorities = CloneRRs(m.Authorities)
	out.Additionals = CloneRRs(m.Additionals)
	return &out
}

func CloneRRs(in []RR) []RR {
	out := make([]RR, len(in))
	for i := range in {
		out[i] = in[i]
		out[i].Data.Texts = append([]string(nil), in[i].Data.Texts...)
		out[i].Data.Raw = append([]byte(nil), in[i].Data.Raw...)
		if in[i].Data.SOA != nil {
			soa := *in[i].Data.SOA
			out[i].Data.SOA = &soa
		}
	}
	return out
}

// DecrementRRs subtracts elapsed whole seconds from every TTL. A cached TTL
// that has not yet crossed a second boundary is reported as one rather than
// zero so a live cache entry never looks expired on the wire.
func DecrementRRs(rrs []RR, elapsedMS int64) []RR {
	out := CloneRRs(rrs)
	if elapsedMS <= 0 {
		return out
	}
	for i := range out {
		elapsedSec := uint32(elapsedMS / 1000)
		if elapsedSec >= out[i].TTL {
			out[i].TTL = 1
		} else {
			out[i].TTL -= elapsedSec
		}
	}
	return out
}

func CanonicalName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" || name == "." {
		return "."
	}
	name = strings.ToLower(name)
	if !strings.HasSuffix(name, ".") {
		name += "."
	}
	return name
}

func FQDN(name string) string {
	name = strings.TrimSpace(name)
	if name == "." {
		return "."
	}
	name = CanonicalName(name)
	return strings.TrimSuffix(name, ".")
}

func NameWithin(name, zone string) bool {
	n, z := CanonicalName(name), CanonicalName(zone)
	if z == "." {
		return true
	}
	return n == z || strings.HasSuffix(n, "."+z)
}

func (r RR) Canonical() string {
	switch r.Type {
	case TypeA:
		ip := net.ParseIP(r.Data.Address)
		if ip == nil || ip.To4() == nil {
			return "invalid:" + r.Data.Address
		}
		return ip.To4().String()
	case TypeAAAA:
		ip := net.ParseIP(r.Data.Address)
		if ip == nil || ip.To16() == nil {
			return "invalid:" + r.Data.Address
		}
		return ip.To16().String()
	case TypeCNAME, TypeNS:
		return CanonicalName(r.Data.Target)
	case TypeMX:
		return fmt.Sprintf("%d %s", r.Data.Preference, CanonicalName(r.Data.Target))
	case TypeTXT:
		return strings.Join(r.Data.Texts, "\x00")
	case TypeSOA:
		if r.Data.SOA == nil {
			return ""
		}
		s := r.Data.SOA
		return fmt.Sprintf("%s %s %d %d %d %d %d", CanonicalName(s.MName), CanonicalName(s.RName), s.Serial, s.Refresh, s.Retry, s.Expire, s.Minimum)
	default:
		return fmt.Sprintf("%x", r.Data.Raw)
	}
}

func (r RR) String() string {
	return fmt.Sprintf("%s %d IN %s %s", CanonicalName(r.Name), r.TTL, r.Type, r.Canonical())
}

func (t Type) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(t.String())), nil
}

func (t *Type) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		unq, err := strconv.Unquote(s)
		if err != nil {
			return err
		}
		v, ok := ParseType(unq)
		if !ok {
			return fmt.Errorf("unsupported DNS type %q", unq)
		}
		*t = v
		return nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 || n > 65535 {
		return fmt.Errorf("invalid DNS type %q", s)
	}
	*t = Type(n)
	return nil
}
