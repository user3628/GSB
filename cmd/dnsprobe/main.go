package main

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"example.com/gsb/dnscache/internal/dnsmsg"
)

type answer struct {
	Name string `json:"name"`
	Type string `json:"type"`
	TTL  uint32 `json:"ttl"`
	Data string `json:"data"`
}

type reply struct {
	Attempt      int      `json:"attempt"`
	RCode        string   `json:"rcode"`
	AnswerCount  int      `json:"answer_count"`
	Answers      []answer `json:"answers,omitempty"`
	Authorities  []answer `json:"authorities,omitempty"`
	ExtendedCode string   `json:"extended_error_code,omitempty"`
	ElapsedMS    int64    `json:"elapsed_ms"`
}

func main() {
	server := flag.String("server", "127.0.0.1:15353", "DNS server address")
	network := flag.String("network", "udp", "udp or tcp")
	name := flag.String("name", "www.example.test", "query name")
	qtype := flag.String("type", "A", "query type")
	timeout := flag.Duration("timeout", 2*time.Second, "per-attempt timeout")
	repeat := flag.Int("repeat", 1, "number of identical queries")
	interval := flag.Duration("interval", 100*time.Millisecond, "delay between repeated queries")
	expectRCode := flag.String("expect-rcode", "", "optional expected rcode name")
	expectExtended := flag.String("expect-extended-code", "", "optional expected EDE business code")
	expectAnswers := flag.Int("expect-answer-count", -1, "optional exact answer count")
	requireTTL := flag.Bool("require-noninc-ttl", false, "require first answer TTL to be non-increasing across attempts")
	rawHex := flag.String("raw-hex", "", "send this raw DNS packet instead of building a query")
	flag.Parse()

	typ, ok := dnsmsg.ParseType(*qtype)
	if !ok && *rawHex == "" {
		fmt.Fprintln(os.Stderr, "invalid DNS type:", *qtype)
		os.Exit(2)
	}
	var rawPacket []byte
	if *rawHex != "" {
		decoded, err := hex.DecodeString(strings.TrimPrefix(*rawHex, "0x"))
		if err != nil {
			fmt.Fprintln(os.Stderr, "invalid raw-hex:", err)
			os.Exit(2)
		}
		rawPacket = decoded
	}
	replies := make([]reply, 0, *repeat)
	var lastTTL uint32
	ttlSeen := false
	failed := false
	for i := 0; i < *repeat; i++ {
		if i > 0 {
			time.Sleep(*interval)
		}
		var rep reply
		var err error
		if len(rawPacket) > 0 {
			rep, err = queryPacket(*server, *network, rawPacket, *timeout, i+1)
		} else {
			rep, err = query(*server, *network, *name, typ, *timeout, i+1)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "query failed:", err)
			failed = true
			break
		}
		replies = append(replies, rep)
		if *expectRCode != "" && !strings.EqualFold(rep.RCode, *expectRCode) {
			fmt.Fprintf(os.Stderr, "attempt %d: rcode %s, want %s\n", rep.Attempt, rep.RCode, *expectRCode)
			failed = true
		}
		if *expectExtended != "" && !strings.EqualFold(rep.ExtendedCode, *expectExtended) {
			fmt.Fprintf(os.Stderr, "attempt %d: extended code %q, want %s\n", rep.Attempt, rep.ExtendedCode, *expectExtended)
			failed = true
		}
		if *expectAnswers >= 0 && rep.AnswerCount != *expectAnswers {
			fmt.Fprintf(os.Stderr, "attempt %d: answer count %d, want %d\n", rep.Attempt, rep.AnswerCount, *expectAnswers)
			failed = true
		}
		if *requireTTL && len(rep.Answers) > 0 {
			ttl := rep.Answers[0].TTL
			if ttlSeen && ttl > lastTTL {
				fmt.Fprintf(os.Stderr, "attempt %d: TTL increased from %d to %d\n", rep.Attempt, lastTTL, ttl)
				failed = true
			}
			lastTTL, ttlSeen = ttl, true
		}
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(map[string]any{"ok": !failed, "replies": replies})
	if failed {
		os.Exit(1)
	}
}

func query(server, network, name string, typ dnsmsg.Type, timeout time.Duration, attempt int) (reply, error) {
	q := dnsmsg.NewQuery(uint16(time.Now().UnixNano()), name, typ)
	packet, err := q.Pack()
	if err != nil {
		return reply{}, err
	}
	return queryPacket(server, network, packet, timeout, attempt)
}

func queryPacket(server, network string, packet []byte, timeout time.Duration, attempt int) (reply, error) {
	start := time.Now()
	conn, err := net.DialTimeout(network, server, timeout)
	if err != nil {
		return reply{}, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))
	var response []byte
	if strings.EqualFold(network, "tcp") {
		if len(packet) > 65535 {
			return reply{}, fmt.Errorf("packet too large")
		}
		if _, err := conn.Write([]byte{byte(len(packet) >> 8), byte(len(packet))}); err != nil {
			return reply{}, err
		}
		if _, err := conn.Write(packet); err != nil {
			return reply{}, err
		}
		response, err = dnsmsg.ReadTCP(conn)
		if err != nil {
			return reply{}, err
		}
	} else {
		if _, err := conn.Write(packet); err != nil {
			return reply{}, err
		}
		buf := make([]byte, 65535)
		n, err := conn.Read(buf)
		if err != nil {
			return reply{}, err
		}
		response = append([]byte(nil), buf[:n]...)
	}
	msg, err := dnsmsg.Parse(response)
	if err != nil {
		return reply{}, err
	}
	r := reply{Attempt: attempt, RCode: rcodeName(msg.Header.RCode()), AnswerCount: len(msg.Answers), ElapsedMS: time.Since(start).Milliseconds()}
	for _, rr := range msg.Answers {
		r.Answers = append(r.Answers, toAnswer(rr))
	}
	for _, rr := range msg.Authorities {
		r.Authorities = append(r.Authorities, toAnswer(rr))
	}
	for _, rr := range msg.Additionals {
		if rr.Type == dnsmsg.TypeOPT {
			if code := extendedCode(rr.Data.Raw); code != "" {
				r.ExtendedCode = code
			}
		}
	}
	return r, nil
}

func toAnswer(rr dnsmsg.RR) answer {
	return answer{Name: dnsmsg.CanonicalName(rr.Name), Type: rr.Type.String(), TTL: rr.TTL, Data: rr.Canonical()}
}

func rcodeName(rc dnsmsg.RCode) string {
	switch rc {
	case dnsmsg.RCodeSuccess:
		return "NOERROR"
	case dnsmsg.RCodeFormErr:
		return "FORMERR"
	case dnsmsg.RCodeServFail:
		return "SERVFAIL"
	case dnsmsg.RCodeNXDomain:
		return "NXDOMAIN"
	case dnsmsg.RCodeNotImp:
		return "NOTIMP"
	case dnsmsg.RCodeRefused:
		return "REFUSED"
	default:
		return fmt.Sprintf("RCODE%d", rc)
	}
}

func extendedCode(raw []byte) string {
	pos := 0
	for pos+4 <= len(raw) {
		code := binary.BigEndian.Uint16(raw[pos : pos+2])
		l := int(binary.BigEndian.Uint16(raw[pos+2 : pos+4]))
		pos += 4
		if pos+l > len(raw) {
			return ""
		}
		text := string(raw[pos : pos+l])
		pos += l
		if code == 15 {
			if idx := strings.Index(text, ":"); idx > 0 {
				return text[:idx]
			}
			return text
		}
	}
	return ""
}
