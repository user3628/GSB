// Package authserver implements a deterministic authoritative DNS server for
// local integration tests. It never contacts the public Internet.
package authserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"example.com/gsb/dnscache/internal/dnsmsg"
	"example.com/gsb/dnscache/internal/errs"
	"example.com/gsb/dnscache/internal/store"
)

type Behavior string

const (
	Normal    Behavior = "normal"
	Drop      Behavior = "drop"
	BadID     Behavior = "bad-id"
	Malformed Behavior = "malformed"
	Refused   Behavior = "refused"
)

type Config struct {
	UDPAddr  string
	TCPAddr  string
	Delay    time.Duration
	Behavior Behavior
}

type Server struct {
	cfg     Config
	logger  *log.Logger
	mu      sync.RWMutex
	zone    *zoneData
	udp     net.PacketConn
	tcp     net.Listener
	closed  bool
	closeCh chan struct{}
	doneCh  chan struct{}
	wg      sync.WaitGroup
	queries atomic.Uint64
}

type rrKey struct {
	owner string
	typ   dnsmsg.Type
}

type zoneData struct {
	zone  string
	rrs   map[rrKey][]dnsmsg.RR
	names map[string]bool
	soa   *dnsmsg.RR
}

func LoadSnapshotFile(path string) (store.ZoneSnapshot, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return store.ZoneSnapshot{}, err
	}
	var snap store.ZoneSnapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		return store.ZoneSnapshot{}, err
	}
	if snap.Zone == "" {
		return store.ZoneSnapshot{}, errs.New(errs.EInvalidRequest, "zone snapshot has no zone")
	}
	if snap.Checksum != "" {
		if got := store.ComputeSnapshotChecksum(snap); !strings.EqualFold(got, snap.Checksum) {
			return store.ZoneSnapshot{}, errs.New(errs.EChecksumMismatch, "zone file checksum mismatch: got %s", got)
		}
	} else {
		snap.Checksum = store.ComputeSnapshotChecksum(snap)
	}
	return snap, nil
}

func New(snap store.ZoneSnapshot, cfg Config, logger *log.Logger) (*Server, error) {
	if logger == nil {
		logger = log.Default()
	}
	z, err := buildZone(snap)
	if err != nil {
		return nil, err
	}
	if cfg.Behavior == "" {
		cfg.Behavior = Normal
	}
	return &Server{cfg: cfg, logger: logger, zone: z, closeCh: make(chan struct{}), doneCh: make(chan struct{})}, nil
}

func buildZone(snap store.ZoneSnapshot) (*zoneData, error) {
	zone := dnsmsg.CanonicalName(snap.Zone)
	z := &zoneData{zone: zone, rrs: make(map[rrKey][]dnsmsg.RR), names: map[string]bool{zone: true}}
	for _, rs := range snap.RRsets {
		owner := dnsmsg.CanonicalName(rs.Owner)
		if !dnsmsg.NameWithin(owner, zone) {
			return nil, fmt.Errorf("RRset owner %s outside zone %s", owner, zone)
		}
		key := rrKey{owner: owner, typ: rs.Type}
		for _, data := range rs.Records {
			rr := dnsmsg.RR{Name: owner, Type: rs.Type, Class: 1, TTL: rs.TTL, Data: data}
			z.rrs[key] = append(z.rrs[key], rr)
			z.names[owner] = true
			if rs.Type == dnsmsg.TypeSOA && owner == zone {
				cp := rr
				z.soa = &cp
			}
		}
	}
	if z.soa == nil && len(z.rrs) > 0 {
		return nil, fmt.Errorf("zone %s has no apex SOA for negative answers", zone)
	}
	return z, nil
}

func (s *Server) QueryCount() uint64 { return s.queries.Load() }

func (s *Server) Update(snap store.ZoneSnapshot) error {
	z, err := buildZone(snap)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.zone = z
	s.mu.Unlock()
	return nil
}

func (s *Server) Start(ctx context.Context) error {
	if s.cfg.UDPAddr != "" {
		udp, err := net.ListenPacket("udp", s.cfg.UDPAddr)
		if err != nil {
			return err
		}
		s.udp = udp
	}
	if s.cfg.TCPAddr != "" {
		tcp, err := net.Listen("tcp", s.cfg.TCPAddr)
		if err != nil {
			if s.udp != nil {
				s.udp.Close()
			}
			return err
		}
		s.tcp = tcp
	}
	if s.udp == nil && s.tcp == nil {
		return errs.New(errs.EInvalidRequest, "authoritative server needs at least one listen address")
	}
	if s.udp != nil {
		s.wg.Add(1)
		go s.serveUDP(ctx)
	}
	if s.tcp != nil {
		s.wg.Add(1)
		go s.serveTCP(ctx)
	}
	go func() {
		select {
		case <-ctx.Done():
			s.Close()
		case <-s.closeCh:
		}
	}()
	return nil
}

func (s *Server) UDPAddr() string {
	if s.udp == nil {
		return ""
	}
	return s.udp.LocalAddr().String()
}

func (s *Server) TCPAddr() string {
	if s.tcp == nil {
		return ""
	}
	return s.tcp.Addr().String()
}

func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		done := s.doneCh
		s.mu.Unlock()
		<-done
		return nil
	}
	s.closed = true
	close(s.closeCh)
	udp, tcp := s.udp, s.tcp
	s.mu.Unlock()
	if udp != nil {
		_ = udp.Close()
	}
	if tcp != nil {
		_ = tcp.Close()
	}
	s.wg.Wait()
	close(s.doneCh)
	return nil
}

func (s *Server) isClosed() bool {
	select {
	case <-s.closeCh:
		return true
	default:
		return false
	}
}

func (s *Server) serveUDP(ctx context.Context) {
	defer s.wg.Done()
	buf := make([]byte, 65535)
	for {
		n, addr, err := s.udp.ReadFrom(buf)
		if err != nil {
			if s.isClosed() {
				return
			}
			continue
		}
		packet := append([]byte(nil), buf[:n]...)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			resp := s.handle(ctx, packet)
			if len(resp) == 0 {
				return
			}
			_ = s.udp.SetWriteDeadline(time.Now().Add(time.Second))
			_, _ = s.udp.WriteTo(resp, addr)
		}()
	}
}

func (s *Server) serveTCP(ctx context.Context) {
	defer s.wg.Done()
	for {
		conn, err := s.tcp.Accept()
		if err != nil {
			if s.isClosed() {
				return
			}
			continue
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer conn.Close()
			for {
				_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
				packet, err := dnsmsg.ReadTCP(conn)
				if err != nil {
					if errors.Is(err, io.EOF) {
						return
					}
					return
				}
				if s.cfg.Behavior == Drop {
					timer := time.NewTimer(30 * time.Second)
					select {
					case <-ctx.Done():
						if !timer.Stop() {
							<-timer.C
						}
					case <-timer.C:
					}
					return
				}
				resp := s.handle(ctx, packet)
				if len(resp) == 0 {
					return
				}
				if err := dnsmsg.WriteTCP(conn, mustParse(resp)); err != nil {
					return
				}
			}
		}()
	}
}

func (s *Server) handle(ctx context.Context, packet []byte) []byte {
	if s.cfg.Delay > 0 {
		t := time.NewTimer(s.cfg.Delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C:
		}
	}
	q, err := dnsmsg.Parse(packet)
	if err != nil || len(q.Questions) != 1 {
		return nil
	}
	s.queries.Add(1)
	if s.cfg.Behavior == Drop {
		return nil
	}
	if s.cfg.Behavior == Malformed {
		return []byte{0xde, 0xad, 0xbe, 0xef, 0x00}
	}
	s.mu.RLock()
	z := s.zone
	s.mu.RUnlock()
	resp := z.answer(q)
	if s.cfg.Behavior == BadID {
		resp.Header.ID++
	}
	if s.cfg.Behavior == Refused {
		resp = dnsmsg.NewResponse(q, dnsmsg.RCodeRefused)
	}
	b, err := resp.Pack()
	if err != nil {
		return nil
	}
	return b
}

func mustParse(b []byte) *dnsmsg.Message {
	m, err := dnsmsg.Parse(b)
	if err != nil {
		return &dnsmsg.Message{Header: dnsmsg.Header{Flags: dnsmsg.FlagQR | uint16(dnsmsg.RCodeServFail)}}
	}
	return m
}

func (z *zoneData) answer(q *dnsmsg.Message) *dnsmsg.Message {
	question := q.Questions[0]
	name := dnsmsg.CanonicalName(question.Name)
	resp := dnsmsg.NewResponse(q, dnsmsg.RCodeSuccess)
	resp.Header.Flags |= dnsmsg.FlagAA
	if !dnsmsg.NameWithin(name, z.zone) {
		resp.Header.Flags = (resp.Header.Flags &^ 0x000f) | uint16(dnsmsg.RCodeRefused)
		return resp
	}
	if question.Type == dnsmsg.TypeANY {
		for key, rrs := range z.rrs {
			if key.owner == name {
				resp.Answers = append(resp.Answers, dnsmsg.CloneRRs(rrs)...)
			}
		}
		return resp
	}
	if rrs := z.rrs[rrKey{owner: name, typ: question.Type}]; len(rrs) > 0 {
		resp.Answers = dnsmsg.CloneRRs(rrs)
		return resp
	}
	if question.Type != dnsmsg.TypeCNAME {
		if rrs := z.rrs[rrKey{owner: name, typ: dnsmsg.TypeCNAME}]; len(rrs) > 0 {
			resp.Answers = dnsmsg.CloneRRs(rrs)
			target := dnsmsg.CanonicalName(rrs[0].Data.Target)
			if targetRRs := z.rrs[rrKey{owner: target, typ: question.Type}]; len(targetRRs) > 0 {
				resp.Answers = append(resp.Answers, dnsmsg.CloneRRs(targetRRs)...)
				return resp
			}
			if !z.names[name] {
				resp.Header.Flags = (resp.Header.Flags &^ 0x000f) | uint16(dnsmsg.RCodeNXDomain)
			}
			z.addNegativeSOA(resp)
			return resp
		}
	}
	if !z.names[name] {
		resp.Header.Flags = (resp.Header.Flags &^ 0x000f) | uint16(dnsmsg.RCodeNXDomain)
	}
	z.addNegativeSOA(resp)
	return resp
}

func (z *zoneData) addNegativeSOA(resp *dnsmsg.Message) {
	if z.soa != nil {
		resp.Authorities = append(resp.Authorities, *z.soa)
	}
	for _, rr := range z.rrs[rrKey{owner: z.zone, typ: dnsmsg.TypeNS}] {
		resp.Authorities = append(resp.Authorities, rr)
	}
}
