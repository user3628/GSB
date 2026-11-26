package resolver

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"example.com/gsb/dnscache/internal/dnsmsg"
	"example.com/gsb/dnscache/internal/errs"
)

type ListenConfig struct {
	UDPAddr        string
	TCPAddr        string
	QueryTimeout   time.Duration
	TCPIdleTimeout time.Duration
}

type Server struct {
	r       *Resolver
	cfg     ListenConfig
	logger  *log.Logger
	metrics *Metrics

	mu      sync.Mutex
	udp     net.PacketConn
	tcp     net.Listener
	closed  bool
	closeCh chan struct{}
	doneCh  chan struct{}
	wg      sync.WaitGroup
}

func NewServer(r *Resolver, cfg ListenConfig, logger *log.Logger) *Server {
	if cfg.QueryTimeout <= 0 {
		cfg.QueryTimeout = 5 * time.Second
	}
	if cfg.TCPIdleTimeout <= 0 {
		cfg.TCPIdleTimeout = 30 * time.Second
	}
	if logger == nil {
		logger = log.Default()
	}
	return &Server{r: r, cfg: cfg, logger: logger, metrics: r.Metrics(), closeCh: make(chan struct{}), doneCh: make(chan struct{})}
}

func (s *Server) Start(ctx context.Context) error {
	if s.cfg.UDPAddr != "" {
		udp, err := net.ListenPacket("udp", s.cfg.UDPAddr)
		if err != nil {
			return errs.Wrap(errs.EInternal, err, "bind UDP DNS listener %s", s.cfg.UDPAddr)
		}
		s.udp = udp
	}
	if s.cfg.TCPAddr != "" {
		tcp, err := net.Listen("tcp", s.cfg.TCPAddr)
		if err != nil {
			if s.udp != nil {
				s.udp.Close()
				s.udp = nil
			}
			return errs.Wrap(errs.EInternal, err, "bind TCP DNS listener %s", s.cfg.TCPAddr)
		}
		s.tcp = tcp
	}
	if s.udp == nil && s.tcp == nil {
		return errs.New(errs.EInvalidRequest, "at least one DNS listen address is required")
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
	s.logger.Printf("DNS listeners ready: udp=%s tcp=%s", s.UDPAddr(), s.TCPAddr())
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

func (s *Server) serveUDP(ctx context.Context) {
	defer s.wg.Done()
	buf := make([]byte, 65535)
	for {
		n, addr, err := s.udp.ReadFrom(buf)
		if err != nil {
			if s.isClosed() {
				return
			}
			s.logger.Printf("UDP read error: %v", err)
			continue
		}
		packet := append([]byte(nil), buf[:n]...)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			response := s.handlePacket(ctx, packet, "udp")
			if len(response) > 0 {
				_ = s.udp.SetWriteDeadline(time.Now().Add(s.cfg.QueryTimeout))
				if _, err := s.udp.WriteTo(response, addr); err != nil && !s.isClosed() {
					s.logger.Printf("UDP write to %s: %v", addr, err)
				}
				s.metrics.Responses.Add(1)
			}
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
			s.logger.Printf("TCP accept error: %v", err)
			continue
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer conn.Close()
			s.handleTCPConn(ctx, conn)
		}()
	}
}

func (s *Server) handleTCPConn(ctx context.Context, conn net.Conn) {
	for {
		_ = conn.SetReadDeadline(time.Now().Add(s.cfg.TCPIdleTimeout))
		packet, err := dnsmsg.ReadTCP(conn)
		if err != nil {
			if errors.Is(err, io.EOF) || s.isClosed() {
				return
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				return
			}
			s.metrics.TCPFrameBad.Add(1)
			s.metrics.RecordError("tcp_frame", err)
			s.logger.Printf("closing TCP connection after framing error: %v", err)
			return
		}
		response := s.handlePacket(ctx, packet, "tcp")
		if len(response) == 0 {
			return
		}
		_ = conn.SetWriteDeadline(time.Now().Add(s.cfg.QueryTimeout))
		if err := writeTCPBytes(conn, response); err != nil {
			s.logger.Printf("TCP write error: %v", err)
			return
		}
		s.metrics.Responses.Add(1)
	}
}

func writeTCPBytes(conn net.Conn, packet []byte) error {
	if len(packet) > 65535 {
		return errs.New(errs.ETCPFrameInvalid, "response packet too large for TCP framing")
	}
	if _, err := conn.Write([]byte{byte(len(packet) >> 8), byte(len(packet))}); err != nil {
		return err
	}
	_, err := conn.Write(packet)
	return err
}

func (s *Server) handlePacket(parent context.Context, packet []byte, transport string) []byte {
	s.metrics.RecordQuery(transport)
	queryCtx := parent
	cancel := func() {}
	if s.cfg.QueryTimeout > 0 {
		queryCtx, cancel = context.WithTimeout(parent, s.cfg.QueryTimeout)
	}
	defer cancel()

	query, err := dnsmsg.Parse(packet)
	if err != nil {
		s.metrics.Malformed.Add(1)
		s.metrics.RecordError("dns_parse", err)
		resp := ResolveErrorResponse(nil, errs.EMessageMalformed, errs.MessageOf(err))
		if id, ok := dnsmsg.HeaderID(packet); ok {
			resp.Header.ID = id
		}
		b, _ := resp.Pack()
		return b
	}
	response, err := s.r.Resolve(queryCtx, query)
	if err != nil {
		code := errs.CodeOf(err)
		s.metrics.RecordError("resolve", err)
		resp := ResolveErrorResponse(query, code, errs.MessageOf(err))
		b, _ := resp.Pack()
		return b
	}
	s.metrics.RecordCode(rcodeMetric(response.Header.RCode()))
	b, err := response.Pack()
	if err != nil {
		s.metrics.RecordError("response_pack", err)
		resp := ResolveErrorResponse(query, errs.EInternal, err.Error())
		b, _ = resp.Pack()
	}
	return b
}

func rcodeMetric(rcode dnsmsg.RCode) string {
	switch rcode {
	case dnsmsg.RCodeSuccess:
		return "OK"
	case dnsmsg.RCodeNXDomain:
		return "NXDOMAIN"
	case dnsmsg.RCodeServFail:
		return "SERVFAIL"
	case dnsmsg.RCodeFormErr:
		return "FORMERR"
	case dnsmsg.RCodeNotImp:
		return "NOTIMP"
	case dnsmsg.RCodeRefused:
		return "REFUSED"
	default:
		return "RCODE" + strconv.Itoa(int(rcode))
	}
}

func (s *Server) isClosed() bool {
	select {
	case <-s.closeCh:
		return true
	default:
		return false
	}
}
