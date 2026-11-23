package upstream

import (
	"context"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"upstreampool/internal/wire"
)

type Server struct {
	listener net.Listener
	readRate int64
	maxFrame uint64
	wg       sync.WaitGroup
	mu       sync.Mutex
	conns    map[net.Conn]struct{}
}

func Start(listenAddr string, readRate int64, maxFrame uint64) (*Server, error) {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	if maxFrame == 0 {
		maxFrame = wire.DefaultMaxFrame
	}
	s := &Server{listener: ln, readRate: readRate, maxFrame: maxFrame, conns: make(map[net.Conn]struct{})}
	s.wg.Add(1)
	go s.acceptLoop()
	log.Printf("upstream listening on %s (read_rate=%d B/s)", ln.Addr(), readRate)
	return s, nil
}

func (s *Server) Addr() string { return s.listener.Addr().String() }

func (s *Server) Close() error {
	err := s.listener.Close()
	s.mu.Lock()
	for c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return err
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() {
				s.mu.Lock()
				delete(s.conns, conn)
				s.mu.Unlock()
				_ = conn.Close()
			}()
			s.handle(conn)
		}()
	}
}

func (s *Server) handle(conn net.Conn) {
	for {
		hdr, err := wire.ReadHeader(conn)
		if err != nil {
			if err != io.EOF {
				log.Printf("upstream %s: read header: %v", conn.RemoteAddr(), err)
			}
			return
		}
		if uint64(hdr.IDLength) > s.maxFrame || hdr.BodyLength > s.maxFrame || hdr.ResponseLength > s.maxFrame {
			log.Printf("upstream %s: frame exceeds limit", conn.RemoteAddr())
			return
		}
		id := make([]byte, hdr.IDLength)
		if _, err := io.ReadFull(conn, id); err != nil {
			return
		}
		if err := s.readRateLimited(conn, int64(hdr.BodyLength)); err != nil {
			return
		}
		if err := s.writeResponse(conn, int64(hdr.ResponseLength)); err != nil {
			return
		}
	}
}

func (s *Server) readRateLimited(r io.Reader, total int64) error {
	if total == 0 {
		return nil
	}
	buf := make([]byte, 32<<10)
	remaining := total
	start := time.Now()
	var read int64
	for remaining > 0 {
		n := int64(len(buf))
		if remaining < n {
			n = remaining
		}
		if _, err := io.ReadFull(r, buf[:n]); err != nil {
			return err
		}
		read += n
		remaining -= n
		if s.readRate > 0 {
			expected := time.Duration(float64(read) / float64(s.readRate) * float64(time.Second))
			if sleep := expected - time.Since(start); sleep > 0 {
				time.Sleep(sleep)
			}
		}
	}
	return nil
}

func (s *Server) writeResponse(w io.Writer, total int64) error {
	buf := make([]byte, 32<<10)
	for i := range buf {
		buf[i] = 0x52
	}
	for total > 0 {
		n := int64(len(buf))
		if total < n {
			n = total
		}
		if _, err := w.Write(buf[:n]); err != nil {
			return err
		}
		total -= n
	}
	return nil
}

func Run(ctx context.Context, listenAddr string, readRate int64, maxFrame uint64) error {
	s, err := Start(listenAddr, readRate, maxFrame)
	if err != nil {
		return err
	}
	<-ctx.Done()
	return s.Close()
}
