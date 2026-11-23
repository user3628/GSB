package target

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"protocolfuzzlab/internal/protocol"
)

const maxFramesPerConnection = 256

type Server struct {
	listener net.Listener
	addr     string
	wg       sync.WaitGroup
	once     sync.Once
}

func Start(ctx context.Context) (*Server, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen loopback: %w", err)
	}
	s := &Server{listener: ln, addr: ln.Addr().String()}
	s.wg.Add(1)
	go s.acceptLoop(ctx)
	return s, nil
}

func (s *Server) Addr() string { return s.addr }

func (s *Server) Close() error {
	var err error
	s.once.Do(func() {
		err = s.listener.Close()
		s.wg.Wait()
	})
	return err
}

func (s *Server) acceptLoop(ctx context.Context) {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handle(ctx, conn)
		}()
	}
}

func (s *Server) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	machine := protocol.NewMachine()
	for i := 0; i < maxFramesPerConnection; i++ {
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		frame, err := protocol.Decode(conn)
		if err != nil {
			code := ""
			switch {
			case errors.Is(err, protocol.ErrBadChecksum):
				code = "E_BAD_CHECKSUM"
			case errors.Is(err, protocol.ErrPayloadOverflow):
				code = "E_PAYLOAD_OVERFLOW"
			case errors.Is(err, protocol.ErrTruncatedFrame):
				code = "E_TRUNCATED_FRAME"
			default:
				return
			}
			current := machine.State()
			response, encErr := protocol.EncodeResponse(code, current, current)
			if encErr == nil {
				_ = writeFrame(conn, response)
			}
			return
		}
		transition := machine.Apply(frame.Type)
		response, err := protocol.EncodeResponse(transition.Code, transition.From, transition.To)
		if err != nil {
			return
		}
		if err := writeFrame(conn, response); err != nil {
			return
		}
		if ctx.Err() != nil {
			return
		}
	}
}

func writeFrame(w io.Writer, frame protocol.Frame) error {
	b, err := protocol.Encode(frame)
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}
