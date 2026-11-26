package resolver

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"example.com/gsb/dnscache/internal/dnsmsg"
	"example.com/gsb/dnscache/internal/errs"
)

type upstreamClient struct {
	addr    string
	network string
	timeout time.Duration
}

var queryID atomic.Uint32

func nextQueryID() uint16 {
	return uint16(queryID.Add(1) * 7919)
}

func (u *upstreamClient) exchange(ctx context.Context, name string, typ dnsmsg.Type) (*dnsmsg.Message, error) {
	hopCtx := ctx
	cancel := func() {}
	if u.timeout > 0 {
		hopCtx, cancel = context.WithTimeout(ctx, u.timeout)
	}
	defer cancel()

	q := dnsmsg.NewQuery(nextQueryID(), name, typ)
	packet, err := q.Pack()
	if err != nil {
		return nil, errs.Wrap(errs.EUpstreamBadResponse, err, "failed to encode upstream query")
	}
	var respBytes []byte
	switch strings.ToLower(u.network) {
	case "tcp":
		respBytes, err = u.exchangeTCP(hopCtx, packet)
	default:
		respBytes, err = u.exchangeUDP(hopCtx, packet)
	}
	if err != nil {
		return nil, u.classifyError(ctx, hopCtx, err)
	}
	resp, err := dnsmsg.Parse(respBytes)
	if err != nil {
		return nil, errs.Wrap(errs.EUpstreamBadResponse, err, "upstream returned a malformed DNS packet")
	}
	if resp.Header.ID != q.Header.ID || resp.Header.Flags&dnsmsg.FlagQR == 0 {
		return nil, errs.New(errs.EUpstreamBadResponse, "upstream response transaction id or QR flag is invalid")
	}
	if len(resp.Questions) != 1 {
		return nil, errs.New(errs.EUpstreamBadResponse, "upstream response has %d questions", len(resp.Questions))
	}
	want, got := q.Questions[0], resp.Questions[0]
	if dnsmsg.CanonicalName(want.Name) != dnsmsg.CanonicalName(got.Name) || want.Type != got.Type || want.Class != got.Class {
		return nil, errs.New(errs.EUpstreamBadResponse, "upstream response question mismatch")
	}
	if resp.Header.Opcode() != 0 {
		return nil, errs.New(errs.EUpstreamBadResponse, "upstream response opcode %d is invalid", resp.Header.Opcode())
	}
	switch resp.Header.RCode() {
	case dnsmsg.RCodeSuccess, dnsmsg.RCodeNXDomain:
		return resp, nil
	case dnsmsg.RCodeRefused:
		return nil, errs.New(errs.EUpstreamRefused, "upstream refused the query")
	default:
		return nil, errs.New(errs.EUpstreamBadResponse, "upstream returned rcode %d", resp.Header.RCode())
	}
}

func (u *upstreamClient) exchangeUDP(ctx context.Context, packet []byte) ([]byte, error) {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "udp", u.addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	if _, err := conn.Write(packet); err != nil {
		return nil, err
	}
	buf := make([]byte, 65535)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), buf[:n]...), nil
}

func (u *upstreamClient) exchangeTCP(ctx context.Context, packet []byte) ([]byte, error) {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", u.addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	q, err := dnsmsg.Parse(packet)
	if err != nil {
		return nil, err
	}
	if err := dnsmsg.WriteTCP(conn, q); err != nil {
		return nil, err
	}
	return dnsmsg.ReadTCP(conn)
}

func (u *upstreamClient) classifyError(baseCtx, hopCtx context.Context, err error) error {
	if errors.Is(err, syscall.ECONNREFUSED) {
		return errs.Wrap(errs.EUpstreamRefused, err, "upstream connection refused")
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) && errors.Is(opErr.Err, syscall.ECONNREFUSED) {
		return errs.Wrap(errs.EUpstreamRefused, err, "upstream connection refused")
	}
	if errors.Is(err, context.Canceled) && baseCtx.Err() != nil {
		return err
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) || isNetTimeout(err) || hopCtx.Err() != nil {
		return errs.Wrap(errs.EUpstreamTimeout, err, "upstream exchange timed out")
	}
	return errs.Wrap(errs.EUpstreamBadResponse, err, "upstream exchange failed")
}

func isNetTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}
