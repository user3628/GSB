package registry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"time"
)

const maxProbeBody = 1 << 20

func executeProbe(ctx context.Context, roundDeadline time.Time, in RuntimeInstance) ProbeOutcome {
	started := time.Now()
	out := ProbeOutcome{ServiceName: in.ServiceName, InstanceID: in.InstanceID, Kind: in.Probe.Kind}
	timeout := time.Duration(in.Probe.TimeoutMS) * time.Millisecond
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	probeDeadline, _ := probeCtx.Deadline()
	defer cancel()

	switch in.Probe.Kind {
	case ProbeHTTP:
		executeHTTPProbe(probeCtx, roundDeadline, probeDeadline, in, &out)
	case ProbeTCP:
		executeTCPProbe(probeCtx, roundDeadline, probeDeadline, in, &out)
	case ProbeLease:
		executeLeaseProbe(in, time.Now().UnixMilli(), &out)
	default:
		out.Code = "E_PROBE_KIND"
		out.Detail = "unsupported probe kind"
	}
	out.LatencyMS = time.Since(started).Milliseconds()
	out.ObservedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return out
}

func executeHTTPProbe(ctx context.Context, roundDeadline, probeDeadline time.Time, in RuntimeInstance, out *ProbeOutcome) {
	u, err := url.Parse(in.Endpoint)
	if err != nil {
		out.Code = "E_HTTP_ENDPOINT"
		out.Detail = err.Error()
		return
	}
	if in.Probe.Path != "" {
		u.Path = strings.TrimRight(u.Path, "/") + "/" + strings.TrimLeft(in.Probe.Path, "/")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		out.Code = "E_HTTP_REQUEST"
		out.Detail = err.Error()
		return
	}
	dialer := &net.Dialer{Timeout: in.ProbeTimeoutDuration()}
	transport := &http.Transport{
		Proxy:                 nil,
		DisableKeepAlives:     true,
		ForceAttemptHTTP2:     false,
		DialContext:           dialer.DialContext,
		ResponseHeaderTimeout: in.ProbeTimeoutDuration(),
	}
	client := &http.Client{Transport: transport}
	defer func() {
		client.CloseIdleConnections()
		transport.CloseIdleConnections()
	}()
	resp, err := client.Do(req)
	if err != nil {
		out.Code = probeErrorCode(err)
		out.Detail = probeErrorDetail(err, roundDeadline, probeDeadline)
		return
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxProbeBody+1))
	if readErr != nil {
		out.Code = probeErrorCode(readErr)
		out.Detail = probeErrorDetail(readErr, roundDeadline, probeDeadline)
		return
	}
	if resp.StatusCode != in.Probe.ExpectedStatus {
		out.Code = "E_PROBE_STATUS_MISMATCH"
		out.Detail = fmt.Sprintf("status=%d expected=%d", resp.StatusCode, in.Probe.ExpectedStatus)
		return
	}
	if !strings.Contains(string(body), in.Probe.ExpectedMarker) {
		out.Code = "E_PROBE_MARKER_MISMATCH"
		out.Detail = fmt.Sprintf("body marker %q not found", in.Probe.ExpectedMarker)
		return
	}
	out.OK = true
	out.Code = "E_PROBE_OK"
	out.Detail = fmt.Sprintf("status=%d marker=%s", resp.StatusCode, in.Probe.ExpectedMarker)
}

func executeTCPProbe(ctx context.Context, roundDeadline, probeDeadline time.Time, in RuntimeInstance, out *ProbeOutcome) {
	dialer := &net.Dialer{Timeout: in.ProbeTimeoutDuration()}
	conn, err := dialer.DialContext(ctx, "tcp", in.Endpoint)
	if err != nil {
		out.Code = probeErrorCode(err)
		out.Detail = probeErrorDetail(err, roundDeadline, probeDeadline)
		return
	}
	_ = conn.Close()
	out.OK = true
	out.Code = "E_TCP_OK"
	out.Detail = "tcp dial succeeded and socket closed"
}

func executeLeaseProbe(in RuntimeInstance, nowMS int64, out *ProbeOutcome) {
	if in.LastHeartbeatMS <= 0 {
		out.Code = "E_LEASE_MISSING"
		out.Detail = "no heartbeat has ever been observed"
		return
	}
	expires := in.LastHeartbeatMS + in.LeaseTTLMS
	if nowMS > expires {
		out.Code = "E_LEASE_EXPIRED"
		out.Detail = fmt.Sprintf("expiredAt=%d now=%d", expires, nowMS)
		return
	}
	out.OK = true
	out.Code = "E_LEASE_VALID"
	out.Detail = fmt.Sprintf("remainingMs=%d", expires-nowMS)
}

func probeErrorCode(err error) string {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || isTimeout(err) {
		return ErrProbeTimeout
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return "E_TCP_REFUSED"
	}
	return "E_PROBE_FAILED"
}

func probeErrorDetail(err error, roundDeadline, probeDeadline time.Time) string {
	if probeErrorCode(err) == ErrProbeTimeout && !roundDeadline.IsZero() && !probeDeadline.IsZero() &&
		!roundDeadline.After(probeDeadline) {
		return err.Error() + "; effective deadline included the round budget"
	}
	return err.Error()
}

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func (in Instance) ProbeTimeoutDuration() time.Duration {
	return time.Duration(in.Probe.TimeoutMS) * time.Millisecond
}
