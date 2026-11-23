package probe

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"syscall"
	"time"

	"gatewayworkbench/internal/model"
)

// Executor 对回环端点发起真实 HTTP/TCP 探测。
type Executor struct {
	client *http.Client
}

func New() *Executor {
	transport := &http.Transport{
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: -1,
		}).DialContext,
		DisableKeepAlives:     true,
		MaxIdleConns:          -1,
		IdleConnTimeout:       1 * time.Second,
		TLSHandshakeTimeout:   2 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	return &Executor{client: &http.Client{Transport: transport}}
}

func isConnRefused(err error) bool {
	var sysErr syscall.Errno
	if errors.As(err, &sysErr) && errors.Is(sysErr, syscall.ECONNREFUSED) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) && strings.Contains(strings.ToLower(opErr.Err.Error()), "connection refused") {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "connection refused")
}

// Probe 执行一次真实探测。超时通过独立 context 触发，触发后立即关闭底层连接，
// 并返回 E_PROBE_TIMEOUT；连接拒绝返回 E_PROBE_REFUSED。
func (e *Executor) Probe(ctx context.Context, ins model.Instance) model.ProbeOutcome {
	start := time.Now()
	out := model.ProbeOutcome{InstanceID: ins.ID}
	timeout := time.Duration(ins.Probe.TimeoutMillis) * time.Millisecond
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if ins.Probe.Mode == "tcp" {
		dialer := net.Dialer{}
		var conn net.Conn
		var err error
		conn, err = dialer.DialContext(probeCtx, "tcp", "127.0.0.1:"+itoa(ins.Port))
		if err != nil {
			e.classify(err, &out)
			out.ElapsedMS = time.Since(start).Milliseconds()
			return out
		}
		// 探测完成即真实关闭，不占用任何连接/并发名额。
		_ = conn.Close()
		out.Success = true
		out.ElapsedMS = time.Since(start).Milliseconds()
		return out
	}

	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet,
		"http://127.0.0.1:"+itoa(ins.Port)+ins.Probe.Path, nil)
	if err != nil {
		out.ErrCode = model.CodeProbeFailed
		out.ErrMsg = err.Error()
		out.ElapsedMS = time.Since(start).Milliseconds()
		return out
	}
	resp, err := e.client.Do(req)
	if err != nil {
		e.classify(err, &out)
		out.ElapsedMS = time.Since(start).Milliseconds()
		return out
	}
	body, readErr := io.ReadAll(resp.Body)
	// 显式关闭：响应体读取后立刻释放底层连接。
	_ = resp.Body.Close()
	if readErr != nil {
		e.classify(readErr, &out)
		out.ElapsedMS = time.Since(start).Milliseconds()
		return out
	}
	out.StatusCode = resp.StatusCode
	switch {
	case resp.StatusCode != ins.Probe.ExpectedStatus:
		out.ErrCode = model.CodeProbeFailed
		out.ErrMsg = "status mismatch"
	case !strings.Contains(string(body), ins.Probe.BodyMarker):
		out.ErrCode = model.CodeProbeFailed
		out.ErrMsg = "body marker mismatch"
	default:
		out.Success = true
	}
	out.ElapsedMS = time.Since(start).Milliseconds()
	return out
}

func (e *Executor) classify(err error, out *model.ProbeOutcome) {
	switch {
	case isConnRefused(err):
		out.ErrCode = model.CodeProbeRefused
		out.ErrMsg = "connection refused: " + err.Error()
	case errors.Is(err, context.DeadlineExceeded) || isTimeout(err):
		out.ErrCode = model.CodeProbeTimeout
		out.ErrMsg = "probe timeout: " + err.Error()
	default:
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			out.ErrCode = model.CodeProbeTimeout
			out.ErrMsg = "probe timeout: " + err.Error()
			return
		}
		out.ErrCode = model.CodeProbeFailed
		out.ErrMsg = err.Error()
	}
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func itoa(port int) string {
	if port == 0 {
		return "0"
	}
	var buf [8]byte
	pos := len(buf)
	for port > 0 {
		pos--
		buf[pos] = byte('0' + port%10)
		port /= 10
	}
	return string(buf[pos:])
}
