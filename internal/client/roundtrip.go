package client

import (
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"time"

	"upstreampool/internal/model"
	"upstreampool/internal/wire"
)

type Options struct {
	DialTimeout    time.Duration
	WriteTimeout   time.Duration
	ReadTimeout    time.Duration
	RateChunkBytes int
}

type Result struct {
	ErrorCode string
	Message   string
}

func Dial(address string, timeout time.Duration) (net.Conn, Result) {
	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		if isTimeout(err) {
			return nil, Result{ErrorCode: model.ErrDialTimeout, Message: err.Error()}
		}
		return nil, Result{ErrorCode: model.ErrDialFailed, Message: err.Error()}
	}
	return conn, Result{}
}

func RoundTrip(conn net.Conn, requestID string, bodyBytes int64, uploadRateBPS int64, responseBytes int64, opts Options) Result {
	if bodyBytes < 0 || responseBytes < 0 || uploadRateBPS <= 0 {
		return Result{ErrorCode: model.ErrSpecInvalid, Message: "invalid request framing values"}
	}
	if bodyBytes > math.MaxUint32*8 {
		return Result{ErrorCode: model.ErrSpecInvalid, Message: "request body exceeds implementation limit"}
	}
	chunkBytes := opts.RateChunkBytes
	if chunkBytes <= 0 {
		chunkBytes = 4096
	}
	deadline := time.Now().Add(opts.WriteTimeout)
	if err := conn.SetWriteDeadline(deadline); err != nil {
		_ = conn.Close()
		return Result{ErrorCode: model.ErrSendFailed, Message: err.Error()}
	}
	if err := wire.WriteHeader(conn, len(requestID), uint64(bodyBytes), uint64(responseBytes)); err != nil {
		_ = conn.Close()
		return writeResult(err)
	}
	if _, err := io.WriteString(conn, requestID); err != nil {
		_ = conn.Close()
		return writeResult(err)
	}

	chunk := make([]byte, chunkBytes)
	for i := range chunk {
		chunk[i] = 0x41
	}
	remaining := bodyBytes
	start := time.Now()
	var sent int64
	for remaining > 0 {
		n := int64(len(chunk))
		if remaining < n {
			n = remaining
		}
		if err := conn.SetWriteDeadline(time.Now().Add(opts.WriteTimeout)); err != nil {
			_ = conn.Close()
			return Result{ErrorCode: model.ErrSendFailed, Message: err.Error()}
		}
		if _, err := conn.Write(chunk[:n]); err != nil {
			_ = conn.Close()
			return writeResult(err)
		}
		sent += n
		remaining -= n
		expected := time.Duration(float64(sent) / float64(uploadRateBPS) * float64(time.Second))
		if sleep := expected - time.Since(start); sleep > 0 {
			time.Sleep(sleep)
		}
	}

	if err := conn.SetReadDeadline(time.Now().Add(opts.ReadTimeout)); err != nil {
		_ = conn.Close()
		return Result{ErrorCode: model.ErrRecvFailed, Message: err.Error()}
	}
	if responseBytes > 0 {
		if _, err := io.CopyN(io.Discard, conn, responseBytes); err != nil {
			_ = conn.Close()
			return readResult(err)
		}
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		_ = conn.Close()
		return Result{ErrorCode: model.ErrRecvFailed, Message: err.Error()}
	}
	return Result{}
}

func writeResult(err error) Result {
	if isTimeout(err) {
		return Result{ErrorCode: model.ErrSendTimeout, Message: err.Error()}
	}
	return Result{ErrorCode: model.ErrSendFailed, Message: err.Error()}
}

func readResult(err error) Result {
	if isTimeout(err) {
		return Result{ErrorCode: model.ErrRecvTimeout, Message: err.Error()}
	}
	return Result{ErrorCode: model.ErrRecvFailed, Message: err.Error()}
}

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func DescribeDial(address string, result Result) string {
	if result.ErrorCode == "" {
		return "connected to " + address
	}
	return fmt.Sprintf("%s: %s", result.ErrorCode, result.Message)
}
