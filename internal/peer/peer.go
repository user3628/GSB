package peer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"syscall"
	"time"

	"nonceguard/internal/model"
)

type ConfirmRequest struct {
	CheckType string `json:"check_type"`
	RequestID string `json:"request_id,omitempty"`
	ClientID  string `json:"client_id,omitempty"`
	Nonce     string `json:"nonce,omitempty"`
	Behavior  string `json:"behavior,omitempty"`
	DelayMs   int64  `json:"delay_ms,omitempty"`
}

type ConfirmResponse struct {
	Ack              bool   `json:"ack"`
	CheckType        string `json:"check_type"`
	PeerInstance     string `json:"peer_instance"`
	ReceivedAtUnixMs int64  `json:"received_at_unix_ms"`
	DispatchedAtMs   int64  `json:"dispatched_at_ms"`
	ConfigVersion    int64  `json:"config_version,omitempty"`
	Message          string `json:"message,omitempty"`
}

type Fault struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context"`
}

func (f *Fault) Error() string { return f.Code + ": " + f.Message }

type Client struct {
	BaseURL      string
	HTTPClient   *http.Client
	AckDeadline  time.Duration
	InstanceName string
}

func NewClient(addr string, timeout time.Duration) *Client {
	base := addr
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		base = "http://" + base
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &Client{
		BaseURL:     strings.TrimRight(base, "/"),
		HTTPClient:  &http.Client{Timeout: timeout},
		AckDeadline: 350 * time.Millisecond,
	}
}

func (c *Client) Confirm(parent context.Context, req ConfirmRequest) (*ConfirmResponse, *Fault) {
	body, _ := json.Marshal(req)
	httpReq, err := http.NewRequestWithContext(parent, http.MethodPost, c.BaseURL+"/peer/v1/"+req.CheckType, bytes.NewReader(body))
	if err != nil {
		return nil, &Fault{Code: model.CodeInternal, Message: err.Error(), Context: map[string]any{"peer_addr": c.BaseURL}}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	started := time.Now()
	resp, err := c.HTTPClient.Do(httpReq)
	elapsedMs := time.Since(started).Milliseconds()
	if err != nil {
		code := model.CodePeerTimeout
		message := "proxy peer confirmation timed out"
		if isConnectionRefused(err) {
			code = model.CodePeerRefused
			message = "proxy peer connection refused"
		} else if !errors.Is(err, context.DeadlineExceeded) && parent.Err() == context.DeadlineExceeded {
			code = model.CodePeerTimeout
		} else if errors.Is(err, context.DeadlineExceeded) {
			code = model.CodePeerTimeout
		}
		return nil, &Fault{Code: code, Message: message, Context: map[string]any{
			"peer_addr":        c.BaseURL,
			"check_type":       req.CheckType,
			"behavior":         req.Behavior,
			"elapsed_ms":       elapsedMs,
			"client_timeout":   c.HTTPClient.Timeout.String(),
			"underlying_error": err.Error(),
		}}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 500 {
		return nil, &Fault{Code: model.CodeInternal, Message: "peer returned server error", Context: map[string]any{"status": resp.StatusCode, "body": string(raw)}}
	}
	var out ConfirmResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, &Fault{Code: model.CodeInternal, Message: "invalid peer response", Context: map[string]any{"body": string(raw), "error": err.Error()}}
	}
	if elapsedMs > c.AckDeadline.Milliseconds() {
		return &out, &Fault{Code: model.CodePeerLateAck, Message: "proxy peer acknowledgement arrived after ack deadline", Context: map[string]any{
			"peer_addr":          c.BaseURL,
			"check_type":         req.CheckType,
			"elapsed_ms":         elapsedMs,
			"ack_deadline_ms":    c.AckDeadline.Milliseconds(),
			"peer_delay_ms":      req.DelayMs,
			"peer_dispatched_at": out.DispatchedAtMs,
		}}
	}
	return &out, nil
}

func isConnectionRefused(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, syscall.ECONNREFUSED) ||
		strings.Contains(strings.ToLower(err.Error()), "connection refused")
}

type Server struct {
	Instance string
	Mux      *http.ServeMux
}

func NewServer(instance string) *Server {
	s := &Server{Instance: instance, Mux: http.NewServeMux()}
	s.Mux.HandleFunc("/healthz", s.health)
	s.Mux.HandleFunc("/peer/v1/signature-check", s.confirm("signature_check"))
	s.Mux.HandleFunc("/peer/v1/nonce-query", s.confirm("nonce_query"))
	s.Mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	return s
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "instance": s.Instance, "service": "loopback-proxy-peer"})
}

func (s *Server) confirm(checkType string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		var req ConfirmRequest
		_ = json.Unmarshal(raw, &req)
		if req.DelayMs > 0 {
			time.Sleep(time.Duration(req.DelayMs) * time.Millisecond)
		}
		now := time.Now().UnixMilli()
		writeJSON(w, http.StatusOK, ConfirmResponse{
			Ack:              true,
			CheckType:        checkType,
			PeerInstance:     s.Instance,
			ReceivedAtUnixMs: now - req.DelayMs,
			DispatchedAtMs:   now,
			Message:          "deterministic loopback acknowledgement",
		})
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func FreeClosedAddr() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		return "", err
	}
	return addr, nil
}
