package config

import (
	"fmt"
	"math"
	"math/big"
	"net"
	"strconv"
	"strings"

	"upstreampool/internal/model"
)

const (
	maxConnections = 1_000_000
	maxQueue       = 1_000_000
	maxRateChunk   = 16 << 20
)

func Validate(cfg *model.Config) error {
	if strings.TrimSpace(cfg.ListenAddr) == "" {
		return specError("listen_addr is required", nil)
	}
	host, _, err := net.SplitHostPort(cfg.ListenAddr)
	if err != nil || !isLoopbackHost(host) {
		return specError("listen_addr must be a loopback host:port", map[string]any{"listen_addr": cfg.ListenAddr})
	}
	if strings.TrimSpace(cfg.DBPath) == "" {
		return specError("db_path is required", nil)
	}
	if cfg.CheckpointInterval < 1 {
		cfg.CheckpointInterval = 1
	}
	if err := validateDefaults(&cfg.Defaults); err != nil {
		return err
	}
	if len(cfg.Endpoints) == 0 {
		return specError("at least one endpoint is required", nil)
	}
	seen := make(map[string]struct{}, len(cfg.Endpoints))
	for i := range cfg.Endpoints {
		ep := &cfg.Endpoints[i]
		if err := ValidateEndpoint(ep, &cfg.Defaults); err != nil {
			return err
		}
		if _, ok := seen[ep.ID]; ok {
			return specError("duplicate endpoint id", map[string]any{"endpoint_id": ep.ID})
		}
		seen[ep.ID] = struct{}{}
	}
	return nil
}

func validateDefaults(d *model.Defaults) error {
	if d.QueueWaitTimeoutMS < 0 || d.DialTimeoutMS <= 0 || d.HandshakeMS < 0 || d.DrainMS < 0 ||
		d.ResponseBaseMS < 0 || d.ResponseRateBPS <= 0 || d.WriteTimeoutMS <= 0 || d.ReadTimeoutMS <= 0 {
		return specError("invalid default timing values", nil)
	}
	if d.QueueLimit < 0 || d.QueueLimit > maxQueue {
		return specError("queue_limit must be between 0 and 1000000", nil)
	}
	if d.RateChunkBytes <= 0 || d.RateChunkBytes > maxRateChunk {
		return specError("rate_chunk_bytes is out of range", nil)
	}
	return nil
}

func ValidateEndpoint(ep *model.EndpointSpec, d *model.Defaults) error {
	ep.ID = strings.TrimSpace(ep.ID)
	if ep.ID == "" {
		return specError("endpoint id is required", nil)
	}
	host, port, err := net.SplitHostPort(strings.TrimSpace(ep.Address))
	if err != nil || !isLoopbackHost(host) {
		return specError("endpoint address must be loopback host:port", map[string]any{"endpoint_id": ep.ID, "address": ep.Address})
	}
	p, err := strconv.Atoi(port)
	if err != nil || p < 1 || p > 65535 {
		return specError("endpoint port is invalid", map[string]any{"endpoint_id": ep.ID, "address": ep.Address})
	}
	if ep.MaxConnections <= 0 || ep.MaxConnections > maxConnections {
		return specError("max_connections must be a positive bounded integer", map[string]any{"endpoint_id": ep.ID})
	}
	if ep.MaxReuses <= 0 || ep.MaxReuses > maxConnections {
		return specError("max_reuses must be a positive bounded integer", map[string]any{"endpoint_id": ep.ID})
	}
	if ep.IdleReclaimMS < 0 {
		return specError("idle_reclaim_ms must be non-negative", map[string]any{"endpoint_id": ep.ID})
	}
	ep.Health = model.Upper(ep.Health)
	if ep.Health == "" {
		ep.Health = model.HealthUp
	}
	if ep.Health != model.HealthUp && ep.Health != model.HealthDown {
		return specError("health must be UP or DOWN", map[string]any{"endpoint_id": ep.ID})
	}
	ep.QueuePolicy = model.Upper(ep.QueuePolicy)
	if ep.QueuePolicy == "" {
		ep.QueuePolicy = model.QueuePolicyQueue
	}
	if ep.QueuePolicy != model.QueuePolicyQueue && ep.QueuePolicy != model.QueuePolicyReject {
		return specError("queue_policy must be QUEUE or REJECT", map[string]any{"endpoint_id": ep.ID})
	}
	if ep.QueueLimit < 0 {
		ep.QueueLimit = d.QueueLimit
	}
	if ep.QueueLimit > maxQueue {
		return specError("queue_limit is too large", map[string]any{"endpoint_id": ep.ID})
	}
	if ep.AcquireTimeoutMS < 0 {
		ep.AcquireTimeoutMS = d.QueueWaitTimeoutMS
	}
	if ep.DialTimeoutMS <= 0 {
		ep.DialTimeoutMS = d.DialTimeoutMS
	}
	return nil
}

func ValidateRequest(req *model.RequestInput, endpoints map[string]model.EndpointSpec) error {
	req.RequestID = strings.TrimSpace(req.RequestID)
	req.ClientID = strings.TrimSpace(req.ClientID)
	req.EndpointID = strings.TrimSpace(req.EndpointID)
	if req.ClientID == "" {
		return specError("client_id is required", nil)
	}
	if req.TimestampMS < 0 {
		return specError("timestamp_ms must be non-negative", map[string]any{"request_id": req.RequestID})
	}
	if req.RequestBytes < 0 || req.ResponseBytes < 0 {
		return specError("request_bytes and response_bytes must be non-negative", map[string]any{"request_id": req.RequestID})
	}
	if req.UploadRateBPS <= 0 {
		return specError("upload_rate_bps must be positive", map[string]any{"request_id": req.RequestID})
	}
	ep, ok := endpoints[req.EndpointID]
	if !ok {
		return specError("request references an undeclared endpoint", map[string]any{"request_id": req.RequestID, "endpoint_id": req.EndpointID})
	}
	if _, err := DurationMS(req.RequestBytes, req.UploadRateBPS); err != nil {
		return specError(err.Error(), map[string]any{"request_id": req.RequestID})
	}
	if _, err := SafeAdd(req.TimestampMS, req.TimestampMS); err != nil {
		return specError("timestamp_ms is too large", map[string]any{"request_id": req.RequestID})
	}
	_ = ep
	return nil
}

func DurationMS(bytes, rate int64) (int64, error) {
	if bytes < 0 || rate <= 0 {
		return 0, fmt.Errorf("invalid byte/rate values")
	}
	if bytes == 0 {
		return 0, nil
	}
	n := new(big.Int).Mul(big.NewInt(bytes), big.NewInt(1000))
	n.Add(n, big.NewInt(rate-1))
	n.Quo(n, big.NewInt(rate))
	if !n.IsInt64() {
		return 0, fmt.Errorf("duration overflows int64")
	}
	return n.Int64(), nil
}

func SafeAdd(a, b int64) (int64, error) {
	if b > 0 && a > math.MaxInt64-b {
		return 0, fmt.Errorf("timestamp overflow")
	}
	if b < 0 && a < math.MinInt64-b {
		return 0, fmt.Errorf("timestamp underflow")
	}
	return a + b, nil
}

func isLoopbackHost(host string) bool {
	h := strings.Trim(host, "[]")
	if strings.EqualFold(h, "localhost") {
		return true
	}
	ip := net.ParseIP(h)
	return ip != nil && ip.IsLoopback()
}

func specError(message string, details map[string]any) error {
	return model.NewError(400, model.ErrSpecInvalid, message, details)
}
