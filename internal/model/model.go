package model

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

const (
	HealthUp   = "UP"
	HealthDown = "DOWN"

	QueuePolicyQueue  = "QUEUE"
	QueuePolicyReject = "REJECT"
)

type Defaults struct {
	QueueWaitTimeoutMS int64 `json:"queue_wait_timeout_ms"`
	QueueLimit         int   `json:"queue_limit"`
	DialTimeoutMS      int64 `json:"dial_timeout_ms"`
	HandshakeMS        int64 `json:"handshake_ms"`
	DrainMS            int64 `json:"drain_ms"`
	ResponseBaseMS     int64 `json:"response_base_ms"`
	ResponseRateBPS    int64 `json:"response_rate_bps"`
	WriteTimeoutMS     int64 `json:"write_timeout_ms"`
	ReadTimeoutMS      int64 `json:"read_timeout_ms"`
	RateChunkBytes     int   `json:"rate_chunk_bytes"`
}

type EndpointSpec struct {
	ID               string `json:"id"`
	Address          string `json:"address"`
	MaxConnections   int    `json:"max_connections"`
	MaxReuses        int    `json:"max_reuses"`
	IdleReclaimMS    int64  `json:"idle_reclaim_ms"`
	Health           string `json:"health"`
	QueuePolicy      string `json:"queue_policy"`
	QueueLimit       int    `json:"queue_limit"`
	AcquireTimeoutMS int64  `json:"acquire_timeout_ms"`
	DialTimeoutMS    int64  `json:"dial_timeout_ms,omitempty"`
}

type Config struct {
	ListenAddr         string         `json:"listen_addr"`
	DBPath             string         `json:"db_path"`
	CheckpointInterval int            `json:"checkpoint_interval"`
	Defaults           Defaults       `json:"defaults"`
	Endpoints          []EndpointSpec `json:"endpoints"`
}

func LoadConfig(path string) (Config, error) {
	var cfg Config
	f, err := os.Open(path)
	if err != nil {
		return cfg, err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return cfg, fmt.Errorf("decode config: %w", err)
	}
	return cfg, nil
}

type RequestInput struct {
	RequestID     string `json:"request_id"`
	TimestampMS   int64  `json:"timestamp_ms"`
	ClientID      string `json:"client_id"`
	EndpointID    string `json:"endpoint_id"`
	RequestBytes  int64  `json:"request_bytes"`
	UploadRateBPS int64  `json:"upload_rate_bps"`
	ResponseBytes int64  `json:"response_bytes"`
}

type SubmitRequest struct {
	TrackID  string         `json:"track_id"`
	BatchID  string         `json:"batch_id"`
	Requests []RequestInput `json:"requests"`
}

type Request struct {
	TrackID       string `json:"track_id"`
	BatchID       string `json:"batch_id"`
	RequestKey    string `json:"request_key"`
	RequestID     string `json:"request_id"`
	TimestampMS   int64  `json:"timestamp_ms"`
	ClientID      string `json:"client_id"`
	EndpointID    string `json:"endpoint_id"`
	RequestBytes  int64  `json:"request_bytes"`
	UploadRateBPS int64  `json:"upload_rate_bps"`
	ResponseBytes int64  `json:"response_bytes"`
	Ordinal       int    `json:"ordinal"`
}

type ErrorBody struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

func (e *ErrorBody) Error() string {
	if e == nil {
		return ""
	}
	return e.Code + ": " + e.Message
}

type BusinessError struct {
	Status int
	Body   ErrorBody
}

func (e *BusinessError) Error() string { return e.Body.Error() }

func NewError(status int, code, message string, details map[string]any) *BusinessError {
	return &BusinessError{Status: status, Body: ErrorBody{Code: code, Message: message, Details: details}}
}

const (
	ErrSpecInvalid        = "E_SPEC_INVALID"
	ErrInvalidTransition  = "E_INVALID_TRANSITION"
	ErrPoolAcquireTimeout = "E_POOL_ACQUIRE_TIMEOUT"
	ErrPoolExhausted      = "E_POOL_EXHAUSTED"
	ErrConflictingBatch   = "E_CONFLICTING_BATCH"
	ErrCheckpointCorrupt  = "E_CHECKPOINT_CORRUPT"
	ErrDialFailed         = "E_DIAL_FAILED"
	ErrDialTimeout        = "E_DIAL_TIMEOUT"
	ErrSendTimeout        = "E_SEND_TIMEOUT"
	ErrRecvTimeout        = "E_RECV_TIMEOUT"
	ErrSendFailed         = "E_SEND_FAILED"
	ErrRecvFailed         = "E_RECV_FAILED"
	ErrConnClosed         = "E_CONNECTION_CLOSED"
	ErrEndpointUnhealthy  = "E_ENDPOINT_UNHEALTHY"
	ErrNotFound           = "E_NOT_FOUND"
	ErrRecoveryPending    = "E_RECOVERY_PENDING"
	ErrInternal           = "E_INTERNAL"
)

func Upper(value string) string { return strings.ToUpper(strings.TrimSpace(value)) }
