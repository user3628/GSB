package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"nonceguard/internal/canonical"
)

const (
	CodeOK                 = "OK"
	CodeRequestInvalid     = "E_REQUEST_INVALID"
	CodeSignatureMismatch  = "E_SIGNATURE_MISMATCH"
	CodeTimestampEarly     = "E_TIMESTAMP_EARLY"
	CodeTimestampLate      = "E_TIMESTAMP_LATE"
	CodeNonceReplayed      = "E_NONCE_REPLAYED"
	CodeStaleSnapshot      = "E_STALE_SNAPSHOT"
	CodeSnapshotDigest     = "E_SNAPSHOT_DIGEST_INVALID"
	CodeSnapshotInvalid    = "E_SNAPSHOT_INVALID"
	CodeConfigVersionGap   = "E_CONFIG_VERSION_GAP"
	CodeConflictingRequest = "E_CONFLICTING_REQUEST"
	CodeBatchInterrupted   = "E_BATCH_INTERRUPTED"
	CodePeerTimeout        = "E_PEER_TIMEOUT"
	CodePeerRefused        = "E_PEER_REFUSED"
	CodePeerLateAck        = "E_PEER_LATE_ACK"
	CodeNotPrechecked      = "E_FAST_NOT_PRECHECKED"
	CodeNotFound           = "E_NOT_FOUND"
	CodeMethodNotAllowed   = "E_METHOD_NOT_ALLOWED"
	CodeBatchAborted       = "E_BATCH_ABORTED_RECOVERED"
	CodeInternal           = "E_INTERNAL"
)

const (
	ModeFull = "full"
	ModeFast = "fast"

	StatusAccepted   = "accepted"
	StatusRejected   = "rejected"
	StatusSkipped    = "skipped"
	StatusChecked    = "checked"
	StatusNotChecked = "not_prechecked"
)

var (
	NoncePattern      = regexp.MustCompile(`^[A-Za-z0-9_-]{16,128}$`)
	digestPattern     = regexp.MustCompile(`^[a-f0-9]{64}$`)
	identifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$`)

	KnownSignatureFields = []string{
		"request_id",
		"timestamp_ms",
		"route_key",
		"client_id",
		"nonce",
		"weight",
	}
)

type Request struct {
	RequestID       string `json:"request_id"`
	TimestampMs     int64  `json:"timestamp_ms"`
	RouteKey        string `json:"route_key"`
	ClientID        string `json:"client_id"`
	Nonce           string `json:"nonce"`
	SignatureDigest string `json:"signature_digest"`
	Weight          int64  `json:"weight"`
}

type Batch struct {
	BatchID       string    `json:"batch_id"`
	ConfigVersion int64     `json:"config_version,omitempty"`
	ReferenceMs   int64     `json:"reference_ms,omitempty"`
	Mode          string    `json:"mode,omitempty"`
	Requests      []Request `json:"requests"`
}

type rawRequest struct {
	RequestID       string      `json:"request_id"`
	TimestampMs     int64       `json:"timestamp_ms"`
	RouteKey        string      `json:"route_key"`
	ClientID        string      `json:"client_id"`
	Nonce           string      `json:"nonce"`
	SignatureDigest string      `json:"signature_digest"`
	Weight          json.Number `json:"weight"`
}

type rawBatch struct {
	BatchID       string            `json:"batch_id"`
	ConfigVersion int64             `json:"config_version"`
	ReferenceMs   int64             `json:"reference_ms"`
	Mode          string            `json:"mode"`
	PrecheckLimit int               `json:"precheck_limit"`
	Requests      []json.RawMessage `json:"requests"`
}

type FieldError struct {
	Index int    `json:"index"`
	Field string `json:"field"`
	Issue string `json:"issue"`
	Value string `json:"value,omitempty"`
}

type ParseError struct {
	Code   string       `json:"code"`
	Issues []FieldError `json:"issues"`
}

func (e *ParseError) Error() string { return CodeRequestInvalid }

func ParseBatch(data []byte) (*Batch, int, *ParseError) {
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.UseNumber()
	var raw rawBatch
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, 0, &ParseError{Code: CodeRequestInvalid, Issues: []FieldError{{Field: "$", Issue: "invalid JSON: " + err.Error()}}}
	}
	if strings.TrimSpace(raw.BatchID) == "" {
		raw.BatchID = "batch-" + HashBytes(data)[:16]
	}
	if !identifierPattern.MatchString(raw.BatchID) {
		return nil, len(raw.Requests), &ParseError{Code: CodeRequestInvalid, Issues: []FieldError{{Field: "batch_id", Issue: "must be 1-128 chars from A-Za-z0-9_.:-"}}}
	}
	if len(raw.Requests) == 0 {
		return nil, 0, &ParseError{Code: CodeRequestInvalid, Issues: []FieldError{{Field: "requests", Issue: "must not be empty"}}}
	}
	if len(raw.Requests) > 10000 {
		return nil, len(raw.Requests), &ParseError{Code: CodeRequestInvalid, Issues: []FieldError{{Field: "requests", Issue: "batch exceeds 10000 records"}}}
	}

	issues := make([]FieldError, 0)
	reqs := make([]Request, 0, len(raw.Requests))
	var totalWeight int64
	for i, item := range raw.Requests {
		var rr rawRequest
		if err := json.Unmarshal(item, &rr); err != nil {
			issues = append(issues, FieldError{Index: i, Field: "$", Issue: "invalid request JSON: " + err.Error()})
			continue
		}
		req := Request{
			RequestID:       rr.RequestID,
			TimestampMs:     rr.TimestampMs,
			RouteKey:        rr.RouteKey,
			ClientID:        rr.ClientID,
			Nonce:           rr.Nonce,
			SignatureDigest: strings.ToLower(strings.TrimSpace(rr.SignatureDigest)),
		}
		valid := true
		check := func(field string, ok bool, issue, value string) {
			if !ok {
				issues = append(issues, FieldError{Index: i, Field: field, Issue: issue, Value: truncate(value, 120)})
				valid = false
			}
		}
		check("request_id", identifierPattern.MatchString(req.RequestID), "must be 1-128 chars from A-Za-z0-9_.:-", req.RequestID)
		check("timestamp_ms", req.TimestampMs > 0, "must be a positive millisecond timestamp", strconv.FormatInt(req.TimestampMs, 10))
		check("route_key", nonEmptyMax(req.RouteKey, 128), "must be 1-128 characters", req.RouteKey)
		check("client_id", identifierPattern.MatchString(req.ClientID), "must be 1-128 chars from A-Za-z0-9_.:-", req.ClientID)
		check("nonce", NoncePattern.MatchString(req.Nonce), "length must be 16-128 and charset A-Za-z0-9_-", req.Nonce)
		check("signature_digest", digestPattern.MatchString(req.SignatureDigest), "must be 64 lowercase hex characters", rr.SignatureDigest)

		if rr.Weight == "" {
			issues = append(issues, FieldError{Index: i, Field: "weight", Issue: "is required"})
			valid = false
		} else {
			weight, err := strconv.ParseInt(string(rr.Weight), 10, 64)
			if err != nil {
				var numErr *strconv.NumError
				if errors.As(err, &numErr) && errors.Is(numErr.Err, strconv.ErrRange) {
					issues = append(issues, FieldError{Index: i, Field: "weight", Issue: "overflows signed 64-bit integer", Value: string(rr.Weight)})
				} else {
					issues = append(issues, FieldError{Index: i, Field: "weight", Issue: "must be a positive integer", Value: string(rr.Weight)})
				}
				valid = false
			} else if weight <= 0 {
				issues = append(issues, FieldError{Index: i, Field: "weight", Issue: "must be a positive integer", Value: string(rr.Weight)})
				valid = false
			} else {
				req.Weight = weight
				if totalWeight > math.MaxInt64-weight {
					issues = append(issues, FieldError{Index: i, Field: "weight_total", Issue: "batch total weight overflows int64", Value: strconv.FormatInt(totalWeight, 10)})
					valid = false
				} else {
					totalWeight += weight
				}
			}
		}
		if valid {
			reqs = append(reqs, req)
		}
	}
	if len(issues) > 0 {
		return nil, len(raw.Requests), &ParseError{Code: CodeRequestInvalid, Issues: issues}
	}
	return &Batch{
		BatchID:       raw.BatchID,
		ConfigVersion: raw.ConfigVersion,
		ReferenceMs:   raw.ReferenceMs,
		Mode:          raw.Mode,
		Requests:      reqs,
	}, len(reqs), nil
}

func (b *Batch) TotalWeight() int64 {
	var total int64
	for _, req := range b.Requests {
		total += req.Weight
	}
	return total
}

func (r Request) ContentHash() string {
	return HashBytes(MustJSON(r))
}

func (r Request) CanonicalField(field string) (string, error) {
	values := canonical.RequestValues(r.RequestID, r.TimestampMs, r.RouteKey, r.ClientID, r.Nonce, r.Weight)
	value, ok := values[field]
	if !ok {
		return "", fmt.Errorf("unknown signature field %q", field)
	}
	return value, nil
}

func HashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func MustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func nonEmptyMax(s string, max int) bool {
	return strings.TrimSpace(s) != "" && len(s) <= max
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func HTTPStatus(code string) int {
	switch code {
	case CodeOK:
		return 200
	case CodeRequestInvalid, CodeSnapshotInvalid:
		return 400
	case CodeSignatureMismatch, CodeTimestampEarly, CodeTimestampLate, CodeNonceReplayed, CodeConflictingRequest:
		return 422
	case CodeStaleSnapshot, CodeConfigVersionGap, CodeSnapshotDigest:
		return 409
	case CodeNotFound:
		return 404
	case CodeMethodNotAllowed:
		return 405
	case CodePeerRefused:
		return 502
	case CodePeerTimeout, CodePeerLateAck:
		return 504
	case CodeBatchInterrupted, CodeBatchAborted:
		return 409
	default:
		return 500
	}
}

func BoundaryDescription() map[string]string {
	return map[string]string{
		"offset_interval":       "[min_offset_ms, max_offset_ms], both boundaries inclusive",
		"nonce_window_interval": "[first_seen_ms, first_seen_ms + nonce_window_ms); left inclusive, right exclusive",
		"expiry_rule":           "at as_of_ms >= first_seen_ms + nonce_window_ms the occupancy is expired and recyclable",
		"submillisecond_rule":   "all persisted and returned instants/durations are integer milliseconds; sub-millisecond elapsed time is truncated toward zero/floor for non-negative durations before comparison",
		"recycle_basis":         "recycling is calculated from persisted first_seen_ms and one bound snapshot version, never from caller clock alone",
	}
}
