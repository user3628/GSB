package freshness

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	MinNonceLength = 16
	MaxNonceLength = 64
)

type ValidationPolicy struct {
	MaxToleranceMS         int64
	MaxWindowMS            int64
	MaxRequestsPerBatch    int
	AllowReferenceOverride bool
}

// ValidateBatch validates the complete batch before any intent or decision is
// created. If any issue is found, no request in the batch is adjudicated.
func ValidateBatch(in BatchInput, p ValidationPolicy, gatewayNowMS int64) (*ValidatedBatch, *AppError) {
	issues := make([]Issue, 0)
	add := func(field, code, message string) {
		issues = append(issues, Issue{Field: field, Code: code, Message: message})
	}

	if in.SchemaVersion == 0 {
		in.SchemaVersion = 1
	}
	if in.SchemaVersion != 1 {
		add("schema_version", "UNSUPPORTED_SCHEMA", "only schema_version 1 is supported")
	}
	if !validToken(in.BatchID, 1, 128, "._:-") {
		add("batch_id", "INVALID_BATCH_ID", "batch_id must be 1..128 chars from A-Z a-z 0-9 . _ : -")
	}
	mode := strings.ToLower(strings.TrimSpace(in.Mode))
	if mode == "" {
		mode = ModeFull
	}
	if mode != ModeFull && mode != ModeFast {
		add("mode", "INVALID_MODE", "mode must be full or fast")
	}
	if gatewayNowMS < 0 {
		add("gateway_reference_time_ms", "INVALID_REFERENCE_TIME", "gateway reference time must be non-negative")
	}
	reference := gatewayNowMS
	referenceSource := "gateway_clock"
	if in.GatewayReferenceTimeMS != nil {
		if !p.AllowReferenceOverride {
			add("gateway_reference_time_ms", "REFERENCE_OVERRIDE_DISABLED", "deterministic reference override is disabled by server policy")
		} else if *in.GatewayReferenceTimeMS < 0 {
			add("gateway_reference_time_ms", "INVALID_REFERENCE_TIME", "gateway reference time must be non-negative")
		} else {
			reference = *in.GatewayReferenceTimeMS
			referenceSource = "deterministic_override"
		}
	}
	if len(in.Requests) == 0 {
		add("requests", "EMPTY_BATCH", "at least one request is required")
	}
	if p.MaxRequestsPerBatch > 0 && len(in.Requests) > p.MaxRequestsPerBatch {
		add("requests", "BATCH_TOO_LARGE", fmt.Sprintf("at most %d requests are allowed", p.MaxRequestsPerBatch))
	}

	seenIDs := make(map[string]int, len(in.Requests))
	requests := make([]Request, 0, len(in.Requests))
	var totalWeight int64
	for i, raw := range in.Requests {
		prefix := "requests[" + strconv.Itoa(i) + "]."
		requestID := strings.TrimSpace(raw.RequestID)
		if requestID == "" {
			requestID = fmt.Sprintf("req-%04d", i+1)
		}
		if !validToken(requestID, 1, 128, "._:-") {
			add(prefix+"request_id", "INVALID_REQUEST_ID", "request_id must be 1..128 chars from A-Z a-z 0-9 . _ : -")
		}
		if first, exists := seenIDs[requestID]; exists {
			add(prefix+"request_id", "DUPLICATE_REQUEST_ID", fmt.Sprintf("request_id duplicates requests[%d]", first))
		} else {
			seenIDs[requestID] = i
		}
		if raw.TimestampMS < 0 {
			add(prefix+"timestamp_ms", "INVALID_TIMESTAMP", "timestamp_ms must be non-negative")
		}
		if !validToken(raw.RouteKey, 1, 256, "._:/-") {
			add(prefix+"route_key", "INVALID_ROUTE_KEY", "route_key must be 1..256 chars from A-Z a-z 0-9 . _ : / -")
		}
		if !validToken(raw.ClientID, 1, 128, "._:-") {
			add(prefix+"client_id", "INVALID_CLIENT_ID", "client_id must be 1..128 chars from A-Z a-z 0-9 . _ : -")
		}
		if !validNonce(raw.Nonce) {
			add(prefix+"nonce", "INVALID_NONCE", fmt.Sprintf("nonce must be %d..%d chars from A-Z a-z 0-9 _ -", MinNonceLength, MaxNonceLength))
		}
		if len(raw.Signature) > 512 {
			add(prefix+"signature", "INVALID_SIGNATURE_LENGTH", "signature is too long")
		}
		if raw.Weight <= 0 {
			add(prefix+"weight", "INVALID_WEIGHT", "weight must be a positive integer")
		} else if raw.Weight > math.MaxInt64-totalWeight {
			add(prefix+"weight", "WEIGHT_OVERFLOW", "batch total_weight exceeds int64")
		} else {
			totalWeight += raw.Weight
		}
		if raw.ToleranceMS < 0 {
			add(prefix+"tolerance_ms", "INVALID_TOLERANCE", "tolerance_ms must be non-negative")
		} else if p.MaxToleranceMS > 0 && raw.ToleranceMS > p.MaxToleranceMS {
			add(prefix+"tolerance_ms", "TOLERANCE_TOO_LARGE", fmt.Sprintf("tolerance_ms exceeds server maximum %d", p.MaxToleranceMS))
		}
		if raw.WindowMS <= 0 {
			add(prefix+"window_ms", "INVALID_WINDOW", "window_ms must be positive")
		} else if p.MaxWindowMS > 0 && raw.WindowMS > p.MaxWindowMS {
			add(prefix+"window_ms", "WINDOW_TOO_LARGE", fmt.Sprintf("window_ms exceeds server maximum %d", p.MaxWindowMS))
		}
		requests = append(requests, Request{
			RequestID:   requestID,
			TimestampMS: raw.TimestampMS,
			RouteKey:    raw.RouteKey,
			ClientID:    raw.ClientID,
			Nonce:       raw.Nonce,
			Signature:   strings.ToLower(strings.TrimSpace(raw.Signature)),
			Weight:      raw.Weight,
			ToleranceMS: raw.ToleranceMS,
			WindowMS:    raw.WindowMS,
		})
	}
	if len(issues) != 0 {
		return nil, NewAppError(CodeRequestInvalid, "batch validation failed; no request was adjudicated", map[string]any{
			"issues": issues,
			"atomic": true,
		})
	}

	validated := &ValidatedBatch{
		BatchID:         in.BatchID,
		Mode:            mode,
		ReferenceTimeMS: reference,
		ReferenceSource: referenceSource,
		Requests:        requests,
		TotalWeight:     totalWeight,
	}
	validated.ContentHash = ContentHash(*validated)
	return validated, nil
}

func validNonce(s string) bool {
	if len(s) < MinNonceLength || len(s) > MaxNonceLength {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-' {
			continue
		}
		return false
	}
	return true
}

func validToken(s string, min, max int, extra string) bool {
	if len(s) < min || len(s) > max {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || strings.IndexByte(extra, c) >= 0 {
			continue
		}
		return false
	}
	return true
}
