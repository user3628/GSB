package model

import (
	"errors"
	"fmt"
)

// Code is a stable business code emitted by the workbench. Codes are part of
// the report contract and must not be translated.
type Code string

const (
	CodeOK              Code = "OK"
	CodePolicyInvalid   Code = "E_POLICY_INVALID"
	CodeRequestInvalid  Code = "E_REQUEST_INVALID"
	CodeNoTunnel        Code = "E_NO_TUNNEL"
	CodeFixedOverflow   Code = "E_FIXED_OVERFLOW"
	CodeResultMismatch  Code = "E_RESULT_MISMATCH"
	CodeSnapshotVersion Code = "E_SNAPSHOT_VERSION"
	CodeRequestBudget   Code = "E_REQUEST_BUDGET"
	CodeSessionConflict Code = "E_SESSION_CONFLICT"
	CodePolicyReplaced  Code = "E_POLICY_REPLACED"
	CodeInternal        Code = "E_INTERNAL"
	CorrectionOrder     Code = "C_ORDER_NORMALIZED"
	CorrectionDecision  Code = "C_DECISION_RECONCILED"
)

// CodedError carries both a stable machine-readable code and enough location
// information to identify the exact input or request that caused it.
type CodedError struct {
	Code       Code   `json:"code"`
	Message    string `json:"message"`
	SessionID  string `json:"session_id,omitempty"`
	StreamID   string `json:"stream_id,omitempty"`
	RequestSeq int64  `json:"request_seq,omitempty"`
	Offset     int64  `json:"offset"`
	Expected   string `json:"expected,omitempty"`
	Actual     string `json:"actual,omitempty"`
}

func (e *CodedError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message == "" {
		return string(e.Code)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func NewError(code Code, message string, offset int64) *CodedError {
	return &CodedError{Code: code, Message: message, Offset: offset}
}

func (e *CodedError) Locate(sessionID, streamID string, seq int64) *CodedError {
	e.SessionID = sessionID
	e.StreamID = streamID
	e.RequestSeq = seq
	return e
}

func CodeOf(err error) Code {
	var coded *CodedError
	if errors.As(err, &coded) {
		return coded.Code
	}
	if err != nil {
		return CodeInternal
	}
	return CodeOK
}

// Policy is one normalized tunnel policy row. InputOffset is retained in
// memory for diagnostics, but deliberately excluded from canonical digests.
type Policy struct {
	TunnelID        string `json:"tunnel_id"`
	DestinationCIDR string `json:"destination_cidr"`
	Priority        int64  `json:"priority"`
	Bandwidth       int64  `json:"bandwidth"`
	ForwardingCost  int64  `json:"forwarding_cost"`
	InputOffset     int64  `json:"-"`
}

type PolicySet struct {
	Version  int64    `json:"version"`
	Policies []Policy `json:"policies"`
	Digest   string   `json:"digest"`
}

type Request struct {
	StreamID       string `json:"stream_id"`
	RequestSeq     int64  `json:"request_seq"`
	Source         string `json:"source"`
	Destination    string `json:"destination"`
	PacketBytes    int64  `json:"packet_bytes"`
	InputOffset    int64  `json:"input_offset"`
	ArrivalOrdinal int64  `json:"arrival_ordinal"`
}

type Allocation struct {
	TunnelID     string `json:"tunnel_id"`
	WeightMicros int64  `json:"weight_micros"`
}

type Decision struct {
	Code            Code         `json:"code"`
	PrimaryTunnelID string       `json:"primary_tunnel_id,omitempty"`
	Allocations     []Allocation `json:"allocations,omitempty"`
	ForwardingCost  int64        `json:"forwarding_cost"`
}

type LedgerEntry struct {
	Sequence             int64  `json:"sequence"`
	SessionID            string `json:"session_id"`
	StreamID             string `json:"stream_id"`
	RequestSeq           int64  `json:"request_seq"`
	TunnelID             string `json:"tunnel_id"`
	ExactNumerator       int64  `json:"exact_numerator"`
	Denominator          int64  `json:"denominator"`
	InitialMicros        int64  `json:"initial_micros"`
	RemainderNumerator   int64  `json:"remainder_numerator"`
	CompensationMicros   int64  `json:"compensation_micros"`
	TruncationUpperBound int64  `json:"truncation_upper_bound_micros"`
}

type LedgerStreamSummary struct {
	StreamID              string `json:"stream_id"`
	AllocationCount       int    `json:"allocation_count"`
	TruncationCount       int    `json:"truncation_count"`
	CompensationMicros    int64  `json:"compensation_micros"`
	ErrorUpperBoundMicros int64  `json:"error_upper_bound_micros"`
}

type LatencySample struct {
	TunnelID   string `json:"tunnel_id"`
	ResultCode Code   `json:"result_code"`
	DurationNS int64  `json:"duration_ns"`
}

type LatencyStat struct {
	TunnelID    string `json:"tunnel_id"`
	ResultCode  Code   `json:"result_code"`
	SampleCount int64  `json:"sample_count"`
	AverageNS   int64  `json:"average_ns"`
	P95NS       int64  `json:"p95_ns"`
	MaxNS       int64  `json:"max_ns"`
}

type Correction struct {
	SessionID   string   `json:"session_id"`
	StreamID    string   `json:"stream_id"`
	RequestSeq  int64    `json:"request_seq"`
	Code        Code     `json:"code"`
	Reason      string   `json:"reason"`
	Before      Decision `json:"before"`
	After       Decision `json:"after"`
	BeforeOrder int64    `json:"before_arrival_ordinal"`
	AfterOrder  int64    `json:"after_settlement_ordinal"`
}

type CodeOccurrence struct {
	Code       Code   `json:"code"`
	SessionID  string `json:"session_id,omitempty"`
	StreamID   string `json:"stream_id,omitempty"`
	RequestSeq int64  `json:"request_seq,omitempty"`
	Offset     int64  `json:"offset"`
	Message    string `json:"message,omitempty"`
}
