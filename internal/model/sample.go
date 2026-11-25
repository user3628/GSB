package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

type SampleInput struct {
	ID          string  `json:"id"`
	TimestampMS int64   `json:"timestamp_ms"`
	Service     string  `json:"service"`
	Frames      []Frame `json:"frames"`
	Weight      int64   `json:"weight"`
	Channel     string  `json:"channel"`
}

type SampleChunk struct {
	BatchID       string        `json:"batch_id"`
	PolicyVersion *uint64       `json:"policy_version,omitempty"`
	Samples       []SampleInput `json:"samples"`
}

type ValidationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
	Value   any    `json:"value,omitempty"`
}

func (e *ValidationError) Error() string {
	if e.Field == "" {
		return e.Message
	}
	return e.Field + ": " + e.Message
}

func validateSample(s *SampleInput, index int) error {
	s.ID = strings.TrimSpace(s.ID)
	s.Service = strings.TrimSpace(s.Service)
	s.Channel = strings.TrimSpace(s.Channel)
	if s.ID == "" {
		return &ValidationError{Field: fmt.Sprintf("samples[%d].id", index), Message: "sample id must not be empty"}
	}
	if s.TimestampMS < 0 {
		return &ValidationError{Field: fmt.Sprintf("samples[%d].timestamp_ms", index), Message: "timestamp must not be negative", Value: s.TimestampMS}
	}
	if s.Service == "" {
		return &ValidationError{Field: fmt.Sprintf("samples[%d].service", index), Message: "service must not be empty"}
	}
	if s.Channel == "" {
		return &ValidationError{Field: fmt.Sprintf("samples[%d].channel", index), Message: "channel must not be empty"}
	}
	if s.Weight <= 0 {
		return &ValidationError{Field: fmt.Sprintf("samples[%d].weight", index), Message: "sample weight must be a positive integer", Value: s.Weight}
	}
	if len(s.Frames) == 0 {
		return &ValidationError{Field: fmt.Sprintf("samples[%d].frames", index), Message: "stack frames must not be empty"}
	}
	for j := range s.Frames {
		if err := s.Frames[j].Normalize(); err != nil {
			return &ValidationError{Field: fmt.Sprintf("samples[%d].frames[%d]", index, j), Message: err.Error()}
		}
	}
	return nil
}

// Normalize validates a chunk without touching persistent state. Validation of
// all samples happens before an intent is even written, which gives the batch
// all-or-nothing behavior required by the API contract.
func (c *SampleChunk) Normalize() error {
	c.BatchID = strings.TrimSpace(c.BatchID)
	if c.BatchID == "" {
		return &ValidationError{Field: "batch_id", Message: "batch id must not be empty"}
	}
	if len(c.Samples) == 0 {
		return &ValidationError{Field: "samples", Message: "sample chunk must contain at least one sample"}
	}
	for i := range c.Samples {
		if err := validateSample(&c.Samples[i], i); err != nil {
			return err
		}
	}
	return nil
}

// ContentHash deliberately excludes the delivery channel. A profiler sample can
// arrive over more than one channel; channel is transport metadata, while the
// identity-bearing payload is timestamp+service+stack+weight. A repeated id with
// the same hash is a duplicate observation, and a different hash is a conflict.
func (s SampleInput) ContentHash() string {
	payload := struct {
		TimestampMS int64   `json:"timestamp_ms"`
		Service     string  `json:"service"`
		Frames      []Frame `json:"frames"`
		Weight      int64   `json:"weight"`
	}{
		TimestampMS: s.TimestampMS,
		Service:     s.Service,
		Frames:      s.Frames,
		Weight:      s.Weight,
	}
	data, _ := json.Marshal(payload)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func CanonicalChunkHash(c SampleChunk) string {
	data, _ := json.Marshal(c)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
