package model

import (
	"fmt"

	"nonceguard/internal/canonical"
)

type Snapshot struct {
	Version         int64    `json:"version"`
	GeneratedAtMs   int64    `json:"generated_at_ms"`
	MinOffsetMs     int64    `json:"min_offset_ms"`
	MaxOffsetMs     int64    `json:"max_offset_ms"`
	NonceWindowMs   int64    `json:"nonce_window_ms"`
	SignatureFields []string `json:"signature_fields"`
	Digest          string   `json:"digest"`
}

func DefaultSnapshot(version int64, generatedAtMs int64) Snapshot {
	fields := append([]string(nil), KnownSignatureFields...)
	cfg := Snapshot{
		Version:         version,
		GeneratedAtMs:   generatedAtMs,
		MinOffsetMs:     -300_000,
		MaxOffsetMs:     300_000,
		NonceWindowMs:   600_000,
		SignatureFields: fields,
	}
	cfg.Digest = canonical.ConfigDigest(canonical.ConfigDigestInput{
		Version:         cfg.Version,
		GeneratedAtMs:   cfg.GeneratedAtMs,
		MinOffsetMs:     cfg.MinOffsetMs,
		MaxOffsetMs:     cfg.MaxOffsetMs,
		NonceWindowMs:   cfg.NonceWindowMs,
		SignatureFields: cfg.SignatureFields,
	})
	return cfg
}

func (s Snapshot) DigestInput() canonical.ConfigDigestInput {
	return canonical.ConfigDigestInput{
		Version:         s.Version,
		GeneratedAtMs:   s.GeneratedAtMs,
		MinOffsetMs:     s.MinOffsetMs,
		MaxOffsetMs:     s.MaxOffsetMs,
		NonceWindowMs:   s.NonceWindowMs,
		SignatureFields: s.SignatureFields,
	}
}

func (s Snapshot) VerifyDigest() bool {
	return s.Digest == canonical.ConfigDigest(s.DigestInput())
}

func (s Snapshot) Validate() error {
	if s.Version <= 0 {
		return fmt.Errorf("version must be positive")
	}
	if s.GeneratedAtMs <= 0 {
		return fmt.Errorf("generated_at_ms must be positive")
	}
	if s.MinOffsetMs > s.MaxOffsetMs {
		return fmt.Errorf("min_offset_ms must be <= max_offset_ms")
	}
	if s.NonceWindowMs <= 0 {
		return fmt.Errorf("nonce_window_ms must be positive")
	}
	if s.NonceWindowMs > 365*24*60*60*1000 {
		return fmt.Errorf("nonce_window_ms exceeds one year")
	}
	if len(s.SignatureFields) == 0 {
		return fmt.Errorf("signature_fields must not be empty")
	}
	seen := map[string]bool{}
	for _, field := range s.SignatureFields {
		known := false
		for _, candidate := range KnownSignatureFields {
			if field == candidate {
				known = true
				break
			}
		}
		if !known {
			return fmt.Errorf("unknown signature field %q", field)
		}
		if seen[field] {
			return fmt.Errorf("duplicate signature field %q", field)
		}
		seen[field] = true
	}
	if !s.VerifyDigest() {
		return fmt.Errorf("snapshot digest does not match canonical content")
	}
	return nil
}
