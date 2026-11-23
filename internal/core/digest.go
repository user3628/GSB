package core

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

func FloatString(v float64) string {
	if v == 0 {
		return "0"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func CanonicalConfig(config map[string]float64) string {
	keys := make([]string, 0, len(config))
	for k := range config {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.Quote(k))
		b.WriteByte(':')
		b.WriteString(FloatString(config[k]))
	}
	b.WriteByte('}')
	return b.String()
}

func SnapshotDigest(s Snapshot) string {
	raw := fmt.Sprintf("snapshot.v1\n%d\n%s\n%s", s.Version, s.GeneratedAt, CanonicalConfig(s.Config))
	sum := sha256.Sum256([]byte(raw))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func ValidateSnapshot(s Snapshot) error {
	if s.Version <= 0 {
		return NewBizError("E_SNAPSHOT_INVALID", "version must be positive", map[string]any{"version": s.Version})
	}
	if _, err := time.Parse(time.RFC3339Nano, s.GeneratedAt); err != nil {
		return NewBizError("E_SNAPSHOT_INVALID", "generated_at must be RFC3339", map[string]any{"generated_at": s.GeneratedAt})
	}
	if len(s.Config) == 0 {
		return NewBizError("E_SNAPSHOT_INVALID", "config must not be empty", nil)
	}
	for key, value := range s.Config {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return NewBizError("E_SNAPSHOT_INVALID", "config values must be finite", map[string]any{"key": key, "value": value})
		}
	}
	want := SnapshotDigest(s)
	if s.Digest == "" || !strings.EqualFold(s.Digest, want) {
		return NewBizError("E_SNAPSHOT_INVALID", "snapshot digest mismatch", map[string]any{
			"expected": want, "actual": s.Digest,
		})
	}
	return nil
}

func PushContentDigest(clientID string, version int64, snapshotDigest string, config map[string]float64) string {
	raw := fmt.Sprintf("push.v1\n%s\n%d\n%s\n%s", clientID, version, snapshotDigest, CanonicalConfig(config))
	sum := sha256.Sum256([]byte(raw))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func ResultDigest(r Result) string {
	decision := "false"
	if r.Decision {
		decision = "true"
	}
	raw := fmt.Sprintf("result.v1\n%d\n%d\n%s\n%s\n%s\n%s", r.PickedVersion, r.Seed, r.SampleDigest,
		FloatString(r.WeightedSum), FloatString(r.Normalized), decision)
	sum := sha256.Sum256([]byte(raw))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func AckDigest(ack Ack) string {
	raw := fmt.Sprintf("ack.v1\n%s\n%s\n%s\n%d\n%s\n%s", ack.AckID, ack.PushID, ack.ClientID, ack.Version, ack.ContentDigest, ack.ResultDigest)
	sum := sha256.Sum256([]byte(raw))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func NewID(prefix string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%d|%d", prefix, time.Now().UnixNano(), randomCounter())))
	return prefix + "-" + hex.EncodeToString(sum[:12])
}

var counter atomic.Uint64

func randomCounter() uint64 {
	return counter.Add(1)
}

func ValidateKeys(keys []string) error {
	if len(keys) == 0 {
		return NewBizError("E_BAD_REQUEST", "keys must not be empty", nil)
	}
	seen := map[string]bool{}
	for _, key := range keys {
		if strings.TrimSpace(key) == "" || seen[key] {
			return errors.New("keys must be non-empty and unique")
		}
		seen[key] = true
	}
	return nil
}
