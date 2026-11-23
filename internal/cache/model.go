package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"time"
)

type State string

const (
	StatePending   State = "PENDING"
	StateWriting   State = "WRITING"
	StateAvailable State = "AVAILABLE"
	StateStale     State = "STALE"
	StateEvicted   State = "EVICTED"
)

var sha256DigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// Entry is one immutable cache catalog record as observed at a catalog version.
type Entry struct {
	EntryID          string    `json:"entry_id"`
	CacheKey         string    `json:"cache_key"`
	StageID          string    `json:"stage_id"`
	InputFingerprint string    `json:"input_fingerprint"`
	ToolchainVersion string    `json:"toolchain_version"`
	ArtifactDigest   string    `json:"artifact_digest"`
	ArtifactSize     int64     `json:"artifact_size"`
	Hits             int64     `json:"hits"`
	RetainUntil      time.Time `json:"retain_until"`
	CreatedAt        time.Time `json:"created_at"`
	LastHitAt        time.Time `json:"last_hit_at"`
	State            State     `json:"state"`
	Version          int64     `json:"version"`
}

type Artifact struct {
	Digest       string `json:"digest"`
	Size         int64  `json:"size"`
	RelativePath string `json:"relative_path"`
	RefCount     int64  `json:"ref_count"`
}

// EntryInput is validated and normalized before it reaches persistence.
type EntryInput struct {
	StageID          string
	InputFingerprint string
	ToolchainVersion string
	ArtifactContent  []byte
	ArtifactDigest   string
	ArtifactSize     int64
	RetainUntil      time.Time
}

func (i EntryInput) Key() Key {
	return Key{StageID: i.StageID, InputFingerprint: i.InputFingerprint, ToolchainVersion: i.ToolchainVersion}
}

type Key struct {
	StageID          string `json:"stage_id"`
	InputFingerprint string `json:"input_fingerprint"`
	ToolchainVersion string `json:"toolchain_version"`
}

func NormalizeKey(key Key) (Key, string, error) {
	stage := strings.TrimSpace(key.StageID)
	fingerprint := normalizeFingerprint(key.InputFingerprint)
	toolchain := strings.TrimSpace(key.ToolchainVersion)
	context := map[string]any{
		"stage_id":          key.StageID,
		"input_fingerprint": key.InputFingerprint,
		"toolchain_version": key.ToolchainVersion,
	}
	if stage == "" {
		return Key{}, "", newError(CodeEntryInvalid, "stage_id must not be empty", context)
	}
	if fingerprint == "" {
		return Key{}, "", newError(CodeEntryInvalid, "input_fingerprint must not be empty", context)
	}
	if toolchain == "" {
		return Key{}, "", newError(CodeEntryInvalid, "toolchain_version must not be empty", context)
	}
	normalized := Key{StageID: stage, InputFingerprint: fingerprint, ToolchainVersion: toolchain}
	payload := fmt.Sprintf("ci-cache-key-v1\x00%d:%s\x00%d:%s\x00%d:%s",
		len(normalized.StageID), normalized.StageID,
		len(normalized.InputFingerprint), normalized.InputFingerprint,
		len(normalized.ToolchainVersion), normalized.ToolchainVersion)
	sum := sha256.Sum256([]byte(payload))
	return normalized, "sha256:" + hex.EncodeToString(sum[:]), nil
}

func normalizeFingerprint(value string) string {
	trimmed := strings.TrimSpace(value)
	lower := strings.ToLower(trimmed)
	if sha256DigestPattern.MatchString(lower) {
		return lower
	}
	return trimmed
}

func NormalizeAndValidate(input EntryInput) (EntryInput, Key, string, error) {
	key, cacheKey, err := NormalizeKey(Key{
		StageID:          input.StageID,
		InputFingerprint: input.InputFingerprint,
		ToolchainVersion: input.ToolchainVersion,
	})
	if err != nil {
		return EntryInput{}, Key{}, "", err
	}
	if !sha256DigestPattern.MatchString(input.ArtifactDigest) {
		return EntryInput{}, Key{}, "", newError(CodeEntryInvalid, "artifact_digest must be sha256:<64 lowercase hex>", map[string]any{
			"artifact_digest": input.ArtifactDigest,
		})
	}
	if input.ArtifactSize < 0 {
		return EntryInput{}, Key{}, "", newError(CodeEntryInvalid, "artifact_size must be non-negative", map[string]any{
			"artifact_size": input.ArtifactSize,
		})
	}
	if int64(len(input.ArtifactContent)) != input.ArtifactSize {
		return EntryInput{}, Key{}, "", newError(CodeEntryInvalid, "artifact_size does not match artifact content", map[string]any{
			"artifact_size":        input.ArtifactSize,
			"actual_content_bytes": len(input.ArtifactContent),
		})
	}
	actual := DigestBytes(input.ArtifactContent)
	if actual != input.ArtifactDigest {
		return EntryInput{}, Key{}, "", newError(CodeEntryInvalid, "artifact_digest does not match artifact content", map[string]any{
			"artifact_digest": input.ArtifactDigest,
			"actual_digest":   actual,
		})
	}
	if input.RetainUntil.IsZero() {
		return EntryInput{}, Key{}, "", newError(CodeEntryInvalid, "retain_until is required", nil)
	}
	input.StageID = key.StageID
	input.InputFingerprint = key.InputFingerprint
	input.ToolchainVersion = key.ToolchainVersion
	input.ArtifactContent = append([]byte(nil), input.ArtifactContent...)
	input.ArtifactDigest = strings.ToLower(input.ArtifactDigest)
	input.RetainUntil = input.RetainUntil.UTC()
	return input, key, cacheKey, nil
}

func DigestBytes(content []byte) string {
	sum := sha256.Sum256(content)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func DigestPath(digest string) (string, error) {
	if !sha256DigestPattern.MatchString(digest) {
		return "", newError(CodeEntryInvalid, "invalid sha256 digest", map[string]any{"digest": digest})
	}
	return digest[len("sha256:"):] + ".bin", nil
}

func (e Entry) Key() Key {
	return Key{StageID: e.StageID, InputFingerprint: e.InputFingerprint, ToolchainVersion: e.ToolchainVersion}
}

func (e Entry) Expired(now time.Time) bool { return !now.Before(e.RetainUntil) }
