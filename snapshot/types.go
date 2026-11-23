// Package snapshot owns the versioned SQLite snapshot store.
package snapshot

import (
	"sandboxpolicy/business"
	"sandboxpolicy/policy"
)

// Snapshot is a complete, immutable policy version.
type Snapshot struct {
	Namespace       string         `json:"namespace"`
	Version         int            `json:"version"`
	ParentVersion   int            `json:"parent_version"`
	Digest          string         `json:"digest"`
	Policy          *policy.Policy `json:"policy"`
	Closure         policy.Closure `json:"closure"`
	CreatedAt       string         `json:"-"`
	policyJSON      []byte
	diagnosticsJSON []byte
}

// ChainEntry is the lightweight, deterministic version-chain view.
type ChainEntry struct {
	Version       int    `json:"version"`
	ParentVersion int    `json:"parent_version"`
	Digest        string `json:"digest"`
}

// ChainWarning describes why loading stopped at the contiguous prefix.
type ChainWarning struct {
	Code            business.Code `json:"code"`
	Message         string        `json:"message"`
	ExpectedVersion int           `json:"expected_version"`
	InvalidVersion  int           `json:"invalid_version,omitempty"`
	FallbackVersion int           `json:"fallback_version,omitempty"`
}

// Chain is the validated contiguous history. Snapshots is intentionally not
// serialized because report output uses the compact Entries view.
type Chain struct {
	Namespace string        `json:"namespace"`
	Entries   []ChainEntry  `json:"entries"`
	Warning   *ChainWarning `json:"warning,omitempty"`
	Snapshots []Snapshot    `json:"-"`
}

// CurrentSnapshot returns the last contiguous snapshot.
func (c *Chain) CurrentSnapshot() (*Snapshot, bool) {
	if c == nil || len(c.Snapshots) == 0 {
		return nil, false
	}
	snapshot := c.Snapshots[len(c.Snapshots)-1]
	return &snapshot, true
}

// Event records queue/conflict/application activity without affecting policy
// versions.
type Event struct {
	ID        int64  `json:"id"`
	Namespace string `json:"namespace"`
	Version   int    `json:"version"`
	Operation string `json:"operation"`
	EventType string `json:"event_type"`
	Code      string `json:"code,omitempty"`
	Detail    string `json:"detail,omitempty"`
	CreatedAt string `json:"created_at,omitempty"`
}
