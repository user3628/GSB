// Package snapshot persists versioned, content-addressed forensic snapshots in
// a local pure-Go SQLite database. Each round of parse+correlation produces a
// replacement snapshot with an increasing version number and the set of format
// versions it depends on.
package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"forensicworkbench/internal/codes"
	"forensicworkbench/internal/correlate"
)

// Snapshot is one immutable, replacement-style forensic snapshot.
type Snapshot struct {
	Version        int64               `json:"version"`
	ContentHash    string              `json:"content_hash"`
	FormatVersions []string            `json:"format_versions"`
	SourcePaths    []string            `json:"source_paths"`
	EventCount     int                 `json:"event_count"`
	Graph          *correlate.Graph    `json:"graph"`
	Errors         []codes.ErrorRecord `json:"errors"`
}

// IncompatError is returned whenever a snapshot cannot be loaded as a whole.
type IncompatError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *IncompatError) Error() string { return e.Code + ": " + e.Message }

func newIncompat(msg string) *IncompatError {
	return &IncompatError{Code: codes.ESnapshotVersionIncompat, Message: msg}
}

// canonicalPayload serializes the version-independent snapshot body. JSON maps
// are emitted with sorted keys via encoding/json, and every slice is pre-sorted
// by callers, so equal content yields byte-identical payloads.
func canonicalPayload(s *Snapshot) ([]byte, string, error) {
	body := struct {
		FormatVersions []string            `json:"format_versions"`
		SourcePaths    []string            `json:"source_paths"`
		EventCount     int                 `json:"event_count"`
		Graph          *correlate.Graph    `json:"graph"`
		Errors         []codes.ErrorRecord `json:"errors"`
	}{
		FormatVersions: s.FormatVersions,
		SourcePaths:    s.SourcePaths,
		EventCount:     s.EventCount,
		Graph:          s.Graph,
		Errors:         s.Errors,
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(raw)
	return raw, hex.EncodeToString(sum[:]), nil
}

// HashContent exposes the deterministic content hash used to distinguish
// replacement snapshots that carry the same version number.
func HashContent(s *Snapshot) (string, error) {
	_, h, err := canonicalPayload(s)
	return h, err
}
