package edgeconfig

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
)

func canonicalJSON(value any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

func sha256Digest(prefix string, value any) (string, string, error) {
	raw, err := canonicalJSON(value)
	if err != nil {
		return "", "", err
	}
	sum := sha256.Sum256(raw)
	return prefix + hex.EncodeToString(sum[:]), string(raw), nil
}

type canonicalKey struct {
	Type    string `json:"type"`
	Value   any    `json:"value"`
	OrderNo int    `json:"order_no"`
}

type canonicalSnapshot struct {
	Version       int64                   `json:"version"`
	GeneratedAtMS int64                   `json:"generated_at_ms"`
	Keys          map[string]canonicalKey `json:"keys"`
}

func SnapshotDigest(snapshot Snapshot) (string, error) {
	keys := make(map[string]canonicalKey, len(snapshot.Keys))
	for _, key := range snapshot.Keys {
		keys[key.Path] = canonicalKey{Type: key.Type, Value: key.Value, OrderNo: key.OrderNo}
	}
	digest, _, err := sha256Digest("sha256:", canonicalSnapshot{
		Version:       snapshot.Version,
		GeneratedAtMS: snapshot.GeneratedAtMS,
		Keys:          keys,
	})
	return digest, err
}

type expectedDigestDocument struct {
	Keys         map[string]canonicalKey `json:"keys"`
	Dependencies map[string]string       `json:"dependencies,omitempty"`
}

func ExpectedDigest(keys []ConfigKey, dependencies map[string]string) (digest string, canonical string, err error) {
	keyMap := make(map[string]canonicalKey, len(keys))
	for _, key := range keys {
		keyMap[key.Path] = canonicalKey{Type: key.Type, Value: key.Value, OrderNo: key.OrderNo}
	}
	deps := make(map[string]string, len(dependencies))
	for id, depDigest := range dependencies {
		deps[id] = depDigest
	}
	return sha256Digest("sha256:", expectedDigestDocument{Keys: keyMap, Dependencies: deps})
}

func RequestHash(payload any) (string, error) {
	raw, err := canonicalJSON(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func SortedStrings(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}
