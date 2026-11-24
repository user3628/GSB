package ledger

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
)

func CanonicalBytes(v any) ([]byte, error) {
	return json.Marshal(v)
}

func DigestBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func CanonicalJSONDigest(v any) (string, error) {
	b, err := CanonicalBytes(v)
	if err != nil {
		return "", err
	}
	return DigestBytes(b), nil
}

func normalizedSnapshot(s Snapshot) Snapshot {
	out := s
	out.Digest = ""
	out.Pools = append([]Pool(nil), s.Pools...)
	out.Tenants = append([]Tenant(nil), s.Tenants...)
	sort.Slice(out.Pools, func(i, j int) bool { return out.Pools[i].ID < out.Pools[j].ID })
	sort.Slice(out.Tenants, func(i, j int) bool { return out.Tenants[i].ID < out.Tenants[j].ID })
	if s.RateTiers != nil {
		out.RateTiers = make(map[string]RateTier, len(s.RateTiers))
		for k, v := range s.RateTiers {
			out.RateTiers[k] = v
		}
	}
	return out
}

func SnapshotDigest(s Snapshot) (string, error) {
	return CanonicalJSONDigest(normalizedSnapshot(s))
}

func EventDigest(e Event) (string, error) {
	return CanonicalJSONDigest(e)
}

func StateDigest(s *LedgerState) (string, error) {
	c := *s
	c.StateDigest = ""
	return CanonicalJSONDigest(c)
}

func normalizeDigest(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

func DigestMatches(provided string, calculated string) bool {
	return normalizeDigest(provided) == normalizeDigest(calculated)
}
