package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
)

// canonicalJSON renders a value with map keys sorted so digest computation is
// deterministic across runs and map iteration orders.
func canonicalJSON(v any) ([]byte, error) {
	return json.Marshal(sortRecursive(v))
}

func sortRecursive(v any) any {
	switch t := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([][2]any, 0, len(keys))
		for _, k := range keys {
			out = append(out, [2]any{k, sortRecursive(t[k])})
		}
		return out
	case []any:
		cp := make([]any, len(t))
		for i := range t {
			cp[i] = sortRecursive(t[i])
		}
		return cp
	default:
		return v
	}
}

// DigestPolicy computes the sha256 digest over a snapshot's version and
// canonicalized parameters, chained onto the previous snapshot digest.
func DigestPolicy(version int64, params PolicyParams, prevDigest string) (string, error) {
	body, err := canonicalJSON(map[string]any{
		"version":     version,
		"prev_digest": prevDigest,
		"params":      params,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

// DigestFragment computes the digest of an incremental fragment relative to its
// base snapshot digest. Only the fragment's affected fields participate.
func DigestFragment(version int64, baseDigest string, fields map[string]any) (string, error) {
	body, err := canonicalJSON(map[string]any{
		"version":     version,
		"base_digest": baseDigest,
		"fields":      fields,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}
