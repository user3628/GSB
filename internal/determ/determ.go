// Package determ contains the deterministic primitives used across the whole
// pipeline: a fixed-seed PRNG, canonical JSON serialization, content
// fingerprints and an advisory file lock. Nothing in here may depend on map
// iteration order, goroutine scheduling or wall-clock time.
package determ

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
)

// Rand is a splitmix64 pseudo random generator. It is fully specified by its
// 64-bit state and therefore produces the identical byte stream on every
// platform and Go version, unlike math/rand whose default source has changed
// across releases.
type Rand struct {
	state uint64
}

// NewRand seeds a generator with a fixed constant.
func NewRand(seed uint64) *Rand { return &Rand{state: seed} }

// Uint64 returns the next pseudo random uint64.
func (r *Rand) Uint64() uint64 {
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// Intn returns a deterministic value in [0,n). It panics for n<=0, matching
// the math/rand convention and exposing programming errors immediately.
func (r *Rand) Intn(n int) int {
	if n <= 0 {
		panic("determ: invalid Intn argument")
	}
	return int(r.Uint64() % uint64(n))
}

// Float64 returns a deterministic value in [0,1).
func (r *Rand) Float64() float64 {
	return float64(r.Uint64()>>11) / float64(uint64(1)<<53)
}

// canonicalType is implemented by encoders that emit keys in a fixed order.
// The canonical encoder below sorts object keys recursively, which is enough
// for the plain structs/maps used by this project.

// MarshalJSON encodes v as canonical JSON: object keys are sorted
// lexicographically and no insignificant whitespace is emitted. The output is
// stable across processes because encoding/json sorts map keys itself; struct
// key order is fixed by the source declarations.
func MarshalJSON(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return canonicalize(raw)
}

// canonicalize re-encodes arbitrary JSON with sorted object keys. Round
// tripping through interface{} preserves numbers for our data because all
// numeric values fit in float64 without precision loss where equality matters
// (fingerprints compare the canonical bytes, never float formatting quirks).
func canonicalize(raw []byte) ([]byte, error) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return json.Marshal(sortValue(v))
}

func sortValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sortStrings(keys)
		out := make(map[string]any, len(t))
		for _, k := range keys {
			out[k] = sortValue(t[k])
		}
		return out
	case []any:
		for i := range t {
			t[i] = sortValue(t[i])
		}
		return t
	default:
		return v
	}
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}

// Fingerprint returns the lowercase hex SHA-256 of the canonical encoding of v.
// It is the single content-identity mechanism used for stage inputs, configs
// and produced artifacts.
func Fingerprint(v any) (string, error) {
	b, err := MarshalJSON(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// FingerprintBytes fingerprints an already canonical byte slice.
func FingerprintBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// ErrLocked indicates TryLock could not acquire the lock immediately. It is
// defined on every platform and returned by both flock(2) and LockFileEx
// implementations.
var ErrLocked = errors.New("determ: file lock already held by another process")
