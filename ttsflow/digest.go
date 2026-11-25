package ttsflow

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"math"
	"sort"
)

func digestBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func digestString(s string) string { return digestBytes([]byte(s)) }

func digestJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("canonical json marshal: %v", err))
	}
	return digestBytes(b)
}

func digestParts(parts ...any) string {
	h := sha256.New()
	for _, p := range parts {
		b, err := json.Marshal(p)
		if err != nil {
			panic(fmt.Sprintf("canonical part marshal: %v", err))
		}
		writeField(h, b)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func writeField(h hash.Hash, b []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(b)))
	_, _ = h.Write(size[:])
	_, _ = h.Write(b)
}

func digestMap(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, k := range keys {
		writeField(h, []byte(k))
		writeField(h, []byte(m[k]))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }
