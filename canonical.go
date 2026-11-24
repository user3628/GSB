package rewardpack

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
)

func hashBytes(parts ...[]byte) string {
	h := sha256.New()
	for _, part := range parts {
		var size [8]byte
		for i := 0; i < 8; i++ {
			size[i] = byte(uint64(len(part)) >> (8 * i))
		}
		_, _ = h.Write(size[:])
		_, _ = h.Write(part)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func digestJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		// NaN/Inf cannot be represented by JSON. This path is only used while
		// diagnosing malformed configs; the normal feature stage returns the
		// required E_DIM_MISMATCH before any artifact is committed.
		b = []byte(fmt.Sprintf("%#v", v))
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func digestBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func roundSignificant(v float64, digits int) (float64, string) {
	if v == 0 {
		return 0, "0"
	}
	text := strconv.FormatFloat(v, 'g', digits, 64)
	parsed, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return v, strconv.FormatFloat(v, 'g', -1, 64)
	}
	return parsed, text
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }
