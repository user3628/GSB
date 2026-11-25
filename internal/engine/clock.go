package engine

import (
	"crypto/rand"
	"encoding/hex"
	"sync/atomic"
)

var intentSeq atomic.Int64

// processTag distinguishes intents across process restarts so that a frozen
// deterministic clock cannot collide with ids from a crashed predecessor.
var processTag = func() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}()

// nextIntentID builds a process-unique intent id even when the injected clock
// is frozen for deterministic tests.
func (e *Engine) nextIntentID(prefix string) string {
	n := intentSeq.Add(1)
	return prefix + "-" + itoa(e.cfg.Now()) + "-" + processTag + "-" + itoa(n)
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
