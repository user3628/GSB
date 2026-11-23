package normalizer

import (
	"encoding/binary"
	"strconv"
	"strings"
)

// textBlockForTest mirrors the sample generator framing for parser unit tests.
func textBlockForTest(kv map[string]string) string {
	order := []string{"type", "time", "user", "target", "session", "pid", "ppid"}
	var sb strings.Builder
	for i, k := range order {
		if i > 0 {
			sb.WriteByte(';')
		}
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(strconv.Quote(kv[k]))
	}
	payload := sb.String()
	out := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(out, uint32(len(payload)))
	copy(out[4:], payload)
	return string(out)
}
