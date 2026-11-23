package samplegen

import (
	"encoding/binary"
	"strconv"
	"strings"

	"forensicworkbench/internal/bincode"
)

func jsonlBytes() []byte {
	// One healthy event per family, sharing session S-A so the timeline merges
	// formats and a single event can be compared across binary/jsonl/text.
	lines := []string{
		`{"event_id":"j1","event_type":"PROCESS_CREATE","time":"2024-01-02T03:04:05.5Z","user":"alice","target":"C:\\win\\explorer.exe","session_id":"S-A","pid":100,"ppid":0}`,
		`{"event_id":"j2","event_type":"NETWORK_CONNECT","time":"2024-01-02T03:04:08Z","user":"alice","target":"192.168.1.20:8080","session_id":"S-A","pid":101,"ppid":100}`,
		`{"event_id":"j3","event_type":"REGISTRY_WRITE","time":"2024-01-02T03:04:11Z","user":"alice","target":"HKLM\\Run\\Updater","session_id":"S-A","pid":101,"ppid":100}`,
	}
	return []byte(strings.Join(lines, "\n") + "\n")
}

// textBlock builds one length-prefixed record from ordered key/value pairs.
func textBlock(kv map[string]string, order []string) []byte {
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
	return out
}

func textBytes() []byte {
	var out []byte
	out = append(out, []byte("FWTX")...)
	out = append(out, 1) // text format version
	blocks := []map[string]string{
		{"type": "PROCESS_CREATE", "time": strconv.FormatInt(baseEpochMS+500, 10), "user": "alice",
			"target": `C:\win\explorer.exe`, "session": "S-A", "pid": "100", "ppid": "0"},
		{"type": "FILE_ACCESS", "time": strconv.FormatInt(baseEpochMS+2500, 10), "user": "alice",
			"target": `C:\data\notes.txt`, "session": "S-A", "pid": "101", "ppid": "100"},
	}
	order := []string{"type", "time", "user", "target", "session", "pid", "ppid"}
	for _, kv := range blocks {
		out = append(out, textBlock(kv, order)...)
	}
	return out
}

// ---- Five malformed samples -------------------------------------------------

// 1. Truncated binary: a healthy record followed by a length prefix that
// promises more bytes than the body contains, then a valid tail cannot exist.
func truncatedBinary() []byte {
	good, _ := bincode.Marshal(bincode.V1, baseEpochMS, []bincode.Record{
		{TypeCode: bincode.TypeProcessExit, TimeEnc: bincode.EncEpochMS,
			RawTime: strconv.FormatInt(baseEpochMS, 10), SessionID: "S-X", PID: 1, Subject: "x", Object: "o"},
	})
	// Strip the valid tail and append an unfulfillable length prefix.
	body := good[:len(good)-bincode.TailLen]
	var pref [4]byte
	binary.BigEndian.PutUint32(pref[:], 500)
	return append(body, pref[:]...)
}

// 2. JSON line with a wrong field primitive type (pid as string).
func malformedJSONL() []byte {
	return []byte(
		`{"event_id":"ok1","event_type":"FILE_ACCESS","time":"2024-01-02T03:04:05Z","user":"u","target":"t","session_id":"S-J","pid":7}` + "\n" +
			`{"event_id":"bad","event_type":"FILE_ACCESS","time":"2024-01-02T03:04:06Z","user":"u","target":"t","session_id":"S-J","pid":"not-a-number"}` + "\n" +
			`{"event_id":"ok2","event_type":"PROCESS_EXIT","time":"2024-01-02T03:04:07Z","user":"u","target":"t","session_id":"S-J","pid":7}` + "\n")
}

// 3. Text block whose length prefix exceeds remaining bytes.
func malformedText() []byte {
	out := append([]byte("FWTX"), 1)
	good := textBlock(map[string]string{
		"type": "PROCESS_EXIT", "time": strconv.FormatInt(baseEpochMS, 10),
		"user": "u", "target": "t", "session": "S-T", "pid": "5", "ppid": "0",
	}, []string{"type", "time", "user", "target", "session", "pid", "ppid"})
	out = append(out, good...)
	var pref [4]byte
	binary.BigEndian.PutUint32(pref[:], 9999)
	out = append(out, pref[:]...)
	out = append(out, 1, 2, 3) // only 3 payload bytes follow
	return out
}

// 4. Binary event carrying a FILETIME that overflows int64 nanoseconds.
func malformedFiletime() []byte {
	// Max uint64-ish FILETIME; far beyond the representable Unix nano range.
	rec := bincode.Record{
		TypeCode: bincode.TypeFileAccess, TimeEnc: bincode.EncFiletime,
		RawTime: "18446744073709551615", SessionID: "S-F", PID: 9,
		Subject: "u", Object: "overflow.txt",
	}
	data, _ := bincode.Marshal(bincode.V2, baseEpochMS, []bincode.Record{rec})
	return data
}

// 5. Binary header advertising an unsupported version number (7).
func malformedVersion() []byte {
	good, _ := bincode.Marshal(bincode.V1, baseEpochMS, nil)
	bad := make([]byte, len(good))
	copy(bad, good)
	bad[4] = 7 // version byte
	return bad
}
