package smuggle

import (
	"fmt"
	"path/filepath"
	"testing"
)

func testConn(id, body string) Connection {
	raw := "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: " + fmt.Sprintf("%d", len(body)) + "\r\n\r\n" + body
	return Connection{ID: id, Proxy: []byte(raw), Backend: []byte(raw)}
}

func TestBatchIsolationAndSnapshots(t *testing.T) {
	st, err := OpenStore(filepath.Join(t.TempDir(), "snap.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := DefaultConfig()
	eng := NewEngine(cfg, st)

	good := testConn("c/good", "hello")
	bad := Connection{
		ID:      "c/bad",
		Proxy:   []byte("POST / HTTP/1.1\r\nContent-Length: 5\r\nContent-Length: 9\r\n\r\nhello"),
		Backend: []byte("POST / HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello"),
	}
	reps := eng.Run([]Connection{good, bad})
	byID := map[string]ConnReport{}
	for _, r := range reps {
		byID[r.Conn] = r
	}
	if byID["c/good"].Verdict != VerdictConsistent || byID["c/good"].Code != CodeOK {
		t.Fatalf("good connection polluted: %+v", byID["c/good"])
	}
	if byID["c/bad"].Verdict != VerdictEncodingAmbiguity || byID["c/bad"].Code != CodeSmuggleAmbiguity {
		t.Fatalf("bad: %+v", byID["c/bad"])
	}
	if byID["c/good"].SnapshotVersion != 1 || byID["c/bad"].SnapshotVersion != 1 {
		t.Fatalf("snapshot versions: %+v", byID)
	}
}

func TestBatchRequestBudget(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxRequestsPerConn = 2
	eng := NewEngine(cfg, nil)
	var raw string
	for i := 0; i < 5; i++ {
		raw += "GET / HTTP/1.1\r\nContent-Length: 0\r\n\r\n"
	}
	reps := eng.Run([]Connection{{ID: "c/budget", Proxy: []byte(raw), Backend: []byte(raw)}})
	r := reps[0]
	if r.Code != CodeRequestBudget {
		t.Fatalf("got code=%s", r.Code)
	}
	if r.ProxyCount != 2 || len(r.ProxyRequests) != 2 {
		t.Fatalf("partial results not retained: %+v", r)
	}
}

func TestBatchContention(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxConcurrency = 2
	cfg.MaxQueue = 1
	eng := NewEngine(cfg, nil)
	var conns []Connection
	for i := 0; i < 5; i++ {
		conns = append(conns, testConn(fmt.Sprintf("c/%d", i), "x"))
	}
	reps := eng.Run(conns)
	for i, r := range reps {
		if i < 3 {
			if r.Code != CodeOK {
				t.Fatalf("conn %d should be adjudicated: %+v", i, r)
			}
		} else {
			if r.Code != CodeBatchContention || r.Conn != fmt.Sprintf("c/%d", i) {
				t.Fatalf("conn %d should hit contention: %+v", i, r)
			}
		}
	}
}
