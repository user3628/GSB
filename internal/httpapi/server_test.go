package httpapi_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"mvccengine/internal/engine"
	"mvccengine/internal/httpapi"
	"mvccengine/pkg/mvcc"
)

func TestHTTPTransactionWriteCommitRead(t *testing.T) {
	eng, err := engine.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	ts := httptest.NewServer(httpapi.NewServer(eng).Handler())
	defer ts.Close()

	begin := postMap(t, ts.URL+"/txns/begin", map[string]any{})
	txnSeq := int64(begin["txn_seq"].(float64))
	postMap(t, ts.URL+"/versions", map[string]any{
		"request_id": "write-http-1",
		"txn_seq":    txnSeq,
		"row_key":    "http:row",
		"content":    "hello-mvcc",
	})
	commit := postMap(t, ts.URL+"/txns/commit", map[string]any{"request_id": "commit-http-1", "txn_seq": txnSeq})
	if commit["status"] != "COMMITTED" {
		t.Fatalf("commit = %v", commit)
	}
	reader := postMap(t, ts.URL+"/txns/begin", map[string]any{})
	readerSeq := int64(reader["txn_seq"].(float64))

	read := postMap(t, ts.URL+"/rows/read", map[string]any{"txn_seq": readerSeq, "row_key": "http:row"})
	if read["visible"] != true {
		t.Fatalf("read = %v", read)
	}
	if int64(read["hit_version_seq"].(float64)) != 1 {
		t.Fatalf("hit = %v", read["hit_version_seq"])
	}

	bad := postExpectError(t, ts.URL+"/txns/commit", map[string]any{"txn_seq": 999}, "E_TXN_INVALID")
	if bad["context"] == nil {
		t.Fatal("error context missing")
	}
}

func TestHTTPRequiredErrorCodes(t *testing.T) {
	eng, err := engine.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	ts := httptest.NewServer(httpapi.NewServer(eng).Handler())
	defer ts.Close()

	postExpectError(t, ts.URL+"/versions", map[string]any{"txn_seq": 1, "row_key": ""}, "E_VERSION_INVALID")

	snapshotPath := filepath.Join(t.TempDir(), "bad.json")
	if err := writeBadSnapshot(snapshotPath); err != nil {
		t.Fatal(err)
	}
	postExpectError(t, ts.URL+"/snapshots/switch", map[string]any{"path": snapshotPath}, "E_SNAPSHOT_INVALID")

	begin := postMap(t, ts.URL+"/txns/begin", map[string]any{})
	txnSeq := int64(begin["txn_seq"].(float64))
	postMap(t, ts.URL+"/versions", map[string]any{"txn_seq": txnSeq, "row_key": "c:1", "content": "one"})
	postMap(t, ts.URL+"/txns/commit", map[string]any{"request_id": "c1", "txn_seq": txnSeq})
	long := postMap(t, ts.URL+"/txns/begin", map[string]any{})
	longSeq := int64(long["txn_seq"].(float64))
	if longSeq != txnSeq+1 {
		t.Fatalf("long seq = %d", longSeq)
	}

	other := postMap(t, ts.URL+"/txns/begin", map[string]any{})
	otherSeq := int64(other["txn_seq"].(float64))
	postMap(t, ts.URL+"/versions", map[string]any{"txn_seq": otherSeq, "row_key": "c:1", "content": "two"})
	postMap(t, ts.URL+"/txns/commit", map[string]any{"request_id": "c2", "txn_seq": otherSeq})
	postExpectError(t, ts.URL+"/reclaim", map[string]any{"row_key": "c:1", "version_seq": 1}, "E_RECLAIM_UNSAFE")

	dupTxn := postMap(t, ts.URL+"/txns/begin", map[string]any{})
	dupSeq := int64(dupTxn["txn_seq"].(float64))
	postMap(t, ts.URL+"/txns/commit", map[string]any{"request_id": "dup-commit", "txn_seq": dupSeq})
	postExpectError(t, ts.URL+"/txns/commit", map[string]any{"request_id": "dup-commit", "txn_seq": 999}, "E_CONFLICTING_REQUEST")

	seedHTTPFork(t, eng)
	if err := eng.Reload(); err != nil {
		t.Fatal(err)
	}
	postExpectError(t, ts.URL+"/chains/validate", map[string]any{"row_key": "chain:http-fork"}, "E_VERSION_CHAIN_BROKEN")
}

func postMap(t *testing.T, url string, body map[string]any) map[string]any {
	t.Helper()
	resp, data := postRaw(t, url, body)
	if resp.StatusCode >= 400 {
		t.Fatalf("POST %s status %d body %s", url, resp.StatusCode, data)
	}
	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("decode %s: %v body=%s", url, err, data)
	}
	return result
}

func postExpectError(t *testing.T, url string, body map[string]any, code mvcc.Code) map[string]any {
	t.Helper()
	resp, data := postRaw(t, url, body)
	if resp.StatusCode < 400 {
		t.Fatalf("expected error %s, got status %d body %s", code, resp.StatusCode, data)
	}
	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("decode error: %v body=%s", err, data)
	}
	if result["code"] != string(code) {
		t.Fatalf("code = %v, want %s body=%s", result["code"], code, data)
	}
	return result
}

func postRaw(t *testing.T, url string, body map[string]any) (*http.Response, []byte) {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(resp.Body)
	return resp, buf.Bytes()
}

func writeBadSnapshot(path string) error {
	return writeFile(path, []byte(`{"snapshot_seq":99,"generated_at_ms":1,"txn_upper_bound":1,"checksum":"0000000000000000000000000000000000000000000000000000000000000000"}`))
}

func seedHTTPFork(t *testing.T, eng *engine.Engine) {
	t.Helper()
	db := eng.DB()
	now := int64(1764000100000)
	_, err := db.Exec(`INSERT OR REPLACE INTO txns(seq,status,started_at_ms,snapshot_seq,upper_bound) VALUES(70,'COMMITTED',?,1,70)`, now)
	if err != nil {
		t.Fatal(err)
	}
	digest := mvcc.DigestContent("fork")
	for _, seq := range []int64{71, 72, 73} {
		var prev any
		if seq != 71 {
			prev = int64(71)
		}
		_, err := db.Exec(`INSERT OR REPLACE INTO versions(row_key,version_seq,create_txn_seq,commit_state,digest,content,prev_version_seq,is_delete,created_at_ms,updated_at_ms)
			VALUES('chain:http-fork',?,70,'COMMITTED',?,'fork',?,0,?,?)`, seq, digest, prev, now, now)
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err = db.Exec(`INSERT OR REPLACE INTO row_heads(row_key,version_seq) VALUES('chain:http-fork',73)`)
	if err != nil {
		t.Fatal(err)
	}
}
