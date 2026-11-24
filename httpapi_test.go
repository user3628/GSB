package walgroup

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPCommitCheckpointReadAndErrors(t *testing.T) {
	e, _ := testEngine(t, Options{SegmentMaxBytes: 512, MaxTxnsPerSegment: 2})
	srv := httptest.NewServer(NewHTTPHandler(e))
	defer srv.Close()

	post := func(path string, body any) (*http.Response, map[string]any) {
		t.Helper()
		b, _ := json.Marshal(body)
		resp, err := http.Post(srv.URL+path, "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		return resp, out
	}
	resp, out := post("/v1/commit-groups", map[string]any{
		"transactions": []map[string]any{
			{"id": "h1", "records": []map[string]any{{"page_no": 1, "value": "one"}}},
			{"id": "h2", "records": []map[string]any{{"page_no": 2, "value": "two"}}},
		},
	})
	if resp.StatusCode != http.StatusOK || out["fsync_count"].(float64) != 1 {
		t.Fatalf("commit status=%d body=%+v", resp.StatusCode, out)
	}
	resp, out = post("/v1/checkpoints", map[string]any{"sequence": 1, "replay_start_lsn": 2})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("checkpoint status=%d body=%+v", resp.StatusCode, out)
	}
	resp, err := http.Get(srv.URL + "/v1/pages/1?lsn=2")
	if err != nil {
		t.Fatal(err)
	}
	var page PageResult
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || page.Value != "one" || page.Version != 1 {
		t.Fatalf("page status=%d page=%+v", resp.StatusCode, page)
	}
	resp, out = post("/v1/commit-groups", map[string]any{
		"transactions": []map[string]any{
			{"id": "h1", "records": []map[string]any{{"page_no": 1, "value": "different"}}},
		},
	})
	if resp.StatusCode != http.StatusBadRequest || out["error"].(map[string]any)["code"] != CodeConflictingTxn {
		t.Fatalf("conflict status=%d body=%+v", resp.StatusCode, out)
	}
	resp, out = post("/v1/reclaim", map[string]any{"segment_nos": []uint64{1}})
	if resp.StatusCode != http.StatusConflict || out["error"].(map[string]any)["code"] != CodeReclaimUnsafe {
		t.Fatalf("reclaim status=%d body=%+v", resp.StatusCode, out)
	}
	resp, out = post("/v1/commit-groups", map[string]any{
		"transactions": []map[string]any{
			{"id": "h3", "records": []map[string]any{{"page_no": 1, "value": "newer"}}},
		},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("commit h3 status=%d body=%+v", resp.StatusCode, out)
	}
	resp, err = http.Get(srv.URL + "/v1/pages/1?lsn=2")
	if err != nil {
		t.Fatal(err)
	}
	out = map[string]any{}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict || out["error"].(map[string]any)["code"] != CodeCheckpointLag {
		t.Fatalf("lag status=%d body=%+v", resp.StatusCode, out)
	}
	resp, err = http.Get(srv.URL + "/v1/recovery")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("recovery status=%d", resp.StatusCode)
	}
	resp.Body.Close()
}
