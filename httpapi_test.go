package speakerdiar

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPAPIEndpointsAndInjection(t *testing.T) {
	dir := t.TempDir()
	handler := NewHTTPHandler(dir, "http-test")

	runReq := httptest.NewRequest(http.MethodPost, "/v1/run", strings.NewReader(`{"verify_incremental":true}`))
	runRec := httptest.NewRecorder()
	handler.ServeHTTP(runRec, runReq)
	if runRec.Code != http.StatusOK {
		t.Fatalf("run status=%d body=%s", runRec.Code, runRec.Body.String())
	}
	var runBody map[string]any
	if err := json.Unmarshal(runRec.Body.Bytes(), &runBody); err != nil {
		t.Fatal(err)
	}
	if runBody["graceful_stop"] != false {
		t.Fatalf("unexpected run body: %s", runRec.Body.String())
	}

	segReq := httptest.NewRequest(http.MethodGet, "/v1/artifacts/segments", nil)
	segRec := httptest.NewRecorder()
	handler.ServeHTTP(segRec, segReq)
	if segRec.Code != http.StatusOK || segRec.Body.Len() == 0 {
		t.Fatalf("segments endpoint code=%d len=%d", segRec.Code, segRec.Body.Len())
	}

	injReq := httptest.NewRequest(http.MethodPost, "/v1/inject/rate_mismatch", strings.NewReader(`{}`))
	injRec := httptest.NewRecorder()
	handler.ServeHTTP(injRec, injReq)
	if injRec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("injection status=%d body=%s", injRec.Code, injRec.Body.String())
	}
	var errBody map[string]any
	if err := json.Unmarshal(injRec.Body.Bytes(), &errBody); err != nil {
		t.Fatal(err)
	}
	if errBody["code"] != CodeRateMismatch {
		t.Fatalf("expected %s, body=%s", CodeRateMismatch, injRec.Body.String())
	}

	cpReq := httptest.NewRequest(http.MethodGet, "/v1/checkpoints", nil)
	cpRec := httptest.NewRecorder()
	handler.ServeHTTP(cpRec, cpReq)
	if cpRec.Code != http.StatusOK || !strings.Contains(cpRec.Body.String(), StageSummary) {
		t.Fatalf("checkpoints endpoint code=%d body=%s", cpRec.Code, cpRec.Body.String())
	}
}
