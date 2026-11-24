package ledger_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	. "ledgerworkbench"
	"ledgerworkbench/probetarget"
	"ledgerworkbench/sample"
)

func postJSON(t *testing.T, url string, body any) (int, map[string]any) {
	t.Helper()
	buf, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.Post(url, "application/json", bytes.NewReader(buf))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	payload, _ := io.ReadAll(response.Body)
	decoded := map[string]any{}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode response %s: %v; body=%s", url, err, string(payload))
	}
	return response.StatusCode, decoded
}

func getJSON(t *testing.T, url string) (int, map[string]any) {
	t.Helper()
	response, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	payload, _ := io.ReadAll(response.Body)
	decoded := map[string]any{}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode response: %v body=%s", err, string(payload))
	}
	return response.StatusCode, decoded
}

func TestHTTPCoreFlowAndUnifiedErrors(t *testing.T) {
	wb := newTestWorkbench(t)
	server := httptest.NewServer(NewHTTPServer(wb).Handler())
	defer server.Close()
	ctxPath := server.URL
	base := sample.FixedBaseTimeMS()

	status, body := postJSON(t, ctxPath+"/v1/snapshots", sample.FixedSnapshot(base))
	if status != http.StatusOK || body["ok"] != true {
		t.Fatalf("snapshot status=%d body=%v", status, body)
	}
	migrations := sample.FixedMigrations(base)
	status, body = postJSON(t, ctxPath+"/v1/migrations", migrations.Valid)
	if status != http.StatusOK || body["ok"] != true {
		t.Fatalf("migration status=%d body=%v", status, body)
	}
	status, body = postJSON(t, ctxPath+"/v1/entries", sample.FixedEntryBatch(base, nil))
	if status != http.StatusOK || body["ok"] != true {
		t.Fatalf("entries status=%d body=%v", status, body)
	}

	status, body = getJSON(t, ctxPath+"/v1/conservation/route-balanced?observe_version=3&strict=1")
	if status != http.StatusOK {
		t.Fatalf("strict conservation status=%d body=%v", status, body)
	}
	data := body["data"].(map[string]any)
	if data["balanced"] != true {
		t.Fatalf("view not balanced: %v", data)
	}

	status, body = getJSON(t, ctxPath+"/v1/conservation/route-balanced?observe_version=999")
	if status != http.StatusConflict {
		t.Fatalf("stale read status=%d", status)
	}
	errorBody := body["error"].(map[string]any)
	if errorBody["code"] != CodeStaleRead {
		t.Fatalf("error code=%v body=%v", errorBody["code"], body)
	}
	context := errorBody["context"].(map[string]any)
	if context["view_version"].(float64) != 3 || context["observed_version"].(float64) != 999 {
		t.Fatalf("stale context wrong: %v", context)
	}

	invalid := sample.FixedInjectionParameters(base).LedgerInvalidSum
	status, body = postJSON(t, ctxPath+"/v1/entries", invalid)
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("invalid status=%d body=%v", status, body)
	}
	errorBody = body["error"].(map[string]any)
	if errorBody["code"] != CodeLedgerInvalid || errorBody["context"] == nil {
		t.Fatalf("invalid error body=%v", body)
	}
}

func TestHTTPProbeErrorsUseLoopbackTarget(t *testing.T) {
	wb := newTestWorkbench(t)
	server := httptest.NewServer(NewHTTPServer(wb).Handler())
	defer server.Close()
	target := probetarget.NewTestServer(t)

	status, body := postJSON(t, server.URL+"/v1/probes/single", ProbeRequest{
		BatchID: "http-ok", RouteKey: "route-probe", BaseURL: target.URL,
		ExpectedOwner: "owner", TimeoutMS: 1000,
	})
	if status != http.StatusOK || body["ok"] != true {
		t.Fatalf("probe ok status=%d body=%v", status, body)
	}

	status, body = postJSON(t, server.URL+"/v1/probes/single", ProbeRequest{
		BatchID: "http-timeout", RouteKey: "route-probe", BaseURL: target.URL,
		ExpectedOwner: "owner", TimeoutMS: 40,
		Injection: &ProbeInjection{Mode: ProbeModeTimeout},
	})
	if status != http.StatusGatewayTimeout {
		t.Fatalf("timeout http status=%d body=%v", status, body)
	}
	errorBody := body["error"].(map[string]any)
	if errorBody["code"] != CodeProbeTimeout {
		t.Fatalf("timeout code body=%v", body)
	}
	context := errorBody["context"].(map[string]any)
	if context["batch_id"] != "http-timeout" || context["endpoint"] == nil {
		t.Fatalf("timeout context missing batch/endpoint: %v", context)
	}
}
