package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"ocrlab/internal/apperr"
	"ocrlab/internal/httpapi"
	"ocrlab/internal/pipeline"
	"ocrlab/internal/querysvc"
)

func TestLocalHTTPImportRunQueryExport(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	p, err := pipeline.Open(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Run(ctx, pipeline.RunOptions{}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(httpapi.New(p))
	defer server.Close()

	first := getBoxes(t, server.URL)
	if first.CacheHit {
		t.Fatal("first HTTP query unexpectedly hit cache")
	}
	second := getBoxes(t, server.URL)
	if !second.CacheHit || second.ResultSHA256 != first.ResultSHA256 || !bytes.Equal(second.Result, first.Result) {
		t.Fatal("HTTP cache hit differs from the initial response")
	}

	imageResponse := getJSON(t, server.URL+"/v1/queries/image?image_id=small-001")
	var imageBody struct {
		Result struct {
			Detection json.RawMessage `json:"detection"`
		} `json:"result"`
	}
	if err := json.Unmarshal(imageResponse, &imageBody); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(imageBody.Result.Detection, first.Result) {
		t.Fatal("HTTP query paths returned different detection bytes")
	}

	latencyResponse := getJSONWithStatus(t, server.URL+"/v1/queries/boxes?image_id=small-001&budget_ms=0&force_recompute=true", http.StatusGatewayTimeout)
	var latencyBody struct {
		QueryID string        `json:"query_id"`
		Error   *apperr.Error `json:"error"`
	}
	if err := json.Unmarshal(latencyResponse, &latencyBody); err != nil {
		t.Fatal(err)
	}
	if latencyBody.QueryID == "" || latencyBody.Error == nil || latencyBody.Error.Code != apperr.CodeLatencyBudget || latencyBody.Error.ElapsedMS < 0 {
		t.Fatalf("unexpected latency error body: %s", latencyResponse)
	}

	postJSON(t, server.URL+"/v1/import", `{}`, http.StatusOK)
	postJSON(t, server.URL+"/v1/run", `{"groups":["small"]}`, http.StatusOK)

	reportPath := filepath.Join(t.TempDir(), "report.json")
	exportResponse := postJSON(t, server.URL+"/v1/export", `{"path":`+quoteJSON(reportPath)+`}`, http.StatusOK)
	if !strings.Contains(string(exportResponse), "report_sha256") {
		t.Fatalf("export response missing digest: %s", exportResponse)
	}
	reportBytes, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	httpReport := getBytes(t, server.URL+"/v1/export")
	if !bytes.Equal(reportBytes, httpReport) {
		t.Fatal("HTTP export bytes differ from the written diagnostic report")
	}
}

func getBoxes(t *testing.T, baseURL string) querysvc.Response {
	t.Helper()
	body := getJSON(t, baseURL+"/v1/queries/boxes?image_id=small-001")
	var response querysvc.Response
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	return response
}

func getJSON(t *testing.T, url string) []byte {
	t.Helper()
	return getJSONWithStatus(t, url, http.StatusOK)
}

func getJSONWithStatus(t *testing.T, url string, status int) []byte {
	t.Helper()
	response, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != status {
		t.Fatalf("unexpected status for %s: %d body=%s", url, response.StatusCode, body)
	}
	return body
}

func getBytes(t *testing.T, url string) []byte {
	t.Helper()
	return getJSONWithStatus(t, url, http.StatusOK)
}

func postJSON(t *testing.T, url, body string, status int) []byte {
	t.Helper()
	response, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != status {
		t.Fatalf("unexpected status for %s: %d body=%s", url, response.StatusCode, responseBody)
	}
	return responseBody
}

func quoteJSON(value string) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}
