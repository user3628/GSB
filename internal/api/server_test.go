package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"localsearch/multirecall/internal/engine"
	"localsearch/multirecall/internal/model"
	"localsearch/multirecall/internal/sample"
)

type envelope struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result"`
	Error  *model.AppError `json:"error"`
}

func requestJSON(t *testing.T, client *http.Client, method, url string, body any) (int, envelope) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(data)
	}
	request, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result envelope
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	return response.StatusCode, result
}

func TestHTTPGenerateRunSummaryAndCheckpoints(t *testing.T) {
	server := httptest.NewServer(NewServer(&engine.Engine{}).Handler())
	defer server.Close()
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	dbPath := filepath.Join(root, "experiment.db")
	status, generated := requestJSON(t, server.Client(), http.MethodPost, server.URL+"/v1/generate", generateRequest{
		OutDir: dataDir, Seed: 61, Items: 40, Queries: 5, VectorDim: 10, Strategy: "weighted",
	})
	if status != http.StatusCreated || !generated.OK {
		t.Fatalf("generate failed: status=%d envelope=%+v", status, generated)
	}
	status, started := requestJSON(t, server.Client(), http.MethodPost, server.URL+"/v1/runs", runRequest{
		DataDir: dataDir, DBPath: dbPath, Mode: "inline", VerifyIncremental: boolPointer(false),
	})
	if status != http.StatusAccepted || !started.OK {
		t.Fatalf("start run failed: status=%d envelope=%+v", status, started)
	}
	var accepted struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(started.Result, &accepted); err != nil {
		t.Fatal(err)
	}
	jobStatus := waitForJob(t, server, accepted.JobID)
	if jobStatus != "completed" {
		t.Fatalf("unexpected job status: %s", jobStatus)
	}
	status, resultEnvelope := requestJSON(t, server.Client(), http.MethodGet, server.URL+"/v1/jobs/"+accepted.JobID+"/result", nil)
	if status != http.StatusOK || !resultEnvelope.OK {
		t.Fatalf("get job result failed: status=%d envelope=%+v", status, resultEnvelope)
	}
	var output model.RunOutput
	if err := json.Unmarshal(resultEnvelope.Result, &output); err != nil {
		t.Fatal(err)
	}
	if output.RunID == "" || output.Summary.ProcessedCount != 5 || output.Summary.SummaryFingerprint == "" {
		t.Fatalf("unexpected run output: %+v", output.Summary)
	}
	status, summaryEnvelope := requestJSON(t, server.Client(), http.MethodGet, server.URL+"/v1/runs/"+output.RunID+"/summary", nil)
	if status != http.StatusOK || !summaryEnvelope.OK {
		t.Fatalf("summary endpoint failed: status=%d envelope=%+v", status, summaryEnvelope)
	}
	status, checkpoints := requestJSON(t, server.Client(), http.MethodGet, server.URL+"/v1/runs/"+output.RunID+"/checkpoints?query_id=q-000001", nil)
	if status != http.StatusOK || !checkpoints.OK {
		t.Fatalf("checkpoints endpoint failed: status=%d envelope=%+v", status, checkpoints)
	}
	var checkpointResult struct {
		Checkpoints []struct {
			Stage    string `json:"stage"`
			Revision int    `json:"revision"`
			Valid    bool   `json:"valid"`
		} `json:"checkpoints"`
	}
	if err := json.Unmarshal(checkpoints.Result, &checkpointResult); err != nil {
		t.Fatal(err)
	}
	if len(checkpointResult.Checkpoints) != 3 {
		t.Fatalf("expected 3 checkpoints, got %+v", checkpointResult.Checkpoints)
	}
}

func TestHTTPInvalidWeightIsStableBusinessError(t *testing.T) {
	server := httptest.NewServer(NewServer(&engine.Engine{}).Handler())
	defer server.Close()
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	if _, err := sample.Generate(sample.GenerateOptions{OutDir: dataDir, Seed: 1, ItemCount: 20, QueryCount: 2, VectorDim: 8}); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dataDir, sample.ConfigFile)
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	var config model.Config
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	config.Fusion.Weights.Vector = -1
	data, _ = json.MarshalIndent(config, "", "  ")
	if err := os.WriteFile(configPath, append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	_, started := requestJSON(t, server.Client(), http.MethodPost, server.URL+"/v1/runs", runRequest{
		DataDir: dataDir, DBPath: filepath.Join(root, "bad.db"), Mode: "inline",
	})
	var accepted struct {
		JobID string `json:"job_id"`
	}
	_ = json.Unmarshal(started.Result, &accepted)
	waitForJob(t, server, accepted.JobID)
	status, result := requestJSON(t, server.Client(), http.MethodGet, server.URL+"/v1/jobs/"+accepted.JobID, nil)
	var payload struct {
		Status string          `json:"status"`
		Error  *model.AppError `json:"error"`
	}
	if err := json.Unmarshal(result.Result, &payload); err != nil {
		t.Fatal(err)
	}
	if status != http.StatusOK || payload.Status != "failed" || payload.Error == nil || payload.Error.Code != model.CodeInvalidWeight {
		t.Fatalf("expected E_INVALID_WEIGHT job error, got status=%d envelope=%+v", status, result)
	}
}

func waitForJob(t *testing.T, server *httptest.Server, id string) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, result := requestJSON(t, server.Client(), http.MethodGet, server.URL+"/v1/jobs/"+id, nil)
		var payload struct {
			Status string `json:"status"`
		}
		_ = json.Unmarshal(result.Result, &payload)
		if payload.Status != "" && payload.Status != "running" {
			return payload.Status
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("job %s did not complete", id)
	return ""
}

func boolPointer(value bool) *bool { return &value }
