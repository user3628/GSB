package api

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"mpckpt/internal/engine"
	"mpckpt/internal/format"
	"mpckpt/internal/model"
	"mpckpt/internal/sample"
	"mpckpt/internal/store"
)

func newAPITestServer(t *testing.T) (*httptest.Server, *engine.Engine) {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	eng, err := engine.New(st, format.DefaultCompatibilityMatrix(), 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	server := httptest.NewServer(New(eng))
	t.Cleanup(server.Close)
	return server, eng
}

func TestHTTPFailureEnvelope(t *testing.T) {
	server, eng := newAPITestServer(t)
	strategy := sample.StrategyV1()
	if _, berr := eng.SubmitStrategy(engine.StrategyRequest{CommitID: "strategy-1", Strategy: strategy}); berr != nil {
		t.Fatal(berr)
	}
	shards := sample.Build().Shards
	shards[0].SizeBytes = 0
	body, _ := json.Marshal(engine.CheckpointRequest{CommitID: "bad-spec", Shards: shards})
	response, err := http.Post(server.URL+"/v1/checkpoints", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d", response.StatusCode)
	}
	var envelope map[string]any
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	if envelope["code"] != model.CodeSpecInvalid || envelope["context"] == nil {
		t.Fatalf("unexpected error envelope: %v", envelope)
	}
	notFound, err := http.Get(server.URL + "/v1/state?version=999")
	if err != nil {
		t.Fatal(err)
	}
	defer notFound.Body.Close()
	_ = json.NewDecoder(notFound.Body).Decode(&envelope)
	if notFound.StatusCode != http.StatusNotFound || envelope["code"] != model.CodeNotFound {
		t.Fatalf("unexpected missing-version response: status=%d envelope=%v", notFound.StatusCode, envelope)
	}
}

func TestHTTPCorruptImportEnvelope(t *testing.T) {
	server, eng := newAPITestServer(t)
	if _, berr := eng.SubmitStrategy(engine.StrategyRequest{CommitID: "strategy-1", Strategy: sample.StrategyV1()}); berr != nil {
		t.Fatal(berr)
	}
	manifest := sample.Manifest(2)
	manifest.Format = format.FormatBinary
	raw, berr := format.Encode(manifest)
	if berr != nil {
		t.Fatal(berr)
	}
	raw[len(raw)-1] ^= 0xff
	body, _ := json.Marshal(map[string]any{
		"commit_id": "corrupt-http", "format": "binary", "data_base64": encodeBase64(raw),
	})
	response, err := http.Post(server.URL+"/v1/import", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var envelope map[string]any
	_ = json.NewDecoder(response.Body).Decode(&envelope)
	if envelope["code"] != model.CodeRecordCorrupt || envelope["context"] == nil {
		t.Fatalf("unexpected corrupt import envelope: %v", envelope)
	}
}

func TestHTTPFlatStrategyShape(t *testing.T) {
	server, _ := newAPITestServer(t)
	strategy := sample.StrategyV1()
	body := map[string]any{
		"commit_id": "flat-strategy", "version": strategy.Version, "generated_at": strategy.GeneratedAt,
		"gpu_cap_bytes": strategy.GPUCapBytes, "write_budget_bytes": strategy.WriteBudgetBytes,
		"time_budget_ms": strategy.TimeBudgetMillis, "loss_scale_initial_fixed": strategy.LossScaleInitialFixed,
		"loss_scale_growth": strategy.LossScaleGrowth, "max_abs_error_fixed": strategy.MaxAbsErrorFixed,
		"max_rel_error_ppm": strategy.MaxRelErrorPPM,
	}
	raw, _ := json.Marshal(body)
	response, err := http.Post(server.URL+"/v1/strategy", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("flat strategy status=%d body=%s", response.StatusCode, body)
	}
}

func encodeBase64(raw []byte) string {
	return base64.StdEncoding.EncodeToString(raw)
}
