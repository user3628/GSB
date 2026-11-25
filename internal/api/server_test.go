package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"csrouter/internal/app"
	"csrouter/internal/domain"
)

func TestPublishCheckpointErrorUsesCurrentPolicyVersion(t *testing.T) {
	application, err := app.New(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = application.Close() })
	if _, err := application.GenerateAndImport(0); err != nil {
		t.Fatal(err)
	}

	server := NewServer(application, "127.0.0.1:0")
	body := bytes.NewBufferString(`{"ticket_id":"S-0001","stage":"priority","config_version":999,"artifact":{"ticket_id":"S-0001"}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/checkpoints/publish?tier=small", body)
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	server.Mux.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var biz domain.BizError
	if err := json.Unmarshal(recorder.Body.Bytes(), &biz); err != nil {
		t.Fatalf("decode error: %v body=%s", err, recorder.Body.String())
	}
	if biz.Code != domain.ErrIllegalStage {
		t.Fatalf("expected %s, got %+v", domain.ErrIllegalStage, biz)
	}
	if biz.TicketID != "S-0001" {
		t.Fatalf("missing ticket id: %+v", biz)
	}
	if biz.ConfigVersion != 1 {
		t.Fatalf("error must report current effective version 1, got %+v", biz)
	}
	if biz.VersionFingerprint == "" {
		t.Fatalf("missing current version fingerprint: %+v", biz)
	}
}
