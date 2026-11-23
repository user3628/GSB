package sample

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"openplatform/authquota"
)

type PKCE struct {
	Verifier  string `json:"verifier"`
	Challenge string `json:"challenge"`
}

type ScriptStep struct {
	RequestCount   int64  `json:"request_count"`
	IdempotencyKey string `json:"idempotency_key"`
	RequestID      string `json:"request_id"`
}

type File struct {
	Now         string                 `json:"now"`
	TokenSecret string                 `json:"token_secret"`
	Tenants     []authquota.TenantSeed `json:"tenants"`
	PKCE        PKCE                   `json:"pkce"`
	Nonces      []string               `json:"nonces"`
	Billing     []ScriptStep           `json:"billing_script"`
}

func Load(path string) (File, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return File{}, err
	}
	var fixture File
	if err := json.Unmarshal(body, &fixture); err != nil {
		return File{}, err
	}
	if len(fixture.Tenants) == 0 {
		return File{}, fmt.Errorf("sample has no tenants")
	}
	if fixture.PKCE.Verifier == "" || fixture.PKCE.Challenge == "" {
		return File{}, fmt.Errorf("sample has no PKCE values")
	}
	if len(fixture.Nonces) == 0 {
		return File{}, fmt.Errorf("sample has no nonce")
	}
	if _, err := fixture.Time(); err != nil {
		return File{}, err
	}
	return fixture, nil
}

func (f File) Time() (time.Time, error) {
	return time.Parse(time.RFC3339Nano, f.Now)
}

func (f File) Primary() authquota.TenantSeed {
	return f.Tenants[0]
}
