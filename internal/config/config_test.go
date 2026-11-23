package config

import "testing"

func TestEnsureLoopback(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:8787", "localhost:8787", "[::1]:8787"} {
		if err := EnsureLoopback(addr); err != nil {
			t.Fatalf("loopback %s rejected: %v", addr, err)
		}
	}
	for _, addr := range []string{"0.0.0.0:8787", "192.168.1.5:8787", ":8787", "127.0.0.1"} {
		if err := EnsureLoopback(addr); err == nil {
			t.Fatalf("non-loopback or malformed address %s accepted", addr)
		}
	}
}

func TestSampleConfigLoads(t *testing.T) {
	cfg, err := Load("../../samples/config.json")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Version() == "" || cfg.SigningKeyID != "local-demo-v1" || !cfg.AllowReferenceOverride {
		t.Fatalf("unexpected sample config: %#v", cfg.View())
	}
	if cfg.View().Version == cfg.SigningKey {
		t.Fatal("config version must not expose the signing key")
	}
}
