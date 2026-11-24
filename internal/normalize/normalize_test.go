package normalize

import (
	"encoding/base64"
	"encoding/hex"
	"testing"

	"supplyaudit/internal/model"
)

func TestComponentKeyPreservesNamespace(t *testing.T) {
	key, err := ComponentKey("@Scope/Name")
	if err != nil {
		t.Fatal(err)
	}
	if key != "@scope/name" {
		t.Fatalf("unexpected key %q", key)
	}
}

func TestVersionNormalizationAndPrecedence(t *testing.T) {
	values := []string{
		"1.0.0-alpha",
		"1.0.0-alpha.1",
		"1.0.0-alpha.beta",
		"1.0.0-beta",
		"1.0.0-beta.2",
		"1.0.0-beta.11",
		"1.0.0-rc.1",
		"1.0.0",
	}
	parsed := make([]model.VersionView, len(values))
	for index, value := range values {
		version, err := ParseVersion(value)
		if err != nil {
			t.Fatalf("parse %q: %v", value, err)
		}
		parsed[index] = version
	}
	for index := 1; index < len(parsed); index++ {
		if CompareVersions(parsed[index-1], parsed[index]) >= 0 {
			t.Fatalf("expected %s < %s", values[index-1], values[index])
		}
	}

	three, err := ParseVersion("2.3.4")
	if err != nil {
		t.Fatal(err)
	}
	four, err := ParseVersion("2.3.4.0+build.99")
	if err != nil {
		t.Fatal(err)
	}
	if three.Canonical != four.Canonical || CompareVersions(three, four) != 0 {
		t.Fatalf("four-part/build normalization mismatch: %#v %#v", three, four)
	}
	if four.Build != "build.99" {
		t.Fatalf("build metadata was not retained: %#v", four)
	}
	if _, err := ParseVersion("1.2"); err == nil {
		t.Fatal("two-part version should be rejected")
	}
}

func TestDigestViews(t *testing.T) {
	sha1Bytes := []byte("01234567890123456789")
	sha1Hex := hex.EncodeToString(sha1Bytes)
	sha256Bytes := []byte("01234567890123456789012345678901")
	sha256Base64 := base64.StdEncoding.EncodeToString(sha256Bytes)

	tests := []struct {
		raw       string
		algorithm string
		bytes     int
		truncated bool
	}{
		{sha1Hex, "sha1", 20, false},
		{sha256Base64, "sha256", 32, false},
		{"sha1:" + sha1Hex[:16], "sha1", 8, true},
		{"sha256:" + sha256Base64[:16], "sha256", 12, true},
		{sha1Hex[:12], "sha1", 6, true},
	}
	for _, test := range tests {
		digest, err := ParseDigest(test.raw)
		if err != nil {
			t.Fatalf("parse %q: %v", test.raw, err)
		}
		if digest.Algorithm != test.algorithm || digest.ByteLength != test.bytes || digest.Truncated != test.truncated {
			t.Fatalf("unexpected digest view for %q: %#v", test.raw, digest)
		}
		if digest.Raw != test.raw {
			t.Fatalf("raw digest was not retained")
		}
	}

	if _, err := ParseDigest("sha1:" + sha256Base64); model.CodeOf(err) != model.CodeDigestInvalid {
		t.Fatalf("algorithm-width mismatch should be E_DIGEST_INVALID, got %v", err)
	}
	if _, err := ParseDigest("md5:" + sha1Hex); model.CodeOf(err) != model.CodeDigestInvalid {
		t.Fatalf("unknown algorithm should be E_DIGEST_INVALID, got %v", err)
	}
}
