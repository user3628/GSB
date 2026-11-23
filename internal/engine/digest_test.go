package engine_test

import (
	"testing"

	"depverify/internal/closure"
	"depverify/internal/engine"
	"depverify/internal/manifest"
	"depverify/internal/model"
	"depverify/internal/samples"
)

func TestDigestMismatchDetected(t *testing.T) {
	b := samples.All()[0]
	components, mErr := manifest.Parse(bytesReader(b.Manifest))
	if mErr != nil {
		t.Fatal(mErr)
	}
	// 人为替换一个组件的登记摘要，模拟整体替换式篡改。
	tampered := make([]model.Component, len(components))
	copy(tampered, components)
	tampered[0].RegisteredDigest = []byte{0xde, 0xad, 0xbe, 0xef}
	compMap := make(map[string]model.Component, len(tampered))
	for _, c := range tampered {
		compMap[c.ID] = c
	}
	g := closure.NewGraph(nil, compMap)
	result := engine.New(g, 32).Verify([]string{tampered[0].ID}, 0, nil)
	var mismatched []string
	for _, d := range result.DigestSources {
		if !d.Match {
			mismatched = append(mismatched, d.Component)
		}
	}
	if len(mismatched) != 1 || mismatched[0] != tampered[0].ID {
		t.Fatalf("want exactly one mismatched digest, got %v", mismatched)
	}
}
